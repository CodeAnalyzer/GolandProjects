package trc

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/codebase/internal/store"
)

// streamBatchSize — размер батча событий для streaming-записи в БД.
// Использует trcBatchSize (настраивается через SetBatchSize).

// ParseFileToDB парсит файл трейса (.trc бинарный, .trc XML-экспорт, .xel
// Extended Events) и стримит события напрямую в БД батчами по
// streamBatchSize, не накапливая полный []TRCEvent в памяти.
// Использует trcBatchSize (настраивается через SetBatchSize).
// Это позволяет парсить файлы размером > 1 ГБ без исчерпания RAM.
//
// Формат файла определяется по сигнатуре содержимого (DetectFormat).
// Для бинарного .trc используется streaming через parseEventsStreamingCB.
// Для XML — token-based streaming через parseXMLReaderCB.
// Для XEL — файл читается целиком (ограничение формата), затем parseXELCB
// стримит события в БД без накопления []TRCEvent.
//
// Возвращает ID созданной сессии и общее количество событий.
func ParseFileToDB(ctx context.Context, path string, db *store.DB) (sessionID int64, totalEvents int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, fmt.Errorf("trc: open %s: %w", path, err)
	}
	defer f.Close()

	// Читаем буфер для детекции формата.
	const headerBufSize = 65536
	headerBuf := make([]byte, 0, headerBufSize)
	chunk := make([]byte, headerBufSize)
	n, err := io.ReadFull(f, chunk)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return 0, 0, fmt.Errorf("trc: read %s: %w", path, err)
	}
	headerBuf = append(headerBuf, chunk[:n]...)

	fmtDetected := DetectFormat(headerBuf)

	fi, statErr := os.Stat(path)
	var fileSize int64
	if statErr == nil {
		fileSize = fi.Size()
	}

	// Общие переменные для заголовка и source_format.
	var h *TraceHeader
	var sourceFormat string

	// Создаём сессию с placeholder-значениями (total_events=0, пустой
	// заголовок). После парсинга обновим real header + total_events.
	// Сессия должна существовать ДО парсинга, т.к. flushBatch вставляет
	// события с foreign key на session_id.
	insertErr := db.QueryRowContext(ctx, 
		`INSERT INTO trc_sessions (file_path, file_size, total_events, provider_name, server_name, major_version, minor_version, build_number, source_format)
		 VALUES ($1, $2, 0, '', '', 0, 0, 0, 'trc_binary') RETURNING id`,
		path, fileSize,
	).Scan(&sessionID)
	if insertErr != nil {
		return 0, 0, fmt.Errorf("failed to insert trc_sessions: %w", insertErr)
	}

	// Подготовка общих компонентов streaming-записи.
	tracker := NewIncrementalParentTracker()
	batch := make([]TRCEvent, 0, trcBatchSize)
	var parseErr error

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := insertTRCEvents(ctx, db, batch, sessionID); err != nil {
			return fmt.Errorf("failed to insert trc_events batch: %w", err)
		}
		batch = batch[:0]
		return nil
	}

	cb := func(ev *TRCEvent) error {
		tracker.Process(ev)
		batch = append(batch, *ev)
		totalEvents++
		if len(batch) >= trcBatchSize {
			return flushBatch()
		}
		return nil
	}

	switch fmtDetected {
	case FormatXML:
		sourceFormat = "trc_xml"
		// Перематываем в начало — parseXMLReaderCB читает с BOM.
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return 0, 0, fmt.Errorf("trc: seek %s: %w", path, err)
		}
		h, parseErr = parseXMLReaderCB(f, cb)

	case FormatXEL:
		sourceFormat = "xel"
		// XEL требует произвольного доступа — читаем весь файл в память.
		// Для очень больших .xel это известное ограничение формата
		// (dictionary-блок, маркеры, TLV — не single-pass).
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return 0, 0, fmt.Errorf("trc: seek %s: %w", path, err)
		}
		data, rerr := io.ReadAll(f)
		if rerr != nil {
			return 0, 0, fmt.Errorf("trc: read %s: %w", path, rerr)
		}
		xelHdr, herr := parseXELHeader(data)
		if herr != nil {
			return 0, 0, fmt.Errorf("trc: parse xel header %s: %w", path, herr)
		}
		h = &TraceHeader{EventClasses: map[int]*EventClassSchema{}}
		parseErr = parseXELCB(data, xelHdr, cb)

	default: // FormatBinary
		sourceFormat = "trc_binary"
		// Парсим заголовок из headerBuf (дочитываем если нужно).
		h, err = ParseHeader(headerBuf)
		if err != nil {
			if len(headerBuf) < headerBufSize && n == headerBufSize {
				return 0, 0, fmt.Errorf("trc: parse header %s: %w", path, err)
			}
			for len(headerBuf) < 1<<20 {
				nn, rerr := io.ReadFull(f, chunk)
				headerBuf = append(headerBuf, chunk[:nn]...)
				if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
					return 0, 0, fmt.Errorf("trc: read %s: %w", path, rerr)
				}
				h, err = ParseHeader(headerBuf)
				if err == nil {
					break
				}
				if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
					break
				}
			}
			if err != nil {
				return 0, 0, fmt.Errorf("trc: parse header %s: %w", path, err)
			}
		}
		// Позиционируемся на EventsOffset.
		if _, err := f.Seek(int64(h.EventsOffset), io.SeekStart); err != nil {
			return 0, 0, fmt.Errorf("trc: seek %s: %w", path, err)
		}
		r := bufio.NewReaderSize(f, 1<<20)
		parseErr = parseEventsStreamingCB(r, cb)
	}

	// Если парсинг завершился с ошибкой — возвращаем (сессия уже создана,
	// вызывающая сторона знает sessionID для очистки).
	if parseErr != nil {
		return sessionID, totalEvents, fmt.Errorf("trc: parse events %s: %w", path, parseErr)
	}

	// Flush remaining pendingCompleted buffers (Completed-only SPID fallback).
	tracker.Flush()

	// Вставляем остаток батча.
	if err := flushBatch(); err != nil {
		return sessionID, totalEvents, err
	}

	// Обновляем сессию: real header + total_events + source_format.
	if _, err := db.ExecContext(ctx, 
		`UPDATE trc_sessions SET total_events = $2, provider_name = $3, server_name = $4,
		 major_version = $5, minor_version = $6, build_number = $7, source_format = $8
		 WHERE id = $1`,
		sessionID, totalEvents,
		h.ProviderName, h.ServerName, h.MajorVersion, h.MinorVersion, h.BuildNumber,
		sourceFormat,
	); err != nil {
		return sessionID, totalEvents, fmt.Errorf("failed to update trc_sessions: %w", err)
	}

	return sessionID, totalEvents, nil
}
