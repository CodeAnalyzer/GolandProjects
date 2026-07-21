package trc

import (
	"bufio"
	"fmt"
	"os"

	"github.com/codebase/internal/store"
)

// streamBatchSize — размер батча событий для streaming-записи в БД.
// 10000 событий × ~2KB/event ≈ 20MB на батч — комфортно для RAM.
const streamBatchSize = 10000

// ParseFileToDB парсит бинарный .trc файл и стримит события напрямую в БД
// батчами по streamBatchSize, не накапливая полный []TRCEvent в памяти.
// Это позволяет парсить файлы размером > 1 ГБ без исчерпания RAM.
//
// Возвращает ID созданной сессии и общее количество событий.
// Для XML-экспорта трейса используйте ParseFile (он использует streaming XML-парсер).
func ParseFileToDB(path string, db *store.DB) (sessionID int64, totalEvents int, err error) {
	f, h, err := parseHeaderFromFile(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	fi, statErr := os.Stat(path)
	var fileSize int64
	if statErr == nil {
		fileSize = fi.Size()
	}

	// Создаём сессию с total_events=0 (placeholder, обновим в конце).
	err = db.QueryRow(
		`INSERT INTO trc_sessions (file_path, file_size, total_events, provider_name, server_name, major_version, minor_version, build_number)
		 VALUES ($1, $2, 0, $3, $4, $5, $6, $7) RETURNING id`,
		path, fileSize,
		h.ProviderName, h.ServerName, h.MajorVersion, h.MinorVersion, h.BuildNumber,
	).Scan(&sessionID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to insert trc_sessions: %w", err)
	}

	r := bufio.NewReaderSize(f, 1<<20) // 1MB buffer
	tracker := NewIncrementalParentTracker()
	batch := make([]TRCEvent, 0, streamBatchSize)

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := insertTRCEvents(db, batch, sessionID); err != nil {
			return fmt.Errorf("failed to insert trc_events batch: %w", err)
		}
		batch = batch[:0]
		return nil
	}

	err = parseEventsStreamingCB(r, func(ev *TRCEvent) error {
		tracker.Process(ev)
		batch = append(batch, *ev)
		totalEvents++
		if len(batch) >= streamBatchSize {
			return flushBatch()
		}
		return nil
	})
	if err != nil {
		return sessionID, totalEvents, fmt.Errorf("trc: parse events %s: %w", path, err)
	}

	// Вставляем остаток батча.
	if err := flushBatch(); err != nil {
		return sessionID, totalEvents, err
	}

	// Обновляем total_events в сессии.
	if _, err := db.Exec(
		`UPDATE trc_sessions SET total_events = $2 WHERE id = $1`,
		sessionID, totalEvents,
	); err != nil {
		return sessionID, totalEvents, fmt.Errorf("failed to update trc_sessions total_events: %w", err)
	}

	return sessionID, totalEvents, nil
}
