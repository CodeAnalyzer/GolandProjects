package trc

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/codebase/internal/store"
	"github.com/lib/pq"
)

// SaveSession сохраняет результат разбора .trc файла в БД. Возвращает ID
// созданной сессии.
func SaveSession(ctx context.Context, db *store.DB, result *TRCParseResult, filePath string, fileSize int64) (int64, error) {
	var sessionID int64
	h := result.Header
	sourceFormat := result.SourceFormat
	if sourceFormat == "" {
		sourceFormat = "trc_binary"
	}
	err := db.QueryRowContext(ctx, 
		`INSERT INTO trc_sessions (file_path, file_size, total_events, provider_name, server_name, major_version, minor_version, build_number, source_format)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id`,
		filePath, fileSize, len(result.Events),
		h.ProviderName, h.ServerName, h.MajorVersion, h.MinorVersion, h.BuildNumber,
		sourceFormat,
	).Scan(&sessionID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert trc_sessions: %w", err)
	}

	if err := insertTRCEvents(ctx, db, result.Events, sessionID); err != nil {
		return 0, fmt.Errorf("failed to insert trc_events: %w", err)
	}

	return sessionID, nil
}

// insertTRCEvents батчево вставляет события через pq.CopyIn. Часто
// встречающиеся колонки (SPID, DatabaseID, StartTime, ...) раскладываются в
// отдельные поля trc_events для удобной фильтрации/сортировки; полный набор
// декодированных Columns сохраняется целиком как JSONB (аналог
// clientEventPayload в internal/rti/store.go) — без потери данных для
// колонок, не вынесенных в отдельные поля.
//
// parent_id вычисляется в ComputeParentIDs (tree.go) как индекс родительского
// события в срезе events. Поскольку trc_events.id генерируется БД (BIGSERIAL),
// parent_id хранит не абсолютный id, а относительный индекс + 1 (1-based),
// который можно разрешить через подзапрос при tree loading.
//
// Для больших файлов (миллионы событий) insert выполняется батчами по
// trcBatchSize событий: каждый батч — отдельная транзакция с CopyIn.
var trcBatchSize = 50000

// SetBatchSize устанавливает размер батча для insert/delete операций.
func SetBatchSize(size int) {
	if size > 0 {
		trcBatchSize = size
	}
}

func insertTRCEvents(ctx context.Context, db *store.DB, events []TRCEvent, sessionID int64) error {
	if len(events) == 0 {
		return nil
	}

	// Фаза 1: параллельная JSON-сериализация (CPU-bound, независима между событиями)
	columnsJSONs := make([][]byte, len(events))
	paramsJSONs := make([]interface{}, len(events))
	if err := serializeParallel(events, columnsJSONs, paramsJSONs); err != nil {
		return fmt.Errorf("serialize trc events: %w", err)
	}

	// Фаза 2: последовательный COPY IN (pq.CopyIn не потокобезопасен)
	// Батчами по trcBatchSize событий, каждая батч — отдельная транзакция.
	for batchStart := 0; batchStart < len(events); batchStart += trcBatchSize {
		batchEnd := batchStart + trcBatchSize
		if batchEnd > len(events) {
			batchEnd = len(events)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		stmt, err := tx.PrepareContext(ctx, pq.CopyIn("trc_events",
			"session_id", "event_class", "event_name", "text_data", "procedure",
			"spid", "database_id", "database_name", "application_name", "login_name", "host_name",
			"start_time", "end_time", "duration_ms", "cpu", "reads", "writes", "row_counts",
			"object_id", "object_name", "event_sequence", "nest_level", "line_number",
			"error", "severity", "success", "params", "columns",
			"parent_id", "depth",
		))
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		for i := batchStart; i < batchEnd; i++ {
			ev := events[i]
			var parentID interface{}
			if ev.ParentID >= 0 {
				parentID = ev.ParentID + 1 // 1-based offset within session
			}
			_, err = stmt.Exec(
				sessionID, ev.EventClass, nullableString(ev.EventName), nullableString(strVal(ev.Columns[1])), nullableString(ev.Procedure),
				nullableInt32(ev.Columns[12]), nullableInt32(ev.Columns[3]), nullableString(strVal(ev.Columns[35])),
				nullableString(strVal(ev.Columns[10])), nullableString(strVal(ev.Columns[11])), nullableString(strVal(ev.Columns[8])),
				nullableTime(ev.Columns[14]), nullableTime(ev.Columns[15]), ev.DurationMs,
				nullableInt64(ev.Columns[18]), nullableInt64(ev.Columns[16]), nullableInt64(ev.Columns[17]), nullableInt64(ev.Columns[48]),
				nullableInt64(ev.Columns[22]), nullableString(strVal(ev.Columns[34])), nullableInt64(ev.Columns[51]),
				nullableInt32(ev.Columns[29]), nullableInt32(ev.Columns[5]),
				nullableInt32(ev.Columns[31]), nullableInt32(ev.Columns[20]), nullableInt32(ev.Columns[23]),
				paramsJSONs[i], string(columnsJSONs[i]),
				parentID, ev.Depth,
			)
			if err != nil {
				_ = stmt.Close()
				_ = tx.Rollback()
				return err
			}
		}

		if _, err := stmt.Exec(); err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			return err
		}
		stmt.Close()

		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

// jsonColumn — сериализуемое представление одной декодированной колонки
// события (используется для JSONB-снапшота всех колонок в trc_events.columns
// и для восстановления через LoadEvents).
type jsonColumn struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
}

// marshalColumns сериализует map[int]any в JSON с явным тегом типа для
// каждого значения, чтобы UnmarshalColumns мог восстановить исходный Go-тип
// ([]byte/SystemTime/int32/int64/string), а не общий float64/string,
// который даёт encoding/json по умолчанию.
func marshalColumns(columns map[int]any) ([]byte, error) {
	out := make(map[string]jsonColumn, len(columns))
	for id, v := range columns {
		key := strconv.Itoa(id)
		switch val := v.(type) {
		case string:
			// PostgreSQL jsonb не принимает нулевые байты (0x00) в строках.
			out[key] = jsonColumn{"string", strings.ReplaceAll(val, "\x00", "")}
		case int32:
			out[key] = jsonColumn{"int32", val}
		case int64:
			out[key] = jsonColumn{"int64", val}
		case SystemTime:
			out[key] = jsonColumn{"systemtime", val}
		case []byte:
			out[key] = jsonColumn{"binary", val}
		default:
			out[key] = jsonColumn{"unknown", val}
		}
	}
	return json.Marshal(out)
}

// unmarshalColumns — обратная операция к marshalColumns.
func unmarshalColumns(data []byte) (map[int]any, error) {
	var raw map[string]struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	columns := make(map[int]any, len(raw))
	for key, entry := range raw {
		id, err := strconv.Atoi(key)
		if err != nil {
			continue
		}
		switch entry.Type {
		case "string":
			var s string
			if err := json.Unmarshal(entry.Value, &s); err == nil {
				columns[id] = s
			}
		case "int32":
			var n int32
			if err := json.Unmarshal(entry.Value, &n); err == nil {
				columns[id] = n
			}
		case "int64":
			var n int64
			if err := json.Unmarshal(entry.Value, &n); err == nil {
				columns[id] = n
			}
		case "systemtime":
			var st SystemTime
			if err := json.Unmarshal(entry.Value, &st); err == nil {
				columns[id] = st
			}
		case "binary":
			var b []byte
			if err := json.Unmarshal(entry.Value, &b); err == nil {
				columns[id] = b
			}
		}
	}
	return columns, nil
}

func strVal(v any) string {
	s, _ := v.(string)
	return s
}

func nullableString(s string) interface{} {
	if s == "" {
		return nil
	}
	// PostgreSQL text/varchar не принимает нулевые байты (0x00).
	// XEL-события могут содержать бинарный мусор в строковых полях.
	s = strings.ReplaceAll(s, "\x00", "")
	if s == "" {
		return nil
	}
	return s
}

func nullableInt32(v any) interface{} {
	if n, ok := v.(int32); ok {
		return n
	}
	return nil
}

func nullableInt64(v any) interface{} {
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	default:
		return nil
	}
}

func nullableTime(v any) interface{} {
	st, ok := v.(SystemTime)
	if !ok {
		return nil
	}
	t, ok := st.ToTime()
	if !ok {
		return nil
	}
	return t
}

// ListSessions возвращает список сессий из БД.
func ListSessions(ctx context.Context, db *store.DB, limit int) ([]TRCSession, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := db.QueryContext(ctx, 
		`SELECT id, file_path, file_size, parsed_at, total_events, provider_name, server_name, major_version, minor_version, build_number
		 FROM trc_sessions ORDER BY parsed_at DESC LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sessions []TRCSession
	for rows.Next() {
		var s TRCSession
		var provider, server sql.NullString
		if err := rows.Scan(&s.ID, &s.FilePath, &s.FileSize, &s.ParsedAt, &s.TotalEvents,
			&provider, &server, &s.MajorVersion, &s.MinorVersion, &s.BuildNumber); err != nil {
			return nil, err
		}
		s.ProviderName = provider.String
		s.ServerName = server.String
		sessions = append(sessions, s)
	}
	return sessions, rows.Err()
}

// GetSession возвращает информацию о сессии по ID.
func GetSession(ctx context.Context, db *store.DB, sessionID int64) (*TRCSession, error) {
	var s TRCSession
	var provider, server sql.NullString
	err := db.QueryRowContext(ctx, 
		`SELECT id, file_path, file_size, parsed_at, total_events, provider_name, server_name, major_version, minor_version, build_number
		 FROM trc_sessions WHERE id = $1`,
		sessionID,
	).Scan(&s.ID, &s.FilePath, &s.FileSize, &s.ParsedAt, &s.TotalEvents,
		&provider, &server, &s.MajorVersion, &s.MinorVersion, &s.BuildNumber)
	if err != nil {
		return nil, err
	}
	s.ProviderName = provider.String
	s.ServerName = server.String
	return &s, nil
}

// GetLatestSessionID возвращает ID последней сессии.
func GetLatestSessionID(ctx context.Context, db *store.DB) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, `SELECT id FROM trc_sessions ORDER BY parsed_at DESC LIMIT 1`).Scan(&id)
	return id, err
}

// batchDeleteSize — размер батча для построчного удаления событий.
// Использует trcBatchSize (настраивается через SetBatchSize).

// DeleteSession удаляет сессию по ID. Сначала батчами удаляются trc_events
// (чтобы избежать длительного CASCADE-удаления в одной транзакции), затем
// удаляется сама сессия.
func DeleteSession(ctx context.Context, db *store.DB, sessionID int64) error {
	// Пакетное удаление событий из trc_events
	for {
		res, err := db.ExecContext(ctx, 
			`DELETE FROM trc_events WHERE session_id = $1 AND id IN (
				SELECT id FROM trc_events WHERE session_id = $1 LIMIT $2
			)`,
			sessionID, trcBatchSize,
		)
		if err != nil {
			return fmt.Errorf("batch delete trc_events: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			break
		}
	}
	// Удаление сессии (CASCADE уже нечего удалять)
	_, err := db.ExecContext(ctx, `DELETE FROM trc_sessions WHERE id = $1`, sessionID)
	return err
}

// PruneSessions удаляет старые сессии, оставляя только последние N.
// При keepLast=0 используется TRUNCATE (мгновенная очистка независимо от
// объёма данных). При keepLast>0 — пакетное удаление событий с последующим
// удалением сессий.
func PruneSessions(ctx context.Context, db *store.DB, keepLast int) (int64, error) {
	if keepLast == 0 {
		// Подсчитать количество сессий до TRUNCATE
		var count int64
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM trc_sessions`).Scan(&count); err != nil {
			return 0, fmt.Errorf("count trc_sessions: %w", err)
		}
		// TRUNCATE мгновенно очищает таблицы без построчного удаления
		if _, err := db.ExecContext(ctx, `TRUNCATE trc_events, trc_sessions RESTART IDENTITY CASCADE`); err != nil {
			return 0, fmt.Errorf("truncate trc tables: %w", err)
		}
		// VACUUM ANALYZE после массового удаления (ошибка не критична)
		_, _ = db.ExecContext(ctx, `VACUUM ANALYZE trc_events, trc_sessions`)
		return count, nil
	}

	// Найти ID сессий на удаление
	rows, err := db.QueryContext(ctx, 
		`SELECT id FROM trc_sessions WHERE id NOT IN (
			SELECT id FROM trc_sessions ORDER BY parsed_at DESC LIMIT $1
		)`,
		keepLast,
	)
	if err != nil {
		return 0, fmt.Errorf("select trc_sessions to delete: %w", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	// Пакетное удаление событий для каждой сессии
	for _, sid := range ids {
		for {
			res, err := db.ExecContext(ctx, 
				`DELETE FROM trc_events WHERE session_id = $1 AND id IN (
					SELECT id FROM trc_events WHERE session_id = $1 LIMIT $2
				)`,
				sid, trcBatchSize,
			)
			if err != nil {
				return 0, fmt.Errorf("batch delete trc_events for session %d: %w", sid, err)
			}
			n, _ := res.RowsAffected()
			if n == 0 {
				break
			}
		}
	}

	// Удаление сессий (CASCADE уже нечего удалять)
	result, err := db.ExecContext(ctx, 
		`DELETE FROM trc_sessions WHERE id NOT IN (
			SELECT id FROM trc_sessions ORDER BY parsed_at DESC LIMIT $1
		)`,
		keepLast,
	)
	if err != nil {
		return 0, fmt.Errorf("delete trc_sessions: %w", err)
	}
	deleted, _ := result.RowsAffected()
	// VACUUM ANALYZE после массового удаления (ошибка не критична)
	_, _ = db.ExecContext(ctx, `VACUUM ANALYZE trc_events, trc_sessions`)
	return deleted, nil
}

// LoadEvents загружает события сессии из БД, восстанавливая полный набор
// декодированных Columns из JSONB-снапшота (см. marshalColumns).
func LoadEvents(ctx context.Context, db *store.DB, sessionID int64) ([]TRCEvent, error) {
	rows, err := db.QueryContext(ctx, 
		`SELECT event_class, event_name, procedure, duration_ms, params, columns,
		        parent_id, depth
		 FROM trc_events WHERE session_id = $1 ORDER BY id`,
		sessionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []TRCEvent
	for rows.Next() {
		ev, err := scanEventRow(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

// scanEventRow scans a single event row from the common column set.
func scanEventRow(rows *sql.Rows) (TRCEvent, error) {
	var ev TRCEvent
	var eventName, procedure sql.NullString
	var paramsJSON, columnsJSON sql.NullString
	var parentID sql.NullInt64
	if err := rows.Scan(&ev.EventClass, &eventName, &procedure, &ev.DurationMs,
		&paramsJSON, &columnsJSON, &parentID, &ev.Depth); err != nil {
		return ev, err
	}
	ev.EventName = eventName.String
	ev.Procedure = procedure.String
	if parentID.Valid {
		ev.ParentID = int(parentID.Int64) - 1 // convert 1-based back to 0-based
	} else {
		ev.ParentID = -1
	}

	if columnsJSON.Valid && columnsJSON.String != "" {
		cols, err := unmarshalColumns([]byte(columnsJSON.String))
		if err != nil {
			return ev, fmt.Errorf("unmarshal columns (event class %d): %w", ev.EventClass, err)
		}
		ev.Columns = cols
	} else {
		ev.Columns = map[int]any{}
	}
	if paramsJSON.Valid && paramsJSON.String != "" {
		if err := json.Unmarshal([]byte(paramsJSON.String), &ev.Params); err != nil {
			return ev, fmt.Errorf("unmarshal params (event class %d): %w", ev.EventClass, err)
		}
	}
	return ev, nil
}

// TRCEventFilter — параметры серверной фильтрации событий.
type TRCEventFilter struct {
	SPID      int    // 0 = all
	Procedure string // "" = all
	EventName string // "" = all
}

// LoadEventsFiltered загружает события сессии с серверной фильтрацией и лимитом.
func LoadEventsFiltered(ctx context.Context, db *store.DB, sessionID int64, f TRCEventFilter, limit int) ([]TRCEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	query := `SELECT event_class, event_name, procedure, duration_ms, params, columns,
	                 parent_id, depth
	          FROM trc_events WHERE session_id = $1`
	args := []interface{}{sessionID}
	argIdx := 2
	if f.SPID > 0 {
		query += fmt.Sprintf(" AND spid = $%d", argIdx)
		args = append(args, f.SPID)
		argIdx++
	}
	if f.Procedure != "" {
		query += fmt.Sprintf(" AND procedure = $%d", argIdx)
		args = append(args, f.Procedure)
		argIdx++
	}
	if f.EventName != "" {
		query += fmt.Sprintf(" AND event_name = $%d", argIdx)
		args = append(args, f.EventName)
		argIdx++
	}
	query += fmt.Sprintf(" ORDER BY id LIMIT $%d", argIdx)
	args = append(args, limit)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to load filtered events: %w", err)
	}
	defer rows.Close()

	var events []TRCEvent
	for rows.Next() {
		ev, err := scanEventRow(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

// LoadSlowEvents загружает самые медленные события сессии.
func LoadSlowEvents(ctx context.Context, db *store.DB, sessionID int64, thresholdMs int, limit int) ([]TRCEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := db.QueryContext(ctx, 
		`SELECT event_class, event_name, procedure, duration_ms, params, columns,
		        parent_id, depth
		 FROM trc_events
		 WHERE session_id = $1 AND duration_ms >= $2
		 ORDER BY duration_ms DESC LIMIT $3`,
		sessionID, thresholdMs, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load slow events: %w", err)
	}
	defer rows.Close()

	var events []TRCEvent
	for rows.Next() {
		ev, err := scanEventRow(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

// LoadErrorEvents загружает события с ненулевым Error.
func LoadErrorEvents(ctx context.Context, db *store.DB, sessionID int64, limit int) ([]TRCEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := db.QueryContext(ctx, 
		`SELECT event_class, event_name, procedure, duration_ms, params, columns,
		        parent_id, depth
		 FROM trc_events
		 WHERE session_id = $1 AND error IS NOT NULL AND error <> 0
		 ORDER BY id LIMIT $2`,
		sessionID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load error events: %w", err)
	}
	defer rows.Close()

	var events []TRCEvent
	for rows.Next() {
		ev, err := scanEventRow(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

// LoadEventsByProcedure загружает события конкретной процедуры.
func LoadEventsByProcedure(ctx context.Context, db *store.DB, sessionID int64, procName string, limit int) ([]TRCEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := db.QueryContext(ctx, 
		`SELECT event_class, event_name, procedure, duration_ms, params, columns,
		        parent_id, depth
		 FROM trc_events
		 WHERE session_id = $1 AND procedure = $2
		 ORDER BY id LIMIT $3`,
		sessionID, procName, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load events by procedure: %w", err)
	}
	defer rows.Close()

	var events []TRCEvent
	for rows.Next() {
		ev, err := scanEventRow(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

// LoadEventCount возвращает количество событий в сессии без их загрузки.
func LoadEventCount(ctx context.Context, db *store.DB, sessionID int64) (int, error) {
	var count int
	err := db.QueryRowContext(ctx, 
		`SELECT count(*) FROM trc_events WHERE session_id = $1`,
		sessionID,
	).Scan(&count)
	return count, err
}

// LoadProceduresAggregated агрегирует статистику по процедурам на стороне БД.
func LoadProceduresAggregated(ctx context.Context, db *store.DB, sessionID int64) ([]TRCProcAgg, error) {
	rows, err := db.QueryContext(ctx, 
		`SELECT procedure,
		        count(*) AS cnt,
		        COALESCE(sum(duration_ms), 0) AS total_ms,
		        COALESCE(min(duration_ms) FILTER (WHERE duration_ms > 0), 0) AS min_ms,
		        COALESCE(max(duration_ms), 0) AS max_ms,
		        COALESCE(avg(duration_ms) FILTER (WHERE duration_ms > 0), 0) AS avg_ms
		 FROM trc_events
		 WHERE session_id = $1 AND procedure IS NOT NULL AND procedure <> ''
		 GROUP BY procedure
		 ORDER BY total_ms DESC`,
		sessionID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load aggregated procedures: %w", err)
	}
	defer rows.Close()

	var aggs []TRCProcAgg
	for rows.Next() {
		var a TRCProcAgg
		if err := rows.Scan(&a.Procedure, &a.Count, &a.TotalMs, &a.MinMs, &a.MaxMs, &a.AvgMs); err != nil {
			return nil, err
		}
		aggs = append(aggs, a)
	}
	return aggs, rows.Err()
}

// LoadEventsForTree загружает события для построения дерева через recursive CTE.
// parent_id в trc_events хранит 1-based offset внутри сессии. Через CTE с
// row_number() маппим offset на реальный id и строим дерево на стороне БД.
// spid: 0 = выбрать SPID с наибольшим числом событий.
// maxDepth: 0 = без ограничения глубины.
// maxNodes: 0 = без ограничения количества узлов.
// procedure: если не пустой — anchor CTE ищет события с этой процедурой
// вместо parent_id IS NULL, и дерево строится только от них.
func LoadEventsForTree(ctx context.Context, db *store.DB, sessionID int64, spid, maxDepth, maxNodes int, procedure string) ([]TRCEvent, error) {
	// Если SPID не указан, выбираем SPID с наибольшим числом событий.
	if spid <= 0 {
		var err error
		if procedure != "" {
			err = db.QueryRowContext(ctx,
				`SELECT spid FROM trc_events
				 WHERE session_id = $1 AND procedure = $2 AND spid IS NOT NULL
				 GROUP BY spid ORDER BY count(*) DESC LIMIT 1`,
				sessionID, procedure,
			).Scan(&spid)
		} else {
			err = db.QueryRowContext(ctx,
				`SELECT spid FROM trc_events
				 WHERE session_id = $1 AND parent_id IS NULL AND spid IS NOT NULL
				 GROUP BY spid ORDER BY count(*) DESC LIMIT 1`,
				sessionID,
			).Scan(&spid)
		}
		if err != nil {
			return nil, nil // нет событий
		}
	}

	// Recursive CTE: нумеруем строки сессии row_number, маппим parent_id
	// (1-based offset) на реальный id через join с numbered CTE.
	// Если procedure задан, anchor ищет события с этой процедурой
	// вместо parent_id IS NULL — дерево строится только от них.
	anchorWhere := "e.session_id = $1 AND e.spid = $2 AND e.parent_id IS NULL AND (e.event_name LIKE '%Starting' OR e.event_name LIKE '%Completed')"
	args := []interface{}{sessionID, spid, maxDepth}
	if procedure != "" {
		anchorWhere = "e.session_id = $1 AND e.spid = $2 AND e.procedure = $4"
		args = append(args, procedure)
	}

	query := `WITH RECURSIVE numbered AS (
		SELECT id, row_number() OVER (ORDER BY id) AS rn
		FROM trc_events WHERE session_id = $1
	),
	tree AS (
		SELECT e.event_class, e.event_name, e.procedure, e.duration_ms,
		       e.params, e.columns, e.parent_id, e.depth, e.id, 1 AS tree_depth
		FROM trc_events e
		JOIN numbered n ON e.id = n.id
		WHERE ` + anchorWhere + `
		UNION ALL
		SELECT c.event_class, c.event_name, c.procedure, c.duration_ms,
		       c.params, c.columns, c.parent_id, c.depth, c.id, t.tree_depth + 1
		FROM trc_events c
		JOIN numbered nc ON c.id = nc.id
		JOIN numbered np ON c.parent_id = np.rn
		JOIN tree t ON np.id = t.id
		WHERE c.session_id = $1 AND c.spid = $2 AND ($3 = 0 OR t.tree_depth < $3)
	)
	SELECT event_class, event_name, procedure, duration_ms, params, columns,
	       parent_id, depth
	FROM tree`

	if maxNodes > 0 {
		paramIdx := "$4"
		if procedure != "" {
			paramIdx = "$5"
		}
		query += ` LIMIT ` + paramIdx
		args = append(args, maxNodes)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to load tree events: %w", err)
	}
	defer rows.Close()

	var events []TRCEvent
	for rows.Next() {
		ev, err := scanEventRow(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}
