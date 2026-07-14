package trc

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/codebase/internal/store"
	"github.com/lib/pq"
)

// SaveSession сохраняет результат разбора .trc файла в БД. Возвращает ID
// созданной сессии.
func SaveSession(db *store.DB, result *TRCParseResult, filePath string, fileSize int64) (int64, error) {
	var sessionID int64
	h := result.Header
	err := db.QueryRow(
		`INSERT INTO trc_sessions (file_path, file_size, total_events, provider_name, server_name, major_version, minor_version, build_number)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
		filePath, fileSize, len(result.Events),
		h.ProviderName, h.ServerName, h.MajorVersion, h.MinorVersion, h.BuildNumber,
	).Scan(&sessionID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert trc_sessions: %w", err)
	}

	if err := insertTRCEvents(db, result.Events, sessionID); err != nil {
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
func insertTRCEvents(db *store.DB, events []TRCEvent, sessionID int64) error {
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
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(pq.CopyIn("trc_events",
		"session_id", "event_class", "event_name", "text_data", "procedure",
		"spid", "database_id", "database_name", "application_name", "login_name", "host_name",
		"start_time", "end_time", "duration_ms", "cpu", "reads", "writes", "row_counts",
		"object_id", "object_name", "event_sequence", "nest_level", "line_number",
		"error", "severity", "success", "params", "columns",
	))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i, ev := range events {
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
		)
		if err != nil {
			return err
		}
	}

	if _, err := stmt.Exec(); err != nil {
		return err
	}
	stmt.Close()

	return tx.Commit()
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
		key := fmt.Sprintf("%d", id)
		switch val := v.(type) {
		case string:
			out[key] = jsonColumn{"string", val}
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
		var id int
		if _, err := fmt.Sscanf(key, "%d", &id); err != nil {
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
func ListSessions(db *store.DB, limit int) ([]TRCSession, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := db.Query(
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
func GetSession(db *store.DB, sessionID int64) (*TRCSession, error) {
	var s TRCSession
	var provider, server sql.NullString
	err := db.QueryRow(
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
func GetLatestSessionID(db *store.DB) (int64, error) {
	var id int64
	err := db.QueryRow(`SELECT id FROM trc_sessions ORDER BY parsed_at DESC LIMIT 1`).Scan(&id)
	return id, err
}

// DeleteSession удаляет сессию по ID (CASCADE удаляет все trc_events).
func DeleteSession(db *store.DB, sessionID int64) error {
	_, err := db.Exec(`DELETE FROM trc_sessions WHERE id = $1`, sessionID)
	return err
}

// PruneSessions удаляет старые сессии, оставляя только последние N.
func PruneSessions(db *store.DB, keepLast int) (int64, error) {
	result, err := db.Exec(
		`DELETE FROM trc_sessions WHERE id NOT IN (
			SELECT id FROM trc_sessions ORDER BY parsed_at DESC LIMIT $1
		)`,
		keepLast,
	)
	if err != nil {
		return 0, err
	}
	rows, _ := result.RowsAffected()
	return rows, nil
}

// LoadEvents загружает события сессии из БД, восстанавливая полный набор
// декодированных Columns из JSONB-снапшота (см. marshalColumns).
func LoadEvents(db *store.DB, sessionID int64) ([]TRCEvent, error) {
	rows, err := db.Query(
		`SELECT event_class, event_name, procedure, duration_ms, params, columns
		 FROM trc_events WHERE session_id = $1 ORDER BY id`,
		sessionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []TRCEvent
	for rows.Next() {
		var ev TRCEvent
		var eventName, procedure sql.NullString
		var paramsJSON, columnsJSON sql.NullString
		if err := rows.Scan(&ev.EventClass, &eventName, &procedure, &ev.DurationMs, &paramsJSON, &columnsJSON); err != nil {
			return nil, err
		}
		ev.EventName = eventName.String
		ev.Procedure = procedure.String

		if columnsJSON.Valid && columnsJSON.String != "" {
			cols, err := unmarshalColumns([]byte(columnsJSON.String))
			if err != nil {
				return nil, fmt.Errorf("unmarshal columns (event class %d): %w", ev.EventClass, err)
			}
			ev.Columns = cols
		} else {
			ev.Columns = map[int]any{}
		}
		if paramsJSON.Valid && paramsJSON.String != "" {
			if err := json.Unmarshal([]byte(paramsJSON.String), &ev.Params); err != nil {
				return nil, fmt.Errorf("unmarshal params (event class %d): %w", ev.EventClass, err)
			}
		}

		events = append(events, ev)
	}
	return events, rows.Err()
}
