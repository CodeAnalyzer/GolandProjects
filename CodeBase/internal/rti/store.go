package rti

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/codebase/internal/store"
	"github.com/lib/pq"
)

// SaveSession сохраняет результат парсинга RTI-лога в БД.
// Возвращает ID созданной сессии.
func SaveSession(db *store.DB, result *RTIParseResult, filePath string) (int64, error) {
	// 1. Создать rti_sessions запись
	var sessionID int64
	err := db.QueryRow(
		`INSERT INTO rti_sessions (file_path, file_size, total_calls, errors_count, max_nest_level, unparsed_lines, client_events_count)
		 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
		filePath, result.Summary.FileSize, result.Summary.TotalCalls,
		result.Summary.ErrorsCount, result.Summary.MaxNestLevel, result.Summary.UnparsedLines,
		result.Summary.ClientEventsCount,
	).Scan(&sessionID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert rti_sessions: %w", err)
	}

	// 2. Batch insert rti_calls
	callIDs, err := insertRTICalls(db, result.Calls, sessionID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert rti_calls: %w", err)
	}

	// 3. Batch insert rti_params
	if err := insertRTIParams(db, result.Calls, callIDs); err != nil {
		return 0, fmt.Errorf("failed to insert rti_params: %w", err)
	}

	// 4. Batch insert rti_checkpoints
	if err := insertRTICheckpoints(db, result.Calls, callIDs); err != nil {
		return 0, fmt.Errorf("failed to insert rti_checkpoints: %w", err)
	}

	// 5. Batch insert rti_blog_blocks
	if err := insertRTIBLogBlocks(db, result.Calls, callIDs, sessionID); err != nil {
		return 0, fmt.Errorf("failed to insert rti_blog_blocks: %w", err)
	}

	// 6. Batch insert rti_blog_tables
	if err := insertRTIBLogTables(db, result.Calls, callIDs, sessionID); err != nil {
		return 0, fmt.Errorf("failed to insert rti_blog_tables: %w", err)
	}

	// 7. Batch insert rti_client_events
	if _, err := insertRTIClientEvents(db, result.ClientEvents, sessionID, callIDs); err != nil {
		return 0, fmt.Errorf("failed to insert rti_client_events: %w", err)
	}

	return sessionID, nil
}

// clientEventPayload — типизированные данные клиентского события, сериализуемые
// в колонку payload (JSONB) rti_client_events. Общие поля (timestamp, category,
// class_name, method_name, pid, kind и т.д.) хранятся в отдельных колонках.
type clientEventPayload struct {
	BPL        []RTIBPLModule     `json:"bpl,omitempty"`
	Connection *RTIConnectionInfo `json:"connection,omitempty"`
	SQL        *RTISQLBlock       `json:"sql,omitempty"`
	TranCount  *int               `json:"tran_count,omitempty"`
	Memory     *RTIMemoryUsage    `json:"memory,omitempty"`
	ErrorText  string             `json:"error_text,omitempty"`
	RawBody    string             `json:"raw_body,omitempty"`
}

func insertRTIClientEvents(db *store.DB, events []*RTIClientEvent, sessionID int64, callIDs map[int64]int64) (map[int64]int64, error) {
	eventIDs := make(map[int64]int64)
	if len(events) == 0 {
		return eventIDs, nil
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(pq.CopyIn("rti_client_events",
		"session_id", "timestamp", "level", "category", "class_name", "method_name",
		"pid", "seq_no", "line_no", "kind", "elapsed_ms", "payload", "server_call_id",
	))
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	for _, ev := range events {
		payload := clientEventPayload{
			BPL:        ev.BPL,
			Connection: ev.Connection,
			SQL:        ev.SQL,
			TranCount:  ev.TranCount,
			Memory:     ev.Memory,
			ErrorText:  ev.ErrorText,
			RawBody:    ev.RawBody,
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		var payloadJSON interface{}
		if string(payloadBytes) != "{}" {
			payloadJSON = string(payloadBytes)
		}

		var ts interface{}
		if !ev.Timestamp.IsZero() {
			ts = ev.Timestamp
		}

		var serverCallDBID interface{}
		if ev.ServerCallID != nil {
			if dbID, ok := callIDs[*ev.ServerCallID]; ok {
				serverCallDBID = dbID
			}
		}

		_, err = stmt.Exec(
			sessionID, ts, ev.Level, ev.Category, ev.ClassName, ev.MethodName,
			ev.PID, ev.SeqNo, ev.Line, ev.Kind, ev.ElapsedMs, payloadJSON, serverCallDBID,
		)
		if err != nil {
			return nil, err
		}
	}

	if _, err := stmt.Exec(); err != nil {
		return nil, err
	}
	stmt.Close()

	// Сопоставляем оригинальные ID событий (в памяти) с ID, назначенными БД,
	// по ключу (session_id, line_no, pid, kind) — аналогично insertRTICalls.
	rows, err := tx.Query(
		`SELECT id, line_no, pid, kind FROM rti_client_events WHERE session_id = $1 ORDER BY id`,
		sessionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type eventKey struct {
		line int
		pid  int
		kind string
	}
	keyToDBID := make(map[eventKey]int64)
	for rows.Next() {
		var dbID int64
		var line, pid int
		var kind string
		if err := rows.Scan(&dbID, &line, &pid, &kind); err != nil {
			return nil, err
		}
		keyToDBID[eventKey{line, pid, kind}] = dbID
	}
	rows.Close()

	for _, ev := range events {
		key := eventKey{ev.Line, ev.PID, ev.Kind}
		if dbID, ok := keyToDBID[key]; ok {
			eventIDs[ev.ID] = dbID
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return eventIDs, nil
}

func insertRTICalls(db *store.DB, calls []*RTICall, sessionID int64) (map[int64]int64, error) {
	callIDs := make(map[int64]int64)
	if len(calls) == 0 {
		return callIDs, nil
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(pq.CopyIn("rti_calls",
		"session_id", "procedure", "enter_line", "exit_line",
		"enter_time", "exit_time", "elapsed_ms", "nest_level",
		"module_id", "module_name", "tran_count", "begin_cnt",
		"ret_val", "ret_val_context", "parent_id", "spid",
	))
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	for _, c := range calls {
		var exitTime interface{}
		if c.ExitTime != nil {
			exitTime = *c.ExitTime
		}
		var retVal interface{}
		if c.RetVal != nil {
			retVal = *c.RetVal
		}
		var parentID interface{}
		if c.ParentID != nil {
			parentID = *c.ParentID
		}
		_, err := stmt.Exec(
			sessionID, c.Procedure, c.EnterLine, c.ExitLine,
			c.EnterTime, exitTime, c.ElapsedMs, c.NestLevel,
			c.ModuleID, c.ModuleName, c.TranCount, c.BeginCnt,
			retVal, c.RetValContext, parentID, c.SPID,
		)
		if err != nil {
			return nil, err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return nil, err
	}
	stmt.Close()

	// Получить ID вставленных записей
	rows, err := tx.Query(
		`SELECT id, enter_line, procedure, spid FROM rti_calls WHERE session_id = $1 ORDER BY id`,
		sessionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Map: original call ID → DB row ID, matched by enter_line + procedure + spid
	type callKey struct {
		enterLine int
		procedure string
		spid      int
	}
	keyToDBID := make(map[callKey]int64)
	for rows.Next() {
		var dbID int64
		var enterLine int
		var proc string
		var spid int
		if err := rows.Scan(&dbID, &enterLine, &proc, &spid); err != nil {
			return nil, err
		}
		keyToDBID[callKey{enterLine, proc, spid}] = dbID
	}
	rows.Close()

	for _, c := range calls {
		key := callKey{c.EnterLine, c.Procedure, c.SPID}
		if dbID, ok := keyToDBID[key]; ok {
			callIDs[c.ID] = dbID
		}
	}

	// Обновить parent_id: оригинальные ID → DB ID
	updateStmt, err := tx.Prepare(`UPDATE rti_calls SET parent_id = $1 WHERE id = $2 AND session_id = $3`)
	if err != nil {
		return nil, err
	}
	defer updateStmt.Close()

	for _, c := range calls {
		if c.ParentID == nil {
			continue
		}
		dbID, ok := callIDs[c.ID]
		if !ok {
			continue
		}
		parentDBID, ok := callIDs[*c.ParentID]
		if !ok {
			continue
		}
		if _, err := updateStmt.Exec(parentDBID, dbID, sessionID); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return callIDs, nil
}

func insertRTIParams(db *store.DB, calls []*RTICall, callIDs map[int64]int64) error {
	var params []struct {
		callID int64
		name   string
		typ    string
		value  string
	}
	for _, c := range calls {
		dbID, ok := callIDs[c.ID]
		if !ok {
			continue
		}
		for _, p := range c.Params {
			params = append(params, struct {
				callID int64
				name   string
				typ    string
				value  string
			}{dbID, p.Name, p.Type, p.Value})
		}
	}
	if len(params) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(pq.CopyIn("rti_params", "call_id", "name", "type", "value"))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, p := range params {
		_, err := stmt.Exec(p.callID, p.name, p.typ, p.value)
		if err != nil {
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	return tx.Commit()
}

func insertRTICheckpoints(db *store.DB, calls []*RTICall, callIDs map[int64]int64) error {
	var checkpoints []struct {
		callID    int64
		label     string
		timestamp time.Time
		elapsedMs int
		lineNo    int
	}
	for _, c := range calls {
		dbID, ok := callIDs[c.ID]
		if !ok {
			continue
		}
		for _, cp := range c.Checkpoints {
			checkpoints = append(checkpoints, struct {
				callID    int64
				label     string
				timestamp time.Time
				elapsedMs int
				lineNo    int
			}{dbID, cp.Label, cp.Timestamp, cp.ElapsedMs, cp.LineNo})
		}
	}
	if len(checkpoints) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(pq.CopyIn("rti_checkpoints", "call_id", "label", "timestamp", "elapsed_ms", "line_no"))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, cp := range checkpoints {
		_, err := stmt.Exec(cp.callID, cp.label, cp.timestamp, cp.elapsedMs, cp.lineNo)
		if err != nil {
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	return tx.Commit()
}

func insertRTIBLogBlocks(db *store.DB, calls []*RTICall, callIDs map[int64]int64, sessionID int64) error {
	type row struct {
		callID    int64
		blockName string
		enterTime time.Time
		exitTime  time.Time
		elapsedMs int
		enterLine int
		exitLine  int
	}
	var rows []row
	for _, c := range calls {
		dbID, ok := callIDs[c.ID]
		if !ok {
			continue
		}
		for _, b := range c.BLogBlocks {
			rows = append(rows, row{dbID, b.BlockName, b.EnterTime, b.ExitTime, b.ElapsedMs, b.EnterLine, b.ExitLine})
		}
	}
	if len(rows) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(pq.CopyIn("rti_blog_blocks",
		"session_id", "call_id", "block_name", "enter_time", "exit_time", "elapsed_ms", "enter_line", "exit_line"))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range rows {
		var enterTime, exitTime interface{}
		if !r.enterTime.IsZero() {
			enterTime = r.enterTime
		}
		if !r.exitTime.IsZero() {
			exitTime = r.exitTime
		}
		if _, err := stmt.Exec(sessionID, r.callID, r.blockName, enterTime, exitTime, r.elapsedMs, r.enterLine, r.exitLine); err != nil {
			return err
		}
	}
	if _, err := stmt.Exec(); err != nil {
		return err
	}
	return tx.Commit()
}

func insertRTIBLogTables(db *store.DB, calls []*RTICall, callIDs map[int64]int64, sessionID int64) error {
	type row struct {
		callID    int64
		tableName string
		columns   string
		rowCount  int
		rowsData  string
		enterLine int
	}
	var rows []row
	for _, c := range calls {
		dbID, ok := callIDs[c.ID]
		if !ok {
			continue
		}
		for _, t := range c.BLogTables {
			rows = append(rows, row{
				dbID, t.TableName,
				strings.Join(t.Columns, "_|_"),
				t.RowCount,
				strings.Join(t.Rows, "\n"),
				t.EnterLine,
			})
		}
	}
	if len(rows) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(pq.CopyIn("rti_blog_tables",
		"session_id", "call_id", "table_name", "columns_header", "row_count", "rows_data", "enter_line"))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range rows {
		if _, err := stmt.Exec(sessionID, r.callID, r.tableName, r.columns, r.rowCount, r.rowsData, r.enterLine); err != nil {
			return err
		}
	}
	if _, err := stmt.Exec(); err != nil {
		return err
	}
	return tx.Commit()
}

// LoadBLogBlocks загружает BLog-блоки вызова из БД.
func LoadBLogBlocks(db *store.DB, sessionID int64, callID int64) ([]RTIBLogBlock, error) {
	rows, err := db.Query(
		`SELECT block_name, enter_time, exit_time, elapsed_ms, enter_line, exit_line
		 FROM rti_blog_blocks WHERE session_id = $1 AND call_id = $2 ORDER BY id`,
		sessionID, callID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var blocks []RTIBLogBlock
	for rows.Next() {
		var b RTIBLogBlock
		var enterTime, exitTime sql.NullTime
		if err := rows.Scan(&b.BlockName, &enterTime, &exitTime, &b.ElapsedMs, &b.EnterLine, &b.ExitLine); err != nil {
			return nil, err
		}
		if enterTime.Valid {
			b.EnterTime = enterTime.Time
		}
		if exitTime.Valid {
			b.ExitTime = exitTime.Time
		}
		blocks = append(blocks, b)
	}
	return blocks, rows.Err()
}

// LoadBLogTables загружает BLog-дампы таблиц вызова из БД.
func LoadBLogTables(db *store.DB, sessionID int64, callID int64) ([]RTIBLogTable, error) {
	rows, err := db.Query(
		`SELECT table_name, columns_header, row_count, rows_data, enter_line
		 FROM rti_blog_tables WHERE session_id = $1 AND call_id = $2 ORDER BY id`,
		sessionID, callID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []RTIBLogTable
	for rows.Next() {
		var t RTIBLogTable
		var columnsHeader, rowsData sql.NullString
		if err := rows.Scan(&t.TableName, &columnsHeader, &t.RowCount, &rowsData, &t.EnterLine); err != nil {
			return nil, err
		}
		if columnsHeader.Valid && columnsHeader.String != "" {
			t.Columns = strings.Split(columnsHeader.String, "_|_")
		}
		if rowsData.Valid && rowsData.String != "" {
			t.Rows = strings.Split(rowsData.String, "\n")
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

// ListSessions возвращает список сессий из БД.
func ListSessions(db *store.DB, limit int) ([]RTISession, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := db.Query(
		`SELECT id, file_path, file_size, parsed_at, total_calls, errors_count, max_nest_level, unparsed_lines, client_events_count
		 FROM rti_sessions ORDER BY parsed_at DESC LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sessions []RTISession
	for rows.Next() {
		var s RTISession
		var maxNestLevel, unparsedLines int
		if err := rows.Scan(&s.ID, &s.FilePath, &s.FileSize, &s.ParsedAt,
			&s.TotalCalls, &s.ErrorsCount, &maxNestLevel, &unparsedLines, &s.ClientEventsCount); err != nil {
			return nil, err
		}
		sessions = append(sessions, s)
	}
	return sessions, rows.Err()
}

// DeleteSession удаляет сессию по ID (CASCADE удаляет все дочерние записи).
func DeleteSession(db *store.DB, sessionID int64) error {
	_, err := db.Exec(`DELETE FROM rti_sessions WHERE id = $1`, sessionID)
	return err
}

// PruneSessions удаляет старые сессии, оставляя только последние N.
func PruneSessions(db *store.DB, keepLast int) (int64, error) {
	result, err := db.Exec(
		`DELETE FROM rti_sessions WHERE id NOT IN (
			SELECT id FROM rti_sessions ORDER BY parsed_at DESC LIMIT $1
		)`,
		keepLast,
	)
	if err != nil {
		return 0, err
	}
	rows, _ := result.RowsAffected()
	return rows, nil
}

// GetSession возвращает информацию о сессии по ID.
func GetSession(db *store.DB, sessionID int64) (*RTISession, error) {
	var s RTISession
	var maxNestLevel, unparsedLines int
	err := db.QueryRow(
		`SELECT id, file_path, file_size, parsed_at, total_calls, errors_count, max_nest_level, unparsed_lines, client_events_count
		 FROM rti_sessions WHERE id = $1`,
		sessionID,
	).Scan(&s.ID, &s.FilePath, &s.FileSize, &s.ParsedAt,
		&s.TotalCalls, &s.ErrorsCount, &maxNestLevel, &unparsedLines, &s.ClientEventsCount)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// LoadCalls загружает вызовы из БД для сессии.
func LoadCalls(db *store.DB, sessionID int64) ([]*RTICall, error) {
	rows, err := db.Query(
		`SELECT id, procedure, enter_line, exit_line, enter_time, exit_time,
		        elapsed_ms, nest_level, module_id, module_name, tran_count,
		        begin_cnt, ret_val, ret_val_context, parent_id, spid
		 FROM rti_calls WHERE session_id = $1 ORDER BY id`,
		sessionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var calls []*RTICall
	for rows.Next() {
		var c RTICall
		var enterTime sql.NullTime
		var exitTime sql.NullTime
		var retVal sql.NullInt64
		var parentID sql.NullInt64
		var moduleName sql.NullString
		if err := rows.Scan(
			&c.ID, &c.Procedure, &c.EnterLine, &c.ExitLine,
			&enterTime, &exitTime, &c.ElapsedMs, &c.NestLevel,
			&c.ModuleID, &moduleName, &c.TranCount, &c.BeginCnt,
			&retVal, &c.RetValContext, &parentID, &c.SPID,
		); err != nil {
			return nil, err
		}
		if enterTime.Valid {
			c.EnterTime = enterTime.Time
		}
		if exitTime.Valid {
			c.ExitTime = &exitTime.Time
		}
		if retVal.Valid {
			v := int(retVal.Int64)
			c.RetVal = &v
		}
		if parentID.Valid {
			pid := parentID.Int64
			c.ParentID = &pid
		}
		if moduleName.Valid {
			c.ModuleName = moduleName.String
		}
		calls = append(calls, &c)
	}

	// Load children IDs
	for _, c := range calls {
		if c.ParentID != nil {
			for _, p := range calls {
				if p.ID == *c.ParentID {
					p.Children = append(p.Children, c.ID)
					break
				}
			}
		}
	}

	// Load BLogBlocks for all calls in one query
	if err := loadAllBLogBlocks(db, sessionID, calls); err != nil {
		return nil, fmt.Errorf("failed to load blog blocks: %w", err)
	}

	// Load BLogTables for all calls in one query
	if err := loadAllBLogTables(db, sessionID, calls); err != nil {
		return nil, fmt.Errorf("failed to load blog tables: %w", err)
	}

	return calls, rows.Err()
}

// loadAllBLogBlocks загружает BLog-блоки для всех вызовов сессии одним запросом.
func loadAllBLogBlocks(db *store.DB, sessionID int64, calls []*RTICall) error {
	rows, err := db.Query(
		`SELECT call_id, block_name, enter_time, exit_time, elapsed_ms, enter_line, exit_line
		 FROM rti_blog_blocks WHERE session_id = $1 ORDER BY id`,
		sessionID)
	if err != nil {
		return err
	}
	defer rows.Close()

	callMap := make(map[int64]*RTICall, len(calls))
	for _, c := range calls {
		callMap[c.ID] = c
	}

	for rows.Next() {
		var callID int64
		var b RTIBLogBlock
		var enterTime, exitTime sql.NullTime
		if err := rows.Scan(&callID, &b.BlockName, &enterTime, &exitTime, &b.ElapsedMs, &b.EnterLine, &b.ExitLine); err != nil {
			return err
		}
		if enterTime.Valid {
			b.EnterTime = enterTime.Time
		}
		if exitTime.Valid {
			b.ExitTime = exitTime.Time
		}
		if c, ok := callMap[callID]; ok {
			c.BLogBlocks = append(c.BLogBlocks, b)
		}
	}
	return rows.Err()
}

// loadAllBLogTables загружает BLog-дампы таблиц для всех вызовов сессии одним запросом.
func loadAllBLogTables(db *store.DB, sessionID int64, calls []*RTICall) error {
	rows, err := db.Query(
		`SELECT call_id, table_name, columns_header, row_count, rows_data, enter_line
		 FROM rti_blog_tables WHERE session_id = $1 ORDER BY id`,
		sessionID)
	if err != nil {
		return err
	}
	defer rows.Close()

	callMap := make(map[int64]*RTICall, len(calls))
	for _, c := range calls {
		callMap[c.ID] = c
	}

	for rows.Next() {
		var callID int64
		var t RTIBLogTable
		var columnsHeader, rowsData sql.NullString
		if err := rows.Scan(&callID, &t.TableName, &columnsHeader, &t.RowCount, &rowsData, &t.EnterLine); err != nil {
			return err
		}
		if columnsHeader.Valid && columnsHeader.String != "" {
			t.Columns = strings.Split(columnsHeader.String, "_|_")
		}
		if rowsData.Valid && rowsData.String != "" {
			t.Rows = strings.Split(rowsData.String, "\n")
		}
		if c, ok := callMap[callID]; ok {
			c.BLogTables = append(c.BLogTables, t)
		}
	}
	return rows.Err()
}

// GetLatestSessionID возвращает ID последней сессии.
func GetLatestSessionID(db *store.DB) (int64, error) {
	var id int64
	err := db.QueryRow(`SELECT id FROM rti_sessions ORDER BY parsed_at DESC LIMIT 1`).Scan(&id)
	return id, err
}

// LoadClientEvents загружает клиентские события из БД для сессии.
func LoadClientEvents(db *store.DB, sessionID int64) ([]*RTIClientEvent, error) {
	rows, err := db.Query(
		`SELECT id, timestamp, level, category, class_name, method_name,
		        pid, seq_no, line_no, kind, elapsed_ms, payload, server_call_id
		 FROM rti_client_events WHERE session_id = $1 ORDER BY id`,
		sessionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*RTIClientEvent
	for rows.Next() {
		var ev RTIClientEvent
		var ts sql.NullTime
		var level, category, className, methodName sql.NullString
		var payloadJSON sql.NullString
		var serverCallID sql.NullInt64

		if err := rows.Scan(
			&ev.ID, &ts, &level, &category, &className, &methodName,
			&ev.PID, &ev.SeqNo, &ev.Line, &ev.Kind, &ev.ElapsedMs,
			&payloadJSON, &serverCallID,
		); err != nil {
			return nil, err
		}
		if ts.Valid {
			ev.Timestamp = ts.Time
		}
		ev.Level = level.String
		ev.Category = category.String
		ev.ClassName = className.String
		ev.MethodName = methodName.String
		if serverCallID.Valid {
			id := serverCallID.Int64
			ev.ServerCallID = &id
		}

		if payloadJSON.Valid && payloadJSON.String != "" {
			var payload clientEventPayload
			if err := json.Unmarshal([]byte(payloadJSON.String), &payload); err != nil {
				return nil, fmt.Errorf("failed to unmarshal client event payload (id=%d): %w", ev.ID, err)
			}
			ev.BPL = payload.BPL
			ev.Connection = payload.Connection
			ev.SQL = payload.SQL
			ev.TranCount = payload.TranCount
			ev.Memory = payload.Memory
			ev.ErrorText = payload.ErrorText
			ev.RawBody = payload.RawBody
		}

		events = append(events, &ev)
	}
	return events, rows.Err()
}
