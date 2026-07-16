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

	// Обновить parent_id: оригинальные ID → DB ID (batched UPDATE FROM VALUES)
	type parentPair struct {
		id       int64
		parentID int64
	}
	var pairs []parentPair
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
		pairs = append(pairs, parentPair{id: dbID, parentID: parentDBID})
	}

	const batchSize = 5000
	for i := 0; i < len(pairs); i += batchSize {
		end := i + batchSize
		if end > len(pairs) {
			end = len(pairs)
		}
		batch := pairs[i:end]
		var sb strings.Builder
		sb.WriteString("UPDATE rti_calls AS t SET parent_id = v.parent_id FROM (VALUES ")
		args := make([]interface{}, 0, len(batch)*2+1)
		for j, p := range batch {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("($%d::bigint, $%d::bigint)", j*2+1, j*2+2))
			args = append(args, p.id, p.parentID)
		}
		sb.WriteString(fmt.Sprintf(") AS v(id, parent_id) WHERE t.id = v.id AND t.session_id = $%d", len(batch)*2+1))
		args = append(args, sessionID)
		if _, err := tx.Exec(sb.String(), args...); err != nil {
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
		if err := rows.Scan(&s.ID, &s.FilePath, &s.FileSize, &s.ParsedAt,
			&s.TotalCalls, &s.ErrorsCount, &s.MaxNestLevel, &s.UnparsedLines, &s.ClientEventsCount); err != nil {
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
	err := db.QueryRow(
		`SELECT id, file_path, file_size, parsed_at, total_calls, errors_count, max_nest_level, unparsed_lines, client_events_count
		 FROM rti_sessions WHERE id = $1`,
		sessionID,
	).Scan(&s.ID, &s.FilePath, &s.FileSize, &s.ParsedAt,
		&s.TotalCalls, &s.ErrorsCount, &s.MaxNestLevel, &s.UnparsedLines, &s.ClientEventsCount)
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

	// Load children IDs — O(n) via map lookup instead of O(n²) linear scan
	callByID := make(map[int64]*RTICall, len(calls))
	for _, c := range calls {
		callByID[c.ID] = c
	}
	for _, c := range calls {
		if c.ParentID != nil {
			if p, ok := callByID[*c.ParentID]; ok {
				p.Children = append(p.Children, c.ID)
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

	// Load params for all calls in one query
	if err := loadAllParams(db, sessionID, calls); err != nil {
		return nil, fmt.Errorf("failed to load params: %w", err)
	}

	// Load checkpoints for all calls in one query
	if err := loadAllCheckpoints(db, sessionID, calls); err != nil {
		return nil, fmt.Errorf("failed to load checkpoints: %w", err)
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

// loadAllParams загружает параметры для всех вызовов сессии одним запросом.
func loadAllParams(db *store.DB, sessionID int64, calls []*RTICall) error {
	rows, err := db.Query(
		`SELECT call_id, name, type, value
		 FROM rti_params
		 WHERE call_id IN (SELECT id FROM rti_calls WHERE session_id = $1)
		 ORDER BY id`,
		sessionID,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	callMap := make(map[int64]*RTICall)
	for _, c := range calls {
		callMap[c.ID] = c
	}

	for rows.Next() {
		var callID int64
		var p RTIParam
		if err := rows.Scan(&callID, &p.Name, &p.Type, &p.Value); err != nil {
			return err
		}
		if c, ok := callMap[callID]; ok {
			c.Params = append(c.Params, p)
		}
	}
	return rows.Err()
}

// loadAllCheckpoints загружает чекпоинты для всех вызовов сессии одним запросом.
func loadAllCheckpoints(db *store.DB, sessionID int64, calls []*RTICall) error {
	rows, err := db.Query(
		`SELECT call_id, label, timestamp, elapsed_ms, line_no
		 FROM rti_checkpoints
		 WHERE call_id IN (SELECT id FROM rti_calls WHERE session_id = $1)
		 ORDER BY id`,
		sessionID,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	callMap := make(map[int64]*RTICall)
	for _, c := range calls {
		callMap[c.ID] = c
	}

	for rows.Next() {
		var callID int64
		var cp RTICheckpoint
		var ts sql.NullTime
		if err := rows.Scan(&callID, &cp.Label, &ts, &cp.ElapsedMs, &cp.LineNo); err != nil {
			return err
		}
		if ts.Valid {
			cp.Timestamp = ts.Time
		}
		if c, ok := callMap[callID]; ok {
			c.Checkpoints = append(c.Checkpoints, cp)
		}
	}
	return rows.Err()
}

// scanCallColumns сканирует стандартный набор колонок rti_calls в RTICall.
func scanCallColumns(rows *sql.Rows) (*RTICall, error) {
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
	return &c, nil
}

const callSelectColumns = `id, procedure, enter_line, exit_line, enter_time, exit_time,
	elapsed_ms, nest_level, module_id, module_name, tran_count,
	begin_cnt, ret_val, ret_val_context, parent_id, spid`

// scanClientEventColumns сканирует стандартный набор колонок rti_client_events.
func scanClientEventColumns(rows *sql.Rows) (*RTIClientEvent, error) {
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
	return &ev, nil
}

// LoadSummary загружает сводку по сессии напрямую из БД через SQL-агрегаты,
// без загрузки всех вызовов в память.
func LoadSummary(db *store.DB, sessionID int64) (*RTISummary, error) {
	session, err := GetSession(db, sessionID)
	if err != nil {
		return nil, fmt.Errorf("session %d not found: %w", sessionID, err)
	}

	summary := &RTISummary{
		FilePath:      session.FilePath,
		FileSize:      session.FileSize,
		TotalCalls:    session.TotalCalls,
		ErrorsCount:   session.ErrorsCount,
		MaxNestLevel:  session.MaxNestLevel,
		UnparsedLines: session.UnparsedLines,
	}

	// Агрегаты из rti_calls
	var totalCalls, errorsCount, maxNest, slowCalls int
	err = db.QueryRow(
		`SELECT count(*),
		        count(*) FILTER (WHERE ret_val IS NOT NULL AND ret_val != 0),
		        COALESCE(max(nest_level), 0),
		        count(*) FILTER (WHERE elapsed_ms >= 100)
		 FROM rti_calls WHERE session_id = $1`,
		sessionID,
	).Scan(&totalCalls, &errorsCount, &maxNest, &slowCalls)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to load call aggregates: %w", err)
	}
	summary.TotalCalls = totalCalls
	summary.ErrorsCount = errorsCount
	summary.MaxNestLevel = maxNest
	summary.SlowCallsCount = slowCalls

	// Top 10 slow calls (без params/checkpoints/blog)
	rows, err := db.Query(
		`SELECT `+callSelectColumns+`
		 FROM rti_calls WHERE session_id = $1 ORDER BY elapsed_ms DESC LIMIT 10`,
		sessionID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load top slow calls: %w", err)
	}
	var topSlow []RTICall
	for rows.Next() {
		c, err := scanCallColumns(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		c.BLogTables = nil
		c.BLogBlocks = nil
		topSlow = append(topSlow, *c)
	}
	rows.Close()
	summary.TopSlow = topSlow

	// Клиентские агрегаты
	var clientCount, clientErrors, clientSlowSQL int
	err = db.QueryRow(
		`SELECT count(*),
		        count(*) FILTER (WHERE kind = 'error' AND payload->>'error_text' != ''),
		        count(*) FILTER (WHERE kind = 'sql_block' AND elapsed_ms >= 100)
		 FROM rti_client_events WHERE session_id = $1`,
		sessionID,
	).Scan(&clientCount, &clientErrors, &clientSlowSQL)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to load client aggregates: %w", err)
	}
	summary.ClientEventsCount = clientCount
	summary.ClientErrorsCount = clientErrors
	summary.ClientSlowSQLCount = clientSlowSQL

	// Top 10 slow client SQL blocks
	clientRows, err := db.Query(
		`SELECT id, timestamp, level, category, class_name, method_name,
		        pid, seq_no, line_no, kind, elapsed_ms, payload, server_call_id
		 FROM rti_client_events
		 WHERE session_id = $1 AND kind = 'sql_block'
		 ORDER BY elapsed_ms DESC LIMIT 10`,
		sessionID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load top slow client SQL: %w", err)
	}
	var topSlowClientSQL []RTIClientEvent
	for clientRows.Next() {
		ev, err := scanClientEventColumns(clientRows)
		if err != nil {
			clientRows.Close()
			return nil, err
		}
		topSlowClientSQL = append(topSlowClientSQL, *ev)
	}
	clientRows.Close()
	summary.TopSlowClientSQL = topSlowClientSQL

	return summary, nil
}

// LoadSlowCalls загружает медленные вызовы из БД с фильтрацией и лимитом на стороне SQL.
// Не загружает params/checkpoints/blog — только базовые поля вызова.
func LoadSlowCalls(db *store.DB, sessionID int64, thresholdMs int, limit int) ([]*RTICall, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := db.Query(
		`SELECT `+callSelectColumns+`
		 FROM rti_calls
		 WHERE session_id = $1 AND elapsed_ms >= $2
		 ORDER BY elapsed_ms DESC LIMIT $3`,
		sessionID, thresholdMs, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load slow calls: %w", err)
	}
	defer rows.Close()

	var calls []*RTICall
	for rows.Next() {
		c, err := scanCallColumns(rows)
		if err != nil {
			return nil, err
		}
		calls = append(calls, c)
	}
	return calls, rows.Err()
}

// LoadErrorCalls загружает вызовы с ненулевым ret_val из БД с лимитом.
func LoadErrorCalls(db *store.DB, sessionID int64, limit int) ([]*RTICall, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := db.Query(
		`SELECT `+callSelectColumns+`
		 FROM rti_calls
		 WHERE session_id = $1 AND ret_val IS NOT NULL AND ret_val != 0
		 ORDER BY id LIMIT $2`,
		sessionID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load error calls: %w", err)
	}
	defer rows.Close()

	var calls []*RTICall
	for rows.Next() {
		c, err := scanCallColumns(rows)
		if err != nil {
			return nil, err
		}
		calls = append(calls, c)
	}
	return calls, rows.Err()
}

// LoadClientErrors загружает клиентские ошибки из БД с лимитом.
func LoadClientErrors(db *store.DB, sessionID int64, limit int) ([]*RTIClientEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := db.Query(
		`SELECT id, timestamp, level, category, class_name, method_name,
		        pid, seq_no, line_no, kind, elapsed_ms, payload, server_call_id
		 FROM rti_client_events
		 WHERE session_id = $1 AND kind = 'error' AND payload->>'error_text' != ''
		 ORDER BY id LIMIT $2`,
		sessionID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load client errors: %w", err)
	}
	defer rows.Close()

	var events []*RTIClientEvent
	for rows.Next() {
		ev, err := scanClientEventColumns(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

// LoadSlowClientSQL загружает медленные клиентские SQL-блоки из БД с лимитом.
func LoadSlowClientSQL(db *store.DB, sessionID int64, thresholdMs int, limit int) ([]*RTIClientEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := db.Query(
		`SELECT id, timestamp, level, category, class_name, method_name,
		        pid, seq_no, line_no, kind, elapsed_ms, payload, server_call_id
		 FROM rti_client_events
		 WHERE session_id = $1 AND kind = 'sql_block' AND elapsed_ms >= $2
		 ORDER BY elapsed_ms DESC LIMIT $3`,
		sessionID, thresholdMs, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load slow client SQL: %w", err)
	}
	defer rows.Close()

	var events []*RTIClientEvent
	for rows.Next() {
		ev, err := scanClientEventColumns(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

// LoadCallsByProcedure загружает вызовы конкретной процедуры из БД с лимитом.
// Загружает params/checkpoints/blog только для найденных вызовов.
func LoadCallsByProcedure(db *store.DB, sessionID int64, procName string, limit int) ([]*RTICall, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := db.Query(
		`SELECT `+callSelectColumns+`
		 FROM rti_calls
		 WHERE session_id = $1 AND procedure = $2
		 ORDER BY id LIMIT $3`,
		sessionID, procName, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load calls by procedure: %w", err)
	}
	defer rows.Close()

	var calls []*RTICall
	for rows.Next() {
		c, err := scanCallColumns(rows)
		if err != nil {
			return nil, err
		}
		calls = append(calls, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(calls) == 0 {
		return calls, nil
	}

	// Build callID map for detail loading
	callMap := make(map[int64]*RTICall, len(calls))
	callIDs := make([]int64, 0, len(calls))
	for _, c := range calls {
		callMap[c.ID] = c
		callIDs = append(callIDs, c.ID)
	}

	// Load params, checkpoints, blog for these calls only
	if err := loadDetailsForCallIDs(db, sessionID, callIDs, callMap); err != nil {
		return nil, err
	}

	return calls, nil
}

// loadDetailsForCallIDs загружает params/checkpoints/blog для указанных call IDs.
func loadDetailsForCallIDs(db *store.DB, sessionID int64, callIDs []int64, callMap map[int64]*RTICall) error {
	if len(callIDs) == 0 {
		return nil
	}

	// Params
	paramRows, err := db.Query(
		`SELECT call_id, name, type, value
		 FROM rti_params
		 WHERE call_id = ANY($1)
		 ORDER BY id`,
		pq.Array(callIDs),
	)
	if err != nil {
		return fmt.Errorf("failed to load params: %w", err)
	}
	defer paramRows.Close()
	for paramRows.Next() {
		var callID int64
		var p RTIParam
		if err := paramRows.Scan(&callID, &p.Name, &p.Type, &p.Value); err != nil {
			return err
		}
		if c, ok := callMap[callID]; ok {
			c.Params = append(c.Params, p)
		}
	}
	if err := paramRows.Err(); err != nil {
		return err
	}

	// Checkpoints
	cpRows, err := db.Query(
		`SELECT call_id, label, timestamp, elapsed_ms, line_no
		 FROM rti_checkpoints
		 WHERE call_id = ANY($1)
		 ORDER BY id`,
		pq.Array(callIDs),
	)
	if err != nil {
		return fmt.Errorf("failed to load checkpoints: %w", err)
	}
	defer cpRows.Close()
	for cpRows.Next() {
		var callID int64
		var cp RTICheckpoint
		var ts sql.NullTime
		if err := cpRows.Scan(&callID, &cp.Label, &ts, &cp.ElapsedMs, &cp.LineNo); err != nil {
			return err
		}
		if ts.Valid {
			cp.Timestamp = ts.Time
		}
		if c, ok := callMap[callID]; ok {
			c.Checkpoints = append(c.Checkpoints, cp)
		}
	}
	if err := cpRows.Err(); err != nil {
		return err
	}

	// BLog blocks
	bbRows, err := db.Query(
		`SELECT call_id, block_name, enter_time, exit_time, elapsed_ms, enter_line, exit_line
		 FROM rti_blog_blocks
		 WHERE session_id = $1 AND call_id = ANY($2)
		 ORDER BY id`,
		sessionID, pq.Array(callIDs),
	)
	if err != nil {
		return fmt.Errorf("failed to load blog blocks: %w", err)
	}
	defer bbRows.Close()
	for bbRows.Next() {
		var callID int64
		var b RTIBLogBlock
		var enterTime, exitTime sql.NullTime
		if err := bbRows.Scan(&callID, &b.BlockName, &enterTime, &exitTime, &b.ElapsedMs, &b.EnterLine, &b.ExitLine); err != nil {
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
	if err := bbRows.Err(); err != nil {
		return err
	}

	// BLog tables
	btRows, err := db.Query(
		`SELECT call_id, table_name, columns_header, row_count, rows_data, enter_line
		 FROM rti_blog_tables
		 WHERE session_id = $1 AND call_id = ANY($2)
		 ORDER BY id`,
		sessionID, pq.Array(callIDs),
	)
	if err != nil {
		return fmt.Errorf("failed to load blog tables: %w", err)
	}
	defer btRows.Close()
	for btRows.Next() {
		var callID int64
		var t RTIBLogTable
		var columnsHeader, rowsData sql.NullString
		if err := btRows.Scan(&callID, &t.TableName, &columnsHeader, &t.RowCount, &rowsData, &t.EnterLine); err != nil {
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
	return btRows.Err()
}

// LoadCallsForTree загружает вызовы для построения дерева через recursive CTE.
// Если rootProcedure пустой, автоматически выбирает корень (NestLevel=1 с наибольшим числом потомков).
// maxTreeNodes ограничивает общее количество загружаемых узлов (default 5000).
func LoadCallsForTree(db *store.DB, sessionID int64, rootProcedure string, maxDepth int, maxTreeNodes int) ([]*RTICall, error) {
	if maxTreeNodes <= 0 {
		maxTreeNodes = 5000
	}

	var rows *sql.Rows
	var err error

	if rootProcedure != "" {
		rows, err = db.Query(
			`WITH RECURSIVE call_tree AS (
				(SELECT `+callSelectColumns+`, 1 AS depth
				FROM rti_calls
				WHERE session_id = $1 AND procedure = $2
				LIMIT 1)
				UNION ALL
				SELECT c.id, c.procedure, c.enter_line, c.exit_line, c.enter_time, c.exit_time,
				       c.elapsed_ms, c.nest_level, c.module_id, c.module_name, c.tran_count,
				       c.begin_cnt, c.ret_val, c.ret_val_context, c.parent_id, c.spid, t.depth + 1
				FROM rti_calls c
				JOIN call_tree t ON c.parent_id = t.id
				WHERE c.session_id = $1 AND ($3 = 0 OR t.depth < $3)
			)
			SELECT id, procedure, enter_line, exit_line, enter_time, exit_time,
			       elapsed_ms, nest_level, module_id, module_name, tran_count,
			       begin_cnt, ret_val, ret_val_context, parent_id, spid
			FROM call_tree LIMIT $4`,
			sessionID, rootProcedure, maxDepth, maxTreeNodes,
		)
	} else {
		rows, err = db.Query(
			`WITH RECURSIVE call_tree AS (
				SELECT `+callSelectColumns+`, 1 AS depth
				FROM rti_calls
				WHERE session_id = $1 AND nest_level = 1
				  AND id = (
				    SELECT c.id FROM rti_calls c
				    WHERE c.session_id = $1 AND c.nest_level = 1
				    ORDER BY (SELECT count(*) FROM rti_calls ch WHERE ch.session_id = $1 AND ch.parent_id = c.id) DESC
				    LIMIT 1
				  )
				UNION ALL
				SELECT c.id, c.procedure, c.enter_line, c.exit_line, c.enter_time, c.exit_time,
				       c.elapsed_ms, c.nest_level, c.module_id, c.module_name, c.tran_count,
				       c.begin_cnt, c.ret_val, c.ret_val_context, c.parent_id, c.spid, t.depth + 1
				FROM rti_calls c
				JOIN call_tree t ON c.parent_id = t.id
				WHERE c.session_id = $1 AND ($2 = 0 OR t.depth < $2)
			)
			SELECT id, procedure, enter_line, exit_line, enter_time, exit_time,
			       elapsed_ms, nest_level, module_id, module_name, tran_count,
			       begin_cnt, ret_val, ret_val_context, parent_id, spid
			FROM call_tree LIMIT $3`,
			sessionID, maxDepth, maxTreeNodes,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load tree calls: %w", err)
	}
	defer rows.Close()

	var calls []*RTICall
	for rows.Next() {
		c, err := scanCallColumns(rows)
		if err != nil {
			return nil, err
		}
		calls = append(calls, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(calls) == 0 {
		return calls, nil
	}

	// Build parent-child relationships
	callByID := make(map[int64]*RTICall, len(calls))
	for _, c := range calls {
		callByID[c.ID] = c
	}
	for _, c := range calls {
		if c.ParentID != nil {
			if p, ok := callByID[*c.ParentID]; ok {
				p.Children = append(p.Children, c.ID)
			}
		}
	}

	return calls, nil
}

// LoadTimelineCalls загружает серверные вызовы для timeline с серверной фильтрацией и лимитом.
func LoadTimelineCalls(db *store.DB, sessionID int64, filter TimelineFilter, limit int) ([]*RTICall, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	var sb strings.Builder
	sb.WriteString(`SELECT ` + callSelectColumns + ` FROM rti_calls WHERE session_id = $1`)
	args := []interface{}{sessionID}
	argIdx := 2
	if filter.TimeFrom != nil {
		sb.WriteString(fmt.Sprintf(" AND enter_time >= $%d", argIdx))
		args = append(args, *filter.TimeFrom)
		argIdx++
	}
	if filter.TimeTo != nil {
		sb.WriteString(fmt.Sprintf(" AND enter_time <= $%d", argIdx))
		args = append(args, *filter.TimeTo)
		argIdx++
	}
	if filter.Procedure != "" {
		sb.WriteString(fmt.Sprintf(" AND procedure ILIKE $%d", argIdx))
		args = append(args, filter.Procedure)
		argIdx++
	}
	sb.WriteString(fmt.Sprintf(" ORDER BY enter_time ASC LIMIT $%d", argIdx))
	args = append(args, limit)

	rows, err := db.Query(sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to load timeline calls: %w", err)
	}
	defer rows.Close()

	var calls []*RTICall
	for rows.Next() {
		c, err := scanCallColumns(rows)
		if err != nil {
			return nil, err
		}
		calls = append(calls, c)
	}
	return calls, rows.Err()
}

// LoadTimelineClientEvents загружает клиентские события для timeline с серверной фильтрацией и лимитом.
func LoadTimelineClientEvents(db *store.DB, sessionID int64, filter TimelineFilter, limit int) ([]*RTIClientEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	var sb strings.Builder
	sb.WriteString(`SELECT id, timestamp, level, category, class_name, method_name,
		        pid, seq_no, line_no, kind, elapsed_ms, payload, server_call_id
		 FROM rti_client_events WHERE session_id = $1`)
	args := []interface{}{sessionID}
	argIdx := 2
	if filter.TimeFrom != nil {
		sb.WriteString(fmt.Sprintf(" AND timestamp >= $%d", argIdx))
		args = append(args, *filter.TimeFrom)
		argIdx++
	}
	if filter.TimeTo != nil {
		sb.WriteString(fmt.Sprintf(" AND timestamp <= $%d", argIdx))
		args = append(args, *filter.TimeTo)
		argIdx++
	}
	if filter.PID != nil && *filter.PID > 0 {
		sb.WriteString(fmt.Sprintf(" AND pid = $%d", argIdx))
		args = append(args, *filter.PID)
		argIdx++
	}
	if filter.ClassName != "" {
		sb.WriteString(fmt.Sprintf(" AND class_name ILIKE $%d", argIdx))
		args = append(args, filter.ClassName)
		argIdx++
	}
	if filter.MethodName != "" {
		sb.WriteString(fmt.Sprintf(" AND method_name ILIKE $%d", argIdx))
		args = append(args, filter.MethodName)
		argIdx++
	}
	sb.WriteString(fmt.Sprintf(" ORDER BY timestamp ASC LIMIT $%d", argIdx))
	args = append(args, limit)

	rows, err := db.Query(sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to load timeline client events: %w", err)
	}
	defer rows.Close()

	var events []*RTIClientEvent
	for rows.Next() {
		ev, err := scanClientEventColumns(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

// LoadClientEventsFiltered загружает клиентские события для client_tree с серверной фильтрацией и лимитом.
func LoadClientEventsFiltered(db *store.DB, sessionID int64, filter TimelineFilter, limit int) ([]*RTIClientEvent, error) {
	return LoadTimelineClientEvents(db, sessionID, filter, limit)
}
