package rti

import (
	"database/sql"
	"fmt"
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
		`INSERT INTO rti_sessions (file_path, file_size, total_calls, errors_count, max_nest_level, unparsed_lines)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		filePath, result.Summary.FileSize, result.Summary.TotalCalls,
		result.Summary.ErrorsCount, result.Summary.MaxNestLevel, result.Summary.UnparsedLines,
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

	return sessionID, nil
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
	defer tx.Rollback()

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
	defer tx.Rollback()

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
	defer tx.Rollback()

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

// ListSessions возвращает список сессий из БД.
func ListSessions(db *store.DB, limit int) ([]RTISession, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := db.Query(
		`SELECT id, file_path, file_size, parsed_at, total_calls, errors_count, max_nest_level, unparsed_lines
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
			&s.TotalCalls, &s.ErrorsCount, &maxNestLevel, &unparsedLines); err != nil {
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
		`SELECT id, file_path, file_size, parsed_at, total_calls, errors_count, max_nest_level, unparsed_lines
		 FROM rti_sessions WHERE id = $1`,
		sessionID,
	).Scan(&s.ID, &s.FilePath, &s.FileSize, &s.ParsedAt,
		&s.TotalCalls, &s.ErrorsCount, &maxNestLevel, &unparsedLines)
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

	return calls, rows.Err()
}

// GetLatestSessionID возвращает ID последней сессии.
func GetLatestSessionID(db *store.DB) (int64, error) {
	var id int64
	err := db.QueryRow(`SELECT id FROM rti_sessions ORDER BY parsed_at DESC LIMIT 1`).Scan(&id)
	return id, err
}
