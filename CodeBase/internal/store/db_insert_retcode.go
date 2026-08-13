package store

import (
	"strconv"
	"strings"

	"github.com/codebase/internal/model"
)

// BatchInsertRetCodes пакетная вставка return code записей
func (db *DB) BatchInsertRetCodes(entries []*model.RetCodeEntry, batchSize int) error {
	if len(entries) == 0 {
		return nil
	}

	if len(entries) <= batchSize {
		return db.insertRetCodesBatch(entries)
	}

	for i := 0; i < len(entries); i += batchSize {
		end := i + batchSize
		if end > len(entries) {
			end = len(entries)
		}

		batch := entries[i:end]
		if err := db.insertRetCodesBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertRetCodesBatch(entries []*model.RetCodeEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Дедуплицируем по ret_code, оставляя последнее вхождение
	seen := make(map[int64]int, len(entries))
	for i, e := range entries {
		seen[e.RetCode] = i
	}
	unique := make([]*model.RetCodeEntry, 0, len(seen))
	for i, e := range entries {
		if seen[e.RetCode] == i {
			unique = append(unique, e)
		}
	}

	var sb strings.Builder
	sb.WriteString(`INSERT INTO ds_return_codes (file_id, ret_code, message, proc_name, module_id) VALUES `)
	args := make([]interface{}, 0, len(unique)*5)
	for i, e := range unique {
		if i > 0 {
			sb.WriteByte(',')
		}
		base := i * 5
		sb.WriteString("($")
		sb.WriteString(strconv.Itoa(base + 1))
		sb.WriteString(",$")
		sb.WriteString(strconv.Itoa(base + 2))
		sb.WriteString(",$")
		sb.WriteString(strconv.Itoa(base + 3))
		sb.WriteString(",$")
		sb.WriteString(strconv.Itoa(base + 4))
		sb.WriteString(",$")
		sb.WriteString(strconv.Itoa(base + 5))
		sb.WriteString(")")
		args = append(args,
			e.FileID,
			e.RetCode,
			sanitizeUTF8String(e.Message),
			NullableString(e.ProcName),
			e.ModuleID,
		)
	}
	sb.WriteString(` ON CONFLICT (ret_code) DO UPDATE SET file_id=EXCLUDED.file_id, message=EXCLUDED.message, proc_name=EXCLUDED.proc_name, module_id=EXCLUDED.module_id`)

	_, err := db.exec(sb.String(), args...)
	return err
}
