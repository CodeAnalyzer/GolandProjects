package store

import (
	"context"
	"strconv"
	"strings"

	"github.com/codebase/internal/model"
)

// BatchInsertMemoryTables пакетная вставка записей признака «таблица в памяти»
// из ModuleTableInMemory.lst (change memory-tables-flag). Записи привязаны к
// файлу (ON DELETE CASCADE): повторная индексация файла заменяет их состав.
func (db *DB) BatchInsertMemoryTables(ctx context.Context, records []*model.MemoryTableRecord, batchSize int) error {
	if len(records) == 0 {
		return nil
	}

	if len(records) <= batchSize {
		return db.insertMemoryTablesBatch(ctx, records)
	}

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		if err := db.insertMemoryTablesBatch(ctx, records[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertMemoryTablesBatch(ctx context.Context, records []*model.MemoryTableRecord) error {
	if len(records) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`INSERT INTO memory_tables (file_id, product_name, module_name, table_name, line_number) VALUES `)
	args := make([]interface{}, 0, len(records)*5)
	for i, r := range records {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('(')
		base := i * 5
		for j := 1; j <= 5; j++ {
			if j > 1 {
				sb.WriteByte(',')
			}
			sb.WriteByte('$')
			sb.WriteString(strconv.Itoa(base + j))
		}
		sb.WriteByte(')')
		args = append(args,
			r.FileID,
			sanitizeUTF8String(r.ProductName),
			sanitizeUTF8String(r.ModuleName),
			sanitizeUTF8String(r.TableName),
			r.LineNumber,
		)
	}

	_, err := db.ExecContext(ctx, sb.String(), args...)
	return err
}
