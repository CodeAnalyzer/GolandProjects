package store

import (
	"database/sql"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

// BatchInsertHDefines пакетная вставка H define-ов
func (db *DB) BatchInsertHDefines(defines []*model.HDefine, batchSize int) error {
	if len(defines) == 0 {
		return nil
	}

	if len(defines) <= batchSize {
		return db.insertHDefinesBatch(defines)
	}

	for i := 0; i < len(defines); i += batchSize {
		end := i + batchSize
		if end > len(defines) {
			end = len(defines)
		}

		batch := defines[i:end]
		if err := db.insertHDefinesBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertHDefinesBatch(defines []*model.HDefine) error {
	if len(defines) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("h_files_defines", "file_id", "define_name", "define_value", "define_type", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, define := range defines {
			_, err := stmt.Exec(
				define.FileID,
				sanitizeUTF8String(define.DefineName),
				sanitizeUTF8String(define.DefineValue),
				sanitizeUTF8String(define.DefineType),
				define.LineNumber,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertAPIMacroInvocations пакетная вставка APIMacroInvocations
func (db *DB) BatchInsertAPIMacroInvocations(invocations []*model.APIMacroInvocation, batchSize int) error {
	if len(invocations) == 0 {
		return nil
	}

	if len(invocations) <= batchSize {
		return db.insertAPIMacroInvocationsBatch(invocations)
	}

	for i := 0; i < len(invocations); i += batchSize {
		end := i + batchSize
		if end > len(invocations) {
			end = len(invocations)
		}

		batch := invocations[i:end]
		if err := db.insertAPIMacroInvocationsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertAPIMacroInvocationsBatch(invocations []*model.APIMacroInvocation) error {
	if len(invocations) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("api_macro_invocations", "file_id", "procedure_name", "macro_type", "target_name", "target_kind", "line_number", "raw_text"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, invocation := range invocations {
			_, err := stmt.Exec(
				invocation.FileID,
				NullableString(invocation.ProcedureName),
				sanitizeUTF8String(invocation.MacroType),
				sanitizeUTF8String(invocation.TargetName),
				sanitizeUTF8String(invocation.TargetKind),
				invocation.LineNumber,
				NullableString(invocation.RawText),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}
