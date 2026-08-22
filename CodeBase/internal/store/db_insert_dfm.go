package store

import (
	"context"
	"database/sql"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

// BatchInsertDFMComponents пакетная вставка DFM компонентов
func (db *DB) BatchInsertDFMComponents(ctx context.Context, components []*model.DFMComponent, batchSize int) error {
	if len(components) == 0 {
		return nil
	}

	if len(components) <= batchSize {
		return db.insertDFMComponentsBatch(ctx, components)
	}

	for i := 0; i < len(components); i += batchSize {
		end := i + batchSize
		if end > len(components) {
			end = len(components)
		}

		batch := components[i:end]
		if err := db.insertDFMComponentsBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertDFMComponentsBatch(ctx context.Context, components []*model.DFMComponent) error {
	if len(components) == 0 {
		return nil
	}

	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("dfm_components", "file_id", "form_id", "component_name", "component_type", "parent_name", "caption", "line_start", "line_end"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, component := range components {
			_, err := stmt.Exec(
				component.FileID,
				component.FormID,
				sanitizeUTF8String(component.ComponentName),
				sanitizeUTF8String(component.ComponentType),
				NullableString(component.ParentName),
				NullableString(component.Caption),
				component.LineStart,
				component.LineEnd,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertDFMForms пакетная вставка DFM форм
func (db *DB) BatchInsertDFMForms(ctx context.Context, forms []*model.DFMForm, batchSize int) error {
	if len(forms) == 0 {
		return nil
	}

	if len(forms) <= batchSize {
		return db.insertDFMFormsBatch(ctx, forms)
	}

	for i := 0; i < len(forms); i += batchSize {
		end := i + batchSize
		if end > len(forms) {
			end = len(forms)
		}

		batch := forms[i:end]
		if err := db.insertDFMFormsBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertDFMFormsBatch(ctx context.Context, forms []*model.DFMForm) error {
	if len(forms) == 0 {
		return nil
	}

	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("dfm_forms", "file_id", "form_name", "form_class", "caption", "line_start", "line_end"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, form := range forms {
			_, err := stmt.Exec(
				form.FileID,
				sanitizeUTF8String(form.FormName),
				sanitizeUTF8String(form.FormClass),
				NullableString(form.Caption),
				form.LineStart,
				form.LineEnd,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}
