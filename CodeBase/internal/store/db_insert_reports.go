package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

// BatchInsertReportForms РїР°РєРµС‚РЅР°СЏ РІСЃС‚Р°РІРєР° report forms.
func (db *DB) BatchInsertReportForms(ctx context.Context, forms []*model.ReportForm, batchSize int) error {
	if len(forms) == 0 {
		return nil
	}

	if len(forms) <= batchSize {
		return db.insertReportFormsBatch(ctx, forms)
	}

	for i := 0; i < len(forms); i += batchSize {
		end := i + batchSize
		if end > len(forms) {
			end = len(forms)
		}

		batch := forms[i:end]
		if err := db.insertReportFormsBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertReportFormsBatch(ctx context.Context, forms []*model.ReportForm) error {
	if len(forms) == 0 {
		return nil
	}

	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("report_forms", "file_id", "report_name", "report_type", "form_name", "form_class", "line_start", "line_end"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, form := range forms {
			_, err := stmt.Exec(
				form.FileID,
				sanitizeUTF8String(form.ReportName),
				sanitizeUTF8String(form.ReportType),
				NullableString(form.FormName),
				NullableString(form.FormClass),
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

// BatchInsertReportFields РїР°РєРµС‚РЅР°СЏ РІСЃС‚Р°РІРєР° report fields.
func (db *DB) BatchInsertReportFields(ctx context.Context, fields []*model.ReportField, batchSize int) error {
	if len(fields) == 0 {
		return nil
	}

	if len(fields) <= batchSize {
		return db.insertReportFieldsBatch(ctx, fields)
	}

	for i := 0; i < len(fields); i += batchSize {
		end := i + batchSize
		if end > len(fields) {
			end = len(fields)
		}

		batch := fields[i:end]
		if err := db.insertReportFieldsBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertReportFieldsBatch(ctx context.Context, fields []*model.ReportField) error {
	if len(fields) == 0 {
		return nil
	}

	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("report_fields", "report_form_id", "field_name", "source_name", "format_mask", "options", "line_number", "raw_text"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, field := range fields {
			var optionsJSON interface{}
			if len(field.Options) > 0 {
				data, err := json.Marshal(field.Options)
				if err != nil {
					return err
				}
				optionsJSON = string(data)
			}

			_, err := stmt.Exec(
				field.ReportFormID,
				sanitizeUTF8String(field.FieldName),
				NullableString(field.SourceName),
				NullableString(field.FormatMask),
				sanitizeNullableJSON(optionsJSON),
				field.LineNumber,
				NullableString(field.RawText),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertReportParams РїР°РєРµС‚РЅР°СЏ РІСЃС‚Р°РІРєР° report params.
func (db *DB) BatchInsertReportParams(ctx context.Context, params []*model.ReportParam, batchSize int) error {
	if len(params) == 0 {
		return nil
	}

	if len(params) <= batchSize {
		return db.insertReportParamsBatch(ctx, params)
	}

	for i := 0; i < len(params); i += batchSize {
		end := i + batchSize
		if end > len(params) {
			end = len(params)
		}

		batch := params[i:end]
		if err := db.insertReportParamsBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertReportParamsBatch(ctx context.Context, params []*model.ReportParam) error {
	if len(params) == 0 {
		return nil
	}

	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("report_params", "report_form_id", "param_name", "param_kind", "component_type", "data_type", "lookup_form", "lookup_table", "lookup_column", "key_column", "required", "default_value", "line_number", "raw_text"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, param := range params {
			_, err := stmt.Exec(
				param.ReportFormID,
				sanitizeUTF8String(param.ParamName),
				sanitizeUTF8String(param.ParamKind),
				NullableString(param.ComponentType),
				NullableString(param.DataType),
				NullableString(param.LookupForm),
				NullableString(param.LookupTable),
				NullableString(param.LookupColumn),
				NullableString(param.KeyColumn),
				param.Required,
				NullableString(param.DefaultValue),
				param.LineNumber,
				NullableString(param.RawText),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertVBFunctions РїР°РєРµС‚РЅР°СЏ РІСЃС‚Р°РІРєР° vb functions.
func (db *DB) BatchInsertVBFunctions(ctx context.Context, functions []*model.VBFunction, batchSize int) error {
	if len(functions) == 0 {
		return nil
	}

	if len(functions) <= batchSize {
		return db.insertVBFunctionsBatch(ctx, functions)
	}

	for i := 0; i < len(functions); i += batchSize {
		end := i + batchSize
		if end > len(functions) {
			end = len(functions)
		}

		batch := functions[i:end]
		if err := db.insertVBFunctionsBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertVBFunctionsBatch(ctx context.Context, functions []*model.VBFunction) error {
	if len(functions) == 0 {
		return nil
	}

	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("vb_functions", "report_form_id", "function_name", "function_type", "signature", "body_text", "line_start", "line_end", "body_hash"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, fn := range functions {
			_, err := stmt.Exec(
				fn.ReportFormID,
				sanitizeUTF8String(fn.FunctionName),
				sanitizeUTF8String(fn.FunctionType),
				NullableString(fn.Signature),
				NullableString(fn.BodyText),
				fn.LineStart,
				fn.LineEnd,
				NullableString(fn.BodyHash),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}
