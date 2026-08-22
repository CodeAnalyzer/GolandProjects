package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

// BatchInsertSQLProcedures пакетная вставка SQL процедур
func (db *DB) BatchInsertSQLProcedures(ctx context.Context, procedures []*model.SQLProcedure, batchSize int) error {
	if len(procedures) == 0 {
		return nil
	}

	if len(procedures) <= batchSize {
		return db.insertSQLProceduresBatch(ctx, procedures)
	}

	for i := 0; i < len(procedures); i += batchSize {
		end := i + batchSize
		if end > len(procedures) {
			end = len(procedures)
		}

		batch := procedures[i:end]
		if err := db.insertSQLProceduresBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

// insertSQLProceduresBatch вставляет одну пачку SQL процедур
func (db *DB) insertSQLProceduresBatch(ctx context.Context, procedures []*model.SQLProcedure) error {
	if len(procedures) == 0 {
		return nil
	}

	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("sql_procedures", "file_id", "proc_name", "parameters", "line_start", "line_end", "body_hash"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, proc := range procedures {
			var paramsJSON interface{}
			if len(proc.Params) > 0 {
				data, err := json.Marshal(proc.Params)
				if err != nil {
					return err
				}
				paramsJSON = string(data)
			}

			_, err := stmt.Exec(
				proc.FileID,
				sanitizeUTF8String(proc.ProcName),
				sanitizeNullableJSON(paramsJSON),
				proc.LineStart,
				proc.LineEnd,
				NullableString(proc.BodyHash),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertSQLTables пакетная вставка SQL таблиц
func (db *DB) BatchInsertSQLTables(ctx context.Context, tables []*model.SQLTable, batchSize int) error {
	if len(tables) == 0 {
		return nil
	}

	if len(tables) <= batchSize {
		return db.insertSQLTablesBatch(ctx, tables)
	}

	for i := 0; i < len(tables); i += batchSize {
		end := i + batchSize
		if end > len(tables) {
			end = len(tables)
		}

		batch := tables[i:end]
		if err := db.insertSQLTablesBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

// insertSQLTablesBatch вставляет одну пачку SQL таблиц
func (db *DB) insertSQLTablesBatch(ctx context.Context, tables []*model.SQLTable) error {
	if len(tables) == 0 {
		return nil
	}

	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("sql_tables", "file_id", "table_name", "context", "is_temporary", "line_number", "column_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, table := range tables {
			_, err := stmt.Exec(
				table.FileID,
				sanitizeUTF8String(table.TableName),
				sanitizeUTF8String(table.Context),
				table.IsTemporary,
				table.LineNumber,
				NullableInt(table.ColNumber),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertSQLColumns пакетная вставка SQL колонок
func (db *DB) BatchInsertSQLColumns(ctx context.Context, columns []*model.SQLColumn, batchSize int) error {
	if len(columns) == 0 {
		return nil
	}

	if len(columns) <= batchSize {
		return db.insertSQLColumnsBatch(ctx, columns)
	}

	for i := 0; i < len(columns); i += batchSize {
		end := i + batchSize
		if end > len(columns) {
			end = len(columns)
		}

		batch := columns[i:end]
		if err := db.insertSQLColumnsBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

// insertSQLColumnsBatch вставляет одну пачку SQL колонок
func (db *DB) insertSQLColumnsBatch(ctx context.Context, columns []*model.SQLColumn) error {
	if len(columns) == 0 {
		return nil
	}

	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("sql_columns", "file_id", "table_name", "column_name", "line_number", "column_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, column := range columns {
			_, err := stmt.Exec(
				column.FileID,
				sanitizeUTF8String(column.TableName),
				sanitizeUTF8String(column.ColumnName),
				column.LineNumber,
				column.ColNumber,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertSQLColumnDefinitions пакетная вставка определений SQL колонок
func (db *DB) BatchInsertSQLColumnDefinitions(ctx context.Context, columns []*model.SQLColumnDefinition, batchSize int) error {
	if len(columns) == 0 {
		return nil
	}

	if len(columns) <= batchSize {
		return db.insertSQLColumnDefinitionsBatch(ctx, columns)
	}

	for i := 0; i < len(columns); i += batchSize {
		end := i + batchSize
		if end > len(columns) {
			end = len(columns)
		}

		batch := columns[i:end]
		if err := db.insertSQLColumnDefinitionsBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

// insertSQLColumnDefinitionsBatch вставляет одну пачку определений SQL колонок
func (db *DB) insertSQLColumnDefinitionsBatch(ctx context.Context, columns []*model.SQLColumnDefinition) error {
	if len(columns) == 0 {
		return nil
	}

	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("sql_column_definitions", "file_id", "table_name", "column_name", "data_type", "definition_kind", "line_number", "column_order"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, column := range columns {
			_, err := stmt.Exec(
				column.FileID,
				sanitizeUTF8String(column.TableName),
				sanitizeUTF8String(column.ColumnName),
				sanitizeUTF8String(column.DataType),
				sanitizeUTF8String(column.DefinitionKind),
				column.LineNumber,
				column.ColumnOrder,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertSQLIndexDefinitions пакетная вставка определений SQL индексов.
func (db *DB) BatchInsertSQLIndexDefinitions(ctx context.Context, items []*model.SQLIndexDefinition, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) <= batchSize {
		return db.insertSQLIndexDefinitionsBatch(ctx, items)
	}
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		if err := db.insertSQLIndexDefinitionsBatch(ctx, items[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertSQLIndexDefinitionsBatch(ctx context.Context, items []*model.SQLIndexDefinition) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("sql_index_definitions", "file_id", "table_name", "index_name", "index_fields", "index_type", "is_unique", "definition_kind", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.FileID, sanitizeUTF8String(item.TableName), sanitizeUTF8String(item.IndexName), NullableString(item.IndexFields), NullableString(item.IndexType), item.IsUnique, sanitizeUTF8String(item.DefinitionKind), item.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertSQLIndexDefinitionFields пакетная вставка полей SQL индексов.
func (db *DB) BatchInsertSQLIndexDefinitionFields(ctx context.Context, items []*model.SQLIndexDefinitionField, batchSize int) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) <= batchSize {
		return db.insertSQLIndexDefinitionFieldsBatch(ctx, items)
	}
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		if err := db.insertSQLIndexDefinitionFieldsBatch(ctx, items[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertSQLIndexDefinitionFieldsBatch(ctx context.Context, items []*model.SQLIndexDefinitionField) error {
	if len(items) == 0 {
		return nil
	}
	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("sql_index_definition_fields", "table_index_id", "field_name", "field_order", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, item := range items {
			if _, err := stmt.Exec(item.TableIndexID, sanitizeUTF8String(item.FieldName), item.FieldOrder, item.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertQueryFragments пакетная вставка SQL фрагментов
func (db *DB) BatchInsertQueryFragments(ctx context.Context, fragments []*model.QueryFragment, batchSize int) error {
	if len(fragments) == 0 {
		return nil
	}

	if len(fragments) <= batchSize {
		return db.insertQueryFragmentsBatch(ctx, fragments)
	}

	for i := 0; i < len(fragments); i += batchSize {
		end := i + batchSize
		if end > len(fragments) {
			end = len(fragments)
		}

		batch := fragments[i:end]
		if err := db.insertQueryFragmentsBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertQueryFragmentsBatch(ctx context.Context, fragments []*model.QueryFragment) error {
	if len(fragments) == 0 {
		return nil
	}

	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("query_fragments", "file_id", "parent_type", "parent_id", "component_name", "component_type", "query_text", "query_hash", "tables_referenced", "context", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, fragment := range fragments {
			var tablesReferencedJSON interface{}
			if len(fragment.TablesReferenced) > 0 {
				data, err := json.Marshal(fragment.TablesReferenced)
				if err != nil {
					return err
				}
				tablesReferencedJSON = string(data)
			}

			_, err := stmt.Exec(
				fragment.FileID,
				sanitizeUTF8String(fragment.ParentType),
				fragment.ParentID,
				sanitizeUTF8String(fragment.ComponentName),
				sanitizeUTF8String(fragment.ComponentType),
				sanitizeUTF8String(fragment.QueryText),
				NullableString(fragment.QueryHash),
				sanitizeNullableJSON(tablesReferencedJSON),
				sanitizeUTF8String(fragment.Context),
				fragment.LineNumber,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertRelations пакетная вставка связей между сущностями.
func (db *DB) BatchInsertRelations(ctx context.Context, relations []*model.Relation, batchSize int) error {
	if len(relations) == 0 {
		return nil
	}

	if len(relations) <= batchSize {
		return db.insertRelationsBatch(ctx, relations)
	}

	for i := 0; i < len(relations); i += batchSize {
		end := i + batchSize
		if end > len(relations) {
			end = len(relations)
		}

		batch := relations[i:end]
		if err := db.insertRelationsBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertRelationsBatch(ctx context.Context, relations []*model.Relation) error {
	if len(relations) == 0 {
		return nil
	}

	return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("relations", "source_type", "source_id", "target_type", "target_id", "relation_type", "confidence", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, relation := range relations {
			_, err := stmt.Exec(
				relation.SourceType,
				relation.SourceID,
				relation.TargetType,
				relation.TargetID,
				relation.RelationType,
				NullableString(relation.Confidence),
				relation.LineNumber,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}
