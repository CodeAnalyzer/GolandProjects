package store

import (
	"database/sql"
	"encoding/json"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

// BatchInsertPASUnits пакетная вставка PAS юнитов
func (db *DB) BatchInsertPASUnits(units []*model.PASUnit, batchSize int) error {
	if len(units) == 0 {
		return nil
	}

	if len(units) <= batchSize {
		return db.insertPASUnitsBatch(units)
	}

	for i := 0; i < len(units); i += batchSize {
		end := i + batchSize
		if end > len(units) {
			end = len(units)
		}

		batch := units[i:end]
		if err := db.insertPASUnitsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

// insertPASUnitsBatch вставляет одну пачку PAS юнитов
func (db *DB) insertPASUnitsBatch(units []*model.PASUnit) error {
	if len(units) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("pas_units", "file_id", "unit_name", "interface_uses", "implementation_uses", "line_start", "line_end"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, unit := range units {
			var interfaceUsesJSON, implementationUsesJSON interface{}

			if len(unit.InterfaceUses) > 0 {
				data, err := json.Marshal(unit.InterfaceUses)
				if err != nil {
					return err
				}
				interfaceUsesJSON = string(data)
			}
			if len(unit.ImplementationUses) > 0 {
				data, err := json.Marshal(unit.ImplementationUses)
				if err != nil {
					return err
				}
				implementationUsesJSON = string(data)
			}

			_, err := stmt.Exec(
				unit.FileID,
				sanitizeUTF8String(unit.UnitName),
				sanitizeNullableJSON(interfaceUsesJSON),
				sanitizeNullableJSON(implementationUsesJSON),
				unit.LineStart,
				unit.LineEnd,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertPASClasses пакетная вставка PAS классов
func (db *DB) BatchInsertPASClasses(classes []*model.PASClass, batchSize int) error {
	if len(classes) == 0 {
		return nil
	}

	if len(classes) <= batchSize {
		return db.insertPASClassesBatch(classes)
	}

	for i := 0; i < len(classes); i += batchSize {
		end := i + batchSize
		if end > len(classes) {
			end = len(classes)
		}

		batch := classes[i:end]
		if err := db.insertPASClassesBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertPASClassesBatch(classes []*model.PASClass) error {
	if len(classes) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("pas_classes", "unit_id", "class_name", "parent_class", "dfm_form_id", "line_start", "line_end"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, class := range classes {
			_, err := stmt.Exec(
				NullableInt64(class.UnitID),
				sanitizeUTF8String(class.ClassName),
				NullableString(class.ParentClass),
				NullableInt64(class.DFMFormID),
				class.LineStart,
				class.LineEnd,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertPASMethods пакетная вставка PAS методов
func (db *DB) BatchInsertPASMethods(methods []*model.PASMethod, batchSize int) error {
	if len(methods) == 0 {
		return nil
	}

	if len(methods) <= batchSize {
		return db.insertPASMethodsBatch(methods)
	}

	for i := 0; i < len(methods); i += batchSize {
		end := i + batchSize
		if end > len(methods) {
			end = len(methods)
		}

		batch := methods[i:end]
		if err := db.insertPASMethodsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertPASMethodsBatch(methods []*model.PASMethod) error {
	if len(methods) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("pas_methods", "class_id", "unit_id", "method_name", "signature", "visibility", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, method := range methods {
			_, err := stmt.Exec(
				NullableInt64(method.ClassID),
				NullableInt64(method.UnitID),
				sanitizeUTF8String(method.MethodName),
				NullableString(method.Signature),
				NullableString(method.Visibility),
				method.LineNumber,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertPASFields пакетная вставка PAS полей
func (db *DB) BatchInsertPASFields(fields []*model.PASField, batchSize int) error {
	if len(fields) == 0 {
		return nil
	}

	if len(fields) <= batchSize {
		return db.insertPASFieldsBatch(fields)
	}

	for i := 0; i < len(fields); i += batchSize {
		end := i + batchSize
		if end > len(fields) {
			end = len(fields)
		}

		batch := fields[i:end]
		if err := db.insertPASFieldsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertPASFieldsBatch(fields []*model.PASField) error {
	if len(fields) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("pas_fields", "class_id", "field_name", "field_type", "dfm_component_id", "visibility", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, field := range fields {
			_, err := stmt.Exec(
				NullableInt64(field.ClassID),
				sanitizeUTF8String(field.FieldName),
				NullableString(field.FieldType),
				NullableInt64(field.DFMComponentID),
				NullableString(field.Visibility),
				field.LineNumber,
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}
