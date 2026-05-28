package store

import (
	"database/sql"
	"encoding/json"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

func (db *DB) BatchInsertJSConstants(constants []*model.JSConstant, batchSize int) error {
	if len(constants) == 0 {
		return nil
	}
	if len(constants) <= batchSize {
		return db.insertJSConstantsBatch(constants)
	}
	for i := 0; i < len(constants); i += batchSize {
		end := i + batchSize
		if end > len(constants) {
			end = len(constants)
		}
		if err := db.insertJSConstantsBatch(constants[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertJSConstantsBatch(constants []*model.JSConstant) error {
	if len(constants) == 0 {
		return nil
	}
	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("js_constants", "file_id", "constant_name", "constant_value", "line_number"))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, constant := range constants {
			if _, err := stmt.Exec(constant.FileID, sanitizeUTF8String(constant.Name), sanitizeUTF8String(constant.Value), constant.LineNumber); err != nil {
				return err
			}
		}
		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertJSFunctions пакетная вставка JS функций
func (db *DB) BatchInsertJSFunctions(functions []*model.JSFunction, batchSize int) error {
	if len(functions) == 0 {
		return nil
	}

	if len(functions) <= batchSize {
		return db.insertJSFunctionsBatch(functions)
	}

	for i := 0; i < len(functions); i += batchSize {
		end := i + batchSize
		if end > len(functions) {
			end = len(functions)
		}

		batch := functions[i:end]
		if err := db.insertJSFunctionsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertJSFunctionsBatch(functions []*model.JSFunction) error {
	if len(functions) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("js_functions", "file_id", "function_name", "signature", "line_start", "line_end", "scenario_type", "parent_object"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, fn := range functions {
			_, err := stmt.Exec(
				fn.FileID,
				sanitizeUTF8String(fn.FunctionName),
				NullableString(fn.Signature),
				fn.LineStart,
				fn.LineEnd,
				NullableString(fn.ScenarioType),
				NullableString(fn.ParentObject),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertSymbols пакетная вставка символов
func (db *DB) BatchInsertSymbols(symbols []*model.Symbol, batchSize int) error {
	if len(symbols) == 0 {
		return nil
	}

	if len(symbols) <= batchSize {
		return db.insertSymbolsBatch(symbols)
	}

	for i := 0; i < len(symbols); i += batchSize {
		end := i + batchSize
		if end > len(symbols) {
			end = len(symbols)
		}

		batch := symbols[i:end]
		if err := db.insertSymbolsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

// insertSymbolsBatch вставляет одну пачку символов
func (db *DB) insertSymbolsBatch(symbols []*model.Symbol) error {
	if len(symbols) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("symbols", "file_id", "symbol_name", "symbol_type", "entity_type", "entity_id", "line_number", "signature"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, symbol := range symbols {
			_, err := stmt.Exec(
				symbol.FileID,
				sanitizeUTF8String(symbol.SymbolName),
				sanitizeUTF8String(symbol.SymbolType),
				sanitizeUTF8String(symbol.EntityType),
				symbol.EntityID,
				symbol.LineNumber,
				sanitizeUTF8String(symbol.Signature),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}

// BatchInsertSMFInstruments пакетная вставка SMF инструментов
func (db *DB) BatchInsertSMFInstruments(instruments []*model.SMFInstrument, batchSize int) error {
	if len(instruments) == 0 {
		return nil
	}

	if len(instruments) <= batchSize {
		return db.insertSMFInstrumentsBatch(instruments)
	}

	for i := 0; i < len(instruments); i += batchSize {
		end := i + batchSize
		if end > len(instruments) {
			end = len(instruments)
		}

		batch := instruments[i:end]
		if err := db.insertSMFInstrumentsBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) insertSMFInstrumentsBatch(instruments []*model.SMFInstrument) error {
	if len(instruments) == 0 {
		return nil
	}

	return db.withCopyInTx(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(pq.CopyIn("smf_instruments", "file_id", "instrument_name", "brief", "deal_object_id", "ds_module_id", "start_state", "scenario_type", "states", "actions", "accounts"))
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, instrument := range instruments {
			statesJSON, err := json.Marshal(instrument.States)
			if err != nil {
				return err
			}
			actionsJSON, err := json.Marshal(instrument.Actions)
			if err != nil {
				return err
			}
			accountsJSON, err := json.Marshal(instrument.Accounts)
			if err != nil {
				return err
			}

			_, err = stmt.Exec(
				instrument.FileID,
				sanitizeUTF8String(instrument.InstrumentName),
				NullableString(instrument.Brief),
				NullableInt64(instrument.DealObjectID),
				NullableInt64(instrument.DsModuleID),
				NullableString(instrument.StartState),
				NullableString(instrument.ScenarioType),
				sanitizeUTF8String(string(statesJSON)),
				sanitizeUTF8String(string(actionsJSON)),
				sanitizeUTF8String(string(accountsJSON)),
			)
			if err != nil {
				return err
			}
		}

		_, err = stmt.Exec()
		return err
	})
}
