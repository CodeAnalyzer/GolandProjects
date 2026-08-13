package store

import (
	"context"
	"database/sql"
)

func (db *DB) withCopyInTx(fn func(tx *sql.Tx) error) (err error) {
	// В связанной транзакции (WithBatchTx) не открываем свою: все batch-вставки
	// файла идут в одной tx, commit делает внешний вызов.
	if db.boundTx != nil {
		return fn(db.boundTx)
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
			return
		}
		err = tx.Commit()
	}()
	return fn(tx)
}

// WithBatchTx выполняет fn с DB-обёрткой, у которой все batch-вставки
// (и exec) идут в одной транзакции: один COMMIT на файл вместо COMMIT
// на каждый BatchInsert*. Commit при успехе, rollback при ошибке.
func (db *DB) WithBatchTx(fn func(txdb *DB) error) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
			return
		}
		err = tx.Commit()
	}()
	return fn(&DB{DB: db.DB, boundTx: tx})
}

// exec выполняет запрос в boundTx, если она задана, иначе напрямую.
func (db *DB) exec(query string, args ...interface{}) (sql.Result, error) {
	if db.boundTx != nil {
		return db.boundTx.Exec(query, args...)
	}
	return db.DB.Exec(query, args...)
}

// Query выполняет SELECT в boundTx, если она задана, иначе напрямую.
// Это критично для Фазы 3: SELECT должен видеть незафиксированные INSERT'ы
// из той же транзакции (иначе FK-резолвы возвращают 0 → FK violations).
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if db.boundTx != nil {
		return db.boundTx.Query(query, args...)
	}
	return db.DB.Query(query, args...)
}

// QueryRow выполняет SELECT (одна строка) в boundTx, если она задана.
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	if db.boundTx != nil {
		return db.boundTx.QueryRow(query, args...)
	}
	return db.DB.QueryRow(query, args...)
}

// QueryContext выполняет SELECT с контекстом в boundTx, если она задана.
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if db.boundTx != nil {
		return db.boundTx.QueryContext(ctx, query, args...)
	}
	return db.DB.QueryContext(ctx, query, args...)
}

// QueryRowContext выполняет SELECT (одна строка) с контекстом в boundTx.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if db.boundTx != nil {
		return db.boundTx.QueryRowContext(ctx, query, args...)
	}
	return db.DB.QueryRowContext(ctx, query, args...)
}
