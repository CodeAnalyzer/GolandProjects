package store

import (
	"context"
	"database/sql"
)

func (db *DB) withCopyInTxCtx(ctx context.Context, fn func(tx *sql.Tx) error) (err error) {
	// В связанной транзакции (WithBatchTx) не открываем свою: все batch-вставки
	// файла идут в одной tx, commit делает внешний вызов.
	if db.boundTx != nil {
		return fn(db.boundTx)
	}
	tx, err := db.BeginTx(ctx, nil)
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


// WithBatchTxCtx выполняет fn с DB-обёрткой, у которой все batch-вставки
// (и exec) идут в одной транзакции: один COMMIT на файл вместо COMMIT
// на каждый BatchInsert*. Commit при успехе, rollback при ошибке.
func (db *DB) WithBatchTxCtx(ctx context.Context, fn func(txdb *DB) error) (err error) {
	tx, err := db.BeginTx(ctx, nil)
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

// WithBatchTx — deprecated thin wrapper, использует context.Background().
func (db *DB) WithBatchTx(fn func(txdb *DB) error) (err error) {
	return db.WithBatchTxCtx(context.Background(), fn)
}

// ExecContext выполняет запрос в boundTx, если она задана, иначе напрямую.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if db.boundTx != nil {
		return db.boundTx.ExecContext(ctx, query, args...)
	}
	return db.DB.ExecContext(ctx, query, args...)
}


// Query выполняет SELECT в boundTx, если она задана, иначе напрямую.
// Это критично для Фазы 3: SELECT должен видеть незафиксированные INSERT'ы
// из той же транзакции (иначе FK-резолвы возвращают 0 → FK violations).
// Deprecated: использовать QueryContext с ctx.
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if db.boundTx != nil {
		return db.boundTx.Query(query, args...)
	}
	return db.DB.Query(query, args...)
}

// QueryRow выполняет SELECT (одна строка) в boundTx, если она задана.
// Deprecated: использовать QueryRowContext с ctx.
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
