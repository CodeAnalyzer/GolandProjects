package store

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/codebase/internal/config"
)

// DB обёртка над sql.DB
type DB struct {
	*sql.DB
}

// Stats статистика индекса
type Stats struct {
	TotalFiles         int
	SQLFiles           int
	HFiles             int
	PASFiles           int
	INCFiles           int
	JSFiles            int
	XMLFiles           int
	SMFFiles           int
	DFMFiles           int
	TPRFiles           int
	RPTFiles           int
	Procedures         int
	Tables             int
	Columns            int
	Units              int
	Classes            int
	Methods            int
	Functions          int
	JSFunctions        int
	SMFInstruments     int
	Forms              int
	DFMQueries         int
	Defines            int
	Relations          int
	QueryFragments     int
	ReportForms        int
	ReportFields       int
	ReportParams       int
	VBFunctions        int
	APIBusinessObjects int
	APIContracts       int
	APIContractParams  int
	APIContractTables  int
	APIContractFields  int
	APIBusinessParams  int
	APIBusinessTables  int
	SQLTableIndexes    int
	APITableIndexes    int
	APIMacros          int
	Errors             int
	PASFields          int
	LastScanID         int64
	LastScanStarted    time.Time
	LastScanFinished   time.Time
	LastScanStatus     string
}

// NewDB создаёт подключение к БД и создаёт её если не существует
func NewDB(cfg config.DBConfig) (*DB, error) {
	// Подключение в два шага нужно потому, что целевая БД может ещё не существовать:
	// сначала идём в postgres/system database, затем в рабочую базу CodeBase.
	// Сначала подключаемся к default database для создания целевой БД
	dsnDefault := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=postgres sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.SSLMode,
	)

	dbDefault, err := sql.Open("postgres", dsnDefault)
	if err != nil {
		return nil, fmt.Errorf("failed to open default database: %w", err)
	}
	defer dbDefault.Close()

	// Проверяем подключение
	if err := dbDefault.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping default database: %w", err)
	}

	// Создаём БД если не существует
	if err := createDatabaseIfNotExists(dbDefault, cfg.Database); err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	// Теперь подключаемся к целевой БД
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Финальный ping подтверждает, что рабочая БД доступна уже после возможного создания.
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Пул соединений настраивается консервативно: CLI-процесс короткоживущий,
	// но indexer может параллельно держать несколько запросов.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	return &DB{db}, nil
}

// createDatabaseIfNotExists создаёт БД если она не существует
func createDatabaseIfNotExists(db *sql.DB, dbName string) error {
	// Проверяем существование БД
	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM pg_database WHERE datname = $1
		)
	`, dbName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check database existence: %w", err)
	}

	if !exists {
		// Создаём БД
		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName))
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
	}

	return nil
}
