//go:build integration

package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// useDBConfig переключает глобальный конфиг на временную БД теста.
func useDBConfig(t *testing.T, db *store.DB) {
	t.Helper()
	writeTempConfig(t, testutil.ConfigFor(t, db))
}

// useUnreachableConfig переключает конфиг на заведомо закрытый порт —
// для проверки offline-ветки openDBOptional.
func useUnreachableConfig(t *testing.T) {
	t.Helper()
	cfg := testutil.ConfigFor(t, testutil.OpenEmpty(t))
	cfg.Port = 1
	writeTempConfig(t, cfg)
}

func writeTempConfig(t *testing.T, cfg config.DBConfig) {
	t.Helper()
	data := fmt.Sprintf(`[database]
host = %q
port = %d
user = %q
password = %q
database = %q
sslmode = %q
connect_timeout = 2
`, cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode)
	path := filepath.Join(t.TempDir(), "codebase.toml")
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	prev := config.GetConfigFile()
	config.SetConfigFile(path)
	t.Cleanup(func() { config.SetConfigFile(prev) })
	if err := config.Load(); err != nil {
		t.Fatalf("load temp config: %v", err)
	}
}

// TestOpenDBReady_SchemaNotInitialized: helper cmd-слоя на пустой БД —
// типизированная ошибка схемы (3.1).
func TestOpenDBReady_SchemaNotInitialized(t *testing.T) {
	db := testutil.OpenEmpty(t)
	useDBConfig(t, db)

	got, err := openDBReady(context.Background())
	if err == nil {
		closeDB(got)
		t.Fatal("openDBReady succeeded on empty DB, want ErrSchemaNotInitialized")
	}
	if !errors.Is(err, store.ErrSchemaNotInitialized) {
		t.Fatalf("error = %v, want ErrSchemaNotInitialized", err)
	}
}

// TestOpenDBReady_SchemaUpdateRequired: устаревшая схема — ErrSchemaUpdateRequired.
func TestOpenDBReady_SchemaUpdateRequired(t *testing.T) {
	db := testutil.Open(t)
	testutil.ResetSchemaVersion(t, db)
	if _, err := db.Exec(`INSERT INTO schema_migrations (version) VALUES ('codebase_schema_v2')`); err != nil {
		t.Fatalf("insert legacy marker: %v", err)
	}
	useDBConfig(t, db)

	got, err := openDBReady(context.Background())
	if err == nil {
		closeDB(got)
		t.Fatal("openDBReady succeeded on stale schema, want ErrSchemaUpdateRequired")
	}
	if !errors.Is(err, store.ErrSchemaUpdateRequired) {
		t.Fatalf("error = %v, want ErrSchemaUpdateRequired", err)
	}
}

// TestOpenDBOptional_SchemaErrorIsHard: для rti/trc файлового режима ошибка
// схемы при доступной БД — жёсткая (не offline).
func TestOpenDBOptional_SchemaErrorIsHard(t *testing.T) {
	db := testutil.OpenEmpty(t)
	useDBConfig(t, db)

	got, err := openDBOptional(context.Background())
	if got != nil || err == nil {
		t.Fatalf("openDBOptional = (%v, %v), want (nil, schema error)", got, err)
	}
	if !errors.Is(err, store.ErrSchemaNotInitialized) {
		t.Fatalf("error = %v, want ErrSchemaNotInitialized", err)
	}
}

// TestOpenDBOptional_UnreachableDBFallsBackToNil: БД недоступна — прежний
// offline-режим: (nil, nil), сервисный слой разбирает файл без сохранения.
func TestOpenDBOptional_UnreachableDBFallsBackToNil(t *testing.T) {
	useUnreachableConfig(t)

	got, err := openDBOptional(context.Background())
	if got != nil || err != nil {
		t.Fatalf("openDBOptional = (%v, %v), want (nil, nil) offline fallback", got, err)
	}
}

// TestOpenDBReady_UnreachableDB: для dsinfo (БД обязательна) недоступная БД —
// типизированная ошибка подключения.
func TestOpenDBReady_UnreachableDB(t *testing.T) {
	useUnreachableConfig(t)

	got, err := openDBReady(context.Background())
	if got != nil || err == nil {
		t.Fatalf("openDBReady = (%v, %v), want (nil, ErrDBConnect)", got, err)
	}
	if !errors.Is(err, errs.ErrDBConnect) {
		t.Fatalf("error = %v, want ErrDBConnect", err)
	}
}

// TestSpecsvcExecute_SchemaNotInitialized: spec-команды на пустой БД —
// различимая ошибка, схема не создаётся (2.2).
func TestSpecsvcExecute_SchemaNotInitialized(t *testing.T) {
	db := testutil.OpenEmpty(t)
	useDBConfig(t, db)

	_, err := specsvcExecute(context.Background(), func(db *store.DB) (interface{}, error) {
		t.Fatal("runner must not run on empty schema")
		return nil, nil
	})
	if !errors.Is(err, store.ErrSchemaNotInitialized) {
		t.Fatalf("error = %v, want ErrSchemaNotInitialized", err)
	}

	var tables int
	if err := db.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'`).Scan(&tables); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if tables != 0 {
		t.Fatalf("tables created on empty DB: %d", tables)
	}
}

// TestExecuteStats_SchemaNotInitialized: stats на пустой БД — различимая
// ошибка без попытки создать схему (2.3, решение В3 — без деградации).
func TestExecuteStats_SchemaNotInitialized(t *testing.T) {
	db := testutil.OpenEmpty(t)
	useDBConfig(t, db)

	_, err := executeStats(context.Background())
	if !errors.Is(err, store.ErrSchemaNotInitialized) {
		t.Fatalf("error = %v, want ErrSchemaNotInitialized", err)
	}
	if !strings.Contains(err.Error(), "run codebase init") {
		t.Fatalf("error = %v, want hint «run codebase init»", err)
	}
}

// TestExecuteHealth_SchemaNotInitialized: health на пустой БД — та же
// жёсткая различимая ошибка (без частичного отчёта).
func TestExecuteHealth_SchemaNotInitialized(t *testing.T) {
	db := testutil.OpenEmpty(t)
	useDBConfig(t, db)

	_, err := executeHealth(context.Background())
	if !errors.Is(err, store.ErrSchemaNotInitialized) {
		t.Fatalf("error = %v, want ErrSchemaNotInitialized", err)
	}
}
