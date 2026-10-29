//go:build integration

package querysvc_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/query"
	"github.com/codebase/internal/querysvc"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// useTestConfig переключает глобальный конфиг на временную БД теста
// (querysvc.Execute/ExecuteWithProfile открывают БД через config.Get).
func useTestConfig(t *testing.T, db *store.DB) {
	t.Helper()
	cfg := testutil.ConfigFor(t, db)
	data := fmt.Sprintf(`[database]
host = %q
port = %d
user = %q
password = %q
database = %q
sslmode = %q
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

// TestExecute_SchemaNotInitialized: query на пустой БД — различимая ошибка
// без попытки создать схему (readonly-schema-policy).
func TestExecute_SchemaNotInitialized(t *testing.T) {
	db := testutil.OpenEmpty(t)
	useTestConfig(t, db)

	_, err := querysvc.Execute(func(q *query.Query) (interface{}, error) {
		t.Fatal("runner must not run on empty schema")
		return nil, nil
	})
	if !errors.Is(err, store.ErrSchemaNotInitialized) {
		t.Fatalf("error = %v, want ErrSchemaNotInitialized", err)
	}
	if !strings.Contains(err.Error(), "run codebase init") {
		t.Fatalf("error = %v, want hint «run codebase init»", err)
	}

	var tables int
	if err := db.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'`).Scan(&tables); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if tables != 0 {
		t.Fatalf("tables created on empty DB: %d", tables)
	}
}

// TestExecute_SchemaUpdateRequired: устаревшая схема (маркер предыдущей
// версии) — различимая ошибка обновления, схема не изменяется.
func TestExecute_SchemaUpdateRequired(t *testing.T) {
	db := testutil.Open(t)
	testutil.ResetSchemaVersion(t, db)
	if _, err := db.Exec(`INSERT INTO schema_migrations (version) VALUES ('codebase_schema_v2')`); err != nil {
		t.Fatalf("insert legacy marker: %v", err)
	}
	useTestConfig(t, db)

	_, err := querysvc.Execute(func(q *query.Query) (interface{}, error) {
		t.Fatal("runner must not run on stale schema")
		return nil, nil
	})
	if !errors.Is(err, store.ErrSchemaUpdateRequired) {
		t.Fatalf("error = %v, want ErrSchemaUpdateRequired", err)
	}
	if !strings.Contains(err.Error(), "run codebase update") {
		t.Fatalf("error = %v, want hint «run codebase update»", err)
	}
}

// TestExecuteWithProfile_SchemaNotInitialized: spec/query путь с профилем —
// та же различимая ошибка на пустой БД.
func TestExecuteWithProfile_SchemaNotInitialized(t *testing.T) {
	db := testutil.OpenEmpty(t)
	useTestConfig(t, db)

	_, err := querysvc.ExecuteWithProfile(context.Background(), nil, func(q *query.Query) (interface{}, error) {
		t.Fatal("runner must not run on empty schema")
		return nil, nil
	})
	if !errors.Is(err, store.ErrSchemaNotInitialized) {
		t.Fatalf("error = %v, want ErrSchemaNotInitialized", err)
	}
}
