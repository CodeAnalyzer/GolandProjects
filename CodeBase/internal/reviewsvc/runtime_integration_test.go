//go:build integration

package reviewsvc_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/review"
	"github.com/codebase/internal/reviewsvc"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// TestExecuteCtx_SchemaNotInitialized: review на пустой БД — различимая ошибка
// без попытки создать схему (readonly-schema-policy).
func TestExecuteCtx_SchemaNotInitialized(t *testing.T) {
	db := testutil.OpenEmpty(t)

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

	_, err := reviewsvc.ExecuteCtx(context.Background(), "somefile.sql", review.Options{}, nil)
	if !errors.Is(err, store.ErrSchemaNotInitialized) {
		t.Fatalf("error = %v, want ErrSchemaNotInitialized", err)
	}
}
