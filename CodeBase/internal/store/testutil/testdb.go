package testutil

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store"
	_ "github.com/lib/pq"
	"github.com/pelletier/go-toml/v2"
)

const envTestDSN = "CODEBASE_TEST_DSN"

// Open создаёт отдельную БД codebase_test_<pid>_<nano>, вызывает InitSchema
// и удаляет её в t.Cleanup. При недоступном Postgres делает t.Skip.
func Open(t *testing.T) *store.DB {
	t.Helper()
	db := OpenEmptyHandles(t, 1)[0]
	if err := db.InitSchema(); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	return db
}

// OpenEmpty как Open, но без InitSchema — для тестов миграции колонок.
func OpenEmpty(t *testing.T) *store.DB {
	t.Helper()
	return OpenEmptyHandles(t, 1)[0]
}

// ConfigFor returns connection settings for the temporary database behind db.
func ConfigFor(t *testing.T, db *store.DB) config.DBConfig {
	t.Helper()
	cfg := resolveAdminConfig(t)
	if err := db.QueryRow(`SELECT current_database()`).Scan(&cfg.Database); err != nil {
		t.Fatalf("read temporary database name: %v", err)
	}
	return cfg
}

// OpenEmptyHandles creates independent handles connected to one temporary database.
func OpenEmptyHandles(t *testing.T, count int) []*store.DB {
	t.Helper()
	if count <= 0 {
		t.Fatal("count must be positive")
	}
	adminCfg := resolveAdminConfig(t)
	adminDSN := store.FormatDSN(adminCfg)
	admin, err := sql.Open("postgres", adminDSN)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	if err := admin.Ping(); err != nil {
		_ = admin.Close()
		t.Skipf("postgres unavailable: %v", err)
	}

	name := fmt.Sprintf("codebase_test_%d_%d", os.Getpid(), time.Now().UnixNano())
	if _, err := admin.Exec(fmt.Sprintf(`CREATE DATABASE "%s"`, name)); err != nil {
		_ = admin.Close()
		t.Fatalf("create test database %s: %v", name, err)
	}

	testCfg := adminCfg
	testCfg.Database = name
	handles := make([]*store.DB, count)
	for i := range handles {
		handles[i], err = store.NewDB(testCfg)
		if err != nil {
			for _, db := range handles[:i] {
				_ = db.Close()
			}
			_, _ = admin.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS "%s" WITH (FORCE)`, name))
			_ = admin.Close()
			t.Fatalf("open test database %s: %v", name, err)
		}
	}

	t.Cleanup(func() {
		for _, db := range handles {
			_ = db.Close()
		}
		_, _ = admin.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS "%s" WITH (FORCE)`, name))
		_ = admin.Close()
	})
	return handles
}

func resolveAdminConfig(t *testing.T) config.DBConfig {
	t.Helper()
	if dsn := strings.TrimSpace(os.Getenv(envTestDSN)); dsn != "" {
		cfg, err := parsePostgresDSN(dsn)
		if err != nil {
			t.Fatalf("invalid %s: %v", envTestDSN, err)
		}
		cfg.Database = "postgres"
		if cfg.ConnectTimeout <= 0 {
			cfg.ConnectTimeout = 5
		}
		return cfg
	}

	cfgPath := findConfigFile()
	if cfgPath == "" {
		return config.DBConfig{
			Host:           "localhost",
			Port:           5435,
			Database:       "postgres",
			User:           "postgres",
			Password:       "123456",
			SSLMode:        "disable",
			ConnectTimeout: 5,
		}
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config %s: %v", cfgPath, err)
	}
	var file struct {
		DB config.DBConfig `toml:"database"`
	}
	if err := toml.Unmarshal(data, &file); err != nil {
		t.Fatalf("parse config %s: %v", cfgPath, err)
	}
	cfg := file.DB
	if cfg.Host == "" {
		cfg.Host = "localhost"
	}
	if cfg.Port == 0 {
		cfg.Port = 5435
	}
	if cfg.User == "" {
		cfg.User = "postgres"
	}
	if cfg.SSLMode == "" {
		cfg.SSLMode = "disable"
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5
	}
	cfg.Database = "postgres"
	return cfg
}

func findConfigFile() string {
	if p := strings.TrimSpace(os.Getenv("CODEBASE_CONFIG")); p != "" {
		return p
	}
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for i := 0; i < 6; i++ {
		candidate := filepath.Join(dir, "codebase.toml")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

func parsePostgresDSN(dsn string) (config.DBConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return config.DBConfig{}, err
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return config.DBConfig{}, fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	cfg := config.DBConfig{
		Host:           u.Hostname(),
		Database:       strings.TrimPrefix(u.Path, "/"),
		User:           u.User.Username(),
		SSLMode:        u.Query().Get("sslmode"),
		ConnectTimeout: 5,
	}
	if pwd, ok := u.User.Password(); ok {
		cfg.Password = pwd
	}
	if port := u.Port(); port != "" {
		p, err := strconv.Atoi(port)
		if err != nil {
			return config.DBConfig{}, fmt.Errorf("invalid port %q", port)
		}
		cfg.Port = p
	}
	if cfg.Host == "" {
		cfg.Host = "localhost"
	}
	if cfg.Port == 0 {
		cfg.Port = 5432
	}
	if cfg.SSLMode == "" {
		cfg.SSLMode = "disable"
	}
	return cfg, nil
}
