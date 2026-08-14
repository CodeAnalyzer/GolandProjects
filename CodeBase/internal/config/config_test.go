package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSetConfigFileAndGetConfigFile(t *testing.T) {
	oldConfigFile := configFile
	t.Cleanup(func() { configFile = oldConfigFile })

	SetConfigFile("test.toml")
	if got := GetConfigFile(); got != "test.toml" {
		t.Fatalf("GetConfigFile() = %q, want %q", got, "test.toml")
	}
}

func TestCreateDefault(t *testing.T) {
	oldCfg := cfg
	t.Cleanup(func() { cfg = oldCfg })

	got := CreateDefault("D:/root")
	if got.RootPath != "D:/root" {
		t.Fatalf("RootPath = %q, want %q", got.RootPath, "D:/root")
	}
	if got.DB.Host != "localhost" || got.DB.Port != 5435 || got.DB.Database != "codebase" || got.DB.User != "postgres" || got.DB.SSLMode != "disable" {
		t.Fatalf("unexpected DB defaults: %+v", got.DB)
	}
	if got.Indexer.Parallel != 4 || got.Indexer.BatchSize != 100 {
		t.Fatalf("unexpected indexer defaults: %+v", got.Indexer)
	}
	if len(got.Indexer.IncludePatterns) == 0 || len(got.Indexer.ExcludePatterns) == 0 {
		t.Fatalf("expected default include and exclude patterns")
	}
	if got.Logging.CommandEnabled == nil || !*got.Logging.CommandEnabled {
		t.Fatalf("expected command logging enabled by default")
	}
	if Get() != got {
		t.Fatalf("CreateDefault must update package config")
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	oldCfg := cfg
	oldConfigFile := configFile
	t.Cleanup(func() {
		cfg = oldCfg
		configFile = oldConfigFile
	})

	path := filepath.Join(t.TempDir(), "codebase.toml")
	content := `root_path = "D:/repo"

[database]
password = "secret"

[indexer]
include_patterns = ["*.sql"]
exclude_patterns = ["*.bak"]
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	SetConfigFile(path)
	if err := Load(); err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	got := Get()
	if got.RootPath != filepath.FromSlash("D:/repo") {
		t.Fatalf("RootPath = %q, want %q", got.RootPath, filepath.FromSlash("D:/repo"))
	}
	if got.DB.Host != "localhost" || got.DB.Port != 5435 || got.DB.Database != "codebase" || got.DB.User != "postgres" || got.DB.Password != "secret" || got.DB.SSLMode != "disable" {
		t.Fatalf("unexpected DB config: %+v", got.DB)
	}
	if got.Indexer.Parallel != 4 || got.Indexer.BatchSize != 100 {
		t.Fatalf("unexpected indexer defaults: %+v", got.Indexer)
	}
	if len(got.Indexer.IncludePatterns) != 1 || got.Indexer.IncludePatterns[0] != "*.sql" {
		t.Fatalf("include patterns = %v, want [*.sql]", got.Indexer.IncludePatterns)
	}
	if got.Logging.CommandEnabled == nil || !*got.Logging.CommandEnabled {
		t.Fatalf("expected command logging default")
	}
}

func TestLoadReturnsErrorWhenConfigFileMissing(t *testing.T) {
	oldCfg := cfg
	oldConfigFile := configFile
	t.Cleanup(func() {
		cfg = oldCfg
		configFile = oldConfigFile
	})

	SetConfigFile(filepath.Join(t.TempDir(), "missing.toml"))
	if err := Load(); err == nil {
		t.Fatalf("expected error for missing config file")
	}
}

func TestSaveWritesCurrentConfig(t *testing.T) {
	oldCfg := cfg
	oldConfigFile := configFile
	t.Cleanup(func() {
		cfg = oldCfg
		configFile = oldConfigFile
	})

	cfg = CreateDefault("D:/save-root")
	cfg.DB.Password = "pass"
	path := filepath.Join(t.TempDir(), "nested", "codebase.toml")

	if err := Save(path); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	if GetConfigFile() != path {
		t.Fatalf("config file = %q, want %q", GetConfigFile(), path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read saved config: %v", err)
	}
	text := string(data)
	if !containsAll(text, []string{"root_path", "D:/save-root", "password", "pass"}) {
		t.Fatalf("saved config does not contain expected values: %s", text)
	}
}

func containsAll(text string, parts []string) bool {
	for _, part := range parts {
		if !strings.Contains(text, part) {
			return false
		}
	}
	return true
}
