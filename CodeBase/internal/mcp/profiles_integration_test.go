//go:build integration

package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
	"github.com/pelletier/go-toml/v2"
)

func TestProfiles_ConcurrentSubprocessInitializeAndListTools(t *testing.T) {
	runProfilesAgainstSchema(t, testutil.Open(t))
}

func TestProfiles_LegacySchemaUpdateThenConcurrentStartup(t *testing.T) {
	seed := testutil.OpenEmpty(t)
	if _, err := seed.Exec(`CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`); err != nil {
		t.Fatalf("create legacy schema_migrations: %v", err)
	}
	if _, err := seed.Exec(`INSERT INTO schema_migrations(version) VALUES ('codebase_schema_legacy')`); err != nil {
		t.Fatalf("insert legacy schema marker: %v", err)
	}
	if err := seed.InitSchemaCtx(context.Background()); err != nil {
		t.Fatalf("apply update-equivalent schema initialization: %v", err)
	}
	runProfilesAgainstSchema(t, seed)
}

func runProfilesAgainstSchema(t *testing.T, seed *store.DB) {
	t.Helper()
	cfg := testutil.ConfigFor(t, seed)
	rootPath := t.TempDir()
	cfgFile := filepath.Join(t.TempDir(), "codebase.toml")
	data, err := toml.Marshal(&config.Config{RootPath: rootPath, DB: cfg})
	if err != nil {
		t.Fatalf("marshal test config: %v", err)
	}
	if err := os.WriteFile(cfgFile, data, 0644); err != nil {
		t.Fatalf("write test config: %v", err)
	}

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	exe := filepath.Join(t.TempDir(), "codebase-profile-test")
	if runtime.GOOS == "windows" {
		exe += ".exe"
	}
	buildCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", exe, ".")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build test server: %v\n%s", err, output)
	}

	profiles := []string{"query", "rti", "trc", "review"}
	errs := make(chan error, len(profiles))
	var wg sync.WaitGroup
	for _, profile := range profiles {
		wg.Add(1)
		go func(profile string) {
			defer wg.Done()
			errs <- runProfileHandshake(exe, cfgFile, profile)
		}(profile)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
}

func runProfileHandshake(exe, cfgFile, profile string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "mcp", "--config", cfgFile, "--profile", profile)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("%s stdin: %w", profile, err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("%s stdout: %w", profile, err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("%s start: %w", profile, err)
	}
	waited := false
	defer func() {
		if waited {
			return
		}
		_ = stdin.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()

	scanner := bufio.NewScanner(stdout)
	readResponse := func(id int) error {
		for scanner.Scan() {
			var response struct {
				ID     json.RawMessage `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  json.RawMessage `json:"error"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &response); err != nil {
				return fmt.Errorf("%s invalid JSON response: %w", profile, err)
			}
			var responseID int
			if err := json.Unmarshal(response.ID, &responseID); err != nil || responseID != id {
				continue
			}
			if len(response.Error) != 0 && string(response.Error) != "null" {
				return fmt.Errorf("%s request %d returned error: %s", profile, id, response.Error)
			}
			if len(response.Result) == 0 || string(response.Result) == "null" {
				return fmt.Errorf("%s request %d returned no result", profile, id)
			}
			return nil
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("%s read response: %w", profile, err)
		}
		return fmt.Errorf("%s connection closed before response %d", profile, id)
	}

	initialize := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"codebase-test","version":"test"}}}` + "\n"
	if _, err := fmt.Fprint(stdin, initialize); err != nil {
		return fmt.Errorf("%s initialize write: %w", profile, err)
	}
	if err := readResponse(1); err != nil {
		return err
	}
	if _, err := fmt.Fprint(stdin, `{"jsonrpc":"2.0","method":"notifications/initialized"}`+"\n"); err != nil {
		return fmt.Errorf("%s initialized write: %w", profile, err)
	}
	if _, err := fmt.Fprint(stdin, `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`+"\n"); err != nil {
		return fmt.Errorf("%s tools/list write: %w", profile, err)
	}
	if err := readResponse(2); err != nil {
		return err
	}
	_ = stdin.Close()
	waitErr := cmd.Wait()
	waited = true
	if waitErr != nil && ctx.Err() == nil {
		return fmt.Errorf("%s server exit: %w", profile, waitErr)
	}
	return nil
}
