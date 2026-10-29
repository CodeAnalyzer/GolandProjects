package cmd

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/codebase/internal/config"
)

// TestDsInfoFlagsRegistered: подкоманды dsinfo и их флаги на месте.
func TestDsInfoFlagsRegistered(t *testing.T) {
	dsinfoFlags := map[string][]string{
		"parse":  {"name"},
		"list":   {"limit"},
		"show":   {"module", "packet", "search", "limit"},
		"delete": {},
		"use":    {},
	}
	for sub, flags := range dsinfoFlags {
		var found bool
		for _, c := range dsinfoCmd.Commands() {
			if c.Name() != sub {
				continue
			}
			found = true
			for _, flag := range flags {
				if c.Flags().Lookup(flag) == nil {
					t.Errorf("dsinfo %s: flag --%s not registered", sub, flag)
				}
			}
		}
		if !found {
			t.Errorf("dsinfo subcommand %s not found", sub)
		}
	}
	if dsinfoCmd.PersistentFlags().Lookup("json") == nil {
		t.Error("dsinfo: persistent flag --json not registered")
	}
}

func TestParseDsInfoID(t *testing.T) {
	if id, err := parseDsInfoID("42"); err != nil || id != 42 {
		t.Fatalf("parseDsInfoID(42) = %d, %v; want 42, nil", id, err)
	}
	for _, bad := range []string{"abc", "0", "-1", ""} {
		if _, err := parseDsInfoID(bad); err == nil {
			t.Fatalf("parseDsInfoID(%q) expected error", bad)
		}
	}
}

// TestDsInfoJSONEnvelopeShape: конверт соответствует общему контракту
// (success/format_version/command/count/items).
func TestDsInfoJSONEnvelopeShape(t *testing.T) {
	data, err := json.Marshal(dsinfoSuccessResponse{
		Success:       true,
		FormatVersion: "1.0",
		Command:       "dsinfo parse",
		Count:         1,
		Items:         struct{}{},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var keys map[string]interface{}
	if err := json.Unmarshal(data, &keys); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"success", "format_version", "command", "count", "items"} {
		if _, ok := keys[key]; !ok {
			t.Errorf("envelope missing key %q in %s", key, data)
		}
	}
}

// TestDsInfoJSONErrorEnvelope: в --json-режиме ошибка печатается structured
// JSON-конвертом. Конфиг задаётся явно (заведомо закрытый порт) — тест не
// зависит от глобального состояния config, оставленного другими тестами.
func TestDsInfoJSONErrorEnvelope(t *testing.T) {
	data := "[database]\nhost = \"127.0.0.1\"\nport = 1\nuser = \"postgres\"\ndatabase = \"postgres\"\nsslmode = \"disable\"\nconnect_timeout = 1\n"
	path := filepath.Join(t.TempDir(), "codebase.toml")
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	prevConfigFile := config.GetConfigFile()
	config.SetConfigFile(path)
	t.Cleanup(func() { config.SetConfigFile(prevConfigFile) })
	if err := config.Load(); err != nil {
		t.Fatalf("load temp config: %v", err)
	}

	oldJSON := dsinfoOutputJSON
	dsinfoOutputJSON = true
	defer func() { dsinfoOutputJSON = oldJSON }()

	// RunE больше не печатает конверт: он пишется один раз на root-уровне
	// (cmd/root.go writeDsinfoErrorResponse). RunE возвращает ошибку как есть.
	runErr := runDsInfoUse(dsinfoUseCmd, nil)
	if runErr == nil {
		t.Fatal("runDsInfoUse returned nil, want underlying error propagated to root")
	}
}

// TestWriteDsinfoErrorResponse: root-конверт ошибки dsinfo — форма и возврат
// исходной ошибки наверх.
func TestWriteDsinfoErrorResponse(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	oldStdout := os.Stdout
	os.Stdout = w

	sentinel := errors.New("dsinfo profile 999 not found")
	retErr := writeDsinfoErrorResponse("dsinfo use", sentinel)
	w.Close()
	os.Stdout = oldStdout

	if !errors.Is(retErr, sentinel) {
		t.Fatalf("writeDsinfoErrorResponse err = %v, want original sentinel", retErr)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(r).Decode(&body); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if body["success"] != false {
		t.Errorf("success = %v, want false", body["success"])
	}
	if body["format_version"] != "1.0" {
		t.Errorf("format_version = %v", body["format_version"])
	}
	if body["command"] != "dsinfo use" {
		t.Errorf("command = %v", body["command"])
	}
	errBody, ok := body["error"].(map[string]interface{})
	if !ok || errBody["code"] != "not_found" || errBody["message"] == "" {
		t.Errorf("error body = %v, want code=not_found+message", body["error"])
	}
}

// TestDetectDsinfoCommandName: имя команды для конверта из argv.
func TestDetectDsinfoCommandName(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"dsinfo", "use", "999", "--json"}, "dsinfo use"},
		{[]string{"dsinfo", "list", "--json"}, "dsinfo list"},
		{[]string{"dsinfo", "--json"}, "dsinfo"},
		{[]string{"dsinfo"}, "dsinfo"},
	}
	for _, tc := range cases {
		if got := detectDsinfoCommandName(tc.args); got != tc.want {
			t.Errorf("detectDsinfoCommandName(%v) = %q, want %q", tc.args, got, tc.want)
		}
	}
}
