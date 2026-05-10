package cmd

import (
	"errors"
	"testing"

	"github.com/codebase/internal/config"
)

func TestSingleLineError(t *testing.T) {
	if got := singleLineError(nil); got != "" {
		t.Fatalf("nil error = %q, want empty", got)
	}
	if got := singleLineError(errors.New("first line\n second\tline")); got != "first line second line" {
		t.Fatalf("singleLineError = %q", got)
	}
}

func TestMachineReadableModes(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		queryJSON  bool
		statsJSON  bool
		healthJSON bool
		machine    bool
		banner     bool
	}{
		{name: "query json", args: []string{"query", "symbol", "--json"}, queryJSON: true, machine: true, banner: false},
		{name: "query ndjson", args: []string{"query", "symbol", "--ndjson"}, queryJSON: true, machine: true, banner: false},
		{name: "query summary", args: []string{"query", "symbol", "--summary"}, queryJSON: true, machine: true, banner: false},
		{name: "stats json", args: []string{"stats", "--json"}, statsJSON: true, machine: true, banner: false},
		{name: "health json", args: []string{"health", "--json"}, healthJSON: true, machine: true, banner: false},
		{name: "text mode", args: []string{"stats"}, banner: true},
		{name: "global json suppresses banner", args: []string{"--json"}, banner: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isQueryJSONMode(tt.args); got != tt.queryJSON {
				t.Fatalf("isQueryJSONMode = %v, want %v", got, tt.queryJSON)
			}
			if got := isStatsJSONMode(tt.args); got != tt.statsJSON {
				t.Fatalf("isStatsJSONMode = %v, want %v", got, tt.statsJSON)
			}
			if got := isHealthJSONMode(tt.args); got != tt.healthJSON {
				t.Fatalf("isHealthJSONMode = %v, want %v", got, tt.healthJSON)
			}
			if got := isMachineReadableMode(tt.args); got != tt.machine {
				t.Fatalf("isMachineReadableMode = %v, want %v", got, tt.machine)
			}
			if got := shouldPrintBanner(tt.args); got != tt.banner {
				t.Fatalf("shouldPrintBanner = %v, want %v", got, tt.banner)
			}
		})
	}
}

func TestDetectQueryCommandName(t *testing.T) {
	tests := []struct {
		args []string
		want string
	}{
		{args: nil, want: "query"},
		{args: []string{"stats"}, want: "query"},
		{args: []string{"query"}, want: "query"},
		{args: []string{"query", "--json"}, want: "query"},
		{args: []string{"query", "--limit", "10", "symbol"}, want: "query"},
		{args: []string{"query", "symbol"}, want: "query symbol"},
		{args: []string{"query", "relations"}, want: "query relations"},
	}
	for _, tt := range tests {
		if got := detectQueryCommandName(tt.args); got != tt.want {
			t.Fatalf("detectQueryCommandName(%v) = %q, want %q", tt.args, got, tt.want)
		}
	}
}

func TestIsCommandLoggingEnabled(t *testing.T) {
	oldCfgFile := config.GetConfigFile()
	t.Cleanup(func() { config.SetConfigFile(oldCfgFile) })

	if got := isCommandLoggingEnabled(); !got {
		t.Fatalf("nil config should enable command logging")
	}

	cfg := config.CreateDefault("root")
	disabled := false
	cfg.Logging.CommandEnabled = &disabled
	if got := isCommandLoggingEnabled(); got {
		t.Fatalf("disabled config should disable command logging")
	}

	enabled := true
	cfg.Logging.CommandEnabled = &enabled
	if got := isCommandLoggingEnabled(); !got {
		t.Fatalf("enabled config should enable command logging")
	}
}
