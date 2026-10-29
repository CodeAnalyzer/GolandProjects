package cmd

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// TestAnalysisDsInfoFlagRegistered — --dsinfo объявлен на группах анализа
// (query/rti/trc) и отсутствует у команд, не принимающих профиль.
func TestAnalysisDsInfoFlagRegistered(t *testing.T) {
	if queryCmd.PersistentFlags().Lookup("dsinfo") == nil {
		t.Error("query: persistent flag --dsinfo not registered")
	}
	if rtiCmd.PersistentFlags().Lookup("dsinfo") == nil {
		t.Error("rti: persistent flag --dsinfo not registered")
	}
	if trcCmd.PersistentFlags().Lookup("dsinfo") == nil {
		t.Error("trc: persistent flag --dsinfo not registered")
	}

	excluded := map[string]bool{
		"init": true, "update": true, "dsinfo": true, "health": true, "stats": true, "review": true,
	}
	for _, c := range rootCmd.Commands() {
		if !excluded[c.Name()] {
			continue
		}
		if c.Flags().Lookup("dsinfo") != nil || c.PersistentFlags().Lookup("dsinfo") != nil {
			t.Errorf("%s: flag --dsinfo must not be registered", c.Name())
		}
	}
}

// TestQueryWarningsEnvelope — warnings входят в JSON-конверт только при
// наличии (omitempty), zero-value конверт не содержит поле.
func TestQueryWarningsEnvelope(t *testing.T) {
	withWarnings := querySuccessResponse{
		Success:  true,
		Command:  "query table",
		Count:    0,
		Items:    []interface{}{},
		Warnings: []string{"линия X не проиндексирована"},
	}
	data, err := json.Marshal(withWarnings)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"warnings":["линия X не проиндексирована"]`) {
		t.Errorf("warnings not in envelope: %s", data)
	}

	withoutWarnings := querySuccessResponse{Success: true, Command: "query table", Count: 0, Items: []interface{}{}}
	data, err = json.Marshal(withoutWarnings)
	if err != nil {
		t.Fatalf("marshal without warnings: %v", err)
	}
	if strings.Contains(string(data), `"warnings"`) {
		t.Errorf("empty warnings must be omitted: %s", data)
	}
}

// TestPrintQueryWarningsHumanReadable — в человекочитаемом режиме
// предупреждения о fallback'ах линий выводятся секцией "Warnings:"; без
// предупреждений вывод пуст.
func TestPrintQueryWarningsHumanReadable(t *testing.T) {
	warnings := []string{"линия fa-prof-11-01-6 продукта fa-prof не проиндексирована, показан код ветки develop — тела могут отличаться от установленной в банке линии"}
	f, err := os.CreateTemp(t.TempDir(), "warnings-*.txt")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	defer f.Close()
	printQueryWarnings(warnings, f)
	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("read temp: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "Warnings:") || !strings.Contains(text, "  - линия fa-prof-11-01-6") {
		t.Fatalf("human-readable warnings = %q", text)
	}

	g, err := os.CreateTemp(t.TempDir(), "warnings-empty-*.txt")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	defer g.Close()
	printQueryWarnings(nil, g)
	empty, err := os.ReadFile(g.Name())
	if err != nil {
		t.Fatalf("read temp: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("warnings without profile = %q, want empty", empty)
	}
}
