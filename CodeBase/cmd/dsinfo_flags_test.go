package cmd

import (
	"encoding/json"
	"os"
	"testing"
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
// JSON-конвертом (без БД — service возвращает database not available).
func TestDsInfoJSONErrorEnvelope(t *testing.T) {
	oldJSON := dsinfoOutputJSON
	dsinfoOutputJSON = true
	defer func() { dsinfoOutputJSON = oldJSON }()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	oldStdout := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	runErr := runDsInfoUse(dsinfoUseCmd, nil)
	w.Close()
	os.Stdout = oldStdout
	if runErr != nil {
		t.Fatalf("runDsInfoUse: %v", runErr)
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
	if !ok || errBody["code"] == "" || errBody["message"] == "" {
		t.Errorf("error body = %v, want code+message", body["error"])
	}
}
