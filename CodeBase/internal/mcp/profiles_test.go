package mcp

import (
	"log"
	"strings"
	"testing"
)

func TestBuildToolRegistryForProfile_Empty_AllTools(t *testing.T) {
	registry, err := buildToolRegistryForProfile(nil, "")
	if err != nil {
		t.Fatal(err)
	}
	full := buildToolRegistry(nil)
	if len(registry) != len(full) {
		t.Errorf("empty profile: got %d tools, want %d", len(registry), len(full))
	}
}

func TestBuildToolRegistryForProfile_RTI(t *testing.T) {
	registry, err := buildToolRegistryForProfile(nil, "rti")
	if err != nil {
		t.Fatal(err)
	}
	expected := mergeMaps(baseTools, rtiTools)
	if len(registry) != len(expected) {
		t.Errorf("rti profile: got %d tools, want %d", len(registry), len(expected))
	}
	for name := range registry {
		if !expected[name] {
			t.Errorf("rti profile: unexpected tool %q", name)
		}
	}
	for name := range expected {
		if _, ok := registry[name]; !ok {
			t.Errorf("rti profile: missing tool %q", name)
		}
	}
}

func TestBuildToolRegistryForProfile_Query(t *testing.T) {
	registry, err := buildToolRegistryForProfile(nil, "query")
	if err != nil {
		t.Fatal(err)
	}
	expected := mergeMaps(baseTools, queryTools)
	if len(registry) != len(expected) {
		t.Errorf("query profile: got %d tools, want %d", len(registry), len(expected))
	}
	for name := range registry {
		if !expected[name] {
			t.Errorf("query profile: unexpected tool %q", name)
		}
	}
}

func TestBuildToolRegistryForProfile_TRC(t *testing.T) {
	registry, err := buildToolRegistryForProfile(nil, "trc")
	if err != nil {
		t.Fatal(err)
	}
	expected := mergeMaps(baseTools, trcTools)
	if len(registry) != len(expected) {
		t.Errorf("trc profile: got %d tools, want %d", len(registry), len(expected))
	}
	for name := range registry {
		if !expected[name] {
			t.Errorf("trc profile: unexpected tool %q", name)
		}
	}
}

func TestBuildToolRegistryForProfile_Review(t *testing.T) {
	registry, err := buildToolRegistryForProfile(nil, "review")
	if err != nil {
		t.Fatal(err)
	}
	expected := mergeMaps(baseTools, reviewTools)
	if len(registry) != len(expected) {
		t.Errorf("review profile: got %d tools, want %d", len(registry), len(expected))
	}
	for name := range registry {
		if !expected[name] {
			t.Errorf("review profile: unexpected tool %q", name)
		}
	}
}

func TestBuildToolRegistryForProfile_Unknown_Error(t *testing.T) {
	_, err := buildToolRegistryForProfile(nil, "unknown")
	if err == nil {
		t.Fatal("expected error for unknown profile")
	}
	if !strings.Contains(err.Error(), "unknown profile") {
		t.Errorf("error should mention unknown profile, got: %v", err)
	}
}

func TestBuildToolRegistryForProfile_AllToolsCoveredByProfiles(t *testing.T) {
	full := buildToolRegistry(nil)
	covered := mergeMaps(baseTools, mergeMaps(queryTools, mergeMaps(rtiTools, mergeMaps(trcTools, reviewTools))))
	for name := range full {
		if !covered[name] {
			t.Errorf("tool %q is not present in any profile", name)
		}
	}
}

func TestLogMCPToolCall_WithProfile(t *testing.T) {
	var buf strings.Builder
	logger := log.New(&buf, "", 0)
	logMCPToolCall(logger, "rti", "codebase_rti_parse", nil, 0, nil)
	line := buf.String()
	if !strings.Contains(line, "profile=rti") {
		t.Errorf("log line should contain profile=rti, got: %s", line)
	}
	if !strings.Contains(line, "tool=codebase_rti_parse") {
		t.Errorf("log line should contain tool=codebase_rti_parse, got: %s", line)
	}
}

func TestLogMCPToolCall_EmptyProfile_All(t *testing.T) {
	var buf strings.Builder
	logger := log.New(&buf, "", 0)
	logMCPToolCall(logger, "", "codebase_query_symbol", nil, 0, nil)
	line := buf.String()
	if !strings.Contains(line, "profile=all") {
		t.Errorf("log line should contain profile=all for empty profile, got: %s", line)
	}
}
