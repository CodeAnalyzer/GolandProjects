package h

import "testing"

func TestParseContent_ParsesDefinesMacrosIncludesAndComments(t *testing.T) {
	content := `#include <common.h>
#include "local.inc"
#define NUM_CONST 42
#define NEG_CONST -7 -- negative value
#define TEXT_MACRO some text
#define SQL_MACRO(a,b) select a, b
#define EMPTY_MACRO(a,b)
#define EMPTY_DEFINE
#define COMMENT_DEFINE 1 -- comment
`

	parser := NewParser()
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}

	if len(result.Includes) != 2 {
		t.Fatalf("includes count = %d, want 2", len(result.Includes))
	}
	if result.Includes[0].IncludePath != "common.h" || result.Includes[0].LineNumber != 1 {
		t.Fatalf("unexpected first include: %+v", result.Includes[0])
	}
	if result.Includes[1].IncludePath != "local.inc" || result.Includes[1].LineNumber != 2 {
		t.Fatalf("unexpected second include: %+v", result.Includes[1])
	}

	defines := map[string]struct {
		value      string
		defineType string
		line       int
	}{}
	for _, define := range result.Defines {
		defines[define.DefineName] = struct {
			value      string
			defineType string
			line       int
		}{value: define.DefineValue, defineType: define.DefineType, line: define.LineNumber}
	}

	expected := map[string]struct {
		value      string
		defineType string
		line       int
	}{
		"NUM_CONST":      {value: "42", defineType: "const", line: 3},
		"NEG_CONST":      {value: "-7", defineType: "const", line: 4},
		"TEXT_MACRO":     {value: "some text", defineType: "macro", line: 5},
		"SQL_MACRO":      {value: "select a, b", defineType: "macro", line: 6},
		"EMPTY_MACRO":    {value: "", defineType: "macro", line: 7},
		"EMPTY_DEFINE":   {value: "", defineType: "const", line: 8},
		"COMMENT_DEFINE": {value: "1", defineType: "const", line: 9},
	}

	if len(defines) != len(expected) {
		t.Fatalf("defines count = %d, want %d: %+v", len(defines), len(expected), defines)
	}
	for name, want := range expected {
		got, ok := defines[name]
		if !ok {
			t.Fatalf("missing define %s", name)
		}
		if got != want {
			t.Fatalf("define %s = %+v, want %+v", name, got, want)
		}
	}
}

func TestParseContent_EmptyContentReturnsEmptySlices(t *testing.T) {
	parser := NewParser()
	result, err := parser.ParseContent("")
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}
	if result.Defines == nil || result.Includes == nil || result.Errors == nil {
		t.Fatalf("expected initialized slices")
	}
	if len(result.Defines) != 0 || len(result.Includes) != 0 || len(result.Errors) != 0 {
		t.Fatalf("unexpected non-empty result: %+v", result)
	}
}
