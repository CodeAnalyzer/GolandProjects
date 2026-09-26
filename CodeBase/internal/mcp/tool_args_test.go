package mcp

import "testing"

func TestFormatToolArgs_AllArgsSorted(t *testing.T) {
	got := formatToolArgs(map[string]interface{}{
		"name": "MyProc",
		"type": "sql_procedure",
		"like": false,
	})
	want := "like:false,name:MyProc,type:sql_procedure"
	if got != want {
		t.Fatalf("formatToolArgs = %q, want %q", got, want)
	}
}

func TestFormatToolArgs_MasksPrivateArgs(t *testing.T) {
	got := formatToolArgs(map[string]interface{}{
		"text":  "select * from t",
		"sql":   "drop table t",
		"limit": 10,
	})
	want := "limit:10,sql:***,text:***"
	if got != want {
		t.Fatalf("formatToolArgs = %q, want %q", got, want)
	}
}

func TestFormatToolArgs_Empty(t *testing.T) {
	if got := formatToolArgs(nil); got != "-" {
		t.Fatalf("formatToolArgs(nil) = %q, want -", got)
	}
	if got := formatToolArgs(map[string]interface{}{}); got != "-" {
		t.Fatalf("formatToolArgs(empty) = %q, want -", got)
	}
}
