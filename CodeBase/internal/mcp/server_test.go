package mcp

import (
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/codebase/internal/config"
)

func TestOptionalInt(t *testing.T) {
	valid := []interface{}{int(7), int64(7), float64(7)}
	for _, value := range valid {
		if got, err := optionalInt(map[string]interface{}{"n": value}, "n"); err != nil || got != 7 {
			t.Errorf("optionalInt(%T) = %d, %v", value, got, err)
		}
	}
	invalid := []interface{}{"7", 1.5, math.NaN(), math.Inf(1), struct{}{}}
	for _, value := range invalid {
		if _, err := optionalInt(map[string]interface{}{"n": value}, "n"); err == nil {
			t.Errorf("optionalInt(%T) accepted invalid value", value)
		}
	}
	if strconv.IntSize == 32 {
		if _, err := optionalInt(map[string]interface{}{"n": float64(1 << 32)}, "n"); err == nil {
			t.Fatal("expected int32 overflow")
		}
	}
}

func TestOptionalInt64(t *testing.T) {
	for _, value := range []interface{}{int(7), int64(7), float64(7)} {
		if got, err := optionalInt64(map[string]interface{}{"n": value}, "n"); err != nil || got != 7 {
			t.Errorf("optionalInt64(%T) = %d, %v", value, got, err)
		}
	}
	for _, value := range []interface{}{"7", 1.5, math.NaN(), math.Inf(1), struct{}{}} {
		if _, err := optionalInt64(map[string]interface{}{"n": value}, "n"); err == nil {
			t.Errorf("optionalInt64(%T) accepted invalid value", value)
		}
	}
	if _, err := optionalInt64(map[string]interface{}{"n": float64(1 << 63)}, "n"); err == nil {
		t.Fatal("expected int64 overflow")
	}
}

func TestTRCHandlersRejectInvalidOptionalArguments(t *testing.T) {
	tests := []struct{ tool, key string }{
		{"codebase_trc_list", "limit"},
		{"codebase_trc_summary", "session_id"}, {"codebase_trc_summary", "file_path"},
		{"codebase_trc_events", "limit"}, {"codebase_trc_events", "spid"}, {"codebase_trc_events", "procedure"}, {"codebase_trc_events", "event_name"}, {"codebase_trc_events", "session_id"}, {"codebase_trc_events", "file_path"},
		{"codebase_trc_events", "spids"}, {"codebase_trc_events", "event_names"}, {"codebase_trc_events", "time_from"}, {"codebase_trc_events", "time_to"}, {"codebase_trc_events", "min_duration_ms"}, {"codebase_trc_events", "after_id"}, {"codebase_trc_events", "format"},
		{"codebase_trc_procedures", "session_id"}, {"codebase_trc_procedures", "file_path"},
		{"codebase_trc_procedures", "spids"}, {"codebase_trc_procedures", "event_names"}, {"codebase_trc_procedures", "top"}, {"codebase_trc_procedures", "sort_by"}, {"codebase_trc_procedures", "group_by_spid"},
		{"codebase_trc_compare_procedures", "focus_spid"}, {"codebase_trc_compare_procedures", "compare_spids"}, {"codebase_trc_compare_procedures", "event_names"}, {"codebase_trc_compare_procedures", "top"}, {"codebase_trc_compare_procedures", "sort_by"}, {"codebase_trc_compare_procedures", "session_id"}, {"codebase_trc_compare_procedures", "file_path"},
		{"codebase_trc_spids", "spids"}, {"codebase_trc_spids", "time_from"}, {"codebase_trc_spids", "time_to"}, {"codebase_trc_spids", "sort_by"}, {"codebase_trc_spids", "limit"}, {"codebase_trc_spids", "session_id"}, {"codebase_trc_spids", "file_path"},
		{"codebase_trc_tree", "max_depth"}, {"codebase_trc_tree", "limit"}, {"codebase_trc_tree", "spid"}, {"codebase_trc_tree", "session_id"}, {"codebase_trc_tree", "file_path"}, {"codebase_trc_tree", "procedure"},
		{"codebase_trc_slow", "threshold_ms"}, {"codebase_trc_slow", "limit"}, {"codebase_trc_slow", "session_id"}, {"codebase_trc_slow", "file_path"},
		{"codebase_trc_errors", "limit"}, {"codebase_trc_errors", "session_id"}, {"codebase_trc_errors", "file_path"},
		{"codebase_trc_delete", "session_id"}, {"codebase_trc_prune", "keep_last"},
	}
	for _, tt := range tests {
		t.Run(tt.tool+"/"+tt.key, func(t *testing.T) {
			_, err := toolRegistry[tt.tool].Handler(context.Background(), map[string]interface{}{tt.key: struct{}{}})
			if err == nil || !strings.Contains(err.Error(), tt.key) {
				t.Fatalf("error = %v, want key %q", err, tt.key)
			}
		})
	}
}

func TestToolRegistryContainsPing(t *testing.T) {
	tool, ok := toolRegistry["codebase_ping"]
	if !ok {
		t.Fatal("codebase_ping not in toolRegistry")
	}
	if tool.Definition.Name != "codebase_ping" {
		t.Fatalf("unexpected name: %s", tool.Definition.Name)
	}
}

func TestToolsListIncludesPing(t *testing.T) {
	list := tools()
	found := false
	for _, tool := range list {
		if tool.Name == "codebase_ping" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("codebase_ping not in tools()")
	}
}

func TestPingHandlerReturnsOK(t *testing.T) {
	tool, ok := toolRegistry["codebase_ping"]
	if !ok {
		t.Fatal("codebase_ping not in toolRegistry")
	}
	result, err := tool.Handler(context.Background(), map[string]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("unexpected result type: %T", result)
	}
	if v, _ := m["ok"].(bool); !v {
		t.Fatal("expected ok=true in ping response")
	}
}

func TestUnknownToolNotInRegistry(t *testing.T) {
	if _, ok := toolRegistry["codebase_unknown"]; ok {
		t.Fatal("unexpected tool codebase_unknown found in registry")
	}
}

func TestDecodeSDKToolArgsNil(t *testing.T) {
	args, err := decodeSDKToolArgs(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args == nil {
		t.Fatal("expected non-nil args map")
	}
}

func TestDecodeSDKToolArgsValid(t *testing.T) {
	raw, _ := json.Marshal(map[string]interface{}{"name": "foo"})
	req := &mcpsdk.CallToolRequest{}
	req.Params = &mcpsdk.CallToolParamsRaw{Arguments: raw}
	args, err := decodeSDKToolArgs(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args["name"] != "foo" {
		t.Fatalf("unexpected args: %v", args)
	}
}

func TestRTIToolsInRegistry(t *testing.T) {
	expected := []string{
		"codebase_rti_parse",
		"codebase_rti_list",
		"codebase_rti_summary",
		"codebase_rti_tree",
		"codebase_rti_errors",
		"codebase_rti_slow",
		"codebase_rti_details",
		"codebase_rti_delete",
		"codebase_rti_prune",
	}
	for _, name := range expected {
		if _, ok := toolRegistry[name]; !ok {
			t.Errorf("tool %s not in registry", name)
		}
	}
}

func TestRTIToolsListIncludesAll(t *testing.T) {
	list := tools()
	got := make(map[string]bool)
	for _, tool := range list {
		got[tool.Name] = true
	}
	expected := []string{
		"codebase_rti_parse",
		"codebase_rti_list",
		"codebase_rti_summary",
		"codebase_rti_tree",
		"codebase_rti_errors",
		"codebase_rti_slow",
		"codebase_rti_details",
		"codebase_rti_delete",
		"codebase_rti_prune",
	}
	for _, name := range expected {
		if !got[name] {
			t.Errorf("tool %s not in tools()", name)
		}
	}
}

func TestRTIParseHandlerRequiresFilePath(t *testing.T) {
	tool, ok := toolRegistry["codebase_rti_parse"]
	if !ok {
		t.Fatal("codebase_rti_parse not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing file_path")
	}
}

func TestRTISlowHandlerRequiresSessionOrFile(t *testing.T) {
	tool, ok := toolRegistry["codebase_rti_slow"]
	if !ok {
		t.Fatal("codebase_rti_slow not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing session_id and file_path")
	}
}

func TestRTIDetailsHandlerRequiresProcedure(t *testing.T) {
	tool, ok := toolRegistry["codebase_rti_details"]
	if !ok {
		t.Fatal("codebase_rti_details not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing procedure")
	}
}

func TestRetCodeToolInRegistry(t *testing.T) {
	if _, ok := toolRegistry["codebase_query_retcode"]; !ok {
		t.Fatal("codebase_query_retcode not in toolRegistry")
	}
}

func TestRetCodeHandlerRequiresArg(t *testing.T) {
	tool, ok := toolRegistry["codebase_query_retcode"]
	if !ok {
		t.Fatal("codebase_query_retcode not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{})
	// Without DB, runQueryOpt returns "database not available"
	if err == nil {
		t.Fatal("expected error for no args without DB")
	}
}

func TestReadMoreToolInRegistry(t *testing.T) {
	if _, ok := toolRegistry["codebase_read_more"]; !ok {
		t.Fatal("codebase_read_more not in toolRegistry")
	}
}

func TestReadMoreInToolsList(t *testing.T) {
	list := tools()
	for _, tool := range list {
		if tool.Name == "codebase_read_more" {
			return
		}
	}
	t.Fatal("codebase_read_more not in tools()")
}

func TestReadMoreHandlerRequiresContinuationID(t *testing.T) {
	tool, ok := toolRegistry["codebase_read_more"]
	if !ok {
		t.Fatal("codebase_read_more not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing continuation_id")
	}
}

func TestReadMoreHandlerUnknownID(t *testing.T) {
	tool, ok := toolRegistry["codebase_read_more"]
	if !ok {
		t.Fatal("codebase_read_more not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"continuation_id": "does-not-exist",
		"chunk":           float64(2),
	})
	if err == nil {
		t.Fatal("expected error for unknown continuation_id")
	}
}

func TestReadMoreHandlerRequiresChunk(t *testing.T) {
	tool, ok := toolRegistry["codebase_read_more"]
	if !ok {
		t.Fatal("codebase_read_more not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"continuation_id": "abc123",
	})
	if err == nil {
		t.Fatal("expected error when chunk is missing")
	}
}

func TestReadMoreHandlerChunkWrongType(t *testing.T) {
	tool, ok := toolRegistry["codebase_read_more"]
	if !ok {
		t.Fatal("codebase_read_more not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"continuation_id": "abc123",
		"chunk":           "abc",
	})
	if err == nil {
		t.Fatal("expected error for non-integer chunk")
	}
}

func TestRTIPruneHandlerRejectsNegativeKeepLast(t *testing.T) {
	tool, ok := toolRegistry["codebase_rti_prune"]
	if !ok {
		t.Fatal("codebase_rti_prune not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"keep_last": float64(-1),
	})
	if err == nil {
		t.Fatal("expected error for negative keep_last")
	}
}

func TestRTIPruneHandlerAcceptsZeroKeepLast(t *testing.T) {
	tool, ok := toolRegistry["codebase_rti_prune"]
	if !ok {
		t.Fatal("codebase_rti_prune not in toolRegistry")
	}
	// keep_last=0 is now valid (TRUNCATE all). Without DB, it will fail
	// with "database not available", but NOT with "keep_last must be positive".
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"keep_last": float64(0),
	})
	if err != nil && err.Error() == "keep_last must be >= 0" {
		t.Fatalf("keep_last=0 should be accepted, got: %v", err)
	}
}

func TestTRCToolsInRegistry(t *testing.T) {
	expected := []string{
		"codebase_trc_parse",
		"codebase_trc_list",
		"codebase_trc_summary",
		"codebase_trc_tree",
		"codebase_trc_errors",
		"codebase_trc_slow",
		"codebase_trc_events",
		"codebase_trc_procedures",
		"codebase_trc_compare_procedures",
		"codebase_trc_spids",
		"codebase_trc_delete",
		"codebase_trc_prune",
	}
	for _, name := range expected {
		if _, ok := toolRegistry[name]; !ok {
			t.Errorf("tool %s not in registry", name)
		}
	}
}

func TestTRCPruneHandlerRejectsNegativeKeepLast(t *testing.T) {
	tool, ok := toolRegistry["codebase_trc_prune"]
	if !ok {
		t.Fatal("codebase_trc_prune not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"keep_last": float64(-1),
	})
	if err == nil {
		t.Fatal("expected error for negative keep_last")
	}
}

func TestTRCPruneHandlerAcceptsZeroKeepLast(t *testing.T) {
	tool, ok := toolRegistry["codebase_trc_prune"]
	if !ok {
		t.Fatal("codebase_trc_prune not in toolRegistry")
	}
	// keep_last=0 is now valid (TRUNCATE all). Without DB, it will fail
	// with "database not available", but NOT with "keep_last must be positive".
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"keep_last": float64(0),
	})
	if err != nil && err.Error() == "keep_last must be >= 0" {
		t.Fatalf("keep_last=0 should be accepted, got: %v", err)
	}
}

func TestOptionalIntSlice(t *testing.T) {
	got, err := optionalIntSlice(map[string]interface{}{"spids": []interface{}{float64(728), float64(700)}}, "spids")
	if err != nil || len(got) != 2 || got[0] != 728 || got[1] != 700 {
		t.Fatalf("got %v, %v", got, err)
	}
	if v, err := optionalIntSlice(map[string]interface{}{}, "spids"); err != nil || v != nil {
		t.Fatalf("absent: got %v, %v", v, err)
	}
	if _, err := optionalIntSlice(map[string]interface{}{"spids": 728}, "spids"); err == nil || !strings.Contains(err.Error(), "spids must be integer array") {
		t.Fatalf("non-array: %v", err)
	}
	if _, err := optionalIntSlice(map[string]interface{}{"spids": []interface{}{float64(728), "700"}}, "spids"); err == nil || !strings.Contains(err.Error(), "spids[1]") {
		t.Fatalf("non-integer element: %v", err)
	}
	if _, err := optionalIntSlice(map[string]interface{}{"spids": []interface{}{1.5}}, "spids"); err == nil {
		t.Fatal("fractional element accepted")
	}
}

func TestOptionalStringSliceStrict(t *testing.T) {
	got, err := optionalStringSlice(map[string]interface{}{"names": []interface{}{"SP:Completed", "RPC:Completed"}}, "names")
	if err != nil || len(got) != 2 {
		t.Fatalf("got %v, %v", got, err)
	}
	if _, err := optionalStringSlice(map[string]interface{}{"names": []interface{}{"SP:Completed", ""}}, "names"); err == nil || !strings.Contains(err.Error(), "names[1]") {
		t.Fatalf("empty string: %v", err)
	}
	if _, err := optionalStringSlice(map[string]interface{}{"names": []interface{}{7}}, "names"); err == nil || !strings.Contains(err.Error(), "names[0]") {
		t.Fatalf("non-string element: %v", err)
	}
}

func TestTRCEventsHandlerScalarArrayConflict(t *testing.T) {
	tool, ok := toolRegistry["codebase_trc_events"]
	if !ok {
		t.Fatal("codebase_trc_events not in toolRegistry")
	}
	for _, args := range []map[string]interface{}{
		{"spid": float64(728), "spids": []interface{}{float64(728)}},
		{"event_name": "SP:Completed", "event_names": []interface{}{"SP:Completed"}},
	} {
		_, err := tool.Handler(context.Background(), args)
		if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("args %v: err = %v, want mutually exclusive", args, err)
		}
	}
}

func TestTRCEventsHandlerLegacyScalarNormalization(t *testing.T) {
	tool, ok := toolRegistry["codebase_trc_events"]
	if !ok {
		t.Fatal("codebase_trc_events not in toolRegistry")
	}
	// spid=-1 нормализуется в spids=[-1] и отклоняется валидацией с индексом
	_, err := tool.Handler(context.Background(), map[string]interface{}{"spid": float64(-1)})
	if err == nil || !strings.Contains(err.Error(), "spids[0]") {
		t.Fatalf("err = %v, want spids[0] validation error", err)
	}
	// spid=0 — легаси-семантика «все» (фильтр не применяется)
	_, err = tool.Handler(context.Background(), map[string]interface{}{"spid": float64(0)})
	if err == nil || !strings.Contains(err.Error(), "either session_id or file_path") {
		t.Fatalf("err = %v, want missing source error (validation passed)", err)
	}
}

func TestTRCEventsHandlerRejectsBadRFC3339(t *testing.T) {
	tool, ok := toolRegistry["codebase_trc_events"]
	if !ok {
		t.Fatal("codebase_trc_events not in toolRegistry")
	}
	for _, key := range []string{"time_from", "time_to"} {
		_, err := tool.Handler(context.Background(), map[string]interface{}{key: "2026-09-14 13:56:50"})
		if err == nil || !strings.Contains(err.Error(), key) || !strings.Contains(err.Error(), "RFC3339") {
			t.Fatalf("%s: err = %v, want RFC3339 error", key, err)
		}
	}
}

func TestTRCEventsSchemaNewParams(t *testing.T) {
	tool, ok := toolRegistry["codebase_trc_events"]
	if !ok {
		t.Fatal("codebase_trc_events not in toolRegistry")
	}
	schema := tool.Definition.InputSchema
	props, _ := schema["properties"].(map[string]interface{})
	for _, key := range []string{"spids", "event_names", "time_from", "time_to", "min_duration_ms", "after_id", "format", "spid", "event_name"} {
		if _, ok := props[key]; !ok {
			t.Errorf("schema missing property %s", key)
		}
	}
	for _, key := range []string{"spids", "event_names"} {
		prop, _ := props[key].(map[string]interface{})
		if prop["type"] != "array" {
			t.Errorf("property %s type = %v, want array", key, prop["type"])
		}
	}
}

func TestTRCProceduresSchemaNewParams(t *testing.T) {
	tool, ok := toolRegistry["codebase_trc_procedures"]
	if !ok {
		t.Fatal("codebase_trc_procedures not in toolRegistry")
	}
	schema := tool.Definition.InputSchema
	props, _ := schema["properties"].(map[string]interface{})
	for _, key := range []string{"spids", "event_names", "top", "sort_by", "group_by_spid"} {
		if _, ok := props[key]; !ok {
			t.Errorf("schema missing property %s", key)
		}
	}
	// scalar-алиасы для procedures не вводятся
	for _, key := range []string{"spid", "event_name"} {
		if _, ok := props[key]; ok {
			t.Errorf("schema must not contain scalar alias %s", key)
		}
	}
}

func TestTRCProceduresHandlerValidatesParams(t *testing.T) {
	tool, ok := toolRegistry["codebase_trc_procedures"]
	if !ok {
		t.Fatal("codebase_trc_procedures not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{"top": float64(1001)})
	if err == nil || !strings.Contains(err.Error(), "top") {
		t.Fatalf("err = %v, want top validation error", err)
	}
	_, err = tool.Handler(context.Background(), map[string]interface{}{"sort_by": "duration"})
	if err == nil || !strings.Contains(err.Error(), "sort_by") {
		t.Fatalf("err = %v, want sort_by validation error", err)
	}
}

func TestTRCCompareProceduresHandlerValidatesParams(t *testing.T) {
	tool, ok := toolRegistry["codebase_trc_compare_procedures"]
	if !ok {
		t.Fatal("codebase_trc_compare_procedures not in toolRegistry")
	}
	// focus в peer-списке: после нормализации пусто
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"focus_spid":    float64(728),
		"compare_spids": []interface{}{float64(728)},
	})
	if err == nil || !strings.Contains(err.Error(), "compare_spids") {
		t.Fatalf("err = %v, want compare_spids empty-after-focus error", err)
	}
	// top за пределами 1..100
	_, err = tool.Handler(context.Background(), map[string]interface{}{
		"focus_spid":    float64(728),
		"compare_spids": []interface{}{float64(700)},
		"top":           float64(101),
	})
	if err == nil || !strings.Contains(err.Error(), "top") {
		t.Fatalf("err = %v, want top validation error", err)
	}
}

func TestTRCCompareAndSpidsSchemas(t *testing.T) {
	compare, ok := toolRegistry["codebase_trc_compare_procedures"]
	if !ok {
		t.Fatal("codebase_trc_compare_procedures not in toolRegistry")
	}
	compareProps, _ := compare.Definition.InputSchema["properties"].(map[string]interface{})
	for _, key := range []string{"focus_spid", "compare_spids", "event_names", "top", "sort_by", "session_id", "file_path"} {
		if _, ok := compareProps[key]; !ok {
			t.Errorf("compare schema missing property %s", key)
		}
	}
	if prop, _ := compareProps["compare_spids"].(map[string]interface{}); prop["type"] != "array" {
		t.Error("compare_spids must be array type")
	}
	// scalar-alias быть не должно
	if _, ok := compareProps["spid"]; ok {
		t.Error("compare schema must not contain scalar alias spid")
	}

	spids, ok := toolRegistry["codebase_trc_spids"]
	if !ok {
		t.Fatal("codebase_trc_spids not in toolRegistry")
	}
	spidsProps, _ := spids.Definition.InputSchema["properties"].(map[string]interface{})
	for _, key := range []string{"spids", "time_from", "time_to", "sort_by", "limit", "session_id", "file_path"} {
		if _, ok := spidsProps[key]; !ok {
			t.Errorf("spids schema missing property %s", key)
		}
	}
}

func TestTRCSpidsHandlerRejectsBadSort(t *testing.T) {
	tool, ok := toolRegistry["codebase_trc_spids"]
	if !ok {
		t.Fatal("codebase_trc_spids not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{"sort_by": "avg_ms"})
	if err == nil || !strings.Contains(err.Error(), "sort_by") {
		t.Fatalf("err = %v, want sort_by enum error", err)
	}
}

func newSDKTestClientSession(t *testing.T, profile string) *mcpsdk.ClientSession {
	t.Helper()
	config.CreateDefault("")
	server := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "codebase-test", Version: "test"}, nil)
	registry, err := buildToolRegistryForProfile(nil, profile)
	if err != nil {
		t.Fatalf("buildToolRegistryForProfile: %v", err)
	}
	registerSDKCoreTools(server, registry, profile, nil)
	serverTransport, clientTransport := mcpsdk.NewInMemoryTransports()
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "client-test", Version: "test"}, nil)
	clientSession, err := client.Connect(context.Background(), clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = clientSession.Close() })
	return clientSession
}

func TestSDKToolsList_NoOutputSchema(t *testing.T) {
	for _, profile := range []string{"", "query", "rti", "trc", "review"} {
		t.Run("profile="+profile, func(t *testing.T) {
			cs := newSDKTestClientSession(t, profile)
			res, err := cs.ListTools(context.Background(), &mcpsdk.ListToolsParams{})
			if err != nil {
				t.Fatalf("ListTools: %v", err)
			}
			if len(res.Tools) == 0 {
				t.Fatal("no tools registered")
			}
			for _, tool := range res.Tools {
				if tool.OutputSchema != nil {
					t.Errorf("tool %s declares outputSchema", tool.Name)
				}
			}
		})
	}
}

func TestSDKCallTool_Ping_NoValidationError(t *testing.T) {
	cs := newSDKTestClientSession(t, "")
	res, err := cs.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: "codebase_ping"})
	if err != nil {
		t.Fatalf("CallTool codebase_ping: %v", err)
	}
	if res.IsError {
		t.Fatal("unexpected isError result for codebase_ping")
	}
	if len(res.Content) != 1 {
		t.Fatalf("expected exactly 1 content block, got %d", len(res.Content))
	}
	if _, ok := res.Content[0].(*mcpsdk.TextContent); !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}
	if res.StructuredContent != nil {
		t.Fatal("expected no structuredContent in ping result")
	}
}

func TestSDKCallTool_ErrorPath_TextOnly(t *testing.T) {
	cs := newSDKTestClientSession(t, "rti")
	res, err := cs.CallTool(context.Background(), &mcpsdk.CallToolParams{
		Name:      "codebase_rti_parse",
		Arguments: map[string]interface{}{"file_path": filepath.Join(t.TempDir(), "missing.rti")},
	})
	if err != nil {
		t.Fatalf("CallTool must not fail with protocol error, got: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected isError=true for missing file")
	}
	if len(res.Content) != 1 {
		t.Fatalf("expected exactly 1 content block, got %d", len(res.Content))
	}
	if _, ok := res.Content[0].(*mcpsdk.TextContent); !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}
	if res.StructuredContent != nil {
		t.Fatal("expected no structuredContent in error result")
	}
}

func TestTimeoutForTool_ParseTRC(t *testing.T) {
	cfg := &config.Config{
		MCP: config.MCPConfig{QueryTimeoutSec: 30, ReviewTimeoutSec: 120},
		TRC: config.TRCConfig{ParseTimeoutSec: 300},
		RTI: config.RTIConfig{ParseTimeoutSec: 300},
	}
	got := timeoutForTool("codebase_trc_parse", cfg)
	want := 300 * time.Second
	if got != want {
		t.Fatalf("codebase_trc_parse: got %v, want %v", got, want)
	}
}

func TestTimeoutForTool_ParseRTI(t *testing.T) {
	cfg := &config.Config{
		MCP: config.MCPConfig{QueryTimeoutSec: 30, ReviewTimeoutSec: 120},
		TRC: config.TRCConfig{ParseTimeoutSec: 300},
		RTI: config.RTIConfig{ParseTimeoutSec: 300},
	}
	got := timeoutForTool("codebase_rti_parse", cfg)
	want := 300 * time.Second
	if got != want {
		t.Fatalf("codebase_rti_parse: got %v, want %v", got, want)
	}
}

func TestTimeoutForTool_ReviewSQL(t *testing.T) {
	cfg := &config.Config{
		MCP: config.MCPConfig{QueryTimeoutSec: 30, ReviewTimeoutSec: 120},
		TRC: config.TRCConfig{ParseTimeoutSec: 300},
		RTI: config.RTIConfig{ParseTimeoutSec: 300},
	}
	got := timeoutForTool("codebase_review_sql", cfg)
	want := 120 * time.Second
	if got != want {
		t.Fatalf("codebase_review_sql: got %v, want %v", got, want)
	}
}

func TestTimeoutForTool_DefaultQuery(t *testing.T) {
	cfg := &config.Config{
		MCP: config.MCPConfig{QueryTimeoutSec: 30, ReviewTimeoutSec: 120},
		TRC: config.TRCConfig{ParseTimeoutSec: 300},
		RTI: config.RTIConfig{ParseTimeoutSec: 300},
	}
	for _, name := range []string{
		"codebase_query_symbol",
		"codebase_trc_summary",
		"codebase_rti_tree",
		"codebase_ping",
	} {
		got := timeoutForTool(name, cfg)
		want := 30 * time.Second
		if got != want {
			t.Fatalf("%s: got %v, want %v", name, got, want)
		}
	}
}

func TestTimeoutForTool_ZeroDisablesTimeout(t *testing.T) {
	cfg := &config.Config{
		MCP: config.MCPConfig{QueryTimeoutSec: 0, ReviewTimeoutSec: 0},
		TRC: config.TRCConfig{ParseTimeoutSec: 0},
		RTI: config.RTIConfig{ParseTimeoutSec: 0},
	}
	for _, name := range []string{
		"codebase_query_symbol",
		"codebase_trc_parse",
		"codebase_rti_parse",
		"codebase_review_sql",
	} {
		got := timeoutForTool(name, cfg)
		if got != 0 {
			t.Fatalf("%s: got %v, want 0", name, got)
		}
	}
}
