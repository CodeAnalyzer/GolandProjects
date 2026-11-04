package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/codebase/internal/query"
)

// Инструмент sql-fragment зарегистрирован в полном реестре и query-профиле;
// описание документа флаг усечения (B5).
func TestSQLFragmentToolRegistered(t *testing.T) {
	full := buildToolRegistry(nil)
	tool, ok := full["codebase_query_sql_fragment"]
	if !ok {
		t.Fatal("codebase_query_sql_fragment отсутствует в полном реестре")
	}
	if tool.Handler == nil {
		t.Fatal("handler не задан")
	}
	if !strings.Contains(tool.Definition.Description, "truncated") {
		t.Fatalf("description must document truncated flag: %s", tool.Definition.Description)
	}

	registry, err := buildToolRegistryForProfile(nil, "query")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := registry["codebase_query_sql_fragment"]; !ok {
		t.Fatal("codebase_query_sql_fragment отсутствует в query-профиле")
	}
}

// Handler без text возвращает ошибку валидации.
func TestSQLFragmentHandler_MissingText(t *testing.T) {
	full := buildToolRegistry(nil)
	tool := full["codebase_query_sql_fragment"]
	if _, err := tool.Handler(context.Background(), map[string]interface{}{}); err == nil {
		t.Fatal("ожидается ошибка валидации без text")
	}
}

// MCP-контракт sql-fragment: ответ — объект {items, truncated}, не массив;
// truncated присутствует всегда, hint — при усечении (change
// optimize-query-search-bottlenecks, B5).
func TestSQLFragmentResponseContract(t *testing.T) {
	for _, out := range []query.QueryFragmentSearchOutput{
		{Items: []query.QueryFragmentResult{}},
		{Items: nil, Truncated: true, Hint: query.QueryFragmentTruncatedHint},
	} {
		data, err := json.Marshal(out)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(data, &obj); err != nil {
			t.Fatalf("response is not a JSON object: %v; data=%s", err, data)
		}
		if _, ok := obj["items"]; !ok {
			t.Fatalf("items отсутствует: %s", data)
		}
		if _, ok := obj["truncated"]; !ok {
			t.Fatalf("truncated отсутствует: %s", data)
		}
		if out.Truncated {
			if _, ok := obj["hint"]; !ok {
				t.Fatalf("hint отсутствует при усечении: %s", data)
			}
		}
	}
}
