package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/codebase/internal/query"
	"github.com/codebase/internal/specfts"
)

// Инструмент desc-search зарегистрирован в query-профиле и в полном реестре.
func TestDescSearchToolRegistered(t *testing.T) {
	full := buildToolRegistry(nil)
	tool, ok := full["codebase_query_desc_search"]
	if !ok {
		t.Fatal("codebase_query_desc_search отсутствует в полном реестре")
	}
	if tool.Definition.Name != "codebase_query_desc_search" {
		t.Fatalf("definition name = %q", tool.Definition.Name)
	}
	if tool.Handler == nil {
		t.Fatal("handler не задан")
	}

	registry, err := buildToolRegistryForProfile(nil, "query")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := registry["codebase_query_desc_search"]; !ok {
		t.Fatal("codebase_query_desc_search отсутствует в query-профиле")
	}

	// text — обязательный параметр схемы
	schema := tool.Definition.InputSchema
	required, ok := schema["required"].([]string)
	if !ok || len(required) != 1 || required[0] != "text" {
		t.Fatalf("schema required = %v, want [text]", required)
	}
}

// Handler без text возвращает ошибку валидации.
func TestDescSearchHandler_MissingText(t *testing.T) {
	full := buildToolRegistry(nil)
	tool := full["codebase_query_desc_search"]
	if _, err := tool.Handler(context.Background(), map[string]interface{}{}); err == nil {
		t.Fatal("ожидается ошибка валидации без text")
	}
}

// MCP-контракт desc-search: ответ — объект {items, has_more, semantic_meta},
// не массив; has_more присутствует всегда, semantic_meta — при необходимости.
func TestDescSearchResponseContract(t *testing.T) {
	for _, out := range []query.DescriptionSearchOutput{
		{Items: []query.DescriptionSearchResult{}},
		{
			Items:   []query.DescriptionSearchResult{},
			HasMore: true,
			SemanticMeta: &specfts.SemanticDiagnostics{
				Reason: "query_coverage",
			},
		},
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
		if _, ok := obj["has_more"]; !ok {
			t.Fatalf("has_more отсутствует: %s", data)
		}
		if out.SemanticMeta != nil {
			if _, ok := obj["semantic_meta"]; !ok {
				t.Fatalf("semantic_meta отсутствует: %s", data)
			}
		}
	}
}

// Kind принимает строковый массив и отклоняет не-строки.
func TestDescSearchHandler_KindValidation(t *testing.T) {
	full := buildToolRegistry(nil)
	tool := full["codebase_query_desc_search"]
	// Валидация аргументов выполняется раньше обращения к БД (db=nil):
	// ошибка kind должна возникнуть до runQueryOpt.
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"text": "тест",
		"kind": []interface{}{42},
	})
	if err == nil {
		t.Fatal("ожидается ошибка валидации kind")
	}
}
