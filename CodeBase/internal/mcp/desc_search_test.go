package mcp

import (
	"context"
	"testing"
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
