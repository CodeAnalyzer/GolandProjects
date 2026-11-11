package mcp

import "testing"

// schemaHasProp — есть ли параметр в properties схемы инструмента.
func schemaHasProp(t *testing.T, name, prop string) bool {
	t.Helper()
	rt, ok := toolRegistry[name]
	if !ok {
		t.Fatalf("инструмент %q не зарегистрирован", name)
	}
	props, _ := rt.Definition.InputSchema["properties"].(map[string]interface{})
	_, has := props[prop]
	return has
}

// TestProfileAwareQueryToolsSchema — dsinfo_id присутствует у всех
// инструментов, выбирающих копию сущности (плюс review, разрешающий ветки
// чужих продуктов по профилю), и отсутствует у остальных.
func TestProfileAwareQueryToolsSchema(t *testing.T) {
	for name := range profileAwareQueryTools {
		if !schemaHasProp(t, name, "dsinfo_id") {
			t.Errorf("%s: отсутствует dsinfo_id", name)
		}
	}
	if !schemaHasProp(t, "codebase_review_sql", "dsinfo_id") {
		t.Error("codebase_review_sql: отсутствует dsinfo_id (ветки чужих продуктов)")
	}

	excluded := []string{
		"codebase_query_callers",
		"codebase_query_relations",
		"codebase_query_inspect",
		"codebase_query_spec_search",
		"codebase_query_spec_config",
		"dsinfo_parse",
		"dsinfo_list",
	}
	for _, name := range excluded {
		if schemaHasProp(t, name, "dsinfo_id") {
			t.Errorf("%s: dsinfo_id не должен регистрироваться (вне границ профиля)", name)
		}
	}
}

// TestProfileAwareEnrichmentToolsSchema — профиль принимают только
// rti/trc-инструменты с обогащением процедур.
func TestProfileAwareEnrichmentToolsSchema(t *testing.T) {
	profiled := []string{
		"codebase_rti_tree",
		"codebase_rti_errors",
		"codebase_rti_slow",
		"codebase_rti_details",
		"codebase_trc_procedures",
	}
	for _, name := range profiled {
		if !schemaHasProp(t, name, "dsinfo_id") {
			t.Errorf("%s: отсутствует dsinfo_id", name)
		}
	}

	plain := []string{
		"codebase_rti_client_tree",
		"codebase_rti_timeline",
		"codebase_trc_events",
		"codebase_trc_tree",
	}
	for _, name := range plain {
		if schemaHasProp(t, name, "dsinfo_id") {
			t.Errorf("%s: dsinfo_id не должен регистрироваться (клиентское обогащение)", name)
		}
	}
}
