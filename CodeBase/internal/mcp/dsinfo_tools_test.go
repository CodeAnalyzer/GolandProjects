package mcp

import (
	"context"
	"strings"
	"testing"
)

// TestDsInfoToolsRegisteredInAllProfiles — все 5 dsinfo-инструментов
// зарегистрированы и входят в baseTools (доступны в каждом профиле).
func TestDsInfoToolsRegisteredInAllProfiles(t *testing.T) {
	names := []string{"dsinfo_parse", "dsinfo_list", "dsinfo_show", "dsinfo_delete", "dsinfo_use"}
	for _, name := range names {
		if _, ok := toolRegistry[name]; !ok {
			t.Errorf("инструмент %q отсутствует в toolRegistry", name)
		}
		if !baseTools[name] {
			t.Errorf("инструмент %q должен входить в baseTools (все профили)", name)
		}
		for _, profile := range ValidProfiles() {
			whitelist := profileToolSets[profile]
			if !whitelist[name] {
				t.Errorf("инструмент %q отсутствует в профиле %q", name, profile)
			}
		}
	}
}

// TestDsInfoToolsSchemas — обязательные и опциональные параметры схем.
func TestDsInfoToolsSchemas(t *testing.T) {
	parse, ok := toolRegistry["dsinfo_parse"]
	if !ok {
		t.Fatal("dsinfo_parse не зарегистрирован")
	}
	parseProps, _ := parse.Definition.InputSchema["properties"].(map[string]interface{})
	for _, key := range []string{"file_path", "name"} {
		if _, ok := parseProps[key]; !ok {
			t.Errorf("schema dsinfo_parse: отсутствует свойство %s", key)
		}
	}
	if req, _ := parse.Definition.InputSchema["required"].([]string); len(req) != 1 || req[0] != "file_path" {
		t.Errorf("schema dsinfo_parse: required должен быть [file_path]")
	}

	show, ok := toolRegistry["dsinfo_show"]
	if !ok {
		t.Fatal("dsinfo_show не зарегистрирован")
	}
	showProps, _ := show.Definition.InputSchema["properties"].(map[string]interface{})
	for _, key := range []string{"profile_id", "module", "packet", "search", "limit"} {
		if _, ok := showProps[key]; !ok {
			t.Errorf("schema dsinfo_show: отсутствует свойство %s", key)
		}
	}
	if req, _ := show.Definition.InputSchema["required"].([]string); len(req) != 1 || req[0] != "profile_id" {
		t.Errorf("schema dsinfo_show: required должен быть [profile_id]")
	}

	deleteTool, ok := toolRegistry["dsinfo_delete"]
	if !ok {
		t.Fatal("dsinfo_delete не зарегистрирован")
	}
	deleteProps, _ := deleteTool.Definition.InputSchema["properties"].(map[string]interface{})
	if _, ok := deleteProps["profile_id"]; !ok {
		t.Error("schema dsinfo_delete: отсутствует свойство profile_id")
	}
	if req, _ := deleteTool.Definition.InputSchema["required"].([]string); len(req) != 1 || req[0] != "profile_id" {
		t.Errorf("schema dsinfo_delete: required должен быть [profile_id]")
	}

	list, ok := toolRegistry["dsinfo_list"]
	if !ok {
		t.Fatal("dsinfo_list не зарегистрирован")
	}
	listProps, _ := list.Definition.InputSchema["properties"].(map[string]interface{})
	if _, ok := listProps["limit"]; !ok {
		t.Error("schema dsinfo_list: отсутствует свойство limit")
	}

	use, ok := toolRegistry["dsinfo_use"]
	if !ok {
		t.Fatal("dsinfo_use не зарегистрирован")
	}
	useProps, _ := use.Definition.InputSchema["properties"].(map[string]interface{})
	if _, ok := useProps["profile_id"]; !ok {
		t.Error("schema dsinfo_use: отсутствует свойство profile_id")
	}
	if _, has := use.Definition.InputSchema["required"]; has {
		t.Error("schema dsinfo_use: profile_id опционален (required быть не должно)")
	}
}

// TestDsInfoHandlersValidateArgs — валидация аргументов до обращения к БД:
// обязательные параметры отклоняются без подключённой БД.
func TestDsInfoHandlersValidateArgs(t *testing.T) {
	parse := toolRegistry["dsinfo_parse"].Handler
	if _, err := parse(context.Background(), map[string]interface{}{}); err == nil || !strings.Contains(err.Error(), "file_path") {
		t.Errorf("dsinfo_parse без file_path: ожидалась ошибка валидации, получено %v", err)
	}

	show := toolRegistry["dsinfo_show"].Handler
	if _, err := show(context.Background(), map[string]interface{}{}); err == nil || !strings.Contains(err.Error(), "profile_id") {
		t.Errorf("dsinfo_show без profile_id: ожидалась ошибка валидации, получено %v", err)
	}

	del := toolRegistry["dsinfo_delete"].Handler
	if _, err := del(context.Background(), map[string]interface{}{}); err == nil || !strings.Contains(err.Error(), "profile_id") {
		t.Errorf("dsinfo_delete без profile_id: ожидалась ошибка валидации, получено %v", err)
	}

	if _, err := show(context.Background(), map[string]interface{}{"profile_id": "abc"}); err == nil || !strings.Contains(err.Error(), "profile_id") {
		t.Errorf("dsinfo_show с нечисловым profile_id: ожидалась ошибка валидации, получено %v", err)
	}
}
