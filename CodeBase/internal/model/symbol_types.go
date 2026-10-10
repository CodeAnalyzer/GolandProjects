package model

import (
	"sort"
	"strings"
)

// SymbolTypes — канонический словарь типов символов unified index (symbols.symbol_type).
// Источник значений — писатели символов в индексаторе и парсерах; kind-типы
// API-контрактов (service, event, callback_event, used_service) записываются
// парсером DSArchitect XML с entity_type = "xml"; тип "xml" — fallback-вид
// для DSArchitect XML вне каталогов Service/Event/Table/Param/UsedService/CallbackEvent.
var SymbolTypes = []string{
	"api_business_object",
	"api_param",
	"api_table",
	"api_table_index",
	"callback_event",
	"class",
	"column_definition",
	"component",
	"constant",
	"define",
	"event",
	"form",
	"function",
	"index",
	"method",
	"procedure",
	"report_form",
	"report_param",
	"service",
	"smf_instrument",
	"spec_capability",
	"spec_change",
	"spec_requirement",
	"spec_usecase",
	"table",
	"unit",
	"used_service",
	"vb_function",
	"xml",
}

// symbolTypeAliases — алиасы в стиле relations-словаря, принимаемые фильтром
// типа наравне с каноническими значениями. Алиас api_contract разворачивается
// во все kind-типы API-контрактов.
var symbolTypeAliases = map[string][]string{
	"sql_procedure": {"procedure"},
	"sql_table":     {"table"},
	"pas_method":    {"method"},
	"js_function":   {"function"},
	"dfm_form":      {"form"},
	"dfm_component": {"component"},
	"api_contract":  {"service", "event", "callback_event", "used_service"},
}

var symbolTypeSet = func() map[string]struct{} {
	set := make(map[string]struct{}, len(SymbolTypes))
	for _, t := range SymbolTypes {
		set[t] = struct{}{}
	}
	return set
}()

// ResolveSymbolTypeAlias нормализует значение фильтра типа символа: обрезает
// пробелы и приводит к нижнему регистру. Канонический тип возвращается как есть,
// алиас relations-стиля транслируется в канонические типы (api_contract — во все
// kind-типы контрактов). Второе возвращаемое значение сообщает, известно ли
// значение (алиас или канонический тип); пустая строка неизвестна.
func ResolveSymbolTypeAlias(raw string) ([]string, bool) {
	key := strings.ToLower(strings.TrimSpace(raw))
	if key == "" {
		return nil, false
	}
	if _, ok := symbolTypeSet[key]; ok {
		return []string{key}, true
	}
	if canonical, ok := symbolTypeAliases[key]; ok {
		return canonical, true
	}
	return nil, false
}

// SymbolTypeAliasesList возвращает отсортированный список алиасов
// relations-стиля (для сообщений об ошибках и документации).
func SymbolTypeAliasesList() []string {
	aliases := make([]string, 0, len(symbolTypeAliases))
	for alias := range symbolTypeAliases {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	return aliases
}

// RelationTypeForSymbol переводит тип символа (symbols.symbol_type) в тип
// сущности словаря relations. Пустой тип символа передаётся как entity_type.
// Типы, одинаковые в обоих словарях (smf_instrument, vb_function, report_form,
// spec_* и др.), возвращаются без изменений.
func RelationTypeForSymbol(symbolType, entityType string) string {
	t := strings.TrimSpace(symbolType)
	if t == "" {
		return strings.TrimSpace(entityType)
	}
	switch strings.ToLower(t) {
	case "procedure":
		return "sql_procedure"
	case "table":
		return "sql_table"
	case "form":
		return "dfm_form"
	case "component":
		return "dfm_component"
	case "method":
		return "pas_method"
	case "function":
		return "js_function"
	case "unit":
		return "pas_unit"
	case "class":
		return "pas_class"
	case "service", "event", "callback_event", "used_service":
		return "api_contract"
	default:
		return t
	}
}
