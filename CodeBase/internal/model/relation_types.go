package model

import "strings"

// RelationTypes — канонический словарь типов связей таблицы relations.
// Источник значений — писатели связей в индексаторе: кодовые связи
// (indexer_relations.go, indexer_postprocess_fragments.go) и спецификационные
// связи графа спек (indexer_postprocess_spec.go, indexer_postprocess_spec_deps.go).
// Список синхронизирован с описанием инструмента codebase_query_relations
// (internal/mcp/registry.go).
var RelationTypes = []string{
	"builds_query",
	"calls_procedure",
	"change_modifies",
	"deletes_from",
	"depends_on_capability",
	"dispatches_to",
	"dispatches_to_subscriber",
	"executes_contract",
	"executes_query",
	"has_field",
	"has_param",
	"implements_contract",
	"inserts_into",
	"publishes_event",
	"references_code",
	"references_table",
	"selects_from",
	"subscribes_to_event",
	"updates",
	"usecase_involves",
	"uses_param",
}

// RelationEntityTypes — канонический словарь типов сущностей графа связей
// (колонки relations.source_type / relations.target_type). Источник значений —
// писатели связей в индексаторе; покрывает кодовые сущности и сущности спек.
var RelationEntityTypes = []string{
	"api_contract",
	"dfm_form",
	"js_function",
	"pas_method",
	"query_fragment",
	"report_field",
	"report_form",
	"report_param",
	"smf_instrument",
	"spec_capability",
	"spec_change",
	"spec_requirement",
	"spec_scenario",
	"spec_usecase",
	"sql_procedure",
	"sql_table",
	"vb_function",
}

var relationTypeSet = func() map[string]struct{} {
	set := make(map[string]struct{}, len(RelationTypes))
	for _, t := range RelationTypes {
		set[t] = struct{}{}
	}
	return set
}()

var relationEntityTypeSet = func() map[string]struct{} {
	set := make(map[string]struct{}, len(RelationEntityTypes))
	for _, t := range RelationEntityTypes {
		set[t] = struct{}{}
	}
	return set
}()

// IsKnownRelationType сообщает, входит ли значение (без учёта окружающих
// пробелов) в канонический словарь типов связей. Пустая строка неизвестна —
// вызывающая сторона трактует пустое значение как «без фильтра» до проверки.
func IsKnownRelationType(raw string) bool {
	_, ok := relationTypeSet[strings.TrimSpace(raw)]
	return ok
}

// IsKnownRelationEntityType сообщает, входит ли значение (без учёта окружающих
// пробелов) в канонический словарь типов сущностей графа связей.
func IsKnownRelationEntityType(raw string) bool {
	_, ok := relationEntityTypeSet[strings.TrimSpace(raw)]
	return ok
}

// relationEntityKindAliases — kind-типы API-контрактов (значения
// symbols.symbol_type), принимаемые фильтром типа сущности графа связей как
// алиасы канонического api_contract.
var relationEntityKindAliases = map[string]struct{}{
	"service":        {},
	"event":          {},
	"callback_event": {},
	"used_service":   {},
}

// NormalizeRelationEntityType нормализует значение фильтра типа сущности графа
// связей: обрезает пробелы и приводит к нижнему регистру. Канонический тип
// словаря возвращается как есть; kind-типы API-контрактов (service, event,
// callback_event, used_service) свёртываются в api_contract. Второе значение
// сообщает, известно ли значение; пустая строка неизвестна.
func NormalizeRelationEntityType(raw string) (string, bool) {
	key := strings.ToLower(strings.TrimSpace(raw))
	if key == "" {
		return "", false
	}
	if _, ok := relationEntityTypeSet[key]; ok {
		return key, true
	}
	if _, ok := relationEntityKindAliases[key]; ok {
		return "api_contract", true
	}
	return "", false
}
