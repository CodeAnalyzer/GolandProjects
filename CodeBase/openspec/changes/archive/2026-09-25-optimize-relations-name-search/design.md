## Context

См. `proposal.md` — Why. Текущий `SearchRelations` (`internal/query/query_relations.go:220`) имеет две ветки:

- **быстрая** (`searchRelationsByNameMatches`, :321) — только когда задано имя строго с одной стороны, а другая сторона без имени и типа; резолвит имя в пары `(entity_type, entity_id)` через `relationEntityMatchQueryParts` (:421, `UNION ALL` по `LOWER(name) = LOWER($1)` или `ILIKE`) и фильтрует `relations` через `JOIN (VALUES …)` (`selectRelationIDsByEntityMatches`, :459);
- **общая** — всё остальное; для неимённых фильтров использует `buildRelationNameExistsCondition` (:135, один коррелированный `EXISTS` на типизированную сущность) или `buildRelationAnyNameExistsCondition` (:185, `OR` из 11 коррелированных `EXISTS` с `ILIKE '%…%'`).

Существующие индексы (менять схему не нужно): `idx_sql_procedures_proc_name_lower`, `idx_sql_tables_table_name_lower`, `idx_pas_methods_method_name_lower`, `idx_js_functions_function_name_lower`, `idx_api_contracts_lower_name_kind`, `idx_report_forms_report_name_lower`, `idx_report_fields_field_name_lower`, `idx_report_params_param_name_lower`, `idx_vb_functions_function_name_lower`, `idx_query_fragments_component_name_lower`, `idx_smf_instruments_instrument_name_lower`, плюс trgm-GIN по каждому имени для подстрочного фолбэка; `idx_relations_source_type_id`, `idx_relations_target_type_id`. Детали ребра собираются `relationSearchBaseQuery` (:520, 44 `LEFT JOIN`) — это вне области изменения.

## Goals / Non-Goals

**Goals:**
- Один путь фильтрации по имени на все комбинации `source_name`/`target_name`/типов.
- Убрать коррелированные `EXISTS` из горячего пути; опираться только на индексированные lookup'ы и составной ключ `relations`.
- Единая семантика сопоставления имени (exact-first + подстрочный фолбэк) во всех случаях.
- Сохранить пересечение при двух именах.

**Non-Goals:**
- Оптимизация `relationSearchBaseQuery` (44 JOIN) и `inspect` — отдельное изменение (P2).
- Исправление бага `sql.ErrNoRows` в `GetProcedureDetails/Result` (P7) — отдельное изменение.
- Полнота логирования аргументов MCP (`formatToolArgs`) — отдельное изменение.
- Изменения схемы БД и индексов.
- Нормализация алиасов типов (`procedure` → `sql_procedure`): контракт остаётся каноническим (`sql_procedure`, `sql_table`, …), как в описании MCP-тула `codebase_query_relations`.

## Decisions

**D1. Единый двухпроходный алгоритм.** Обобщаем быстрый путь на общий случай: сначала резолв имён в пары `(entity_type, entity_id)`, затем выборка `relations`. Альтернативы: (a) добавить индексы под `OR-EXISTS` — невозможен из-за коррелированного `ILIKE`; (b) денормализовать имена в `relations` на этапе индексации — изменение схемы и переиндексация, вынесено в отдельное изменение; (c) materialized view — устаревание и рефреш.

**D2. Exact-first, унифицированный.** Для каждой стороны сначала точное совпадение (`LOWER(name) = LOWER($1)`), при пустом результате — подстрочный фолбэк (`ILIKE '%…%'`). Это уже поведение быстрого пути; теперь оно становится единственным. Прямое следствие — сценарий «Точное совпадение имени приоритетно над подстрокой».

**D3. Пересечение двух имён.** Первый проход даёт `matches_src` и `matches_tgt`. Второй проход джойнит `relations` к обоим наборам `VALUES`: `source`-сторона к `matches_src`, `target`-сторона к `matches_tgt`. Пустой любой набор → пустой результат. Альтернатива — последовательные запросы с пересечением в Go — отклонена: лишний round-trip и потеря `LIMIT`-семантики.

**D4. Типизированные фильтры.** Явный `source_type`/`target_type` сначала ограничивает тип в первом проходе (`entityType` в `relationEntityMatchQueryParts`), а во втором — дополнительно фиксирует `r.source_type`/`r.target_type`. Это покрывает и `name` без типа, и `name` с типом, и смешанные случаи.

**D5. Канонические типы сущностей в спеке.** Два существующих сценария требования используют `--source-type procedure` / `--target-type table`, что не соответствует ни реализации (switch по `sql_procedure`/`sql_table`), ни контракту MCP-тула. При `MODIFIED` приводим их к каноническому виду; алиасы не вводим.

**D6. Ограничение совпадений имён.** На сторону берём не более `relationEntityMatchLimit(limit)` совпадений (`limit*4`, минимум 50). Точные совпадения по имени редки и в лимит не упираются; подстрочный фолбэк может усекаться — это известный компромисс (см. Risks).

**D7. Удаление коррелированных EXISTS.** `buildRelationAnyNameExistsCondition` и `buildRelationNameExistsCondition` удаляются; диспетчеризация `SearchRelations` сводится к построению двух наборов `VALUES` и одного запроса выборки id.

## Risks / Trade-offs

- [Изменение семантики для общего пути: было чистое `ILIKE`, стало exact-first] → **BREAKING** зафиксирован в `proposal.md`; покрыто сценариями; ранее уже действовало в быстром пути.
- [Усечение подстрочного фолбэка по `relationEntityMatchLimit`] → лимит `limit*4` (мин. 50) на сторону; при необходимости поднимается без изменения контракта.
- [`ORDER BY r.id DESC` не покрыт индексом] → кандидатов мало (ограничены наборами `VALUES`), сортировка дешёвая; отдельный индекс — вне области.
- [Детализация ребра (44 JOIN) не оптимизируется] → `limit`-запросы и `inspect` останутся дорогими; вынесено в P2, явный Non-Goal.
- [Два имени с большими подстрочными наборами дают большой `VALUES`] → параметры байндатся, размер ограничен `limit*4` на сторону; наблюдаемо на реальном индексе.

## Migration Plan

Изменений схемы и данных нет. Раскатка — обычный релиз бинарника; откат — revert коммита. Проверка на реальном индексе: `EXPLAIN` по запросу выборки id не должен содержать `Seq Scan on relations`; прогон логов `WORK-LOG` до/после по `codebase_query_relations`.

## Open Questions

- (нет)
