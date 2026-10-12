## Why

`codebase_query_relations` / `query relations` не валидирует `relation_type` (а также `source_type`/`target_type`), а таблица `relations` (~3.48M строк) не имеет индекса с ведущим `relation_type`. Опечатка в `relation_type` (`__no_such_relation__`, TC-075) уходит в полный seq scan + sort и обрывается по таймауту БД (`pq: canceling statement due to user request`) вместо мгновенной ошибки валидации. Тот же дефект плана замедляет и **валидные** запросы: type-only (`references_table`, ~210 мс warm, 1.27M отброшенных строк) и 3-предикатный `source_type + target_type + relation_type + ORDER BY id DESC` (5.37 с cold).

## What Changes

- **BREAKING** (strict-валидация): неизвестный `relation_type` теперь возвращает ошибку со списком валидных типов (не пустой результат и не таймаут).
- **BREAKING** (strict-валидация): неизвестный `source_type` / `target_type` также возвращает ошибку со списком валидных типов.
- Канонический словарь `relation_type` (21 значение) и entity-типов графа (17 значений) выносится в `internal/model` и синхронизируется с описанием инструмента `registry.go` и дельта-спекой.
- Добавляются два индекса `relations`: `(relation_type, id DESC)` для type-only запросов и `(source_type, relation_type, id DESC)` для 3-предикатного паттерна — вместо единственного составного с ведущим `source_type`.
- Пересматривается ранее принятое решение `redundant-indexes-cleanup-a41c07` («отдельный индекс под relation_type не создаётся»): деградация чтения признана неприемлемой, индекс возвращается под новым именем, легаси-`DROP INDEX IF EXISTS idx_relations_relation_type` сохраняется как идемпотентная чистка.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `query/api-relations-queries`: строгая валидация `relation_type`, `source_type`, `target_type` в `query relations` / `codebase_query_relations` со списком валидных значений.
- `infrastructure/database-schema`: два индекса на `relations` (`(relation_type, id DESC)` и `(source_type, relation_type, id DESC)`); снятие утверждения «отдельный индекс под relation_type не создаётся».

## Impact

- Код запросов: `internal/query/query_relations.go` (`SearchRelations`, `SearchRelationsByEntity`), новый словарь и валидация в `internal/model` + `internal/query/query.go`.
- Ошибки и классификация: `internal/errs/errs.go` (новый `ErrUnknownRelationType`), `cmd/query_execution.go` (`classifyQueryError` → `invalid_arguments`).
- Схема БД: `internal/store/db_schema.go` (блок индексов `relations`, строки 944-947), `internal/store/schema_integration_test.go` (проверка новых индексов).
- Документация инструмента: `internal/mcp/registry.go` (описание `codebase_query_relations`: дополнить словари).
- Тесты и артефакты: `Tests/TEST_CASES.md` (TC-075 — ожидание ошибки валидации), `Tests/CODEBASE_QUERY_TEST_RESULTS_2026-10-11.md`.
- Спеки: `openspec/specs/query/api-relations-queries/spec.md`, `openspec/specs/infrastructure/database-schema/spec.md`.
- Источник: `Bug-reports/BUG-relations-invalid-relation-type-timeout-20261011.md`.
