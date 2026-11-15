## Why

В каталоге review 59 правил, но не все имеют юнит-тесты поведения: у 27 правил нет прямого unit-теста (4 из них — вообще без тестов, ещё 4 покрыты только integration-тестами с БД), а у 2 правил проверена лишь одна сторона (у `diffTypesComparison` нет negative, у `foreignProcedureUsing` нет unit-positive). Из-за этого регрессии в детекторах (FP/FN, якоря строк, обработка макросов) выявляются только на реальной кодовой базе FA или не выявляются вовсе.

## What Changes

- Зафиксировать контракт тестового покрытия: каждое правило review MUST иметь unit-тест, исполняемый без БД и тега `integration`, минимум с одним положительным и одним отрицательным кейсом.
- Добавить недостающие unit-тесты для 27 правил без прямого покрытия; для правил, зависящих от индекса/каталога, использовать тестовые double-объекты (in-memory кэши `procProductIDCache`/`tableProductIDCache`/`indexCandCache`), а не реальную БД.
- Дозаполнить недостающие стороны у `diffTypesComparison` (negative) и `foreignProcedureUsing` (unit-positive).
- Ввести общий тестовый хелпер для проверки правила на наборе кейсов `{source, wantCount, wantRule, wantLine}`, чтобы новые правила сразу получали pos+neg покрытие.
- Для lookup-ов без nil-guard (`findAPITableNames`, `lookupProcedureCreateFiles`, `cachedFindColumnDefinitionType`) добавить guard при `db == nil` как тестовый seam, согласованный с остальными cache-lookup'ами.
- В `checkDatatypeExecParams` разрешить работу на предзаполненном `procParamsCache` при `db == nil` (guard срабатывает только когда нет ни БД, ни кэша).
- Не менять продовое поведение правил и состав каталога; изменение затрагивает тестовый слой и точечные nil-guard/teстовые seam-правки в lookup-слое.

## Capabilities

### New Capabilities

- `review/test-coverage`: обязательное unit-покрытие каждого правила каталога review положительным и отрицательным кейсами без зависимости от БД; правила, требующие метаданных индекса/каталога, тестируются через детерминированные double-объекты.

### Modified Capabilities

<!-- Нет: поведение правил не меняется, меняется только верификация. -->

## Impact

- Тесты: `internal/review/*_test.go` (новые и расширенные), тестовые хелперы в `internal/review`.
- `internal/review/review_lookup.go`: nil-guard в `findAPITableNames` и `lookupProcedureCreateFiles` (только тестовый seam; при не-nil `db` поведение не меняется).
- `internal/review/runner.go`: nil-guard в `cachedFindColumnDefinitionType` (cache miss без БД → «тип неизвестен»).
- `internal/review/review_rules.go`: `checkDatatypeExecParams` не выходит сразу при `db == nil`, если задан `procParamsCache` (тестовый seam).
- Код правил (`internal/review/review_rules.go`, `catalog.go`, `types.go`) не меняется, кроме случаев, когда тест выявляет дефект — тогда исправление оформляется отдельно.
- Не затрагивает БД, CLI, MCP и формат findings.
