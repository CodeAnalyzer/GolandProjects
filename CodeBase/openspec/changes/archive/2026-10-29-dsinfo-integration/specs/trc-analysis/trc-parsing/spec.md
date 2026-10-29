# Дельта: trc-parsing — enrichment с профилем

## MODIFIED Requirements

### Requirement: Enrichment из индекса

Система SHALL обогащать события и агрегации из индекса: сопоставление имени
процедуры с `sql_procedures` для получения пути к файлу и номеров строк.
При заданном контексте профиля dsinfo (цепочка разрешения —
`dsinfo-analysis/dsinfo-resolution`) enrichment SHALL выбирать копию
процедуры с приоритетом продуктов и линий профиля (ось `profile_match` после
`not_generated`), а результат SHALL дополнительно содержать `product` и
`branch_used` — `branch_key` выбранной копии и `branch_requested`. Если выбранная линия не входит в линии
профиля, результат SHALL содержать предупреждение о возможном различии
кода (вычисляется по фактическим элементам ответа).

#### Scenario: Enrichment процедуры

- **GIVEN** .trc файл с событием, где procedure = `MyProc`, и проиндексированный проект
- **WHEN** выполняется enrichment
- **THEN** результат содержит путь к файлу процедуры и номера строк из индекса

#### Scenario: Enrichment с профилем

- **GIVEN** `MyProc` в develop и в `fa-contracts-11-01-7`; профиль задаёт линию `fa-contracts-11-01-7`
- **WHEN** выполняется enrichment трейса с контекстом профиля
- **THEN** источник — копия из `fa-contracts-11-01-7`, результат содержит `product = fa-contracts` и `branch_used = fa-contracts-11-01-7`
