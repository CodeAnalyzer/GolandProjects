# Дельта: rti-parsing — enrichment с профилем

## MODIFIED Requirements

### Requirement: Enrichment из индекса

Система SHALL обогащать вызовы процедур из RTI-лога данными из индекса:
путь к файлу процедуры, номера строк, module_id. При заданном контексте
профиля dsinfo (цепочка разрешения — `dsinfo-analysis/dsinfo-resolution`)
enrichment SHALL выбирать копию процедуры с приоритетом продуктов и линий
профиля (ось `profile_match` после `not_generated`), а результат SHALL
дополнительно содержать `product` и `branch_used` — `branch_key` выбранной
копии и `branch_requested`. Если выбранная линия не входит в линии
профиля, результат SHALL содержать предупреждение о возможном различии
кода (вычисляется по фактическим элементам ответа).

#### Scenario: Enrichment вызова

- **GIVEN** RTI-файл с вызовом `MyProc` и проиндексированный проект, где `MyProc` определена в `path/to/MyProc.sql`
- **WHEN** выполняется `codebase rti details file.rti --proc MyProc`
- **THEN** результат содержит путь к файлу `path/to/MyProc.sql` и номера строк

#### Scenario: Enrichment с профилем

- **GIVEN** `MyProc` в develop и в `fa-contracts-11-01-7` с разными телами; профиль задаёт линию `fa-contracts-11-01-7`
- **WHEN** выполняется `codebase rti details file.rti --proc MyProc --dsinfo <id>`
- **THEN** источник — копия из `fa-contracts-11-01-7`, результат содержит `product = fa-contracts` и `branch_used = fa-contracts-11-01-7`

#### Scenario: Линия профиля не проиндексирована

- **GIVEN** `MyProc` есть только в develop, а профиль задаёт непроиндексированную линию продукта
- **WHEN** выполняется enrichment с профилем
- **THEN** источник — develop-копия, `branch_used = develop`
- **AND** результат содержит предупреждение о возможном различии кода
