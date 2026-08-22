# PAS/DFM Parsing

## Purpose

Парсинг Pascal-файлов (.pas) и Delphi Form-файлов (.dfm): извлечение units, classes, methods, fields, forms, components, captions, SQL-фрагментов и прямых ссылок между PAS-классами и DFM-формами.

## Requirements

### Requirement: Извлечение Pascal units и classes

Система SHALL извлекать из PAS-файлов определения units, классов (с указанием parent class), методов (с signature и visibility) и полей классов.

#### Scenario: Класс с методами

- **GIVEN** PAS-файл с `unit MyUnit; type TMyForm = class(TForm) procedure Execute; function GetData: string; end;`
- **WHEN** выполняется индексация файла
- **THEN** unit `MyUnit` сохранён в `pas_units`
- **AND** класс `TMyForm` сохранён в `pas_classes` с parent class `TForm`
- **AND** методы `Execute` и `GetData` сохранены в `pas_methods` с указанием visibility

### Requirement: Извлечение DFM forms и components

Система SHALL извлекать из DFM-файлов определения форм (с `form_name`, `form_class`, `caption`) и компонентов (с `component_name`, `component_type`, `caption`, `parent_name`).

#### Scenario: Форма с компонентами

- **GIVEN** DFM-файл с формой `AimCnt` класса `TAimCnt`, содержащей компонент `dlName: TLabel` с caption "Наименование"
- **WHEN** выполняется индексация файла
- **THEN** форма `AimCnt` сохранена в `dfm_forms` с class `TAimCnt` и caption
- **AND** компонент `dlName` сохранён в `dfm_components` с type `TLabel` и caption "Наименование"

### Requirement: Связь PAS-классов с DFM-формами

Система SHALL автоматически связывать PAS-классы с соответствующими DFM-формами по совпадению имени класса и имени формы, устанавливая прямую ссылку `dfm_form_id` в `pas_classes`.

#### Scenario: Автоматическая связь

- **GIVEN** PAS-файл с классом `TAimCnt` и DFM-файл с формой `AimCnt` класса `TAimCnt`
- **WHEN** выполняется постобработка индексации
- **THEN** в `pas_classes` для класса `TAimCnt` установлено `dfm_form_id` ссылающееся на форму `AimCnt`

### Requirement: Связь PAS-полей с DFM-компонентами

Система SHALL автоматически связывать поля PAS-классов с DFM-компонентами по совпадению имени поля и имени компонента, устанавливая прямую ссылку `dfm_component_id` в `pas_fields`.

#### Scenario: Привязка компонента к полю

- **GIVEN** PAS-класс `TMyForm` с полем `dlName: TLabel` и DLM-форма `MyForm` с компонентом `dlName` типа `TLabel`
- **WHEN** выполняется постобработка индексации
- **THEN** в `pas_fields` для поля `dlName` установлено `dfm_component_id` ссылающееся на компонент `dlName`

### Requirement: SQL-фрагменты в PAS-коде

Система SHALL извлекать SQL-фрагменты из PAS-методов (строковые литералы с SQL, встроенные запросы) и сохранять их в `query_fragments` с связью к родительскому методу. Строковые выражения и константы в PAS (concatenation, `const`-присваивания) разрешаются в итоговую SQL-строку через `resolvePasStringExpr` / `trackStringAssignment`, чтобы фрагмент попадал в `query_fragments` как раскрытый SQL, а не как сырая константа.

#### Scenario: Встроенный SQL-запрос

- **GIVEN** PAS-метод, содержащий строковый литерал `'SELECT * FROM tContract WHERE ID = :ID'`
- **WHEN** выполняется индексация файла
- **THEN** SQL-фрагмент сохранён в `query_fragments` с указанием родительского метода

#### Scenario: SQL из строковой константы

- **GIVEN** PAS с `const SQL_GET = 'SELECT * FROM tContract'` и использованием `SQL_GET` в методе
- **WHEN** выполняется индексация
- **THEN** `resolvePasStringExpr` разрешает `SQL_GET` в его значение, и SQL-фрагмент сохранён как `SELECT * FROM tContract`

### Requirement: Batch-резолв PAS↔DFM связей в постобработке

Система SHALL в постобработке (`indexer_postprocess_pas.go`) выполнять резолв PAS↔DFM связей батчами, а не построчными запросами: `FindLatestPASClassIDsByNames` (классы по именам), `FindLatestDFMFormIDsByClassNames` (формы по class-именам), `FindPASFieldDFMLinkCandidates` (кандидаты на link field↔component). Это устраняет N+1 запросов при больших проектах. Pending-сущности накапливаются во время индексации и резолвятся глобально после завершения.

#### Scenario: Резолв 200 PAS-классов одним batch-запросом

- **GIVEN** в pending накопилось 200 PAS-классов для связывания с DFM-формами
- **WHEN** выполняется `postProcessPASPending`
- **THEN** все 200 имён резолвятся в ID одним batch-запросом `FindLatestPASClassIDsByNames`
- **AND** отсутствуют построчные `SELECT ... WHERE class_name = ?` (нет N+1)

#### Scenario: Field↔component через кандидатов

- **GIVEN** проиндексированные PAS-поля и DFM-компоненты
- **WHEN** выполняется `FindPASFieldDFMLinkCandidates`
- **THEN** возвращены кандидаты на link (поле + компонент по совпадению имени) одним запросом
- **AND** для каждого кандидата устанавливается `dfm_component_id` в `pas_fields`

## Related code

- `internal/parser/pas/pas_parser.go` — `Parse`, извлечение units, classes, methods, fields, SQL fragments, `resolvePasStringExpr`, `trackStringAssignment`
- `internal/parser/dfm/` — DFM-парсер: forms, components, captions
- `internal/indexer/indexer_sql_pas.go` — `parsePASFile`, `parseDFMFile`
- `internal/indexer/indexer_postprocess_pas.go` — постобработка: `postProcessPASPending`, batch-резолв связей
- `internal/store/db_insert_pas.go` — batch insert для PAS entities
- `internal/store/db_insert_dfm.go` — batch insert для DFM entities
- `internal/store/db_lookup_pas.go` — `FindLatestPASClassIDsByNames`, `FindLatestDFMFormIDsByClassNames`, `FindPASFieldDFMLinkCandidates`, lookup + batch update DFM links

## Notes

- PAS-файлы читаются в кодировке CP1251
- DFM-файлы читаются в кодировке CP1251
- Связь PAS↔DFM устанавливается в постобработке после индексации всех файлов
- Batch-резолв связей (через `FindLatestPASClassIDsByNames` и т.п.) устраняет N+1 запросов при больших проектах
- `resolvePasStringExpr` / `trackStringAssignment` разрешают строковые выражения и константы в итоговый SQL-фрагмент, чтобы `query_fragments` содержали раскрытый SQL
