# Scripting Parsing

## Purpose

Парсинг JavaScript-файлов (.js) и SMF-сценариев (.smf): извлечение JS-функций, констант, вызовов процедур, SQL-запросов и SMF-инструментов (модели Ф.О., состояния, действия, счета) со встроенным JavaScript.

## Requirements

### Requirement: Извлечение JS-функций и констант

Система SHALL извлекать из JS-файлов определения функций (с именем, файлом, строкой) и констант для unified symbols index.

#### Scenario: JS-функция

- **GIVEN** JS-файл с `function OnClick() { ... }`
- **WHEN** выполняется индексация файла
- **THEN** функция `OnClick` сохранена в `js_functions` с указанием файла и строки
- **AND** функция доступна через `query symbol --name OnClick --type js_function`

#### Scenario: JS-константа

- **GIVEN** JS-файл с `const MAX_LIMIT = 100;`
- **WHEN** выполняется индексация файла
- **THEN** константа `MAX_LIMIT` сохранена и доступна через `query symbol --name MAX_LIMIT --type constant`

### Requirement: Извлечение вызовов процедур из JS

Система SHALL извлекать вызовы хранимых процедур (`exec`, `ExecProc`) из JS-кода и создавать relations `calls_procedure` от `js_function` к `sql_procedure`.

#### Scenario: Вызов процедуры из JS-функции

- **GIVEN** JS-функция `OnClick`, содержащая `exec MyProc @Param = 1`
- **WHEN** выполняется индексация и постобработка
- **THEN** в графе связей создана relation `calls_procedure` от `js_function:OnClick` к `sql_procedure:MyProc`

### Requirement: Извлечение SQL-запросов из JS

Система SHALL извлекать SQL-запросы из JS-кода (строковые литералы с SQL) и сохранять их в `query_fragments` с связью к родительской JS-функции.

#### Scenario: Встроенный SQL в JS

- **GIVEN** JS-функция, содержащая строковый литерал с SQL-запросом
- **WHEN** выполняется индексация файла
- **THEN** SQL-фрагмент сохранён в `query_fragments` с указанием родительской JS-функции

### Requirement: Извлечение SMF-инструментов

Система SHALL извлекать из SMF-файлов модели Ф.О. (инструменты) с их именем, типом сценария (`instrument_model`, `mass_operation`, и др.), brief (краткое название) и встроенным JavaScript.

#### Scenario: SMF-инструмент

- **GIVEN** SMF-файл с инструментом `CreditMassOperation` типа `mass_operation`
- **WHEN** выполняется индексация файла
- **THEN** инструмент `CreditMassOperation` сохранён в `smf_instruments` с типом `mass_operation`
- **AND** доступен через `query smf-instrument --name CreditMassOperation`

#### Scenario: Поиск SMF по типу

- **GIVEN** проиндексированные SMF-файлы с разными типами сценариев
- **WHEN** выполняется `query smf-type --type mass_operation`
- **THEN** возвращены все инструменты с типом `mass_operation`

### Requirement: SMF встроенный JavaScript

Система SHALL извлекать встроенный JavaScript из SMF-файлов и индексировать его как JS-функции с связью к родительскому SMF-инструменту. SMF include-файлы загружаются по нескольким путям (`buildIncludeCandidates`), и SMF-переменные извлекаются из include через `extractSMFVarsFromIncludesWithBasePath` для разрешения ссылок между SMF-инструментом и его include-зависимостями.

#### Scenario: Встроенная JS-функция в SMF

- **GIVEN** SMF-файл со встроенным JS-кодом, содержащим функцию `onExecute`
- **WHEN** выполняется индексация файла
- **THEN** функция `onExecute` сохранена в `js_functions` с указанием родительского SMF-файла

#### Scenario: SMF include-файл

- **GIVEN** SMF-файл с include-ссылкой на `common.smf` в одном из путей поиска
- **WHEN** выполняется индексация через `buildIncludeCandidates` + `extractSMFVarsFromIncludesWithBasePath`
- **THEN** переменные из `common.smf` разрешены и доступны в контексте SMF-инструмента

### Requirement: Анализ использования объектов внутри JS-функций (`assignParentObjects`)

Система SHALL анализировать использование объектов внутри JS-функций (`assignParentObjects`) и устанавливать связь «объект → функция» для последующего построения графа вызовов и использования сущностей.

#### Scenario: Использование объекта в JS-функции

- **GIVEN** JS-функция `OnClick`, использующая объект `grData` (компонент формы)
- **WHEN** выполняется индексация и `assignParentObjects`
- **THEN** установлена связь между `OnClick` и объектом `grData` для дальнейшего построения relations

### Requirement: Batch-резолв диапазонов JS-функций по файлу

Система SHALL при постобработке JS-call relations резолвить ID и диапазоны JS-функций по файлу одним batch-запросом `FindJSFunctionIDRangesByFile` (возвращает `[]JSFuncRange` с `FuncID`, `LineStart`, `LineEnd`), что устраняет N+1 запросов при определении, какой JS-функции принадлежит call на конкретной строке.

#### Scenario: Резолв 100 JS-функций одним batch-запросом

- **GIVEN** JS-файл с 100 функциями и pending-список из 500 JS-вызовов процедур
- **WHEN** выполняется `postProcessJSCallRelations`
- **THEN** все 100 функций резолвятся одним batch-запросом `FindJSFunctionIDRangesByFile`
- **AND** для каждого call определяется принадлежность к функции по диапазону строк без N+1 запросов

## Related code

- `internal/parser/js/` — JS-парсер: functions, constants, SQL extraction, `assignParentObjects`
- `internal/parser/smf/` — SMF-парсер: instruments, embedded JS, `buildIncludeCandidates`, `extractSMFVarsFromIncludesWithBasePath`
- `internal/indexer/indexer.go` — `parseJSFile`, `parseSMFFile`
- `internal/indexer/indexer_relations.go` — JS-call relations, `assignParentObjects`
- `internal/indexer/indexer_postprocess_fragments.go` — `postProcessJSCallRelations`
- `internal/store/db_insert_j.go` — batch insert для JS entities
- `internal/store/db_lookup_j.go` — `FindJSFunctionIDsByFile`, `FindJSFunctionIDRangesByFile`, `FindJSConstantIDsByFile`, `FindLatestSMFInstrumentIDByFile`
- `internal/indexer/indexer_postprocess_sql_calls.go` — извлечение вызовов процедур из JS

## Notes

- JS и SMF файлы читаются в кодировке CP1251
- SMF-инструменты индексируются с полями `instrument_name`, `brief`, `scenario_type`
- Вызовы процедур из JS создают relations с `source_type = js_function`
- SMF include-механизм: include-файлы загружаются по нескольким путям (`buildIncludeCandidates`), переменные извлекаются из include через `extractSMFVarsFromIncludesWithBasePath`
- `assignParentObjects` строит связь «объект → функция» для последующего построения relations
- Batch-резолв `FindJSFunctionIDRangesByFile` устраняет N+1 при определении принадлежности call-а к JS-функции по строке
