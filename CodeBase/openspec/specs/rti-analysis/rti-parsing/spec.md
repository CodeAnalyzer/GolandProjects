# RTI Parsing

## Purpose

Парсинг RTI-трейс логов Diasoft 5NT (.rti и .hrti): извлечение серверных вызовов процедур (Enter/Exit), параметров, контрольных точек, кодов возврата, бизнес-лог блоков, дампов таблиц, enrichment из индекса. Поддержка HRTI (Hashed RTI) с авто-детекцией и декодированием TDsHash.

## Requirements

### Requirement: Парсинг серверных вызовов

Система SHALL парсить RTI-логи и извлекать серверные вызовы процедур с их иерархией (SPID, nest_level), временами (Enter/Exit, elapsed_ms), кодами возврата (ret_val) и параметрами.

#### Scenario: Простой вызов

- **GIVEN** RTI-файл с вызовом `Enter: MyProc @Param = 1` и `Exit: MyProc ret_val = 0`
- **WHEN** выполняется `codebase rti parse file.rti`
- **THEN** вызов сохранён в `rti_calls` с параметром `@Param = 1`, elapsed_ms и ret_val = 0

#### Scenario: Вложенные вызовы

- **GIVEN** RTI-файл с вызовом `ProcA` вызывающим `ProcB`
- **WHEN** выполняется парсинг
- **THEN** `ProcB` имеет nest_level на 1 больше, чем `ProcA`
- **AND** в дереве вызовов `ProcB` является дочерним узлом `ProcA`

### Requirement: Извлечение параметров вызовов

Система SHALL извлекать параметры каждого вызова процедуры из RTI-лога с их именами, значениями и типами данных.

#### Scenario: Параметры вызова

- **GIVEN** RTI-файл с вызовом `MyProc @Param1 = 1, @Param2 = 'test'`
- **WHEN** выполняется парсинг
- **THEN** параметры `@Param1 = 1` и `@Param2 = 'test'` сохранены в `rti_params`

### Requirement: Извлечение контрольных точек

Система SHALL извлекать контрольные точки (`M_BUSINESSLOG_CHECKPOINT`) с их label, timestamp и elapsed_ms.

#### Scenario: Checkpoint

- **GIVEN** RTI-файл с checkpoint `MyProc_Begin_1` в момент `16:59:03.500`
- **WHEN** выполняется парсинг
- **THEN** checkpoint сохранён в `rti_checkpoints` с label, timestamp и elapsed_ms

### Requirement: Извлечение бизнес-лог блоков

Система SHALL извлекать бизнес-лог блоки (`M_BUSINESSLOG_BLOCK_BEGIN`/`M_BUSINESSLOG_BLOCK_END`) с их именем, Enter/Exit временами и elapsed_ms.

#### Scenario: BLog блок

- **GIVEN** RTI-файл с `M_BUSINESSLOG_BLOCK_BEGIN('Основной блок')` и `M_BUSINESSLOG_BLOCK_END`
- **WHEN** выполняется `codebase rti blog file.rti --proc MyProc`
- **THEN** блок `Основной блок` возвращён с Enter/Exit временами и elapsed_ms

### Requirement: Извлечение дампов таблиц

Система SHALL извлекать дампы таблиц из бизнес-лога (`M_LOG_TABLE`/`M_LOG_TABLE_LISTID`) с именем таблицы, колонками (имя:тип) и строками данных.

#### Scenario: Дамп таблицы

- **GIVEN** RTI-файл с `M_LOG_TABLE('tAccrualData', 'ID:int, Amount:numeric')` и строками данных
- **WHEN** выполняется `codebase rti blog file.rti --proc MyProc`
- **THEN** дамп таблицы `tAccrualData` возвращён с колонками и строками

### Requirement: HRTI авто-детекция и декодирование

Система SHALL автоматически детектировать HRTI-формат по маркерам `6D6...6D6` (закодированные строковые значения) и декодировать все строковые поля через алгоритм TDsHash (XOR + mod 128 с алфавитом из 128 символов, ключи M1=3, M2=102).

#### Scenario: Авто-детекция HRTI

- **GIVEN** HRTI-файл с закодированными значениями `6D6...6D6` в параметрах
- **WHEN** выполняется парсинг
- **THEN** формат автоматически определён как HRTI
- **AND** все строковые поля декодированы (параметры, контексты ошибок, имена блоков BLog, ячейки таблиц)

#### Scenario: Декодирование параметра

- **GIVEN** HRTI-файл с параметром `6D6abc6D6` (закодированное имя процедуры)
- **WHEN** выполняется парсинг с авто-декодированием
- **THEN** параметр декодирован в читаемое русское имя процедуры

### Requirement: Разрешение кодов возврата

Система SHALL разрешать числовые коды возврата (ret_val) в текстовые сообщения через справочник `ds_return_codes`.

#### Scenario: Разрешение кода ошибки

- **GIVEN** RTI-файл с вызовом, имеющим `ret_val = 1001`
- **WHEN** выполняется `codebase rti errors file.rti`
- **THEN** возвращено текстовое описание ошибки из `ds_return_codes`

### Requirement: Прескрининг строк и пропуск plain M_LOG

Система SHALL применять прескрининг каждой строки RTI-лога по первому байту (`parser.go:199-203`) для быстрого отбрасывания нерелевантных строк (оптимизация 85–95% на типичном RTI). Строки `M_LOG` без Enter/Exit в BLog-секциях пропускаются (`parser.go:295-302`) — это plain log-сообщения, не содержащие structured-данных вызовов/блоков.

#### Scenario: Прескрининг нерелевантной строки

- **GIVEN** RTI-файл с большим объёмом строк, не относящихся к вызовам/блокам/checkpoint-ам
- **WHEN** выполняется парсинг с прескринингом по первому байту
- **THEN** нерелевантные строки отбрасываются без полной regex-обработки
- **AND** скорость парсинга выше на 85–95% по сравнению с full regex на каждой строке

#### Scenario: Пропуск plain M_LOG

- **GIVEN** RTI-файл с `M_LOG`-сообщениями внутри BLog-секции, не обрамлёнными Enter/Exit
- **WHEN** выполняется парсинг
- **THEN** такие plain log-сообщения пропускаются (не попадают в `rti_calls` и не создают «пустых» вызовов)

### Requirement: Справочник Module ID → Product Name

Система SHALL содержать полный справочник Module ID → Product Name (94 записи, `internal/rti/symbols.go`, `moduleIDMap`) и предоставлять функцию `ModuleNameByID(moduleID)` для разрешения числового module_id в имя продукта Diasoft в enrichment и сводках.

#### Scenario: Разрешение module_id

- **GIVEN** RTI-вызов с `module_id = 25`
- **WHEN** выполняется enrichment через `ModuleNameByID(25)`
- **THEN** возвращено имя продукта (например, «Credit»)

### Requirement: Настраиваемые пороги slow и top-N

Система SHALL предоставлять параметры для порога медленности и ограничения top-N медленных вызовов: `SetSlowThresholdMs`/`GetSlowThresholdMs` (по умолчанию из `[rti] slow_threshold_ms` конфига) и `SetTopSlowCount` (по умолчанию из `[rti] top_slow_count`). Эти параметры используются в `ExecuteSlow` для фильтрации и ограничения результата.

#### Scenario: Установка порога медленности

- **GIVEN** конфигурация с `[rti] slow_threshold_ms = 500`
- **WHEN** выполняется `codebase rti slow file.rti` без `--slow-ms`
- **THEN** `GetSlowThresholdMs()` возвращает 500, фильтруются вызовы с elapsed_ms >= 500

#### Scenario: Установка порога через CLI-флаг

- **GIVEN** вызов `codebase rti slow file.rti --slow-ms 1000`
- **WHEN** `ExecuteSlow` использует `p.ThresholdMs = 1000`
- **THEN** фильтруются вызовы с elapsed_ms >= 1000 (флаг перекрывает конфиг)

### Requirement: Enrichment из индекса

Система SHALL обогащать вызовы процедур из RTI-лога данными из индекса: путь к файлу процедуры, номера строк, module_id.

#### Scenario: Enrichment вызова

- **GIVEN** RTI-файл с вызовом `MyProc` и проиндексированный проект, где `MyProc` определена в `path/to/MyProc.sql`
- **WHEN** выполняется `codebase rti details file.rti --proc MyProc`
- **THEN** результат содержит путь к файлу `path/to/MyProc.sql` и номера строк

## Related code

- `internal/rti/parser.go` — `ParseFile`, `parseContent` (regex state machine, CP866/UTF8), авто-детект HRTI, прескрининг по первому байту, пропуск plain `M_LOG`, `SetSlowThresholdMs`/`GetSlowThresholdMs`/`SetTopSlowCount`
- `internal/rti/hrti.go` — `decodeHRTIString`, `isHRTIContent`, `DecodeHRTIResult`, TDsHash decoder
- `internal/rti/model.go` — `RTICall`, `RTIParam`, `RTICheckpoint`, `RTIBLogBlock`, `RTIBLogTable`, `RTISummary`
- `internal/rti/symbols.go` — `moduleIDMap` (94 записи), `ModuleNameByID`
- `internal/rti/enrich.go` — `EnrichCalls`, `ProcedureLookup` interface
- `internal/parser/retcode/retcode.go` — `HasReturnCodes`, `Parse` (retcode-прескрининг)

## Notes

- RTI-файлы читаются с авто-детекцией кодировки (CP866/UTF8)
- HRTI декодирование выполняется пост-обработкой (после полного парсинга, не на лету)
- Числовые значения и NULL в HRTI не кодируются и проходят как есть
- Разрешение коллизии ASCII↔Русский в TDsHash: prefer Russian для букв, ASCII для пробела и знаков
- Execution-слой `internal/rtisvc/runtime.go` — общая точка входа для CLI (`cmd/rti.go`) и MCP-инструментов `codebase_rti_*`; устраняет дублирование оркестрации. Парсинг/анализ специфицированы здесь; сохранение сессий и fallback при недоступной БД — в `rti-analysis/rti-storage`; транспорт MCP — в `mcp-server/mcp-transport-tools`.
- Прескрининг по первому байту — основная оптимизация парсинга (85–95% прироста скорости на типичных RTI); без неё парсер упирался бы в regex на каждой строке
- Справочник `moduleIDMap` (94 записи) зашит в коде (`symbols.go`), а не в БД — соответствует набору продуктов Diasoft 5NT
