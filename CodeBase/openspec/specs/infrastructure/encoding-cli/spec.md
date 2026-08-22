# Encoding and CLI

## Purpose

Детекция кодировок CP866/CP1251/UTF8 с эвристическим выбором для legacy-форматов Diasoft 5NT. CLI-фреймворк на Cobra: команды init/update/query/review/rti/trc/stats/health/mcp, JSON envelope для machine-readable modes, логирование команд.

## Requirements

### Requirement: Детекция кодировок

Система SHALL автоматически детектировать кодировку файлов (CP866, CP1251, UTF-8) через эвристику `DetectFromBytes` для legacy-форматов, включая TPR и препроцессированные .t01. Алгоритм `DetectFromBytes` работает по 4 шагам: (1) если нет байт `> 0x7F` → ASCII (совместим с CP866); (2) если `utf8.Valid(data)` → UTF-8; (3) «почти UTF-8»: ≥80% высоких байт входят в валидные многобайтные UTF-8 последовательности (`isLikelyUTF8`) → UTF-8 (для RTI-логов с единичными CP866-артефактами); (4) эвристика по маркерным диапазонам: `cp866Score` (байты 0x80–0x9F, заглавные А-Я в CP866) против `cp1251Score` (байты 0xC0–0xDF, заглавные А-Я в CP1251) — побеждает больший счёт; при равенстве — CP866 (по умолчанию для Diasoft SQL).

#### Scenario: Детекция CP866

- **GIVEN** SQL-файл в кодировке CP866 с кириллическими комментариями
- **WHEN** выполняется чтение файла через `DetectFromBytes`
- **THEN** кодировка определена как CP866

#### Scenario: Детекция UTF-8

- **GIVEN** файл в кодировке UTF-8 с BOM
- **WHEN** выполняется чтение файла
- **THEN** кодировка определена как UTF-8

#### Scenario: «Почти UTF-8» с единичными артефактами

- **GIVEN** RTI-лог, преимущественно в UTF-8, но с единичными CP866-байтами (ё/Ё или смешанная кодировка)
- **WHEN** `DetectFromBytes` проверяет `isLikelyUTF8(data)`
- **THEN** если ≥80% высоких байт входят в валидные UTF-8 последовательности, кодировка определена как UTF-8

#### Scenario: Эвристика CP866 vs CP1251 при равенстве

- **GIVEN** файл с одинаковым числом байт в диапазонах 0x80–0x9F и 0xC0–0xDF
- **WHEN** `DetectFromBytes` сравнивает `cp866Score` и `cp1251Score`
- **THEN** при равенстве счёта возвращается CP866 (default для Diasoft SQL)

### Requirement: Детекция кодировки XML

Система SHALL определять кодировку XML-файлов через `DetectXMLEncoding(data)` по двум шагам: (1) ищет `encoding="..."` в XML declaration (первые 200 байт) через `xmlEncodingRegexp`, поддерживает `windows-1251`/`cp1251`/`windows1251` → WIN1251, `utf-8`/`utf8` → UTF8, `cp866`/`ibm866` → CP866; (2) если declaration нет или encoding не указан — проверяет `utf8.Valid(data)`, иначе предполагает WIN1251 (Diasoft heuristic для XML).

#### Scenario: XML с явной кодировкой

- **GIVEN** XML-файл с `<?xml version="1.0" encoding="windows-1251"?>`
- **WHEN** `DetectXMLEncoding` парсит declaration
- **THEN** кодировка определена как WIN1251

#### Scenario: XML без declaration, валидный UTF-8

- **GIVEN** XML-файл без `<?xml ...?>` declaration, контент валиден как UTF-8
- **WHEN** `DetectXMLEncoding` проверяет `utf8.Valid(data)`
- **THEN** кодировка определена как UTF8

#### Scenario: XML без declaration, невалидный UTF-8

- **GIVEN** XML-файл без declaration, контент не валиден как UTF-8
- **WHEN** `DetectXMLEncoding` откатывается к Diasoft heuristic
- **THEN** кодировка определена как WIN1251

### Requirement: Кодировки по типам файлов

Система SHALL использовать следующие кодировки по умолчанию для файлов Diasoft 5NT: SQL (.sql) и H (.h) — CP866; PAS (.pas), DFM (.dfm), INC (.inc), SMF (.smf), JS (.js) — CP1251; TPR и RPT — авто-детекция.

#### Scenario: Чтение SQL в CP866

- **GIVEN** SQL-файл в кодировке CP866
- **WHEN** выполняется индексация
- **THEN** файл прочитан в CP866 и кириллические символы корректно декодированы

#### Scenario: Чтение PAS в CP1251

- **GIVEN** PAS-файл в кодировке CP1251
- **WHEN** выполняется индексация
- **THEN** файл прочитан в CP1251 и кириллические символы корректно декодированы

### Requirement: CLI-команды

Система SHALL предоставлять CLI-команды: `init` (полная индексация), `update` (инкрементальное обновление), `query` (запросы к индексу), `review` (проверка SQL), `rti` (RTI-анализатор), `trc` (TRC-анализатор), `stats` (статистика), `health` (проверка готовности), `mcp` (MCP-сервер).

#### Scenario: Список команд

- **GIVEN** установленный `codebase.exe`
- **WHEN** выполняется `codebase --help`
- **THEN** отображены все доступные команды с описанием

### Requirement: JSON envelope для machine-readable modes

Система SHALL предоставлять machine-readable режимы `--json`, `--summary`, `--ndjson` для query/stats/health команд, возвращающие структурированный JSON с полями `success`, `format_version`, `command`, `count`, `items`, `meta`.

#### Scenario: JSON envelope

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `codebase stats --json`
- **THEN** результат в JSON envelope с `success = true`, `format_version = "1.0"`, `command = "stats"`

#### Scenario: NDJSON поток

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `codebase query symbol --name API --ndjson`
- **THEN** каждый результат возвращён как отдельная JSON-строка (без envelope)

### Requirement: Гарантии machine-readable modes

Система SHALL гарантировать для machine-readable modes: подавление banner/output noise, structured JSON format для ошибок, пустые результаты как `[]` (не `null`), `format_version` зафиксирован как `1.0`.

#### Scenario: Пустой результат

- **GIVEN** проиндексированный проект без процедур с именем `NonExistent`
- **WHEN** выполняется `codebase query symbol --name NonExistent --json`
- **THEN** результат содержит `"items": []` (не `null`)

#### Scenario: Ошибка в JSON формате

- **GIVEN** БД недоступна
- **WHEN** выполняется `codebase stats --json`
- **THEN** результат содержит `"success": false` и описание ошибки в JSON

### Requirement: Логирование CLI-команд

Система SHALL логировать все CLI-команды в файл `codebase_YYYYMMDD.log` (один файл на день) с информацией: started_at, command, duration, status, error. Логирование включено по умолчанию.

#### Scenario: Логирование команды

- **GIVEN** включённое логирование (`logging.command_enabled = true`)
- **WHEN** выполняется `codebase init`
- **THEN** в файл `codebase_YYYYMMDD.log` записана информация о команде: started_at, command, duration, status

### Requirement: Логирование ошибок индексатора

Система SHALL записывать ошибки индексатора в отдельный log-файл `indexer_errors_YYYYMMDD_HHMMSS.log` на каждый запуск, с указанием пути файла, на котором произошла ошибка.

#### Scenario: Ошибка индексации

- **GIVEN** файл с синтаксической ошибкой, вызывающей panic в парсере
- **WHEN** выполняется `codebase init`
- **THEN** ошибка записана в `indexer_errors_YYYYMMDD_HHMMSS.log` с указанием пути файла

### Requirement: Health checks

Система SHALL предоставлять команду `health` для проверки readiness: config (загрузка конфига), database (подключение к БД), schema (наличие таблиц), index readiness (completed scan run). Проверка `index readiness` выполняется через `db.HasCompletedInit(ctx)` (наличие `scan_runs.status = completed`). Реализация — `systemsvc.ExecuteHealth(db)` (общий execution-слой для CLI `cmd/health.go` и MCP-инструмента `codebase_health`).

#### Scenario: Все проверки пройдены

- **GIVEN** сконфигурированный проект с БД и завершённым scan run
- **WHEN** выполняется `codebase health`
- **THEN** статус `ok` для всех проверок: config, database, schema, index

#### Scenario: БД недоступна

- **GIVEN** конфигурация с недоступной БД
- **WHEN** выполняется `codebase health`
- **THEN** статус `database: fail`, остальные проверки могут быть `ok` или `skip`

#### Scenario: БД есть, но инициализация не завершена

- **GIVEN** сконфигурированный проект с БД и схемой, но без завершённого `scan_runs`
- **WHEN** выполняется `codebase health` (через `systemsvc.ExecuteHealth`)
- **THEN** общий статус `degraded`, проверка `index: missing` с сообщением `"no completed scan run found"`

### Requirement: Stats через systemsvc

Система SHALL предоставлять команду `stats` для агрегированной статистики индекса (через `systemsvc.ExecuteStats(db)`, общий execution-слой для CLI `cmd/stats.go` и MCP-инструмента `codebase_stats`). Реализация читает конфиг через `config.Get()` (возвращает `ErrConfigNotLoaded`, если не загружен) и `db.GetStats(ctx)`.

#### Scenario: Stats из CLI

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `codebase stats --json`
- **THEN** возвращена статистика в JSON envelope через `systemsvc.ExecuteStats`

#### Scenario: Stats через MCP

- **GIVEN** запущенный MCP-сервер и проиндексированный проект
- **WHEN** вызывается `codebase_stats`
- **THEN** возвращена та же статистика (без envelope) через тот же `systemsvc.ExecuteStats`

### Requirement: Sentinel-ошибки (internal/errs)

Система SHALL определять пакетные sentinel-ошибки в `internal/errs` для типовых отказов системы: `ErrConfigNotLoaded`, `ErrDBConnect`, `ErrSchemaInit`, `ErrQueryFailed`, `ErrReviewFailed`, `ErrStatsFailed`, `ErrHealthCheckFailed`, `ErrNoRelationFilters`. Вызывающий код оборачивает фактические ошибки через `fmt.Errorf("%w: %w", sentinel, err)`, что позволяет потребителям делать `errors.Is(err, errs.ErrXxx)` для классификации без привязки к тексту. Доработка «sentinel-errors-refactor» — устраняет string-matching в обработке ошибок.

#### Scenario: Классификация ошибки через errors.Is

- **GIVEN** `systemsvc.ExecuteStats` возвращает `fmt.Errorf("%w: %w", errs.ErrStatsFailed, dbErr)`
- **WHEN** вызывающий код проверяет `errors.Is(err, errs.ErrStatsFailed)`
- **THEN** проверка возвращает `true` независимо от текста `dbErr`

#### Scenario: Не загружен конфиг

- **GIVEN** запуск без успешно загруженного `codebase.toml`
- **WHEN** вызывается `systemsvc.ExecuteHealth` или `ExecuteStats`
- **THEN** возвращается `errs.ErrConfigNotLoaded` (без обёртывания)

#### Scenario: Отсутствуют фильтры relations

- **GIVEN** вызов `query relations` без фильтров (`--source-type`, `--target-type` и т.п.)
- **WHEN** `querysvc` проверяет наличие фильтров
- **THEN** возвращается `errs.ErrNoRelationFilters`

## Related code

- `internal/encoding/encoding.go` — `DetectFromBytes` (4-шаговая эвристика), `isLikelyUTF8` (80% порог), `DetectXMLEncoding`, `xmlEncodingRegexp`, `DetectEncoding`, `ReadFile`, `ConvertToUTF8`, `DecodeBytes`
- `internal/errs/errs.go` — пакетные sentinel-ошибки (`ErrConfigNotLoaded`, `ErrDBConnect`, `ErrSchemaInit`, `ErrQueryFailed`, `ErrReviewFailed`, `ErrStatsFailed`, `ErrHealthCheckFailed`, `ErrNoRelationFilters`)
- `internal/systemsvc/runtime.go` — `ExecuteHealth`, `ExecuteStats` (общий execution-слой для CLI и MCP, использует sentinel-ошибки и `HasCompletedInit`)
- `cmd/root.go` — корневая команда, bootstrap CLI, логирование
- `cmd/init.go` — команда init
- `cmd/update.go` — команда update
- `cmd/query.go` — регистрация query-флагов и подкоманд
- `cmd/query_execution.go` — выполнение query и форматирование вывода
- `cmd/review.go` — команда review
- `cmd/rti.go` — команда rti
- `cmd/trc.go` — команда trc
- `cmd/stats.go` — команда stats (делегирует в `systemsvc.ExecuteStats`)
- `cmd/health.go` — команда health (делегирует в `systemsvc.ExecuteHealth`)
- `cmd/mcp.go` — команда mcp

## Notes

- SQL и H файлы читаются в CP866; PAS, DFM, INC, JS, SMF — в CP1251
- TPR и RPT файлы используют авто-детекцию кодировки через `DetectFromBytes`
- XML-файлы (DSArchitect XML и т.п.) используют `DetectXMLEncoding` (declaration → UTF8 validity → WIN1251 fallback)
- `format_version` зафиксирован как `1.0` для всех machine-readable modes
- Лог-файлы команд: `codebase_YYYYMMDD.log` (один на день)
- Лог-файлы ошибок индексатора: `indexer_errors_YYYYMMDD_HHMMSS.log` (на каждый запуск)
- Конфиг ищется рядом с executable, а не в текущем рабочем каталоге
- `systemsvc` — общий execution-слой для `health`/`stats` (CLI + MCP), закреплён за этой capability (отдельная capability `infrastructure/health-stats` НЕ заводится — health checks и так описаны здесь). Аналогично `querysvc`/`reviewsvc`/`rtisvc`/`trcsvc`, устраняет дублирование оркестрации между CLI и MCP
- `systemsvc.ExecuteHealth`/`ExecuteStats` на текущий момент используют `context.Background()` (не принимают ctx) — дешёвые синхронные операции, отмена не критична (см. `mcp-transport-tools`)
- Sentinel-ошибки `internal/errs` используются по всему коду для классификации через `errors.Is` (доработка «sentinel-errors-refactor») — устраняет string-matching в обработке ошибок
