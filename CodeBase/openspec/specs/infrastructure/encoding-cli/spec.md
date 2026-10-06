# Encoding and CLI

## Purpose

Детекция кодировок CP866/CP1251/UTF8 с эвристическим выбором для legacy-форматов Diasoft 5NT. CLI-фреймворк на Cobra: команды init/update/query/review/rti/trc/stats/health/mcp, JSON envelope для machine-readable modes, логирование команд.

## Requirements

### Requirement: Детекция кодировок

Система SHALL автоматически детектировать кодировку файлов (CP866, CP1251, UTF-8) через эвристику `DetectFromBytes` для legacy-форматов, включая TPR и препроцессированные .t01. Алгоритм `DetectFromBytes` работает по 4 шагам: (1) если нет байт `> 0x7F` → кодировка prior (ASCII, декод — identity); (2) если `utf8.Valid(data)` → UTF-8; (3) «почти UTF-8»: ≥80% высоких байт входят в валидные многобайтные UTF-8 последовательности (`isLikelyUTF8`) → UTF-8 (для RTI-логов с единичными CP866-артефактами); (4) эвристика по маркерным диапазонам: `cp866Score` (байты 0x80–0x9F, заглавные А-Я в CP866, редкие спецсимволы в CP1251) против `cp1251Score` (байты 0xC0–0xDF, заглавные А-Я в CP1251, псевдографика в CP866; плюс артефакт-набор 0xF2–0xFF, где в CP1251 строчные р–я, а в CP866 украинские буквы и спецруны, кроме легитимных в CP866 № 0xFC и ° 0xF8). Побеждает бо́льший счёт; при равенстве или слабом сигнале — prior вызывающего. Вызов без prior (`DetectFromBytes(data)`) SHALL эквивалентен prior-у CP866 — существующие потребители (RTI, review) не меняют поведение. Сигнатура mojibake точна: байты CP1251-кириллицы при CP866-декоде дают руны U+2500–U+259F и украинские буквы, которые не встречаются в легитимном русском тексте — поэтому наличие артефактных байт голосует за CP1251, а не приводит к перекодировке по одному совпадению.

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
- **WHEN** `DetectFromBytes(data)` вызывается без prior
- **THEN** при равенстве счёта возвращается CP866 (default для Diasoft SQL, поведение существующих потребителей RTI/review не меняется)

#### Scenario: Prior расширения при ничьей

- **GIVEN** PAS-файл (карта расширений даёт prior WIN1251) с равными cp866Score и cp1251Score
- **WHEN** детекция вызывается с prior WIN1251
- **THEN** возвращается WIN1251

#### Scenario: Артефакт-набор перевешивает заглавные

- **GIVEN** SQL-файл в CP1251 со строчным русским текстом (байты 0xC0–0xDF частично отсутствуют, но 0xF2–0xFF — строчные т–я — многочисленны) и парой байт 0x80–0x9F
- **WHEN** выполняется детекция по содержимому
- **THEN** cp1251Score превосходит cp866Score, кодировка определена как WIN1251

#### Scenario: Легитимная псевдографика в CP866 не переворачивает детекцию

- **GIVEN** CP866-файл с рамками (байты 0xC0–0xDF) в комментариях и обильной кириллицей в диапазонах 0x80–0x9F / 0xA0–0xAF
- **WHEN** выполняется детекция по содержимому
- **THEN** cp866Score превосходит cp1251Score, кодировка определена как CP866

#### Scenario: ASCII-файл возвращает prior

- **GIVEN** файл без байт `> 0x7F` и prior WIN1251
- **WHEN** выполняется детекция по содержимому
- **THEN** возвращается prior (WIN1251), декодирование — identity

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

Система SHALL использовать карту расширений как **prior** (предположение по умолчанию и tie-breaker), а фактическую кодировку single-byte legacy-форматов — SQL (.sql), H (.h), TPR (.tpr), PAS (.pas), INC (.inc), JS (.js), SMF (.smf), DFM (.dfm), RPT (.rpt) — определять по содержимому через детекцию из требования «Детекция кодировок»: валидный/«почти валидный» UTF-8 с кириллицей → UTF-8; иначе счёт маркерных диапазонов CP866 vs CP1251 с победой по счёту и prior-ом при ничьей/слабом сигнале. Детектированная кодировка используется для декодирования и сохраняется в `files.encoding`. Форматы с собственной семантикой кодировки не меняются: XML — `DetectXMLEncoding`; MD (.md) — авто-детекция с приоритетом UTF-8 (openspec-артефакты финпродуктов хранятся в UTF-8, но встречаются legacy-файлы в CP1251): BOM/валидность UTF-8 → UTF-8, иначе CP1251 fallback; YAML (.yaml) — UTF-8; T01 (.t01) — CP866 (генерируемые препроцессором копии).

#### Scenario: Чтение SQL в CP866

- **GIVEN** SQL-файл в кодировке CP866 (prior карты)
- **WHEN** выполняется индексация
- **THEN** детекция по содержимому подтверждает CP866, файл прочитан в CP866 и кириллические символы корректно декодированы

#### Scenario: SQL в CP1251 детектируется по содержимому

- **GIVEN** SQL-файл модуля fa-administrator в кодировке CP1251 (карта даёт prior CP866)
- **WHEN** выполняется индексация
- **THEN** детекция по содержимому определяет WIN1251 (артефакт-набор и заглавные А-Я перевешивают)
- **AND** кириллические комментарии и header-описания процедур декодированы без mojibake
- **AND** в `files.encoding` сохранено WIN1251

#### Scenario: Чтение PAS в CP1251

- **GIVEN** PAS-файл в кодировке CP1251 (prior карты)
- **WHEN** выполняется индексация
- **THEN** детекция подтверждает CP1251, кириллические символы корректно декодированы

#### Scenario: PAS в CP866 детектируется по содержимому

- **GIVEN** PAS-файл, сохранённый в CP866 (отклонение от prior WIN1251)
- **WHEN** выполняется индексация
- **THEN** детекция по содержимому определяет CP866 (байты 0x80–0x9F перевешивают)
- **AND** кириллица декодирована корректно

#### Scenario: UTF-8 исходник среди single-byte форматов

- **GIVEN** SQL-файл, сохранённый в UTF-8 с кириллическими комментариями
- **WHEN** выполняется индексация
- **THEN** валидность UTF-8 определяется раньше CP866/CP1251-эвристики, файл декодирован как UTF-8

#### Scenario: Файл с невалидным байтом получает детерминированный фолбэк

- **GIVEN** SQL-файл, содержащий байт 0x98, не мапящийся ни в UTF-8, ни в CP1251
- **WHEN** выполняется индексация
- **THEN** файл не является валидным UTF-8, детекция уходит в счёт диапазонов и возвращает кодировку по счёту/prior без ошибки декодирования

#### Scenario: Markdown openspec-спеки в UTF-8

- **GIVEN** `spec.md` в UTF-8 (без BOM) с кириллическими требованиями
- **WHEN** выполняется индексация
- **THEN** файл корректно декодирован как UTF-8, кириллица требований сохранена без искажений

#### Scenario: Legacy-README в CP1251

- **GIVEN** markdown-файл, сохранённый в CP1251
- **WHEN** выполняется индексация
- **THEN** детектор распознаёт невалидный UTF-8 и читает файл как CP1251; кириллица декодирована корректно

#### Scenario: YAML-файл в UTF-8

- **GIVEN** YAML-файл в UTF-8
- **WHEN** выполняется индексация
- **THEN** файл прочитан в UTF-8 и кириллические символы корректно декодированы

#### Scenario: MD-файл в CP1251

- **GIVEN** MD-файл в CP1251
- **WHEN** выполняется индексация
- **THEN** детектор распознаёт невалидный UTF-8 и читает файл как CP1251; кириллица декодирована корректно

#### Scenario: T01-копия читается в CP866 без детекции

- **GIVEN** препроцессированный `.t01`-файл в CP866
- **WHEN** выполняется индексация
- **THEN** файл читается в CP866 по карте (генерируемая копия, детекция по содержимому не применяется)

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

Система SHALL предоставлять команду `stats` для агрегированной статистики индекса (через `systemsvc.ExecuteStats(db)`, общий execution-слой для CLI `cmd/stats.go` и MCP-инструмента `codebase_stats`). Реализация читает конфиг через `config.Get()` (возвращает `ErrConfigNotLoaded`, если не загружен) и `db.GetStats(ctx, lsaGeneration)`. Статистика SHALL читаться из сохранённого снапшота, обновляемого по завершении `init`/`update`, без пересчёта счётчиков на каждый вызов. Снапшот SHALL быть привязан к поколению LSA и переиспользоваться только при совпадении запрошенного поколения. Если снапшот отсутствует или его поколение не совпадает с запрошенным, система SHALL вычислить статистику живым подсчётом (фолбэк) и сохранить результат в снапшот для последующих вызовов. Метрики полнотекстового слоя спек MUST быть generation-осведомлёнными: `spec_vocab_terms` и `spec_embeddings` считаются по активному поколению LSA-модели (generation из sidecar-файла состояния модели, разрешаемого через конфигурацию); статистика также содержит `spec_lsa_generations` — число поколений, удерживаемых в БД. Если файл состояния модели недоступен или не содержит generation, счётчики `spec_vocab_terms`/`spec_embeddings` возвращаются без фильтра по поколению (полное число строк), а `spec_lsa_generations` — по фактическому числу различимых поколений.

#### Scenario: Stats из CLI

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `codebase stats --json`
- **THEN** возвращена статистика в JSON envelope через `systemsvc.ExecuteStats`

#### Scenario: Stats через MCP

- **GIVEN** запущенный MCP-сервер и проиндексированный проект
- **WHEN** вызывается `codebase_stats`
- **THEN** возвращена та же статистика (без envelope) через тот же `systemsvc.ExecuteStats`

#### Scenario: Чтение из снапшота

- **GIVEN** завершённый `init` или `update` сохранил снапшот статистики для активного поколения LSA
- **WHEN** выполняется `codebase stats --json`
- **THEN** возвращена статистика из снапшота
- **AND** счётчики таблиц не пересчитываются запросами `COUNT(*)`

#### Scenario: Смена поколения LSA — снапшот пересчитывается

- **GIVEN** снапшот посчитан для поколения LSA `gen-previous`, а активное поколение — `gen-active`
- **WHEN** выполняется `codebase stats --json`
- **THEN** статистика пересчитывается живым подсчётом для `gen-active`
- **AND** снапшот перезаписывается с поколением `gen-active`

#### Scenario: Фолбэк при отсутствии снапшота

- **GIVEN** БД без сохранённого снапшота статистики
- **WHEN** выполняется `codebase stats --json`
- **THEN** статистика вычисляется живым подсчётом и возвращается успешно
- **AND** результат сохранён в снапшот, так что следующий вызов читает его без пересчёта

#### Scenario: Счётчики LSA по активному поколению

- **GIVEN** в БД удерживаются два поколения LSA (активное 11 000 терминов и предыдущее 11 200 терминов), state-файл модели указывает на активное поколение
- **WHEN** выполняется `codebase stats --json`
- **THEN** `spec_vocab_terms` = 11 000 (термины активного поколения, не сумма 22 200), `spec_embeddings` = числу векторов активного поколения, `spec_lsa_generations` = 2

#### Scenario: Фолбэк при недоступном state

- **GIVEN** в БД есть строки `spec_vocab`/`spec_embeddings`, но sidecar-файл состояния LSA-модели отсутствует или повреждён
- **WHEN** выполняется `codebase stats --json`
- **THEN** статистика возвращается успешно: `spec_vocab_terms`/`spec_embeddings` — полное число строк без фильтра по поколению, `spec_lsa_generations` — фактическое число поколений; ошибка чтения state не заваливает команду

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

- `internal/encoding/encoding.go` — `DetectFromBytesWithPrior` (4-шаговая эвристика с prior: ASCII/UTF-8/«почти UTF-8»/счёт диапазонов, слитый проход `scanUTF8AndScore`), `DetectFromBytes` (делегирует с prior CP866), `DetectXMLEncoding`, `xmlEncodingRegexp`, `DetectEncoding`, `DetectMarkdownEncoding`, `ReadFile`, `ConvertToUTF8`, `DecodeBytes`
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

- Кодировка single-byte legacy-форматов (SQL, H, TPR, PAS, INC, JS, SMF, DFM, RPT) определяется по содержимому через `DetectFromBytesWithPrior`; карта расширений служит prior (см. `indexing/file-walking`, «Детекция кодировки по содержимому в walk-воркере»)
- T01-копии читаются в CP866 по карте (детекция не применяется); XML-файлы (DSArchitect XML и т.п.) используют `DetectXMLEncoding` (declaration → UTF8 validity → WIN1251 fallback)
- `format_version` зафиксирован как `1.0` для всех machine-readable modes
- Лог-файлы команд: `codebase_YYYYMMDD.log` (один на день)
- Лог-файлы ошибок индексатора: `indexer_errors_YYYYMMDD_HHMMSS.log` (на каждый запуск)
- Конфиг ищется рядом с executable, а не в текущем рабочем каталоге
- `systemsvc` — общий execution-слой для `health`/`stats` (CLI + MCP), закреплён за этой capability (отдельная capability `infrastructure/health-stats` НЕ заводится — health checks и так описаны здесь). Аналогично `querysvc`/`reviewsvc`/`rtisvc`/`trcsvc`, устраняет дублирование оркестрации между CLI и MCP
- `systemsvc.ExecuteHealth`/`ExecuteStats` на текущий момент используют `context.Background()` (не принимают ctx) — дешёвые синхронные операции, отмена не критична (см. `mcp-transport-tools`)
- Sentinel-ошибки `internal/errs` используются по всему коду для классификации через `errors.Is` (доработка «sentinel-errors-refactor») — устраняет string-matching в обработке ошибок
