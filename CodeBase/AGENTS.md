# CodeBase

Основной проект workspace: локальный индексатор исходников Diasoft 5NT
(SQL, H, PAS, INC, JS, SMF, DFM, TPR, RPT, XML, MD, YAML; опционально `.t01`)
для семантической навигации по кодовой базе, статического анализа SQL,
анализа RTI/TRC логов и MCP-сервера.

## Контекст работы

### Структура продукта

- **Индексатор** (`init` / `update`) — обход файлов, парсинг по форматам,
  извлечение сущностей, построение графа связей, пост-обработка, LSA-обучение.
- **Запросы** (`query`) — поиск сущностей и связей, OpenSpec-спеки (полнотекст +
  семантика), покрытие/зависимости/история.
- **Review** (`review`) — статические проверки SQL перед деплоем.
- **RTI-анализатор** (`rti`) — разбор и анализ RTI/HRTI трейс-логов.
- **TRC-анализатор** (`trc`) — разбор `.trc`/`.xml`/`.xel` SQL Server Profiler.
- **MCP-сервер** (`mcp`) — JSON-RPC over stdio, те же возможности для агентов.

Реальный источник данных для отладки — проекты **FA** (`D:/GITHUB/GolandProjects/FA`),
на них указывает `root_path` в `codebase.toml`.

## Архитектура

### Архитектурные слои

1. **CLI-оболочка** — `main.go` → `cmd/` (Cobra): регистрация команд, флаги,
   выбор конфига, логирование, machine-readable режимы.
2. **Транспорт MCP** — `internal/mcp/`: stdio JSON-RPC, registry инструментов,
   пагинация крупных ответов, профили (`query`/`rti`/`trc`/`review`).
3. **Сервисный слой** (`*svc`) — `querysvc`, `specsvc`, `systemsvc`, `rtisvc`,
   `trcsvc`, `reviewsvc`: тонкие обёртки над доменными пакетами, общие для CLI и MCP.
4. **Доменные пакеты**:
   - `internal/indexer/` — оркестрация пайплайна (`runner.go`: `InitCtx`/`UpdateCtx`),
     пост-обработка (`indexer_postprocess_*.go`, `indexer_relations.go`).
   - `internal/parser/*/` — парсеры по форматам: `sql`, `h`, `pas`, `dfm`, `js`,
     `smf`, `tpr`, `rpt`, `dsxml`, `apimacro`, `openspecmd`, `retcode`.
   - `internal/query/` — запросы к индексу; `internal/specsvc/` + `internal/specfts/`
     — спеки и поиск (tsvector + LSA).
   - `internal/rti/`, `internal/trc/`, `internal/review/` — логика анализаторов.
5. **Персистентность** — `internal/store/`: PostgreSQL (schema, batch insert,
   lookup, scan runs, spec-*, stats snapshot, LSA).
6. **Инфраструктура** — `internal/config/`, `encoding/`, `fswalk/`, `model/`,
   `util/`, `errs/`.

Поток данных: walk → decode → parse → `model.*` → batch store → relations
post-processing → spec-профили → LSA.

### Точки входа

- `main.go` → `cmd.Execute()` (`cmd/root.go`); версия `0.9.2`, build `1539`.
- CLI-команды: `init`, `update`, `query`, `stats`, `health`, `review`, `rti`,
  `trc`, `mcp`.
- MCP: `cmd/mcp.go` → `mcp.RunStdio(version, profile, logger)` — JSON-RPC over
  stdin/stdout; `--profile` ограничивает набор инструментов.
- Индексация: `indexer.New(db, cfg).InitCtx(...)` / `.UpdateCtx(...)`.

## Специфика реализации

- **Кодировки**: CP866/WIN1251/UTF8 с эвристикой (`internal/encoding`),
  включая TPR и препроцессированные `.t01`; MD — авто (UTF-8 → CP1251), YAML — UTF-8.
- **Параллельная индексация**: worker-пулы, per-file транзакция
  (`processFileCtx`: batch-вставки + SELECT-back резолвы в одном COMMIT).
- **Отложенный резолв**: `pending*`-ссылки (классы/методы/поля, вызовы, фрагменты,
  JS, `.t01`-подписчики, API-макросы) собираются при парсинге и резолвятся глобально
  в параллельной пост-обработке; `DELETE` выполняется до горутин.
- **Инкрементальный update**: pre-filter по mtime+size, hash SHA-256,
  каскадное удаление устаревших/исчезнувших файлов.
- **LSA/лексика**: `specfts` (tokenizer, TF-IDF, LSA через gonum SVD),
  sidecar-модель `spec_lsa_model.bin` + `spec_lsa_state.json`; `stats_snapshot`.
- **Machine-readable contract**: `--json` envelope (`success`, `format_version=1.0`,
  `command`, `count`, `items`, `meta`), `--summary`, `--ndjson`; banner подавляется,
  ошибки — structured JSON, пустые результаты — `[]`.
- **RTI**: HRTI hash-decoding (TDsHash, M1=3, M2=102); **TRC**: content sniffing
  `.trc`/`.xml`/`.xel`.
- Если `--config` не задан, `codebase.toml` ищется **рядом с executable**.
- Комментарии и документация в коде — на русском; код — idiomatic Go.

## Внешние зависимости

- **Runtime**: Go 1.25+, PostgreSQL 14+ (по умолчанию порт 5435; `pg_trgm`,
  tsvector `'russian'`).
- **Go-модули**: `github.com/lib/pq`, `github.com/spf13/cobra`,
  `github.com/pelletier/go-toml/v2`, `github.com/modelcontextprotocol/go-sdk`,
  `github.com/kljensen/snowball`, `gonum.org/v1/gonum`, `golang.org/x/text`,
  `golang.org/x/sys`.
- **Данные**: кодовая база Diasoft 5NT (FA), RTI/HRTI логи, SQL Server Profiler
  `.trc`/`.xml`/`.xel`, OpenSpec-директории продуктов.
- **Клиенты**: MCP-клиенты (opencode и др.) через stdio.

## OpenSpec

### Структура

- `openspec/config.yaml` — конфигурация и контекст продукта.
- `openspec/specs/<domain>/<feature>/spec.md` — источник истины. Домены:
  `indexing`, `query`, `review`, `rti-analysis`, `trc-analysis`, `mcp-server`,
  `infrastructure`.
- `openspec/changes/<name>/` — незавершённое изменение: `proposal.md` (why),
  `design.md` (how), `tasks.md` (steps), `specs/` (дельта, зеркалит `specs/`).
- `openspec/changes/archive/<date>-<name>/` — завершённые изменения.
- Инструменты: `.opencode/skills/openspec-*`, `.opencode/commands/opsx-*.md`.

### Команды

Выполняются из корня `openspec/` проекта.

| Команда | Назначение |
|---|---|
| `openspec list` / `openspec list --specs` | список changes / specs |
| `openspec validate <change\|spec>` | валидация одного артефакта |
| `openspec validate --all` (`--changes`, `--specs`) | валидация всего |
| `openspec show <change> --json --deltas-only` | распознанные дельты (отладка) |
| `openspec archive <change-name>` | применить дельты и перенести в `archive/` |
| `openspec templates` | пути к шаблонам артефактов (источник формата) |
| `openspec instructions --change <name>` | инструкции по артефактам change |

### Правила OpenSpec (главное)

- **Спеки — источник истины**, change — единица работы, дельта описывает только
  изменения (`ADDED`/`MODIFIED`/`REMOVED`), архивирование возвращает change в истину.
- **`proposal.md`**: обязательные секции `## Why`, `## What Changes`,
  `## Capabilities` (`### New Capabilities`, `### Modified Capabilities`), `## Impact`.
- **`design.md`**: `## Context`, `## Goals / Non-Goals`, `## Decisions`,
  `## Risks / Trade-offs`.
- **`tasks.md`**: группы `## 1.` с чек-боксами `- [ ] 1.1 ...`.
- **`spec.md`**: обязательные `## Purpose` и `## Requirements` (контейнер);
  `### Requirement` (ровно 3 решётки) + `#### Scenario` (ровно 4, минимум один);
  опционально `## Related code`, `## Notes`.
- **Ключевые слова**: только английские `SHALL`/`MUST` в теле требования (русские
  `ДОЛЖЕН`/`ДОЛЖНА` не распознаются — warning, а в `--strict` ошибка).
- **Дельта**: заголовки `## ADDED/MODIFIED/REMOVED Requirements` (мн. ч., уровень 2);
  `MODIFIED` воспроизводит полный текст требования; delta-заголовки запрещены в
  основном spec (`delta-header`).
- **Сценарии** behavior-oriented: GIVEN/WHEN/THEN + AND.
- **Кодировка** всех файлов OpenSpec — **UTF-8**.

Полные правила: [openspec/AGENTS.md](openspec/AGENTS.md)
