# Bug Report: пачка LOW док-дрейфа — спеки расходятся с кодом в счётчиках, расположении функций и формулировках (7 пунктов)

**Дата:** 2026-10-09
**Файл:** спеки `openspec/specs/` (rti-parsing, mcp-transport-tools, mcp-pagination, spec-queries, trc-storage, api-macros-t01); `cmd/query_spec.go`, `cmd/root.go`
**Версия CodeBase:** 0.9.2
**Статус:** Исправлено (в рамках openspec\changes\archive\2026-10-10-fix-spec-doc-drift-batch)

## Summary

По результатам полного аудита «29 спек ↔ код» набралась пачка расхождений класса LOW: поведение системы соответствует замыслу, но **текст спек** зафиксировал устаревшие счётчики, не те файлы вызова, не тот уровень запуска или иную структуру выдачи. Каждый пункт сам по себе косметика; вместе они подтачивают статус спек как «источника истины». Исправление — правки текста спек (пункт 4 — опционально код/спека по выбору), без изменения поведения.

Критичность: **LOW** (док-дрейф без поведенческих последствий).

---

## Environment

- CodeBase 0.9.2, аудированы `openspec/specs/**` против `cmd/` + `internal/`

---

## Пункты

### 1. moduleIDMap: 438 записей вместо «94 записи»

- **Спека:** `rti-analysis/rti-parsing/spec.md:112` — «полный справочник Module ID → Product Name (**94 записи**, `internal/rti/symbols.go`, `moduleIDMap`)» и Notes (строка 163): «Справочник `moduleIDMap` (**94 записи**) зашит в коде».
- **Код:** `internal/rti/symbols.go` — фактически **438** записей (источник: DEALMAIN.H 10682–12173 + API/ADP/Solution-модули, как указано в шапке файла).
- **Фикс:** заменить «94 записи» на «438 записей» или убрать счётчик вовсе (рекомендуется — счётчики в спеках устаревают при каждом пополнении справочника).

### 2. MCP: «все 55 инструментов» — фактически 65; desc_search и spec_config не перечислены

- **Спека:** `mcp-server/mcp-transport-tools/spec.md`:
  - строка 300 — «MCP-сервер регистрирует все **55** инструментов»;
  - строка 61 — список Query-инструментов (28 шт.) без `codebase_query_desc_search`;
  - строка 388 — список Spec-инструментов (6 шт.) без `codebase_query_spec_config`.
- **Код:** `internal/mcp/registry.go` — полный реестр **65**: 4 base (`ping`, `health`, `stats`, `read_more`) + 29 query (28 + `desc_search`) + 7 spec (6 + `spec_config`) + 12 rti + 12 trc + 1 review. Спека внутренне непоследовательна: перечислено 63 инструмента, а сценарий говорит «55».
- **Фикс:** обновить счётчик до 65 (или формулировку «все зарегистрированные инструменты»), добавить `desc_search` и `spec_config` в соответствующие списки (кратко, со ссылкой на профильные спеки `query/description-search` и `query/spec-queries`).

### 3. SetPaginationTTL вызывается в root.go, а не в RunStdio

- **Спека:** `mcp-server/mcp-pagination/spec.md:84` — «SHALL применять TTL … при старте MCP-сервера через `SetPaginationTTL(d)`»; сценарий (строки 88–91): «**WHEN** `RunStdio` инициализирует `globalPages` и вызывает `SetPaginationTTL(...)`»; Notes (строка 133).
- **Код:** `RunStdio` (`internal/mcp/server.go:28–32`) инициализирует только `globalPages` (chunk size) и запускает GC-loop; `SetPaginationTTL` вызывается в **`cmd/root.go:284`** при загрузке конфигурации (рядом с `util.SetRegexpCacheMaxEntries`, строка 287).
- **Поведение эквивалентно** (root.go исполняется до RunStdio в том же процессе), это атрибуция вызова не тому уровню.
- **Фикс:** в спеке заменить «RunStdio вызывает» на «bootstrap CLI (`cmd/root.go`) вызывает при загрузке конфига, до входа в `RunStdio`».

### 4. spec by-code: «результат группируется по capability» — реализация возвращает плоский список

- **Спека:** `query/spec-queries/spec.md:11` — «Результат **группируется по capability**; внутри — требования и сценарии…».
- **Код:** `internal/specsvc/specsvc.go`, `ExecuteSpecByCode` (строка ~400) — плоский список `Hits`, упорядоченный по `capability_name`, с полями `source_type`/`snippet` на каждый хит. Данные те же, структура выдачи — плоская.
- **Фикс (одно из двух):** (а) спека — заменить на «результат — плоский список упоминаний, упорядоченный по capability» (минимально); (б) код — сгруппировать хиты по capability в `SpecByCodeResult` (если группировка нужна потребителям).
- **Рекомендация:** вариант (а) — потребители (CLI/MCP) уже адаптированы к плоской структуре.

### 5. spec deps: CLI help «outgoing|incoming» vs спечные «depends_on|depended_by»

- **Спека:** `query/spec-queries/spec.md:40,45,51` — направление `depends_on | depended_by`.
- **Код:** `cmd/query_spec.go:70` — Use-string `[--direction outgoing|incoming]`; реализация (`specsvc.go:482–490`) принимает **оба** набора как алиасы.
- **Фикс:** привести Use-string в `cmd/query_spec.go:70` к `depends_on|depended_by` (с сохранением алиасов), либо в спеке явно указать алиасы. Рекомендуется правка CLI help — он то место, куда смотрит пользователь первым.

### 6. trc-storage: «ParseToDB» — функция называется ParseFileToDB

- **Спека:** `trc-analysis/trc-storage/spec.md:105` — «`internal/trc/parse_to_db.go` — `ParseToDB`, связка парсинг + сохранение в БД».
- **Код:** `internal/trc/parse_to_db.go:29` — `ParseFileToDB` (в `trc-parsing` спека именует её правильно, строка 111/122).
- **Фикс:** заменить `ParseToDB` на `ParseFileToDB`.

### 7. api-macros-t01: indexAPIMacros приписана indexer_sql_pas.go; типы макросов lowercase

- **Спека:** `indexing/api-macros-t01/spec.md:55` — «при индексации API-макросов (`indexAPIMacros`, **`indexer_sql_pas.go`**)».
- **Код:** `indexAPIMacros` определена в **`internal/indexer/indexer.go:1600`** (вызывается из `indexer_sql_pas.go:376`). Кроме того, типы макросов хранятся в БД в lowercase (`create_proc`/`init_event`/`exec_contract`), тогда как спека оперирует именами макросов `API_CREATE_PROC` и т.п. — представление значений, не противоречащее замыслу, но полезно зафиксировать явно.
- **Фикс:** в строке 55 указать `indexer.go` (или «`indexer.go` (определение), вызывается из `indexer_sql_pas.go`»); в Notes добавить фразу «значения macro_type в БД хранятся в lowercase».

---

## Impact

- **LOW по каждому пункту:** расхождение документа с реальностью без неверного поведения системы.
- Суммарный риск: пользователи и LLM-агенты ориентируются на счётчики/имена из спек (например, агент ищет инструмент из списка «55» или проверяет «94 записи») — мелкие, но систематические ложные выводы.
- Аудит остальных ~20 спек из 29 расхождений класса MEDIUM+ не выявил (кроме двух отдельных отчётов: `BUG-mcp-timeout-zero-unreachable-20261009.md`, `BUG-symbol-search-type-naming-drift-20261009.md`).

---

## Suggested Fix

Все пункты правятся одной волной правок спек (`openspec/specs/...`), без изменения кода, кроме пунктов 4 (опция б) и 5.

### Файлы для изменения

1. **`openspec/specs/rti-analysis/rti-parsing/spec.md`** — строки 112, 163: счётчик moduleIDMap.
2. **`openspec/specs/mcp-server/mcp-transport-tools/spec.md`** — строки 61, 300, 388: списки инструментов и счётчик.
3. **`openspec/specs/mcp-server/mcp-pagination/spec.md`** — строки 84, 88–91, 133: атрибуция `SetPaginationTTL` → `cmd/root.go`.
4. **`openspec/specs/query/spec-queries/spec.md`** — строка 11: «группируется» → плоский список (рекомендуемая опция).
5. **`cmd/query_spec.go:70`** — Use-string `depends_on|depended_by` (алиасы `outgoing|incoming` сохранить).
6. **`openspec/specs/trc-analysis/trc-storage/spec.md`** — строка 105: `ParseToDB` → `ParseFileToDB`.
7. **`openspec/specs/indexing/api-macros-t01/spec.md`** — строка 55: расположение `indexAPIMacros`; Notes про lowercase.

### Проверка

- `openspec validate --specs` после правок.
- Опционально: grep-контроль «magic numbers в спеках» (счётчики вида «N записей/инструментов») — либо выносить в Notes с пометкой «по состоянию на <дата>», либо не указывать вовсе.
