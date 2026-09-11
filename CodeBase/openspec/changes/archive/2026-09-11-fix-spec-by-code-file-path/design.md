## Context

See proposal.md — Why.

Текущая реализация `ExecuteSpecByCode` (`internal/specsvc/specsvc.go:359`) выполняет SQL-запрос к `spec_code_mentions` с JOIN'ами на `spec_capabilities`, `spec_requirements`, `spec_scenarios`, `spec_usecases` и `ds_products`, но не JOIN'ит с таблицей `files`. Таблица `spec_code_mentions` имеет колонку `file_id` (FK → `files.id`), которая указывает на spec-файл, где найдено упоминание код-сущности. Таблица `files` содержит `path` (абсолютный) и `rel_path` (относительный).

Все остальные query-инструменты проекта (`codebase_query_procedure`, `codebase_query_table`, `codebase_query_method` и др.) возвращают путь файла через `f.rel_path` с JSON-тегом `json:"file"` и Go-полем `File string`.

## Goals / Non-Goals

**Goals:**
- Добавить поле `file` (относительный путь spec-файла) в результат `ExecuteSpecByCode`.
- Следовать устоявшемуся паттерну query-инструментов: SQL `f.rel_path`, Go `File string`, JSON `json:"file"`.

**Non-Goals:**
- Не добавлять абсолютный путь (`files.path`) — все query-инструменты используют `rel_path`.
- Не изменять MCP-описание инструмента — оно уже корректно упоминает `file path`.
- Не добавлять путь к файлу код-сущности (процедуры, таблицы) — `spec_code_mentions.file_id` указывает на spec-файл, не на файл кода.
- Не изменять структуру группировки или дедупликации результатов.

## Decisions

### D1: Использовать `files.rel_path`, а не `files.path`

**Решение:** Выбирать `f.rel_path` из `JOIN files f ON f.id = m.file_id`.

**Обоснование:** Все query-инструменты проекта (`TableResult.File`, `MethodResult.File`, `ProcedureResult` и др.) используют `rel_path` с JSON-тегом `json:"file"`. Использование `path` (абсолютного) нарушило бы консистентность и привело бы к машинно-зависимым путям в ответах MCP.

**Альтернатива:** `files.path` (абсолютный) — отклонена по соображениям консистентности и портативности.

### D2: Поле `File` в `SpecByCodeHit`, JSON-тег `json:"file"`

**Решение:** Добавить поле `File string` в структуру `SpecByCodeHit` с JSON-тегом `json:"file"`.

**Обоснование:** Единообразие с другими результатами query-пакета. MCP-клиент получает поле `file` во всех query-инструментах одинаково.

**Альтернатива:** `json:"file_path"` — отклонена, т.к. ни один другой query-результат не использует такое имя.

### D3: JOIN по `spec_code_mentions.file_id`, не по `spec_capabilities.file_id`

**Решение:** `JOIN files f ON f.id = m.file_id` (где `m` = `spec_code_mentions`).

**Обоснование:** `spec_code_mentions.file_id` указывает на файл, в котором **найдено упоминание** код-сущности. Для capability/requirement/scenario это тот же файл, что и `spec_capabilities.file_id` (все лежат в одном `spec.md`). Но для usecase-упоминаний `spec_code_mentions.file_id` указывает на usecase-файл (например `scenarios/scenario-sms-disable.md`), а `spec_capabilities.file_id` — на `spec.md` связанной capability. Путь файла упоминания — более точная информация: он показывает, где именно в тексте найдено имя код-сущности.

**Альтернатива:** JOIN по `spec_capabilities.file_id` — отклонена, т.к. для usecase-упоминаний это дало бы путь к spec.md capability, а не к файлу, где реально найдено упоминание.

### D4: COALESCE для защиты от отсутствующего файла

**Решение:** Использовать `COALESCE(f.rel_path, '')` в SELECT.

**Обоснование:** `spec_code_mentions.file_id` имеет `NOT NULL` (FK с `ON DELETE CASCADE`), поэтому файл всегда должен существовать. Но `COALESCE` соответствует паттерну других query-запросов и защищает от теоретических рассинхронизаций.

## Risks / Trade-offs

- **[Риск] Рассинхронизация `file_id` при переиндексации]** → `ON DELETE CASCADE` гарантирует, что при удалении файла удаляются и его mentions. При повторной индексации `file_id` обновляется. Риск минимален.
- **[Trade-off] Для usecase-упоминаний путь отличается от пути capability]** → Это сознательное решение (D3). Путь файла упоминания точнее отражает источник ссылки. Документировано в сценарии спеки «Путь файла для упоминания из usecase».
