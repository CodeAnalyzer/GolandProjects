## Why

`codebase_query_spec_history` при вызове по change возвращает только список затронутых capability без delta-секций (ADDED/MODIFIED/REMOVED) — поле `changes` в ответе `null`. Спека `query/spec-queries` требует delta-секции в обоих режимах (по capability и по change), а также сценарий `skip_specs` с пометкой «связь извлечена из proposal». Сейчас асимметрия: режим по capability delta возвращает, режим по change — нет; `skip_specs`-сценарий не реализован вовсе. Баг-репорт: `Bug-reports/BUG-spec-history-by-change-missing-delta-sections.md`.

## What Changes

- `executeSpecHistoryByChange` (`internal/specsvc/specsvc.go`) добавляет `LEFT JOIN spec_change_delta` и заполняет плоский список `Changes` delta-секциями (section, requirement_name, body_text) для каждой затронутой capability.
- `SpecHistoryEntry` расширяется полями `capability_name`, `skip_specs` (bool) и `delta_source` (`"delta"` | `"proposal"`) для пометки `skip_specs`-changes, где связь извлечена из proposal.
- `SpecHistoryResult.Changes` инициализируется пустым слайсом в `executeSpecHistoryByChange` (вместо `nil`), чтобы JSON-ответ содержал `[]`, а не `null`.
- Для change со `skip_specs: true` (нет записей в `spec_change_delta`) возвращается одна entry на capability с `skip_specs: true`, `delta_source: "proposal"`, пустыми `section`/`requirement_name` — реализация сценария «связь извлечена из proposal».
- `Capabilities` (summary-список) сохраняется как есть — даёт быстрый обзор затронутых capability без необходимости агрегировать плоский `Changes`.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `query/spec-queries`: требование «История capability по changes (spec_history)» уточняется — явно описывает delta-секции в обратном режиме (по change) и форму пометки `skip_specs`.

## Impact

- **Код:** `internal/specsvc/specsvc.go` — `executeSpecHistoryByChange` (SQL + scan), типы `SpecHistoryEntry`, `SpecHistoryResult`.
- **API:** MCP-инструмент `codebase_query_spec_history` — ответ в режиме по change меняет форму: `changes` перестаёт быть `null`, появляются новые поля `capability_name`/`skip_specs`/`delta_source` у entry. Обратно совместимое расширение (новые поля, `[]` вместо `null`).
- **БД:** новый запрос только на чтение, schema не меняется (`spec_change_delta` уже существует).
- **Тесты:** требуется покрытие режима по change с delta и skip_specs-сценария.
