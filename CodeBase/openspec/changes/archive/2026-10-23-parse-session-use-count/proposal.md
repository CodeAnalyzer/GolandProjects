# Счётчик использования сессий (refcount) для reuse-кэша parse

## Why

После введения reuse (архив `2026-10-20-parse-session-reuse-by-file-hash`) одна
сессия регулярно обслуживает несколько анализов одного и того же файла — например,
два MCP-агента разбирают один лог параллельно. Сегодня `rti delete` / `trc delete`
любого из них молча уничтожает общие данные: второй агент теряет сессию, а
следующий `parse` заново гоняет минуты разбора. Нужен счётчик владения, при
котором удаление общим держателем только декрементирует, а окончательное удаление
происходит при последнем держателе.

## What Changes

- В `rti_sessions` и `trc_sessions` добавляется колонка `use_count INTEGER NOT
  NULL DEFAULT 0` (идемпотентная миграция). Legacy-строки получают 0 — ведут себя
  как сегодня (не реюзятся, удаляются окончательно).
- Инвариант: каждый успешный `parse`, вернувший `session_id = N`, атомарно
  инкрементирует `use_count` сессии N (первый парсинг — вместе с записью хеша,
  0 → 1; reuse — инкремент; конфликт параллельных разборов — инкремент
  победителя, проигравший удаляется окончательно со своим счётчиком 0).
- **BREAKING** (смягчено флагом): `rti delete` / `trc delete` без флага при
  `use_count > 1` декрементирует счётчик и оставляет сессию; при `use_count <= 1`
  удаляет окончательно (как сегодня). Ответ честно разделяет исходы: `deleted`
  (окончательно) / `decremented` + `use_count` (осталось держателей).
- Новый флаг `--hard` в CLI и параметр `hard` в MCP `codebase_rti_delete` /
  `codebase_trc_delete` — принудительное окончательное удаление мимо счётчика.
  Принудительный перепарсинг теперь `delete --hard` + `parse` (ранее —
  `delete` + `parse`; прежняя связка при `use_count > 1` больше не перепарсивает).
- `prune` игнорирует счётчик и всегда удаляет окончательно (уборщик не должен
  блокироваться общими сессиями; он же — граница утечек анонимных ссылок).
- `ParseResult` (`rti parse` / `trc parse`, CLI и MCP) аддитивно получает поле
  `use_count` — актуальное значение после инкремента; формат остальных полей не
  меняется.
- `rti list` / `trc list` (CLI и MCP) показывают `use_count` каждой сессии.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `rti-analysis/rti-storage`: счётчик использования в reuse; удаление как
  декремент/окончательное удаление + `--hard`; `use_count` в списке; prune
  игнорирует счётчик
- `trc-analysis/trc-storage`: то же для TRC-сессий

## Impact

- **Схема БД**: `internal/store/db_schema.go` — `ALTER TABLE rti_sessions /
  trc_sessions ADD COLUMN IF NOT EXISTS use_count INTEGER NOT NULL DEFAULT 0`.
- **Доменные пакеты**: `internal/rti/store.go`, `internal/trc/store.go` —
  `SetSessionHash` инкрементирует счётчик; атомарный acquire-по-хешу для reuse и
  победителя конфликта; release-декремент для delete.
- **Сервисный слой**: `internal/rtisvc/`, `internal/trcsvc/` (types.go,
  runtime.go) — `ParseResult.use_count`, `DeleteParams.Hard`, `DeleteResult` с
  разделением deleted/decremented.
- **CLI**: `cmd/rti.go`, `cmd/trc.go` — флаг `--hard` у delete, вывод use_count.
- **MCP**: `internal/mcp/registry.go` — параметр `hard` у delete-инструментов,
  обновлённые описания; `use_count` в ответах parse/list.
- **Совместимость**: ответ delete при `use_count > 1` меняет `deleted` с `true`
  на `false` (плюс `decremented: true`) — клиенты, проверяющие `deleted`, должны
  перейти на разделённые поля; сценарий `delete + parse` как принудительный
  перепарсинг заменяется на `delete --hard + parse`.
- **Документация**: README.md — разделы RTI/TRC (parse, delete, list).
