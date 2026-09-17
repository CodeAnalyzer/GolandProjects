## Why

Существующие TRC-инструменты возвращают внутренне противоречивые результаты: PostgreSQL и file-mode по-разному учитывают нулевые длительности, `filtered_count` отражает размер ограниченной выдачи, агрегация вызовов задваивает процедуры через statement-события, TRC MCP-обработчики скрывают ошибки optional-аргументов, а enrichment зависит от случайной выборки первых событий. Эти дефекты мешают доверять результатам одного и того же анализа при разных источниках данных.

## What Changes

- Унифицировать вычисление `count`, `total_ms`, `min_ms`, `max_ms` и `avg_ms` между PostgreSQL и Go; нулевая длительность является валидным значением и участвует во всех метриках.
- Исправить `EventsResult.filtered_count`: поле отражает полное количество событий, удовлетворяющих существующим фильтрам, до `LIMIT`; размер фактически возвращённого массива публикуется как `returned_count`.
- **BREAKING** Изменить существующую агрегацию `codebase_trc_procedures` и CLI `trc procedures`: учитывать только события `SP:Completed`, чтобы `count` означал число завершённых вызовов процедур и не задваивался через `SP:StmtCompleted`/`SQL:StmtCompleted`. Настраиваемый `event_scope` не добавляется.
- Исправить только TRC MCP-обработчики так, чтобы ошибки разбора optional-аргументов возвращались клиенту, а не заменялись нулевыми значениями. Остальные MCP-профили не входят в объём.
- Выполнять enrichment для всех процедур итоговой агрегации, не ограничивая покрытие первыми 1000 событиями сессии.
- Сохранить существующие фильтры и лимиты `codebase_trc_events`; курсорная пагинация, новые фильтры, сравнение SPID и SPID summary не добавляются.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `trc-analysis/trc-aggregation-tree`: уточняется семантика метрик, completed-only агрегации процедур, `filtered_count` и полного enrichment для saved-session и file-mode.
- `mcp-server/mcp-transport-tools`: TRC-инструменты обязаны возвращать ошибки для некорректных optional-аргументов и публикуют скорректированный контракт результатов событий и процедур.

## Impact

- Код: `internal/trc/aggregate.go`, `internal/trc/store.go`, `internal/trcsvc/types.go`, `internal/trcsvc/runtime.go`, `internal/mcp/registry.go`, `cmd/trc.go` и соответствующие тесты.
- API: меняется смысл существующего `codebase_trc_procedures` — агрегируются только `SP:Completed`; `EventsResult` получает `returned_count`, а `filtered_count` больше не ограничивается `limit`.
- Данные и схема PostgreSQL не изменяются; ранее сохранённые TRC-сессии повторно парсить не требуется.
- Новые зависимости не добавляются.
