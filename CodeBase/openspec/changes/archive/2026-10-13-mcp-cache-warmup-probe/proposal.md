# Прогрев кэшей при старте MCP (warm-up probe)

## Why

Первый вызов каждого MCP-инструмента профиля `query` платит разовую цену за чтение
холодных страниц PostgreSQL/OS-кэша и ленивую загрузку LSA/embeddings-кэшей
(лог `CodeBase/Logs/codebase_20261011.log`: `callers` для `CardTran_Insert`
14.3 с → 19 мс; `spec_search` semantic 32.5 с → 6.2 с; суммарная «холодная
надбавка» прогона ~126 с). Зазор между `initialize` и первым реальным вызовом
(~1 мин: IDE стартует opencode при своём запуске, сессия открывается позже)
позволяет перенести эту цену в фон без влияния на пользователя.

Исследование на живой БД FA (DBeaver) подтвердило: это чистый I/O, а не
структурная проблема планов (`EXPLAIN BUFFERS`: read=244 → read=0, 750×
cold/warm). Горячий набор (`relations` ≈ 1.35 ГБ с индексами, `symbols` ≈ 1.26 ГБ,
`query_fragments` ≈ 1.25 ГБ) превышает `shared_buffers` (1 ГБ) в ~4 раза, поэтому
прогрев должен быть **точечным** (полосы листьев индексов через реплей реальных
tool-вызовов), а не полным (`pg_prewarm` всего `relations` отпадает).

## What Changes

- Фоновая горутина warm-up probe в `RunStdio`: после `buildToolRegistryForProfile`
  и до `server.Run` запускает набор дешёвых реплей-вызовов tool-хендлеров из
  собранного `registry` (те же пути доступа к индексам, автоматический прогрев
  ленивых LSA/embeddings-кэшей).
- Порядок проб: LSA/embeddings-зависимые (`desc_search`, `spec_search`) →
  `callers` (полосы листьев `idx_relations_target_type_id` +
  `idx_relations_source_type_id` + PK `sql_procedures`/`files`) → лёгкие
  name-пробы. `sql_fragment` — последняя или исключена (GIN-индекс 547 МБ,
  вытесняет прогретое; точечные trgm-запросы греют себя сами).
- Имя процедуры для пробы `callers` — константа `FCD_GL_Audit_Insert`
  (1090 прямых + 2904 косвенных строк `relations`; средняя полоса fan-out,
  подтверждена данными Q1/Q2).
- Новые параметры конфигурации `[mcp]`: `warmup_enabled` (nil/true — включён,
  false — выключен) и `warmup_timeout_sec` (nil = 60, явный 0 = без таймаута,
  отрицательное — ошибка) по образцу `query_timeout_sec`.
- Probe не запускается при `logger == nil` (неотключаемая фоновая нагрузка без
  наблюдаемости недопустима).
- Best-effort: ошибки отдельных проб игнорируются (лог), probe никогда не влияет
  на транспортный контракт (не пишет в stdout).

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `infrastructure/configuration`: секция `[mcp]` расширяется параметрами
  `warmup_enabled` / `warmup_timeout_sec` (трёх-состоятельная семантика
  опциональных параметров по образцу `query_timeout_sec`).
- `mcp-server/mcp-transport-tools`: новое требование «Warm-up probe при старте
  сервера» — фоновый прогрев горячих путей после инициализации registry,
  асинхронный, best-effort, с общим таймаутом пачки.

## Impact

- Код: `internal/mcp/server.go` (запуск probe в `RunStdio`), новый
  `internal/mcp/warmup.go` (таблица проб + оркестратор), новый
  `internal/mcp/warmup_test.go`; `internal/config/config.go` (поля MCPConfig,
  дефолты в `applyDefaults`, валидация).
- Транспорт MCP: контракт не меняется (tools/list, ответы, пагинация); probe
  пишет только в лог-файл через существующий `logger`.
- БД: разовая дополнительная фоновая нагрузка при старте (ограничена таймаутом,
  отключается `warmup_enabled=false`).
- Источник-предложение: `Modifications/mcp-cache-warmup-probe-b7e4d1.md`
  (обновляется фактами исследования; шаг `pg_prewarm` для больших индексов
  `relations` исключён, для малых таблиц — вне скоупа базовой версии).
- Вне скоупа: аудит 7 индексов `relations` (969 МБ) на избыточность; правка
  `shared_buffers` на сервере (параллельная административная задача).
