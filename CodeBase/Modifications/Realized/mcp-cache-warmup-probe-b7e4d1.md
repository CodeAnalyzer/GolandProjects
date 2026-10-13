# Прогрев кэшей при старте MCP (warm-up probe)

Устранение «холодного старта» MCP-сервера профиля `query`: первый вызов каждого
инструмента платит разовую цену за чтение страниц PostgreSQL/OS-кэша и за
ленивую загрузку LSA/embeddings-кэшей (spec/desc semантический слой). Лог
прогона `codebase_20261011.log` показывает 5–750-кратную разницу между первым и
повторным идентичным запросом (`callers` для `CardTran_Insert`: 14.3 с → 19 мс). Предлагается фоновый warm-up probe при старте
сервера: набор дешёвых представительных запросов к горячим путям, ограниченный
таймаутом, не блокирующий MCP-хендшейк.

Статус: **реализовано** в OpenSpec change `mcp-cache-warmup-probe`
(`openspec/changes/mcp-cache-warmup-probe/`, 2026-10-13). Ниже — уточнения
по итогам исследования на живой БД FA (DBeaver, Q1–Q4) и smoke-прогона.

---

## Контекст проблемы

Прогон 2026-10-11 (`CodeBase/codebase_20261011.log`, 294 вызова `codebase_query*`)
даёт чёткий cold/warm-паттерн: **первый** вызов типа всегда в разы дороже
повторного идентичного. Суммарная «холодная надбавка» по 58 повторяющимся
запросам — **~126 с**.

| Запрос (`tool | args`) | 1-й, ms | тёплый, ms |
|---|---|---|
| `spec_search` «массовое создание плановых событий» | 32562 | 6179 |
| `desc_search` text=*** | 25712 | 1584 |
| `api_table pAPI_Account_Limit` | 12273 | 26 |
| `table tConsSalePortfolio` | 9592 | 308 |
| `form_component dlDealNumber` (like) | 9072 | 254 |
| `spec_coverage product=fa-contracts` | 8244 | 1019 |
| `sql_fragment text=***` | 6860 | 2 |
| `table_index tConsSalePortfolio` | 5582 | 126 |
| `spec_config product=fa-contracts` | 5155 | 210 |
| `procedure CardTran_Insert` | 4658 | 6 |
| `api_contract GetListLimit` (like) | 4623 | 206 |
| `vb_function DsRptTemplateShow` | 3555 | 53 |

`codebase_ping`/`health`/`stats` на старте быстрые (0/715/522 мс) — значит
соединение инициализируется нормально, а дорого стоит **первое касание данных**:
страниц таблиц/индексов индекса (buffer cache PostgreSQL + page cache ОС) и
ленивая загрузка LSA/embeddings в in-process кэши.

### Корневая причина (две составляющие)

1. **Страницы БД холодные.** Первый seq/index-scan по `symbols`, `relations`,
   `api_contracts`, `sql_procedures`, `dfm_components`, `query_fragments`,
   `report_*` вычитывает все нужные страницы с диска; последующие — из
   shared_buffers/OS-кэша (в логе они в 10–200 раз быстрее; кэш переживает даже
   перезапуск процесса — вторая «сессия» по тем же таблицам уже тёплая).
2. **Ленивые модельные кэши.** `spec_search`(semantic) и `desc_search`(semantic)
   при первом обращении грузят sidecar-модель и эмбеддинги:
   - `specsvc/spec_embeddings_cache.go:163-211` — `specSemanticCache.get` →
     `specfts.LoadLSAModel` + `loadSpecEmbeddingsCached`;
   - `query/desc_embeddings_cache.go:55-79` — `loadDescEmbeddingsCached`.
   Это и есть основная часть 32.5 с / 25.7 с «холодного» первого вызова; после
   прогрева те же запросы — 6.2 с / 1.6 с.

### Эталонный пример: `callers` — крупнейший рабочий набор в логе

`codebase_query_callers` для `CardTran_Insert` — **14331 мс** первый вызов
(самый тяжёлый холодный запрос профиля `query`). Разбор в DBeaver
(`EXPLAIN (ANALYZE, BUFFERS)`) доказал, что это **чистый I/O, а не структурная
проблема обхода**:

| Прогон | Buffers | Execution Time |
|---|---|---|
| 1-й (частично тёплый) | `shared hit=16393 read=244` | 374 мс |
| 3-й (полностью тёплый) | `shared hit=16632` (**read=0**) | **19 мс** |
| Лог 2026-10-11 (холодный, под нагрузкой) | — | 14331 мс |

Соотношение cold/warm ≈ **750×**. План — полностью индексный (`Bitmap Index
Scan on idx_sql_procedures_proc_name_trgm`, `Index Scan using
idx_relations_target_type_id`, PK-сканы), без seq scan по большим таблицам; fan-out
умеренный (18 target-процедур → 468 прямых + 777 косвенных вызывающих, 45
«промежуточных»). Рабочее множество — **16 632 касания буферов ≈ 130 МБ**, самое
большое среди всех вызовов лога; холодно это и есть вычитывание этих страниц с
диска (на прогоне параллельно падали 4 MCP-процесса — диск под contention,
отсюда секунды). Кэш переживает перезапуск процесса: та же логика уже тёплая во
второй «сессии» лога.

Следствие: микро-оптимизации плана (`LEFT JOIN LATERAL` вместо 5
взаимоисключающих LEFT JOIN; вынос `LIMIT` в ветки `UNION`) бьют по **19-мс**
запросу — не имеют смысла. Единственное реальное действие для `callers` —
прогрев кэша: probe обязан касаться пары `relations` + `sql_procedures` + `files`.

---

## Решение

Фоновый **warm-up probe** сразу после открытия БД, до `server.Run`: набор
представительных запросов по горячим путям, с ограничением по времени и
best-effort семантикой (ошибки не фатальны, только лог).

### Решения по дизайну

1. **Асинхронно, не блокируя старт.** Проба запускается горутиной до
   `server.Run` (`server.go:51`) и работает параллельно хендшейку/первым
   запросам. Синхронный режим задержал бы `initialize` и рискнул бы клиентским
   таймаутом — отклоняется. Гарантия: к моменту «настоящей» работы агента
   сервер уже прогрет.
2. **Проба = реплей реальных tool-хендлеров.** Используется уже собранный
   `registry` (`server.go:45` → `buildToolRegistryForProfile`), а не отдельные
   SQL-запросы. Это гарантирует прогрев **тех же** путей доступа (индексов) и
   автоматически прогревает ленивые LSA/embeddings-кэши через
   `desc_search`/`spec_search`. Имена-аргументы могут не существовать в другом
   продукте — не страшно: любой запрос всё равно касается нужных страниц/индексов.
3. **Профильно-специфичный список проб.** Для `query` — таблицы индекса кода и
   spec/desc; для `rti`/`trc` — соответствующие таблицы логов; для `review`
   проб нет (SQL-линт не читает индекс). База (`ping/health/stats`) отдельными
   пробами не покрывается (и так быстра).
4. **Ограничение по времени** (`warmup_timeout_sec`, дефолт 20, `0` = без
   таймаута) — c `context.WithTimeout` на всю пачку; `SELECT count(*)` по
   `relations`/`symbols` может занять секунды, но это и есть разовое чтение,
   которое иначе заплатил бы первый пользовательский вызов.
5. **Параметры конфигурации** — по образцу существующих timeout-флагов MCP
   (`mcp.query_timeout_sec`): опциональные указатели с nil→дефолт.
6. **Опция `pg_prewarm`** (вторично): если установлено расширение, прогрев
   горячих отношений можно делегировать `pg_prewarm(regclass)` — дешевле по SQL,
   но требует contrib-расширения; реализуется как fallback-ветка, не обязательна
   для базовой версии.

### Список проб (профиль `query`)

| # | tool | args | прогревает |
|---|---|---|---|
| 1 | `codebase_query_symbol` | `{"name":"", "like":true, "limit":1}` | `symbols` (btree) |
| 2 | `codebase_query_relations` | `{"relation_type":"references_table","limit":1}` | `relations` |
| 3 | `codebase_query_api_contract` | `{"name":"API","like":true,"limit":1}` | `api_contracts` |
| 4 | `codebase_query_table_index` | `{"name":"XPK","like":true,"limit":1}` | `sql_index_definitions*` |
| 5 | `codebase_query_form_component` | `{"name":"dl","like":true,"limit":1}` | `dfm_components` |
| 6 | `codebase_query_sql_fragment` | `{"text":"select","limit":1}` | `query_fragments` (trgm/FTS) |
| 7 | `codebase_query_desc_search` | `{"text":"договор","limit":1}` | desc FTS + **desc LSA/embeddings** |
| 8 | `codebase_query_spec_search` | `{"query":"договор","layer":"semantic","limit":1}` | spec FTS + **spec LSA/embeddings** |
| 9 | `codebase_query_callers` | `{"procedure":"<частая процедура>","limit":1}` | `relations` + `sql_procedures` + `files` (**крупнейший рабочий набор**) |

Точные `args` — конфигурируемая константа в коде (одно место правки), чтобы
подстраиваться под конкретную БД без изменения логики. Проба №9 (`callers`)
приоритетна: её рабочий набор (~16.6k буферов в эталонном замере) — самый
большой в логе, поэтому эффект прогрева максимален; имя процедуры следует
брать из данных (самая «вызываемая»), а не хардкодить.

---

## Затронутые файлы

- `internal/mcp/server.go` — старт probe в `RunStdio` (`:45-51`), новый
  `warmupQueryProfile`/`warmupForProfile`, проб-таблица, логирование.
- `internal/mcp/warmup.go` — новый файл (список проб + оркестрация).
- `internal/mcp/warmup_test.go` — новый тест (probe не падает при пустых
  результатах/ошибках; соблюдается таймаут; профиль без проб — no-op).
- `internal/config/config.go` — поля `WarmupEnabled *bool`,
  `WarmupTimeoutSec *int` в `MCPConfig` (`:75-79`), дефолты в `applyDefaults`
  (`:512-515`) и валидация (`:370-374` по образцу).
- `cmd/root.go` — при необходимости `applyConfigToPackages` (`:270-289`)
  проброс (если параметры не читаются напрямую из `config.Get()` в mcp-пакете).
- `internal/store/db_warmup.go` — **опционально** (ветка `pg_prewarm`).

---

## План (4 шага)

### Шаг 1: Конфигурация

В `MCPConfig` добавить:

```go
WarmupEnabled    *bool `toml:"warmup_enabled"`     // nil = true
WarmupTimeoutSec *int  `toml:"warmup_timeout_sec"` // nil = 20, 0 = без таймаута
```

Дефолты в `applyDefaults` (`true`, `20`), валидация `>= 0` для таймаута (по
образцу `query_timeout_sec`, `config.go:370`).

### Шаг 2: Оркестратор проб (`internal/mcp/warmup.go`)

- Таблица `warmupProbesByProfile map[string][]warmupProbe` (`warmupProbe{tool
  string; args map[string]interface{}}`), минимум — профиль `query` (таблица выше).
- `func warmupForProfile(ctx context.Context, profile string,
  registry map[string]registeredTool, logger *log.Logger)`:
  - `probes := warmupProbesByProfile[profile]`; пусто → выход;
  - обернуть ctx в `WithTimeout` при `warmup_timeout_sec > 0`;
  - для каждой пробы: если хендлер есть в `registry` — вызвать
    `rt.Handler(ctx, args)`, ошибку игнорировать; накапливать длительность;
  - залогировать `profile=... warmup probes=N elapsed=... errors=M`
    (через тот же `logger`, что и tool-вызовы).

### Шаг 3: Запуск в `RunStdio` (`server.go`)

После `buildToolRegistryForProfile` и до `server.Run`:

```go
if warmupEnabled(cfg) {
    go warmupForProfile(context.Background(), profile, registry, logger)
}
```

`go` — намеренно: не задерживаем `server.Run`. Если `logger == nil` — probe
можно не запускать (warm-up бессмысленен вне логируемого режима) или запускать
без логов; решение зафиксировать комментарием.

### Шаг 4 (опционально): Ветка `pg_prewarm`

В `internal/store/db_warmup.go` — `PrewarmHotRelations(ctx, names []string)`:
проверить наличие `pg_prewarm` в `pg_extension`; при наличии вызвать
`SELECT pg_prewarm($1::regclass)` по горячим таблицам/индексам, иначе вернуть
`ErrPrewarmUnavailable` (probe молча падает на шаг 2). Не блокирует основную
реализацию.

---

## Проверка

1. `go build ./...` — чисто.
2. `go test ./internal/mcp/... ./internal/config/... -count=1` — PASS
   (новый `warmup_test.go`: no-op для профиля без проб; probe переживает ошибки
   хендлеров; таймаут соблюдается).
3. Smoke на живой БД (лог): `--profile=query` → в логе строка `warmup ...`;
   далее сравнить `duration_ms` первого `desc_search`/`spec_search`/`table`
   с прогоном при `warmup_enabled=false` — ожидается падение «первого» вызова
   до уровня «тёплого».
4. Отключаемость: `warmup_enabled=false` → лог probe отсутствует, поведение
   прежнее.
5. Регресс: `codebase_ping`/`health`/`stats` и все tool-вызовы — без изменений
   (probe не пишет в stdout, только в лог-файл).

---

## Ожидаемый эффект

| Метрика | Было (первый вызов) | Стало |
|---|---|---|
| `desc_search` (1-й) | 25.7 с | ~1.5 с (warm) |
| `spec_search` semantic (1-й) | 32.5 с | ~6 с (warm) |
| `callers` `CardTran_Insert` (1-й) | 14.3 с | ~19 мс (warm) |
| `table`/`api_table`/`form_component` (1-й) | 9–12 с | 30–300 мс |
| Суммарная «холодная надбавка» прогона | ~126 с | ~0 (перенесена в фон) |
| Старт MCP (`initialize`) | — | без изменений (probe асинхронный) |

Замечание: суммарная I/O-работа не исчезает — она переносится из
пользовательского вызова в фон. Выигрыш — предсказуемость: «первый» вызов
перестаёт быть выбросом 5–30 с.

---

## Риски и компромиссы

- **Гонка probe с первым реальным запросом** — probe может ещё идти, и первый
  вызов частично холодный. Митигация: короткие предпочтительные пробы
  (semantic/desc — самые дорогие) в начале списка; оставшийся холодный путь —
  тот же, что был раньше, регресса нет.
- **Дополнительная фоновая нагрузка на БД** при интерактивном запуске — разовая,
  ограничена таймаутом, отключается флагом.
- **`SELECT count(*)`/широкие probes на больших таблицах** (`relations`)
  потребляют буферы и I/O — приемлемо (это и есть цена прогрева); при желании
  список проб сузить до индексов через `pg_prewarm`.
- **Имена-аргументы проб** могут не находить строк в чужом продукте — на
  прогрев страниц не влияет (скан всё равно происходит); при необходимости
  список проб выносится в конфиг.
- **Ложные срабатывания в тестах**, ожидающих точное число лог-строк — probe
  добавляет одну строку; учесть в тест-инфраструктуре.

---

## Совместимость

- Публичный контракт MCP (tools/list, responses) не меняется — probe пишет
  только в лог.
- Поведение при `warmup_enabled=false` (или неизвестном профиле) — прежнее.
- Дефолт `enabled=true` меняет только тайминги, не результаты.
- Спека `mcp-server/*` описывает набор инструментов/профили; warm-up — внутренняя
  деталь реализации, укладывается в текущие требования (при необходимости —
  дельта через OpenSpec change).

## Уточнения по итогам исследования (2026-10-13, живая БД FA)

- **Q1/Q2 (fan-out):** лидер `ReturnCode_Insert` (7557 прямых вызывающих) —
  «хаб», отвергнут: probe сам стал бы тяжёлым холодным запросом. Выбран
  `FCD_GL_Audit_Insert`: 1090 прямых + 2904 косвенных строк `relations`
  (средняя полоса листьев индексов).
- **Q3 (размеры):** `relations` 383 МБ + 969 МБ индексов (7 шт.), `symbols`
  ≈ 1.26 ГБ, `query_fragments` ≈ 1.25 ГБ при `shared_buffers = 1 ГБ` —
  превышение ~4×. Полный `pg_prewarm` `relations`-индексов **исключён**;
  `pg_prewarm`-ветка (шаг 4) отложена целиком.
- **Q4 (индексы):** `callers` греет полосы листьев `idx_relations_target_type_id`
  + `idx_relations_source_type_id` + PK `sql_procedures`/`files` — probe №2
  (limit=1) греет только верхушки B-деревьев и **не заменяет** пробу №9.
- **Таймаут:** дефолт 60 с (холодная LSA-загрузка ≈ 32 с; 20 с из исходного
  текста обрезали бы самую ценную пробу).
- **Порядок проб** зафиксирован: LSA/embeddings (`desc_search`, `spec_search`)
  → `callers` → лёгкие name-пробы → `sql_fragment` последней (GIN 547 МБ может
  вытеснить прогретые полосы).
- **Probe №1 исправлена:** пустое имя отклоняется `requiredString` до касания
  БД → `name="Insert", like=true`.
- **Race подтверждён smoke-прогоном:** вызов `desc_search` *во время* probe
  упирается в 30-с tool-таймаут (дисковая конкуренция с LSA-загрузкой); после
  завершения probe (46.8 с, 9/9 проб) тот же вызов — **113 мс**, `callers`
  `CardTran_Insert` — **35 мс** (холодные эталоны: 25.7 с / 14.3 с). Зазор
  `initialize` → первый реальный вызов (~1 мин в жизни IDE) покрывает probe.
- **logger == nil:** probe не запускается (зафиксировано в спеке и коде).



## Связанные артефакты

- Лог-источник: `CodeBase/codebase_20261011.log` (294 вызова, cold/warm-паттерн)
- Прогон: `Tests/CODEBASE_QUERY_TEST_RESULTS_2026-10-11.md`
- Код-точки: `internal/mcp/server.go:45-51`; `internal/config/config.go:75-79`;
  `internal/specsvc/spec_embeddings_cache.go:163-211`;
  `internal/query/desc_embeddings_cache.go:55-79`
- Родственные перф-артефакты: `Modifications/Realized/redundant-indexes-cleanup-a41c07.md`,
  `Modifications/Realized/query-search-optimization-plan.md`
