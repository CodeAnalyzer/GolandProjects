# Ревью change: add-view-usage-rule

**Дата:** 2026-11-14
**Ревьюер:** opencode (автоматическое ревью артефактов + исследование кода/живого индекса)
**Вердикт:** **Changes requested** — технический подход одобрен, артефакты требуют правок до старта реализации.
**Валидация OpenSpec:** `openspec validate add-view-usage-rule` → `Change 'add-view-usage-rule' is valid` (exit 0).

---

## 1. Резюме

Изменение добавляет deploy-stopper `viewUsageInServerCode` (severity 1), который
флагует обращения к представлениям в серверном SQL, и подавляет ложные
`tableExistsInMetadata` для имён-view. Подход (ленивый кэш из `query_fragments`
по образцу `isMemoryTable`, fail-open, без DDL/переиндексации) корректен и
подтверждён замерами на живом индексе.

Однако артефакты содержат **два блокирующих дефекта** и несколько важных
неточностей:

1. **Task 2.3 невыполнима.** В `VIEW.SQL` есть view-поверх-view
   (`ImportFileGroup → tImportFileGroup`), поэтому после реализации правило
   даст там finding, а не «0 findings», как требует приёмка. То же в
   `view_trash.sql` (`→ tFund`).
2. **Неверно определены точки документирования.** Списки правил в MCP и CLI
   генерируются динамически (`review.RuleListString()`) и правки не требуют;
   ручной список в `README.md` (единственный, который реально нужно обновить)
   в Impact/tasks не указан.
3. Заявленные метрики (`44 обращения / 26 файлов`) на текущем индексе не
   воспроизводятся: live — `49 / 30` (47 select + 2 update), а после свёртки
   параллельных ветвей — `33` уникальных `(path,line,name)`.
4. Дизайн-риск branch-scope и план выкатки правила, ломающего CI на 26–30
   существующих файлах, не описаны.

---

## 2. Объём и методика ревью

**Изученные артефакты:**

| Артефакт | Объём |
|---|---|
| `proposal.md` | 55 строк |
| `design.md` | 141 строка |
| `tasks.md` | 27 строк, 5 групп / 12 задач |
| `specs/review/deploy-stoppers/spec.md` | 195 строк (ADDED + MODIFIED) |
| `.openspec.yaml` | schema spec-driven, created 2026-11-14 |

**Исследованный код:**

- `internal/review/runner.go` (Runner, `RunSQLFileCtx`, сбросы кэшей, `buildRuleTasks`, параллельный запуск)
- `internal/review/review_lookup.go` (`isMemoryTable`, `normalizeTableKey`, `prewarmTableCreators`, `cachedLookupTableCreator`)
- `internal/review/review_rules.go` (`checkTableExistsInMetadata`, `isTableExistsNoiseName`, `onlyQuotedOccurrence`)
- `internal/review/catalog.go`, `types.go`, `review_helpers.go` (`normalizeIdentifier`, `enabledRuleSet`)
- `cmd/review.go`, `internal/mcp/registry.go`, `internal/reviewsvc/runtime.go`
- `internal/parser/sql/sql_parser.go` (`createTableRe`, `component_type` фрагментов)
- `internal/indexer/runner.go` (hash pre-filter)
- `openspec/specs/review/deploy-stoppers/spec.md` (исходное требование)
- `README.md` (ручной список правил)

**Проверки на живом индексе** (PostgreSQL 18, порт 5435, БД `codebase`,
2 418 236 фрагментов, 106 315 `.sql`-файлов):

- число view-фрагментов и уникальных имён;
- устойчивость regexp (NULL-capture, мульти-view фрагменты, `#else`-префиксы);
- коллизии view с `CREATE TABLE` / API-таблицами / memory-таблицами;
- количество серверных обращений к view;
- `EXPLAIN (ANALYZE, BUFFERS)` запроса извлечения имён;
- фактический `codebase review` на `VIEW.SQL` и `USR_GPB_CreditCards2.sql`.

---

## 3. Проверка артефактов

### 3.1. proposal.md — соответствует требованиям, но метрики устарели

Секции `## Why`, `## What Changes`, `## Capabilities`, `## Impact` на месте.
Проблема и мотивация изложены корректно: ложные severity-1
`tableExistsInMetadata` на view-имена действительно существуют и подтверждены
запуском review.

Замечания:

- **F3 (High).** Числа «44 обращения в 26 файлах (42 select + 2 update)»,
  «`tfund` — 25 ссылок в 7 файлах» на текущем индексе не воспроизводятся
  (см. §5). Индекс содержит параллельные ветки (`fa-cards` и
  `fa-cards-110109`, `fa-contracts-11016/11017`), поэтому одна и та же ссылка
  учитывается дважды. Нужно либо привести актуальные числа, либо явно указать
  методику/срез (дата, ветка, дедупликация).
- **F2 (High).** В `## Impact` указан `internal/mcp/` («список правил в описании
  инструмента»), хотя список там динамический. Пропущен `README.md` (§1037+),
  где правила перечислены вручную по severity. См. §4.1.

### 3.2. design.md — подход корректен, есть неточности

Секции `## Context`, `## Goals / Non-Goals`, `## Decisions`,
`## Risks / Trade-offs` присутствуют. Ключевые решения (D1 ленивый кэш,
D2 fail-open, D3 регистрация/контексты, D4 подавление) обоснованы и
согласуются с кодом.

Сильные стороны:

- кэш повторяет проверенный паттерн `isMemoryTable` (`review_lookup.go:689`),
  включая флаг загруженности и поведение при ошибке;
- fail-open выбран правильно для бан-правила (FP опаснее FN);
- повторное использование фильтров `checkTableExistsInMetadata`
  (`review_rules.go:1888-1999`) исключает расхождение «шумовых» правил;
- отсутствие DDL подтверждено: `CurrentSchemaVersion = "codebase_schema_v7"`
  (`db_schema.go:13`) менять не нужно.

Замечания:

- **F5 (Medium).** Предложенный SQL хрупок:
  `regexp_match(...)[1]` может быть `NULL` (квитированные/скобочные
  идентификаторы) — нужен `sql.NullString`/фильтр; срезание схемы
  `^(?:[a-z_]...\.)?` работает только для нижнего регистра и только для одного
  префикса, тогда как `normalizeTableKey` (`review_lookup.go:675`) срезает всё
  до последней точки. Рекомендуется выбирать сырое имя и нормализовать его в Go
  через `normalizeTableKey`, а не дублировать нормализацию в SQL.
  На текущем корпусе проблем нет (0 NULL-capture, все префиксы `public.`
  в нижнем регистре), но это скрытая мина для будущих данных.
- **F4 (Medium).** Набор view-имён глобальный (все продукты/ветки), а
  `tableExistsInMetadata` после change `2026-11-11-fix-review-branch-scoping`
  branch-scoped. Если имя является view только в чужой ветке, а в проверяемой
  ветке это реально отсутствующая таблица, подавление скроет дефект. Коллизий
  view↔таблица сейчас 0, но 9 имён определены в двух ветках — ветки не
  идентичны. Решение нужно либо явно принять как FN (и записать в Risks), либо
  ограничить набор областью файла.
- **F9 (Low).** Ссылка «hash pre-filter `runner.go:324`» неоднозначна: в
  проекте два `runner.go`; фактическое место —
  `internal/indexer/runner.go:319-324`.
- **F13 (Low).** `## Open Questions` = «(нет)», хотя реально открыты вопросы
  branch-scope (F4) и приёмки view-поверх-view (F1).

### 3.3. Дельта-спека — структурно корректна

- `## ADDED Requirements` → новое требование с 7 сценариями;
- `## MODIFIED Requirements` → «Существование таблицы в метаданных»
  воспроизводит полный текст исходного требования
  (`openspec/specs/review/deploy-stoppers/spec.md:388-498`) и добавляет абзац
  о подавлении; все 13 существующих сценариев сохранены + 1 новый;
- заголовок MODIFIED совпадает с основным спеком; `SHALL`/`MUST` в теле есть;
  уровни заголовков соблюдены.

Замечания:

- **F1 (High/blocking).** Сценарий «Определение представления не является
  нарушением» (строки 38-43) сформулирован вокруг файла `VIEW.SQL`, но в этом
  файле есть `create view ImportFileGroup ... from tImportFileGroup`, где
  `tImportFileGroup` — тоже view. Формально GIVEN про тела Batch корректен, но
  при буквальном прочтении «review файла VIEW.SQL → findings нет» сценарий
  вводит в заблуждение и противоречит task 2.3. Нужно сузить GIVEN до сниппета
  (только Batch) либо явно сказать, что finding по другим view-определениям
  файла возможен.
- **F8 (Low).** «Правило применяется ко всем `.sql`-файлам» — CLI/MCP
  поддерживают также `.h` (`cmd/review.go:49`, `registry.go:373`), а design D3
  прямо говорит о симметрии с `tableExistsInMetadata`, который работает и на
  `.h`. Формулировку стоит согласовать (`.sql`/`.h`).
- **F7 (Low).** Нет сценария на дедупликацию повторов и на
  `CurrentProductID`, хотя `tableExistsInMetadata` пишет его и дедуплицирует по
  `name:line`.
- **F12 (Low).** Сценарий «Ошибка БД не валит review» покрывает только новое
  правило. Стоит зафиксировать контракт: при ошибке загрузки множества
  `tableExistsInMetadata` **не подавляется** и ведёт себя как раньше.

### 3.4. tasks.md — одна невыполнимая задача и одна неверная

- **F1 (blocking).** Task 2.3: «прогнать на `VIEW.SQL` — 0 findings». Неверно:
  см. §5, там ровно 1 view-поверх-view (`tImportFileGroup`). Приёмка упадёт.
  Правильно: ожидать 1 finding (строка `create view ImportFileGroup`) или
  изменить решение D3.
- **F2 (High).** Task 4.1 требует дополнить список правил в MCP-описании и
  справке CLI — это no-op, оба места динамические:
  `registry.go:373` → `review.RuleListString()`;
  `cmd/review.go:245` → `review.RuleListString()`. Вместо этого нужен
  `README.md`: «Deploy stoppers» (строки 1037-1067).
- **F3 (High).** Task 5.1/5.2 опираются на устаревшие числа и «26 файлов».
  После правки метрик — пересчитать ожидания.
- **F7 (Low).** Task 2.2 не требует `seen name:line` и `CurrentProductID`.
- **F11 (Low).** Task 1.2: unit-тест без БД должен явно проверять `db == nil` и
  nil-кэш, иначе существующие тесты (`table_exists_metadata_test.go:19`
  конструирует `Runner{tableCreatorCache: ...}` без `db`) могут паниковать,
  если `isViewTable` не повторит nil-safe поведение `isMemoryTable`.

---

## 4. Техническая проверка подхода

### 4.1. Точки документирования (F2)

| Место | Как формируется | Нужна правка? |
|---|---|---|
| `internal/mcp/registry.go:373` | `review.RuleListString()` из `ruleCatalog` | **Нет** |
| `cmd/review.go:245` | `review.RuleListString()` | **Нет** |
| `README.md:1037-1067` | ручной список по severity | **Да** |
| `openspec/specs/review/deploy-stoppers/spec.md` | архивирование change | обновится при archive |

### 4.2. Конкурентность и кэш

- `RunSQLFileCtx` (`runner.go:82-113`) сбрасывает memory-кэш до старта правил;
  добавление сброса view-кэша в это же место корректно.
- `reviewsvc.ExecuteCtx` создаёт `NewRunner` на каждый вызов
  (`reviewsvc/runtime.go:14`), CLI обрабатывает один файл за запуск — кэш «на
  файл» соответствует реальности. При будущем пакетном review стоимость
  ~27 мс на файл будет повторяться (см. §5).
- `isViewTable` под мьютексом, два правила (`tableExistsInMetadata` и новое)
  сериализуются только на время одной загрузки; гонок нет. Важно: подавление
  будет работать и при `--rules tableExistsInMetadata` (правило само лениво
  загрузит множество), это соответствует D4.
- Fail-open: при ошибке запроса флаг загруженности ставится, множество пустое,
  новое правило молчит, `tableExistsInMetadata` возвращается к текущему
  поведению. Контракт корректен.

### 4.3. Извлечение имён (F5)

Проверено на живом индексе:

- `156` view-фрагментов, `105` уникальных имён — совпадает с design;
- `0` фрагментов, где `WHERE ~*` сработал, а capture-group не дал имя;
- `0` фрагментов с более чем одним `create view` (первый матч `regexp_match`
  достаточен);
- `7` фрагментов начинаются с `#else create view ...` (препроцессорные ветки) —
  regex их корректно ловит;
- все `21` файл-определений view — `.sql`, `0` в других расширениях;
- `37` определений используют префикс `public.` (в нижнем регистре) — срезание
  работает;
- коллизий view-имени с `sql_tables(context='create')`, API-таблицами и
  `memory_tables` — `0`.

Вывод: на текущих данных подход рабочий. Замечание F5 — про устойчивость к
будущим данным и про устранение дублирования логики нормализации.

### 4.4. Производительность

`EXPLAIN (ANALYZE, BUFFERS)` запроса из design D1:

```
Bitmap Heap Scan on query_fragments (actual rows=156)
  Recheck Cond: query_text ~* '...'
  Rows Removed by Index Recheck: 154
  -> Bitmap Index Scan on idx_query_fragments_query_text_trgm
Planning Time: 2.032 ms
Execution Time: 26.865 ms
```

То есть trigram GIN действительно проталкивает regex в index cond, время
**~27 мс**, а не 50–130 мс — даже лучше заявленного. Порог task 5.2
(≤0.2 с/файл) реалистичен. Рекомендуется зафиксировать точный EXPLAIN в
design/tasks, чтобы деградацию индекса было видно при повторных замерах.

---

## 5. Проверка на живом индексе (evidence)

| Проверка | Результат |
|---|---|
| View-фрагменты (`component_type='sql_script'`) | 156 |
| Уникальные имена view | 105 |
| Файлы-определения view | 21, все `.sql` |
| NULL-capture при `create view` | 0 |
| Фрагменты с 2+ view-определениями | 0 |
| Коллизии с `CREATE TABLE` / API / memory | 0 / 0 / 0 |
| Серверные обращения к view (raw) | **49 в 30 файлах** (47 select + 2 update) |
| То же после свёртки параллельных ветвей | 49 ссылок в 23 путях, **33 уникальных** `(path,line,name)` |
| `tfund` | 27 ссылок в 9 файлах (raw); 15 в 6 путях после свёртки |
| `EXPLAIN ANALYZE` | Bitmap Index Scan, 156 строк, **26.865 ms** |
| `codebase review VIEW.SQL` (текущий код) | 1 finding `tableExistsInMetadata` → `tImportFileGroup`, line 350 |
| `codebase review USR_GPB_CreditCards2.sql` | 10 findings `tableExistsInMetadata` → `tFund` (совпадает с task 5.1) |
| View-поверх-view | `VIEW.SQL:347 → tImportFileGroup`; `view_trash.sql:333 → tFund` |

**Вывод по F1:** после подавления `tableExistsInMetadata` на `VIEW.SQL`
исчезнет, но появится `viewUsageInServerCode` для `tImportFileGroup` (view
внутри тела view). «0 findings» недостижим. Аналогично `view_trash.sql`.
Это следствие осознанного решения D3 («view-on-view флагается») — значит,
неверна именно приёмка, а не правило.

**Вывод по F3:** числа `44/26` и `25/7` относятся к другому состоянию индекса
или к иной методике (вероятно, без параллельных веток и/или без view-
определений). На текущем индексе актуальны `49/30` (raw) или `33` уникальных
ссылок после свёртки ветвей. Метрики нужно перемерить и зафиксировать срез.

---

## 6. Замечания (сводка)

| ID | Severity | Где | Суть | Рекомендация |
|---|---|---|---|---|
| F1 | **Blocking** | `tasks.md:12`, дельта-спека:38-43 | Task 2.3 «0 findings на VIEW.SQL» невыполнима: в файле view-поверх-view | Ожидать 1 finding на `VIEW.SQL` (и 1 на `view_trash.sql`) или изменить D3; сузить GIVEN сценария до Batch |
| F2 | High | `proposal.md:46-55`, `tasks.md:21` | MCP/CLI-списки динамические — правки не нужны; пропущен ручной список `README.md` | Убрать MCP/CLI из Impact/4.1, добавить `README.md:1037-1067` |
| F3 | High | `proposal.md:10-15`, `design.md:18-21`, `tasks.md:25-26` | Метрики `44/26`, `25/7` не воспроизводятся (`49/30`, `27/9`; после свёртки ветвей `33` уникальных) | Перемерить, указать дату/ветку/правило дедупликации, обновить приёмку |
| F4 | Medium | `design.md:52-77` | Глобальный набор view-имён vs branch-scoped `tableExistsInMetadata`; 9 имён в 2 ветках | Ограничить область или явно принять FN в Risks |
| F5 | Medium | `design.md:57-76`, `tasks.md:6` | NULL-capture, lowercase-only/один префикс, расхождение с `normalizeTableKey` | Выбирать сырое имя, нормализовать в Go; `sql.NullString` |
| F6 | Medium | `design.md:132-137`, proposal | Нет плана выкатки: новый severity-1 по умолчанию заблокирует 26–30 существующих файлов | Baseline/waiver или поэтапное включение; согласовать с CI |
| F7 | Low | `tasks.md:11` | Нет дедупликации `name:line` и `CurrentProductID` | Добавить по образцу `checkTableExistsInMetadata` |
| F8 | Low | дельта-спека:8 | «ко всем `.sql`» vs поддержка `.h` | Уточнить `.sql`/`.h` |
| F9 | Low | `design.md:80-82` | Неоднозначная ссылка `runner.go:324` | `internal/indexer/runner.go:319-324` |
| F10 | Low | `design.md:122-130`, `tasks.md:26` | Оценка 50–130 мс завышена (факт ~27 мс) | Зафиксировать EXPLAIN/число |
| F11 | Low | `tasks.md:6`, `table_exists_metadata_test.go:19` | Существующие тесты создают `Runner` без `db` | `isViewTable` должен быть nil-safe как `isMemoryTable` |
| F12 | Low | дельта-спека:64-69 | Нет контракта «при ошибке БД tableExists не подавляется» | Добавить сценарий |
| F13 | Low | `design.md:139-141` | Open Questions пуст, хотя F1/F4 открыты | Перенести туда или закрыть |

---

## 7. Что сделано хорошо

- `openspec validate` проходит; структура дельты и уровни заголовков корректны.
- MODIFIED-требование воспроизводит полный текст и все существующие сценарии —
  архивирование не потеряет поведение.
- Подход минимально инвазивен: без DDL, без bump `CurrentSchemaVersion`, без
  переиндексации; работает на текущем индексе.
- Ленивый кэш повторяет уже проверенный `isMemoryTable`, включая семантику
  «пусто ≠ не загружено» и fail-open.
- Повторное использование фильтров `tableExistsInMetadata` — единая точка
  фильтрации шума.
- Производительность подтверждена: index cond по trigram GIN, ~27 мс.
- Данные подтверждают безопасность основной идеи: 0 коллизий view↔таблица,
  0 NULL-capture, все определения во `.sql`.

---

## 8. Обязательные правки артефактов перед реализацией

1. **tasks.md 2.3** — заменить ожидание «0 findings на VIEW.SQL» на фактическое
   (1 finding view-поверх-view) и добавить `view_trash.sql`; либо пересмотреть
   решение D3 и исключить тела view-определений из правила.
2. **Дельта-спека** — сузить сценарий «Определение представления не является
   нарушением» до сниппета Batch; при необходимости добавить сценарий
   «view-определение, ссылающееся на view, флагается».
3. **tasks.md 4.1 + proposal Impact** — убрать MCP/CLI (динамические), добавить
   `README.md:1037-1067`.
4. **proposal/design** — обновить метрики до актуальных с указанием среза и
   дедупликации ветвей.
5. **design D1** — нормализовать имена в Go (`normalizeTableKey`), обработать
   `NULL`; не дублировать срезание схемы в SQL.
6. **design Risks** — добавить branch-scope (F4) и план выкатки/подавления
   существующего долга (F6).
7. **tasks.md 2.2** — дедупликация `name:line` и `CurrentProductID`.
8. **Мелочи** — `.sql`/`.h`, ссылка на `internal/indexer/runner.go`, точный
   EXPLAIN, nil-safe unit-тест, сценарий ошибки БД для подавления.

---

## 9. Итог

Изменение **можно реализовывать после правки артефактов**. Техническое решение
зрелое и подтверждено данными; блокирует не подход, а неверная приёмка
(VIEW.SQL), устаревшие метрики и неверно указанные точки документации. После
устранения F1–F3 и учёта F4–F6 change готов к реализации.
