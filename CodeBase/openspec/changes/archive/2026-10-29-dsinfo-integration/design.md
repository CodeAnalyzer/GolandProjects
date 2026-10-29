# Design: dsinfo-integration

## Context

Срезы-предшественники реализованы и определяют фактические точки расширения.

- `branch-aware-indexing` (архив 2026-10-24): `ds_products_branch`
  (`branch_key`, `is_default`, `path`, `commit_sha`), `files.ds_product_branch_id`,
  единая нормализация `internal/branchkey` (`NormalizeProduct` — из манифеста
  dsinfo, `NormalizeBranchKey` — из git-ветки).
- `branch-aware-lookup` (архив 2026-10-26): в **query-слое** двухфазный дедуп
  `chosenBranchQuery` с приоритетом `not_generated → is_default → больший id`,
  `branch_used` = нормализованный `branch_key`; 20 вызовов хелпера плюс
  специальные `SearchSymbol`, `SearchInContent`, `SearchTable`,
  `SearchQueryFragment`, `GetProcedureResult`/`GetProcedureDetails`,
  `LookupRetCode`/`LookupRetCodeByMessage`. `store.LookupScope{ProductID,
  BranchID}` и оси `branch_match → product_match → default_branch → id`
  существуют **только** в `FindLatest*` пост-обработки связей: там scope
  задаётся файлом-источником, и профиль туда не передаётся.
- `dsinfo-parser`: `dsinfo_products(product_name, branch_key, hotfix_name)`,
  `dsinfo_state.active_dsinfo_id`; `branch_key` манифеста —
  `product_name + первые три числовых токена` (`NormalizeProduct`).

Недостающее звено: превратить профиль в контекст выбора ветки для запросов и
enrichment. Решения обсуждения 2026-10-27: (1) границы поверхностей
зафиксированы явно; (2) порядок осей — `not_generated` первым, затем
`profile_match`; (3) матчинг веток — точный плюс fallback по числовым токенам
хвоста. Приоритет источников контекста (2026-10-24): `--dsinfo` >
`CODEBASE_DSINFO_ID` > активный профиль > `default_branch`.

## Goals / Non-Goals

**Goals:**

- Матчинг манифеста профиля на `ds_products`/`ds_products_branch` на лету,
  устойчивый к нестандартным именам линий.
- Единая цепочка разрешения контекста для CLI и MCP.
- `profile_match` в выборе копии там, где выбор вообще происходит; явные
  границы для поверхностей без выбора копии.
- `branch_requested`/`branch_used` + предупреждение при fallback.
- Приоритет продуктов профиля в RTI/TRC enrichment.

**Non-Goals:**

- Жёсткая фильтрация (`--strict-dsinfo`) — отклонена.
- Использование секции «Модули» (`dsinfo_modules`) для уточнения
  клиентских RTI-событий (BPL → продукт) — задел на будущее.
- Cross-branch diff, показ хотфикс-тегов/`commit_sha` рядом с датой пакета
  (митигация компромисса «линия, не хотфикс» — отдельная доработка).
- Хранение статусов матчинга в БД (снимок не изменяется — принцип
  dsinfo-parser D2).
- Выбор конфигурации спек по профилю (`query spec ...` остаётся на
  default-конфигурации; см. границы).

## Decisions

### D1. Матчинг на лету: точное совпадение + fallback по версии линии

SQL живёт в `internal/store` (конвенция проекта), сервисная обёртка — в
`internal/dsinfosvc`:

```
store.LoadDsinfoProductMatches(ctx, dsinfoID) []DsinfoProductMatch
dsinfosvc.ResolveProfile(ctx, db, dsinfoID) ([]ResolvedProduct, error)
```

`ResolvedProduct{ProductName, RawName, RequestedBranchKey, MatchedBranchID,
MatchedBranchKey, Status}`.

Алгоритм:

1. Выбрать строки `dsinfo_products` профиля и для каждого `product_name`
   найти `ds_products` (точное имя) и все его `ds_products_branch`.
2. Точное совпадение `branch_key` манифеста с `branch_key` линии — высший
   приоритет.
3. Fallback **в пределах того же продукта**: последовательность числовых
   **сегментов** `branch_key` манифеста (`fa-contracts-11-01-7` → `11,01,7`;
   цифры внутри буквенных токенов вида `ftfns440…` игнорируются) должна
   быть суффиксом последовательности числовых сегментов `branch_key` линии
   (`sup/11-01-7` → `11,01,7`). Несколько кандидатов — `is_default`, затем
   больший id (детерминизм).
4. Статусы: нет продукта — `product_not_indexed`; продукт есть, кандидатов
   нет — `branch_not_indexed`; иначе `ok` (в `MatchedBranchKey` — фактический
   ключ линии).

**Многострочные продукты** (реальный манифест: fa-contracts-ext — линии
7-02-2 и 9-01-553, fa-payments-ext — 3 строки): строки сопоставляются
независимо, а карта выбора строится по продукту — проиндексированные строки
приоритетнее fallback'а; среди проиндексированных выбирается default-линия,
затем старшая версия (числовое сравнение сегментов, `branchkey.CompareVersion`),
затем больший id. Fallback (default-ветка продукта) — только если ни одна
строка продукта не проиндексирована. `Requested` продукта — список всех его
линий манифеста (включая отсутствующие): `used ∈ Requested` предупреждения
не даёт.

Сводка `parse`/`show` считается по distinct-продуктам: `ProductsTotal`/`ProductsMatched`
— уникальные `product_name` (matched — есть ≥1 ok-строка), `BranchesMissing` —
число строк `branch_not_indexed`.

Следствия, фиксируемые явно:

- Линия `develop` индексного продукта имеет `branch_key = develop`; манифест
  never даёт `develop` (product без цифр даёт `branch_key = product_name`,
  продукт с цифрами — версию). Поэтому профиль для develop-кода всегда
  попадает в fallback: `branch_requested` = линия манифеста,
  `branch_used = develop`, ответ содержит предупреждение. Это ожидаемое
  поведение, а не ошибка матчинга.
- Fallback ограничен продуктом, поэтому не может «перескочить» на чужой
  продукт с той же версией.
- Хотфикс (`hotfix_name`) не участвует в матчинге (Non-Goal).

### D2. `profile_match` в query-слое поверх двухфазного дедупа

`query.Query` получает контекст профиля (вариант конструктора/опция, чтобы не
ломать `query.New(db)` в 10 существующих точках):

```go
type ProfileContext struct {
    ProfileID int64
    Branches  map[int64]int64     // product_id -> branch_id: проиндексированная линия (default → старшая версия → id) или default-линия при fallback
    Requested map[string][]string // product_name -> все линии манифеста продукта (вкл. отсутствующие)
}
query.New(db, query.WithProfile(pc)) // без аргумента — прежнее поведение
```

Ось приоритета в query-слое:

```
not_generated DESC,
profile_match DESC,   -- CASE WHEN (f.ds_product_id, f.ds_product_branch_id) IN (VALUES ...) THEN 1 ELSE 0 END
is_default DESC,
id DESC
```

- `profile_match` вычисляется по паре (продукт, ветка), поэтому профиль
  выбирает линию только для своих продуктов; чужие продукты падают на
  `is_default` без предупреждений. При `branch_not_indexed` карта указывает
  на default-линию продукта: профиль всё равно выбирает свой продукт, а
  внутри него — default-ветку.
- При пустом контексте ось вырождается в константу (0 строк VALUES) — на
  одно-веточном индексе и без профиля поведение идентично срезу B.
- Точки применения: `procedurePriorityOrder`-подобные ORDER BY
  (`GetProcedureResult`, `GetProcedureDetails`, `LookupRetCode`,
  `LookupRetCodeByMessage`), `chosenBranchQuery` (фаза 1 — 20 вызовов через
  параметр `priorityTail`), `SearchSymbol`/`SearchInContent`, `SearchTable`
  (его `candidates`-фаза), `SearchQueryFragment` (фаза 1 по `content_key`).
- Двухфазная семантика не меняется: профиль влияет только на выбор ветки в
  фазе 1, фаза 2 по-прежнему возвращает все строки выбранной ветки
  (одноимённые внутри ветки сохраняются, P2.2 среза B).
- `branch_requested` заполняется в Go после сканирования по `item.Product`
  из `Requested` — все линии продукта через запятую (пусто для продуктов
  вне профиля).
- `store.FindLatest*`, оси `branch_match`/`product_match` и tier-фильтр
  пост-обработки **не трогаются**: граф строится по scope источника.
- Границы (см. требование `dsinfo-resolution`): `FindCallers`,
  `SearchRelations`, inspect и spec-поверхности не выполняют выбор копии —
  профиль на них не влияет; semantic-слой desc-search не имеет продукта и
  тоже не участвует (лексический слой — участвует).

### D3. CLI и MCP поверхности

- CLI: persistent `--dsinfo <id>` на группах `query`, `rti`, `trc`
  (`cmd/query.go`, `cmd/rti.go`, `cmd/trc.go`); листовые команды читают из
  группы. `init`, `update`, `review`, `dsinfo`, `health`, `stats` флаг не
  регистрируют.
- MCP: параметр `dsinfo_id` только у инструментов, **учитывающих** профиль
  (entity/API/desc/symbol/retcode query-инструменты; rti/trc-инструменты с
  enrichment). `codebase_query_callers`, `codebase_query_relations`,
  `codebase_query_inspect`, spec-инструменты, `dsinfo_*` и review — без
  параметра. CLI-флаг на этих командах принимается, но не влияет (границы
  задокументированы).
- Env `CODEBASE_DSINFO_ID` читается в обоих транспортах одинаково
  (серверный процесс наследует окружение); некорректный формат — ошибка
  наравне с несуществующим id — **при применении контекста**. На
  поверхностях вне границ профиля и при отсутствии элементов для enrichment
  контекст не разрешается вовсе: там невалидный id не диагностируется
  (ослабленная семантика цепочки, решение по code review).
- Терминологическое примечание README: «профиль dsinfo» ≠ `--profile`
  MCP-сервера (уже зафиксировано в dsinfo-parser).

### D4. `branch_requested` / `branch_used` / предупреждение

- `branch_used` — существующее поле среза B (нормализованный `branch_key`).
- `branch_requested` — все линии манифеста продукта через запятую; пусто,
  если продукт вне профиля или профиль не задан.
- **Предупреждения вычисляются по фактическому ответу, а не по профилю**
  (фикс P1.1 code review): после выполнения запроса коллектор
  `query.CollectBranchWarnings` обходит элементы результата (рефлексивно,
  включая вложенные списки параметров/таблиц API и узлы RTI-дерева),
  читает тройку `Product`/`BranchRequested`/`BranchUsed` и формирует
  предупреждение, когда использованная линия не входит в запрошенные.
  Продукты, которых в ответе нет, предупреждений не порождают (нет шума от
  60+ неиндексированных строк манифеста); пустой ответ — без предупреждений.
- CLI: конверт `querySuccessResponse` получает `warnings []string`
  (`omitempty`) рядом с `meta`; в `--summary`/человекочитаемом выводе —
  строки после результата.
- MCP: `warnings []string` в верхнем уровне результата инструмента
  (`runQueryOpt` добавляет поле только при непустом списке); rti/trc-сервисы
  заполняют `result.Warnings` тем же коллектором.
- Агрегация: одна строка на (продукт, запрошенные линии, использованная
  линия): «линия(и) X продукта P не проиндексирована(ы), показан код ветки
  Y — тела могут отличаться…»; без ветки — «копия выбрана без привязки к
  ветке». Профиль не влияет на поверхности вне границ и не порождает
  предупреждений.

### D5. RTI/TRC enrichment

- `rtisvc`/`trcsvc` уже создают `query.New(db)` внутри функций; контекст
  пробрасывается через Params-структуры enrichment-команд
  (`DsinfoID int64`) → разрешение после открытия БД →
  `query.New(db, query.WithProfile(pc))`.
- Интерфейс `rti.ProcedureLookup`/`trc.ProcedureLookup` не меняется:
  `GetProcedureResult` читает контекст профиля из `Query`. Это сохраняет все
  существующие фейки тестов без правок.
- `ProcedureEnrichment` получает `Product` и `BranchUsed`
  (json `product`, `branch_used`; значение — `branch_key`). Старые поля не
  меняются.
- TRC-агрегации: `enrichProcedureAggregates` получает `*query.Query` с
  профилем (оба пути — `ExecuteProcedures` и агрегатный).
- Глобальный поиск имени без известного продукта использует ту же ось
  `profile_match` (D2), поэтому профильный продукт/линия выигрывает у
  чужого продукта.

### D6. Неизменность снимка и идемпотентность parse

`dsinfo parse` НЕ перезаписывает профиль при изменении индекса; reuse по
`(file_hash, parser_version)` сохраняется. Таблица сопоставления —
результат живого JOIN при выводе, поэтому повторный `show` (и `parse` при
reuse) после `codebase update` показывает свежие статусы.

## Risks / Trade-offs

- [Fallback по числовым токенам может совпасть с чужой линией того же
  продукта] → сравнение ограничено продуктом, точное совпадение имеет
  приоритет, при нескольких кандидатах — `is_default`/id; покрывается
  тестом.
- [Профиль задаёт приоритет, часть линий не проиндексирована] → fallback на
  default обязателен; каждая подмена видима (`branch_requested` + warning).
- [Активный профиль забыт и влияет на все вызовы] → `dsinfo list` помечает
  активный (`*`); env/флаг перекрывают; README фиксирует семантику.
- [Границы CLI и MCP различаются] → CLI-флаг принимается на всей группе, но
  влияет по границам; MCP-параметр выставляется только на учитывающих
  инструментах. Поведение зафиксировано требованием и README.
- [Карта профиля в каждом lookup'е удорожает SQL] → до 62 пар
  `(product_id, branch_id)` в VALUES; при деградации планов — материализация
  карты в temp-таблицу на время запроса.
- [MCP-инструменты: рост числа параметров] → параметр один и опциональный,
  только на учитывающих инструментах; схемы обновляются по образцу
  существующих optional-полей.

## Migration Plan

Без миграции схемы. Порядок развёртывания строго после срезов A и B.
Обратно совместимо: без профиля — поведение среза B; старые MCP-клиенты
просто не передают `dsinfo_id`. Дельты `query/entity-queries` и
`query/api-relations-queries` входят в это изменение и снимают справочные
отсылки к будущему профилю при архивации.
