# Bug: резолвер упоминаний спек выбирает `MAX(id)` без скоупа — покрытие уходит на копии (`UPLOAD/.t01`), канонический исходник остаётся «непокрытым»

**Дата:** 2026-09-30
**Статус:** Close
**Приоритет:** Medium (искажение метрики покрытия спеками, не блокирует deploy)
**Компонент:** Spec post-processing (`internal/indexer` + `internal/store`)
**Функция:** `FindLatestSQLProcedureIDsByNames` (и родственные name→id lookup’ы)
**Версия CodeBase:** 0.9.2 build 1539

## Суть

При построении связей `references_code` (покрытие кода спеками) резолвер имён берёт **последний по `id`** одноимённый артефакт **из всей БД, без привязки к продукту и без отсечения генерируемых копий**. Если одна и та же процедура объявлена в исходном каталоге и в сгенерированных копиях (`*/UPLOAD/*.sql`, `*.t01`, а также в чужом продукте), покрытие «прилипает» к копии, а **канонический исходный файл остаётся без входящей связи** и в отчётах о непокрытом коде показывается как пробел, хотя фактически он покрыт.

Это тот же класс дефекта, что и в `BUG-datatype-ptable-cross-product-type-lookup-20260930.md` (глобальный lookup + `ORDER BY id DESC` без скоупа).

## Файл воспроизведения

- Спека: `C:\NT\FA#\7.2GIT\fa-contracts\openspec\specs\accrual-core\base-algorithms\rest\cons-min-rest\spec.md`
  - строка 95: `- \`Consumer/SERVER/Accrual/BaseAlg_ConsMinRest.sql\` — расчёт минимального остатка за период начисления`
- Код (две одноимённые записи `sql_procedures` с именем `BaseAlg_ConsMinRest`):
  - канонический: `fa-contracts/Consumer/SERVER/Accrual/BaseAlg_ConsMinRest.sql`
  - копия: `fa-contracts/LoanBureau/Server/UPLOAD/BaseAlg_ConsMinRest.sql`

## Шаги воспроизведения

1. Проиндексировать `fa-contracts` (индекс содержит и исходники, и каталоги `UPLOAD/`, `*.t01`).
2. Убедиться, что спека `accrual-core/base-algorithms/rest/cons-min-rest` упоминает `BaseAlg_ConsMinRest` (по бэктику/пути `.sql`).
3. Проверить покрытие: `codebase query spec-coverage --product fa-contracts --name accrual-core/base-algorithms/rest/cons-min-rest` → в `covered` присутствует `BaseAlg_ConsMinRest` (покрытие есть).
4. Выполнить запрос «непокрытый код продукта» (перечисление `sql_procedures` продукта с анти-join к `relations(references_code)`).

## Фактический результат

В выборке непокрытого кода присутствует **канонический** файл:

```
sql_procedure  BaseAlg_ConsMinRest  fa-contracts/Consumer/SERVER/Accrual/BaseAlg_ConsMinRest.sql
```

...при том что по индексу сущность считается покрытой. Аналогично всплывают и дубликаты одной процедуры (в разных копиях):

```
BaseAlgAmrtCostSinglePmnt -> fa-contracts/Consumer/SERVER/Accrual/BaseAlgAmrtCostSinglePmnt.sql
BaseAlgAmrtCostSinglePmnt -> fa-contracts/Consumer/SERVER/UPLOAD/BaseAlgAmrtCostSinglePmnt.sql
BaseAlgAmrtCostSinglePmnt -> fa-contracts/API_Credit/Server/UPLOAD/BaseAlgAmrtCostSinglePmnt.sql
BaseAlgAmrtCostSinglePmnt -> fa-contracts/API_Credit/Server/UPLOAD/BaseAlgAmrtCostSinglePmnt.t01
```

Подтверждение, что покрытие ушло на копию:

```json
// codebase query spec-by-code --name BaseAlg_ConsMinRest
{ "mention_name": "BaseAlg_ConsMinRest", "mention_kind": "procedure", "resolved": true,
  "hits": [ { "capability_name": "accrual-core/base-algorithms/rest/cons-min-rest", ... } ] }
```

Связь `references_code` указывает на **id копии**, поэтому у канонического id входящей связи нет.

## Ожидаемый результат

Упоминание `BaseAlg_ConsMinRest` в спеке продукта `fa-contracts` SHALL резолвиться в **канонический** артефакт продукта (`Consumer/SERVER/Accrual/...`), а не в генерируемую копию (`*/UPLOAD/*`, `*.t01`) и не в артефакт чужого продукта. Канонический файл не должен попадать в отчёт о непокрытом коде.

## Технический анализ (корневая причина)

### 1) Резолвер имени в id берёт `id DESC` глобально

`internal/store/db_lookup_sql.go:29-75` — `FindLatestSQLProcedureIDsByNames`:

```sql
SELECT DISTINCT ON (proc_key) proc_key, id
FROM (
    SELECT LOWER(proc_name) AS proc_key, id
    FROM sql_procedures
    WHERE LOWER(proc_name) = ANY($1)
) AS procedures
ORDER BY proc_key, id DESC          -- :55 — самая «свежая» запись по id
```

- фильтра по продукту (`files.ds_product_id`) нет;
- фильтра по типу файла/каталогу нет (`UPLOAD/`, `.t01`, `DB/`, `Seed/` — не исключены);
- при нескольких одноимённых записях всегда побеждает максимальный `id` (обычно — позже проиндексированная копия).

Родственные функции используют тот же паттерн:
- `FindLatestSQLTableIDsByNames` — `db_lookup_sql.go:131`, `ORDER BY table_key, id DESC` (`:156`);
- `FindDFMFormIDsByNames` — `db_lookup_spec_mentions.go:82`, `MAX(id)` (`:93`);
- `FindSMFInstrumentIDsByNames` — `:116`, `MAX(id)` (`:127`);
- `FindPASMethodIDsByNames` — `:150`, `MAX(id)` (`:161`);
- `FindAPIContractIDsByNames` — `:184`, `MAX(id)` (`:195`);
- `FindReportFormIDsByNames` — `:218`, `MAX(id)` (`:229`).

### 2) Куда попадает результат

`internal/indexer/indexer_postprocess_spec.go`:
- `:45` — `lookup.Procedures` = `FindLatestSQLProcedureIDsByNames(procNames)`; второй вызов для `unknown` имён — `:118`.
- `:209-216` (`buildSpecMentionRelations`) — для `mention_kind = "procedure"` создаётся relation с `target_type = 'sql_procedure'` и **этим** `target_id`.

То есть в `relations.references_code.target_id` проставляется id копии (max id), а не исходника.

### 3) Почему копии вообще близки к «канону»

`use cases`:
- `UPLOAD/` — каталоги предпроцессора/выгрузки с копиями `.sql` и `.t01` (тот же `proc_name`);
- один и тот же алгоритм может быть скопирован в другой модуль/продукт (напр. `LoanBureau/Server/UPLOAD`, `API_Credit/Server/UPLOAD`).

Итог: покрытие консистентно «садится» на копию, а исходник становится мнимым пробелом; в отчёте появляются дубли одной сущности.

## Влияние

- **Искажение покрытия спек:** канонические сущности ложно «непокрыты», копии ложно «покрыты».
- **Шум в отчётах:** в выборке `Uncovered_code2.txt` (fa-contracts) ~**1300 строк** — копии `UPLOAD/.t01`, не отдельные артефакты.
- Затрагивает все потребители связей `references_code`:
  - `codebase query spec-coverage`, `spec-by-code`, `spec-search`;
  - запросы «код, не покрытый спеками»;
  - любые impact/аудит-отчёты, где важно, «покрыта ли конкретная исходная процедура».
- Аналогично деформируется резолвинг форм/SMF/методов/API/отчётов (родственные функции с `MAX(id)`).

## Предлагаемое исправление

1. **Отсекать генерируемые копии в резолвере.** В кандидатах исключить `*/UPLOAD/*` и `extension = 't01'` (и служебные `DB/`, `Seed/`, `Patch/`), если остаются непустые кандидаты.
2. **Скоупить по продукту.** Передавать в lookup продукт спеки (или `ds_product_id`) и при прочих равных предпочитать артефакт **того же** продукта. Иначе — детерминированный fallback.
3. **Задать порядок предпочтения вместо `id DESC`:** негенерируемый источник → свой продукт → канонический каталог (`SERVER`/`Server` исходного модуля) → стабильный tie-break (напр. `MIN(id)` или `rel_path`).
4. **Приоритет пути.** Если упоминание — это путь с расширением (`.sql`/`.dfm`/`.xml`…), сначала резолвить **по точному `rel_path`**, и лишь затем — по имени.
5. **Применить ту же логику** к `FindLatestSQLTableIDsByNames`, `FindDFMFormIDsByNames`, `FindSMFInstrumentIDsByNames`, `FindPASMethodIDsByNames`, `FindAPIContractIDsByNames`, `FindReportFormIDsByNames`.
6. **Тесты:**
   - вход: две записи `sql_procedures` с именем `P` — источник `SERVER/.../P.sql` и копия `.../UPLOAD/P.sql` (id копии больше); спека продукта упоминает `P`; ожидание — relation указывает на **источник**;
   - кейс с кросс-продуктовой копией: ожидание — резолв в артефакт продукта спеки;
   - кейс с упоминанием пути `.sql`: ожидание — резолв по точному пути.

### Временный обход (до фикса)

При сравнении покрытия сопоставлять сущности **по имени** (`kind` + `LOWER(name)`), а не по `target_id`, и исключать из перечисления копии (`rel_path !~ '/UPLOAD/' AND extension <> 't01'`). Это нейтрализует промах резолвера по id.

## Связанные файлы

- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\store\db_lookup_sql.go`
  - `FindLatestSQLProcedureIDsByNames` :29-75 (`ORDER BY proc_key, id DESC` :55)
  - `FindLatestSQLTableIDsByNames` :131 (`ORDER BY table_key, id DESC` :156)
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\store\db_lookup_spec_mentions.go`
  - `FindDFMFormIDsByNames` :82/93, `FindSMFInstrumentIDsByNames` :116/127, `FindPASMethodIDsByNames` :150/161, `FindAPIContractIDsByNames` :184/195, `FindReportFormIDsByNames` :218/229
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\indexer\indexer_postprocess_spec.go`
  - вызовы lookup :45, :118; `buildSpecMentionRelations` :182-244 (procedure → sql_procedure :215-216)
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\specsvc\specsvc.go` (потребитель `references_code` в покрытии :799-908)
- `C:\NT\FA#\7.2GIT\fa-contracts\openspec\specs\accrual-core\base-algorithms\rest\cons-min-rest\spec.md` (строка 95)
- `C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Accrual\BaseAlg_ConsMinRest.sql` (канонический источник)
- `C:\NT\FA#\7.2GIT\fa-contracts\LoanBureau\Server\UPLOAD\BaseAlg_ConsMinRest.sql` (копия)

## Способ обнаружения

- Анализ результата запроса «код fa-contracts, не покрытый спеками» (`Uncovered_code2.txt`): в выдаче оказались процедуры (`BaseAlg_ConsMinRest`, `BaseAlgAmrtCostSinglePmnt`, `BaseAlgCalcCreditPortfolio`, `BaseAlgCheckLastWorkDayMonth`, `BaseAlgSumRegistryOfOuterAmount`), которые по `spec-coverage` считаются покрытыми.
- Сопоставление путей: канонический источник помечен непокрытым, а покрытие ссылается на копию в `*/UPLOAD/*`.
- Верификация кода: `db_lookup_sql.go:48-56` — `DISTINCT ON (proc_key) ... ORDER BY proc_key, id DESC` без продукта/пути.
