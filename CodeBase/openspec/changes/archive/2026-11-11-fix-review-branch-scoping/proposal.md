## Why

После перехода индекса на многоветочную модель (одна и та же логическая единица
индексируется отдельно для каждой релизной линии) review продолжил выполнять
DB-lookup'ы без учёта ветки: `indexedFile` не содержит `ds_product_branch_id`,
а запросы либо не имеют scope, либо передают `LookupScope{BranchID: 0}`. Из-за
этого копии из разных веток складываются в один результат и правила формируют
ложные findings. На текущей БД это, например, **10 635** имён процедур,
«дублированных» только потому, что они существуют в develop и релизных линиях
одного продукта (из них 9 489 имеют идентичный `body_hash`), **771**
расхождение типов колонок между ветками и **81** расхождение сигнатур процедур.

Review проверяет файл, который деплоится в конкретную ветку. Значит lookup
обязан отвечать на вопрос «есть ли это в ветке проверяемого файла», а не «есть
ли это где угодно».

## What Changes

- Ввести единый `ReviewScope` для всех DB-lookup'ов review:
  - продукт и ветка — из проверяемого файла (`files.ds_product_id`,
    `files.ds_product_branch_id`); `branch_id = NULL` трактуется как
    product-scope без ветки;
  - для чужих продуктов — цепочка `--dsinfo <id>` (CLI) / `dsinfo_id` (MCP) >
    `CODEBASE_DSINFO_ID` > активный профиль (`dsinfo use`) > default-ветка
    продукта (`ds_products_branch.is_default`);
  - для EXT-продуктов (ветки `prj/*`; поставка в банк = `develop ∪ prj/<банк>`)
    область — объединение: файл `prj/*`-ветки видит свою ветку и default-ветку,
    файл default-ветки — default и все `prj/*`, чужие EXT-продукты —
    default ∪ `prj/*` (или default ∪ профильная prj).
- Разделить lookup'ы на три режима:
  - **existence strict** — `execNotExistsProc`, `tableExistsInMetadata`,
    `indexExistsInDB`: сущность должна существовать именно в ветке своего
    продукта;
  - **value priority** — типы колонок, параметры процедур, индексы и поля
    индексов, тип макроса: приоритет `файл → продукт+ветка → ветка продукта →
    продукт → default-ветка → id DESC`, без объединения чужих веток;
  - **identity (без ветки)** — `foreignTablesUsing`, `foreignPTablesUsing`,
    `foreignProcedureUsing`: «чужой продукт» — ось продукта, ветка не влияет;
    для процедур свой продукт проверяется раньше объявления чужим.
- Исключения (глобально по имени, без веточного скоупинга): декларации
  in-memory таблиц (`memory_tables`) и константы `#define`
  (`h_files_defines`).
- `procDuplicate`: считать дубли в пределах продукта и ветки проверяемого файла
  **и** отдельно детектировать дублирование имени в одном файле (сейчас
  `SELECT DISTINCT f.id` его не ловит).
- Добавить `--dsinfo` в CLI `review` и `dsinfo_id` в MCP
  `codebase_review_sql`.
- Затрагиваемые правила: `execNotExistsProc`, `procDuplicate`,
  `indexExistsInDB`, `tableExistsInMetadata`, `indexWrong`, `deferredUpdate`,
  `memoryUniqueIndexUpdate`, `useFuncInIndCol`, `excessProcParams`,
  `duplicateOutputVariable`, `datatype` (включая `EXEC`-параметры),
  `dateIntoString`, `emptyStringDate`, `isNullSameTypes`,
  `diffTypesComparison`, `floatToStringConvert`, `foreignTablesUsing`,
  `foreignPTablesUsing`, `foreignProcedureUsing`.

## Capabilities

### New Capabilities
- `review/review-scope`: разрешение области видимости review (файл → продукт +
  ветка, профиль dsinfo для чужих продуктов, default-ветка), область
  EXT-продуктов (`develop ∪ prj`), режимы strict/priority/identity для
  DB-lookup'ов с явными глобальными исключениями (`memory_tables`,
  `#define`), семантика `procDuplicate` и поверхности `--dsinfo`/`dsinfo_id`.

### Modified Capabilities
- `review/deploy-stoppers`: «Дублирование процедур» переходит на product+branch
  и within-file; «Вызов несуществующих процедур», «Использование индексов,
  отсутствующих в БД», «Существование таблицы в метаданных» становятся
  branch-scoped.
- `review/fine-code`: требование «Потенциальная потеря точности» дополняется
  веткой в приоритете разрешения типа колонки (файл → продукт+ветка → ветка
  продукта → default → глобально).
- `dsinfo-analysis/dsinfo-resolution`: цепочка разрешения контекста анализа
  распространяется на команду `review`.
- `mcp-server/mcp-transport-tools`: `codebase_review_sql` начинает принимать
  `dsinfo_id`; требование «Параметр dsinfo_id у инструментов анализа» перестаёт
  исключать review.

Требования `review/postgre-requirements` текстуально не меняются: их поведение
определяется центральным требованием `review/review-scope` о scope всех
DB-lookup'ов; ни одно из их требований не задаёт scope явно.

## Impact

- **Данные/БД**: `internal/store/db_lookup_sql.go` (`BatchLookupProcedureParams`,
  `BatchLookupProcedureProductIDs`, `BatchLookupTableProductIDs`,
  `BatchLookupTableCreators`, `BatchFindColumnDefinitionTypes`,
  `FindLatestSQLColumnDefinitionType`, `FindAPIColumnDefinitionType`) получают
  scope; `LookupScope` переиспользуется.
- **Review**: `internal/review/types.go` (`indexedFile`), `runner.go`
  (`reviewExecContext`, prewarm-кэши), `review_lookup.go` (все lookup'ы),
  `review_rules.go` (правила из списка), `reviewsvc/runtime.go`.
- **Поверхности**: `cmd/review.go` (флаг `--dsinfo`),
  `internal/mcp/registry.go` (`dsinfo_id` в `codebase_review_sql`).
- **Спеки**: новый `review/review-scope`, дельты в `review/deploy-stoppers`,
  `review/fine-code`, `dsinfo-analysis/dsinfo-resolution`,
  `mcp-server/mcp-transport-tools`.
- **Совместимость**: набор правил и их severity не меняются. Меняется только
  область поиска: часть прежних findings исчезает (межветочные ложные
  срабатывания, включая 6 ложных `tableExistsInMetadata` у prj-файлов
  EXT-продуктов), при этом появляются корректные findings для сущностей,
  отсутствующих именно в целевой ветке (замер масштаба: `fa-contracts-11-01-7`
  — 28 имён, `fa-contracts-11-01-6` — 63, `fa-cards-11-01-9` — 53,
  develop-ветки прочих продуктов — 0–16). Для файлов без ветки
  (`branch_id NULL`) поведение — product-scope, как и раньше.
