# Proposal: fix-datatype-cross-product-lookup

## Why

Правило `datatype` (severity 3) разрешает тип целевой колонки INSERT/UPDATE
глобальным lookup'ом по паре `(table_name, column_name)` с `ORDER BY id DESC
LIMIT 1`, без привязки к продукту анализируемого файла. Одноимённые p-таблицы
объявлены в нескольких продуктах с разными типами (fa-contracts vs
fa-reports/ADP-копии, dbo.spid в 7 продуктах), и побеждает файл,
проиндексированный последним. Результат — ложные findings «Потеря точности»
на корректном коде (bug-report
`Bug-reports/BUG-datatype-ptable-cross-product-type-lookup-20260930.md`:
6 ложных срабатываний в `CreditTurnoverLink_MassProcess.sql`) и, симметрично,
замаскированные реальные потери точности. Диагностика на локальном индексе:
825 пар, где fa-contracts проигрывает первенство, из них 30 — с реальной
сменой типа после продуктового скопирования.

## What Changes

- Резолвер типов колонок в review (`FindLatestSQLColumnDefinitionType`,
  `BatchFindColumnDefinitionTypes`) получает контекст анализируемого файла
  (`file_id`, `ds_product_id`) и выбирает определение колонки по трёхуровневому
  приоритету: определение из **того же файла** → из **того же продукта** →
  глобальный latest-wins (как сейчас).
- `cachedFindColumnDefinitionType` и prewarm `prewarmColTypeCache` в Runner
  передают контекст файла в store-методы; кэш типов ключуется с учётом контекста
  (ключ остаётся `table|col` в пределах одного RunSQLFileCtx — контекст
  фиксирован на запуск).
- API-fallback `FindAPIColumnDefinitionType` получает ту же приоритизацию
  по продукту (same-product → latest).
- Поведение правила `datatype` меняется наблюдаемо: исчезают ложные срабатывания
  на кросс-продуктовых дубликатах DDL; могут появиться новые корректные findings,
  которые раньше маскировались чужим типом.
- Парсерные артефакты в `sql_column_definitions` (имена колонок с ведущей
  запятой `,name`, типы с несбалансированными скобками `numeric(15`, `DSMONEY)`)
  **не входят** в этот change — фиксируются отдельным изменением (см. design, Non-Goals).

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/fine-code`: требование «Потенциальная потеря точности» дополняется
  правилом разрешения типа колонки-приёмника — приоритет определений из текущего
  файла и текущего продукта над глобальными; добавляются сценарии отсутствия
  ложного срабатывания на кросс-продуктовом дубликате DDL и корректного
  разрешения #temp-таблицы из того же файла.

## Impact

- `internal/store/db_lookup_sql.go` — `FindLatestSQLColumnDefinitionType`,
  `FindAPIColumnDefinitionType`, `BatchFindColumnDefinitionTypes` (новые параметры
  контекста, SQL с приоритизацией).
- `internal/review/runner.go` — `cachedFindColumnDefinitionType`,
  `prewarmColTypeCache` (проброс `file.ID`/`file.DsProductID`).
- `internal/review/review_rules.go` — `checkDatatypeInsertSelect`,
  `checkDatatypeUpdateSet` и другие потребители `cachedFindColumnDefinitionType`
  (сигнатуры вызовов).
- Unit-тесты review: воспроизведение кейса «два файла, одна p-таблица, разные
  типы» (см. tasks).
- Совместимость: CLI/MCP-контракты не меняются; меняется только набор findings
  (ожидаемо для bug-fix).
