# Bug: `datatype` — ложное срабатывание при разрешении типа колонки p-таблицы из чужого продукта

**Дата:** 2026-09-30
**Статус:** Open
**Приоритет:** Low (False positive, severity 3 — fine code, не блокирует deploy)
**Компонент:** SQL Review (`datatype` rule)
**Правило:** `datatype`
**Версия CodeBase:** 0.9.2 build 1539

## Файл воспроизведения

`C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Accrual\CreditTurnoverLink_MassProcess.sql`

## Шаги воспроизведения

```powershell
PS C:\NT\FA#\7.2GIT> codebase review C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Accrual\CreditTurnoverLink_MassProcess.sql
```

## Фактический результат

`Findings: 6` — все severity 3, правило `datatype`, тип `DSIDENTIFIER -> DSINT_KEY`:

```
- [3] datatype line=126 object=pConsQtyListRight.QtyType
  Потеря точности типов данных: DSIDENTIFIER -> DSINT_KEY
- [3] datatype line=158 object=pConsQtyListLeft.QtyType
  Потеря точности типов данных: DSIDENTIFIER -> DSINT_KEY
- [3] datatype line=465 object=pConsQtyListRight.QtyType
  Потеря точности типов данных: DSIDENTIFIER -> DSINT_KEY
- [3] datatype line=497 object=pConsQtyListLeft.QtyType
  Потеря точности типов данных: DSIDENTIFIER -> DSINT_KEY
- [3] datatype line=760 object=pConsQtyListLeft.QtyType
  Потеря точности типов данных: DSIDENTIFIER -> DSINT_KEY
- [3] datatype line=803 object=pConsQtyListRight.QtyType
  Потеря точности типов данных: DSIDENTIFIER -> DSINT_KEY
```

Все шесть — это операторы `insert into pConsQtyListRight/Left (...) select ...`, например строки 127–141 и 466–480. Реальной потери точности нет: целевая p-таблица объявлена как `DSIDENTIFIER` (совпадает с источником).

## Ожидаемый результат

`Findings: 0`. Ложные срабатывания не должны появляться.

## Технический анализ (корневая причина)

### 1) Чекер берёт тип целевой колонки глобальным запросом к индексу

`checkDatatypeInsertSelect` (review_rules.go:2213, вызов из `checkDatatype` :1929) определяет тип приёмника через `cachedFindColumnDefinitionType` (review_rules.go:2238).

`cachedFindColumnDefinitionType` (review/runner.go:392) → `FindLatestSQLColumnDefinitionType` (store/db_lookup_sql.go:173):

```sql
SELECT data_type FROM sql_column_definitions
WHERE LOWER(table_name) = LOWER($1)
  AND LOWER(column_name) = LOWER($2)
  AND TRIM(COALESCE(data_type, '')) <> ''
  AND data_type <> 'DSUNKNOWN'
ORDER BY id DESC
LIMIT 1
```

**Ключевая проблема:** поиск идёт **только по паре `(table_name, column_name)`**, без привязки к продукту, файлу или схеме, и берётся **самая свежая запись по `id DESC`**. Если одноимённая таблица объявлена в нескольких продуктах — побеждает тот файл, который был проиндексирован позже.

### 2) `QtyType` в pConsQtyListRight/Left объявлен дважды с разными типами

| Файл | Строка | Тип `QtyType` | CodeBase file_id |
|---|---|---|---|
| `fa-contracts/Consumer/SERVER/Consumer/pConsQtyListRight_TmpTbl.sql` | 18 | **DSIDENTIFIER** | 47110 |
| `fa-contracts/Consumer/SERVER/Consumer/pConsQtyListLeft_TmpTbl.sql` | 18 | **DSIDENTIFIER** | 47109 |
| `fa-reports/ADP_ConsumerToReports/Server/ContractCredit/api_rpt_ContractCredit_tmptbl.sql` | 875 | **DSINT_KEY** | 142907 |
| `fa-reports/ADP_ConsumerToReports/Server/ContractCredit/api_rpt_ContractCredit_tmptbl.sql` | 837 | **DSINT_KEY** | 142907 |

Подтверждено через CodeBase (`query_symbol --name QtyType --type column_definition`). Файл `fa-reports` проиндексирован позже (file_id 142907 против 47110), поэтому `ORDER BY id DESC LIMIT 1` возвращает `DSINT_KEY` из копии в `fa-reports`, а не `DSIDENTIFIER` из исходной DDL `fa-contracts`.

### 3) Источник выражения — `DSIDENTIFIER`

В операторе `insert into pConsQtyListRight(... QtyType ...) select ..., ct.ConsDebtSubcontoID, ...` источником колонки `QtyType` является `ct.ConsDebtSubcontoID` из `pCons_CTL_Turnover`:

`fa-contracts/Consumer/SERVER/Accrual/pCons_CTL_Turnover_TmpTbl.sql:12` — `pCons_CTL_Turnover.ConsDebtSubcontoID DSIDENTIFIER`.

### 4) Почему выводится «потеря точности»

`isPotentialPrecisionLoss` (review_helpers.go:1188) через `numericPrecisionScale` (:1210) сопоставляет:
- `DSIDENTIFIER` → precision **15** (review_helpers.go:1229);
- `DSINT_KEY` → precision **10** (review_helpers.go:1221).

Условие `sourceP > targetP` (15 > 10) истинно → правило рапортует «Потеря точности типов данных: DSIDENTIFIER -> DSINT_KEY», хотя фактический тип приёмника в `fa-contracts` — тоже `DSIDENTIFIER`.

## Итог

Ложное срабатывание вызвано не кодом `CreditTurnoverLink_MassProcess.sql`, а дефектом резолвера типов CodeBase: тип колонки p-таблицы разрешается из **чужих дублирующих DDL** (`fa-reports ADP_ConsumerToReports`) без привязки к исходному продукту. Комбинация «глобальный lookup + ORDER BY id DESC + уникальные p-таблицы, объявленные в разных продуктах» + «DSIDENTIFIER=15 > DSINT_KEY=10» даёт 6 ложных finding'ов.

## Предлагаемое исправление

1. **Скоупить поиск типа колонки.** В `FindLatestSQLColumnDefinitionType` учитывать продукт/схему исходного файла (например, передавать `product` или `file_id`), либо в `cachedFindColumnDefinitionType` сначала искать определение колонки **в текущем файле**, затем — в текущем продукте, и только потом — глобальный fallback.
2. **Исключать файлы конвертации/адаптеров.** Файлы вида `fa-reports/ADP_*` и прочие `*_tmptbl.sql` конвертационных утилит не должны перекрывать DDL исходного продукта для одноимённых p-таблиц.
3. **Не «склеивать» одноимённые p-таблицы разных продуктов.** p-таблицы локальны для сессии/продукта; их определения из разных продуктов не должны конкурировать в одном пространстве имён.
4. **Unit-тест:** вход — два файла с одинаковой p-таблицей и разным типом колонки, индекс второго файла позже; ожидание — тип берётся из файла/продукта, соответствующего анализируемому SQL.

## Влияние

- Регулярные ложные `datatype` findings (severity 3) для любых p-таблиц, объявленных в нескольких продуктах с разными типами одноимённых колонок.
- Шум в отчётах review, снижение доверия к правилу `datatype`.
- Blocker'ом не является (fine code, deploy не блокируется).

## Связанные файлы

- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_rules.go` (`checkDatatype` :1929, `checkDatatypeInsertSelect` :2213, `checkDatatypeUpdateSet` :2150)
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\runner.go` (`cachedFindColumnDefinitionType` :392)
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\store\db_lookup_sql.go` (`FindLatestSQLColumnDefinitionType` :173, `FindAPIColumnDefinitionType` :193)
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_helpers.go` (`isPotentialPrecisionLoss` :1188, `numericPrecisionScale` :1210)
- `C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Accrual\CreditTurnoverLink_MassProcess.sql`
- `C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Consumer\pConsQtyListRight_TmpTbl.sql`
- `C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Consumer\pConsQtyListLeft_TmpTbl.sql`
- `C:\NT\FA#\7.2GIT\fa-reports\ADP_ConsumerToReports\Server\ContractCredit\api_rpt_ContractCredit_tmptbl.sql`
