# Fallback поиска `report_form`-символов по form_name/form_class

Устранение асимметрии unified-индекса `symbols`: инструмент `symbol` с
`type=report_form` находит сущность только по `report_name` (BCH_CREXML и т. п.),
но не по имени DFM-формы (`form_name`/`form_class`: FormRepBCHPeriod /
TFormRepBCHPeriod). Для `type=form` аналогичный fallback на `signature`
(DFM-класс) уже реализован — для `report_form` его нет.

Статус: **реализовано** 2026-10-18 (build 1609) — change
`openspec/changes/archive/2026-10-18-symbol-report-form-name-fallback`; повторный прогон
TC-142 — PASS (`Tests/CODEBASE_QUERY_TEST_RESULTS_2026-10-18.md`).
Дополнительно к исходному плану: инструмент `report_form` расширен полем
`form_class` (устранение инверсии асимметрии), условие lookup и JOIN
управляются общим предикатом `needsReportFormsLookup`.

---

## Контекст проблемы

Факты прогона тест-кейсов 2026-10-11 (`Tests/CODEBASE_QUERY_TEST_RESULTS_2026-10-11.md`, TC-142)
+ SQL-диагностика БД:

- **TC-142** (`symbol {"name":"FormRepBCHPeriod","type":"report_form"}`) → `count=0`,
  при том что `report_form` (TC-079) находит 6 записей с `form_name=FormRepBCHPeriod`
  (BCH_CREXML, BCH_CREXMLv10.1_Correction, BCH_RUTDF_Period,
  BCH_SingleFormat_Correction, BCH_SingleFormat_Period, BCH_UCH_Period — все
  `fa-contracts/LoanBureau/Reporting/Business/*.rpt`, id 5313/5315/5316/5318/5323/5324).
- SQL-проверка: пропусков индексации **нет** — все 1260 `report_forms` имеют
  соответствующие `symbols`-строки (`report_forms_without_symbol = 0`).
- Но `symbol_name` для `report_form` = только `report_name`
  (`internal/indexer/indexer.go:1022-1029`: `SymbolName: result.Form.ReportName`),
  плюс внутренние элементы (params, методы, компоненты). **`form_name`/`form_class`
  в `symbols` не попадают** — они хранятся только как атрибуты таблицы
  `report_forms` (поля `form_name`, `form_class`).
- Асимметрия в lookup-условии: для `type=form` `buildSymbolLookupCondition`
  (`internal/query/query.go:261-275`) расширяет поиск на `s.signature`
  (DFM-класс формы); для `report_form` фолбэка на `form_class`/`form_name` нет.

### Корневая причина

`SearchSymbol` (`internal/query/query.go:297-357`) ищет только по
`s.symbol_name` (+ специальный случай `signature` для `form`). Данные DFM-класса
отчётной формы физически лежат в другой таблице (`report_forms`), имена из
которой в unified-индекс не проецируются. Инструмент `report_form`
(`SearchReportForm`, query.go:430-449) ищет по `rf.report_name` **или**
`rf.form_name` — поэтому видит то, чего не видит `symbol`.

---

## Затронутые файлы

- `internal/query/query.go` — `buildSymbolLookupCondition`, `SearchSymbol`
  (расширение условия и JOIN `report_forms`)
- `internal/query/query_test.go` — тесты lookup-условия для `report_form`
- `internal/query/query_symbol_types_integration_test.go` — интеграционный тест
  (символ report_form находится по form_name и form_class)

---

## План (1 шаг)

### Расширить lookup для `type=report_form` (по аналогии с `form` → `signature`)

**Текущее поведение:** для непустого типа (кроме `form`) условие вырождается в
`LOWER(s.symbol_name) = LOWER($n)` (query.go:263-265) — точное совпадение по
`report_name`; `form_name`/`form_class` недостижимы.

**Новое поведение:**

1. В `buildSymbolLookupCondition` для `primaryType == "report_form"` вернуть
   составное условие:

   ```sql
   (LOWER(s.symbol_name) = LOWER($n)
    OR LOWER(rf.form_name)  = LOWER($n)
    OR LOWER(rf.form_class) = LOWER($n))
   -- like-режим: ILIKE по всем трём полям
   ```

2. В `SearchSymbol` при `primaryType == "report_form"` добавить
   `LEFT JOIN report_forms rf ON s.symbol_type = 'report_form' AND s.entity_id = rf.id`
   (по аналогии с JOIN-ами в `query_relations.go:399` / `query_sql.go:309`;
   `entity_id` для report_form-символов уже заполняется, indexer.go:1064-1065).
   JOIN добавляется только при явном фильтре типа — общий поиск без `type`
   не деградирует.

3. `DISTINCT` не требуется: `symbols.entity_id → report_forms.id` — связь 1:1
   (один символ report_form на запись отчёта), дублей не порождает (в отличие
   от fan-out из BUG-methods-by-table-duplicate-rows).

**Ожидаемый эффект:**

| Запрос | Было | Стало |
|---|---|---|
| `symbol {"name":"FormRepBCHPeriod","type":"report_form"}` (TC-142) | count=0 | count=6 (все 6 отчётов с этим DFM-классом) |
| `symbol {"name":"TFormRepBCHPeriod","type":"report_form"}` | count=0 | count=6 (по form_class) |
| `symbol {"name":"BCH_UCH_Period","type":"report_form"}` | count=1 | count=1 (по report_name, без изменений) |
| `symbol` без type, name=FormRepBCHPeriod | count=0 | count=0 (fallback только при явном type, как у `form`) |

---

## Порядок выполнения

1. Правка `buildSymbolLookupCondition` + `SearchSymbol` (JOIN).
2. Юнит-тесты lookup-условия (`query_test.go`, по образцу
   `TestBuildSymbolLookupCondition_FormTypeSearchesNameAndSignature`).
3. Интеграционный тест: сидинг report_form с form_name/form_class, поиск по
   всем трём именам (`query_symbol_types_integration_test.go`).
4. `go build ./... && go test ./internal/query/...`.
5. Верификация на живой БД: MCP `codebase_query_symbol` с
   `{"name":"FormRepBCHPeriod","type":"report_form"}` → count=6; контроль
   регрессии: `{"name":"BCH_UCH_Period","type":"report_form"}` → count=1;
   `{"name":"FormRepBCHPeriod"}` без type → count=0.

---

## Совместимость

- Поведение без фильтра типа не меняется (условие fallback активируется только
  при явном `type=report_form`/алиасе, как и для `form`).
- Существующие точные поиска по `report_name` продолжают работать (первая ветка
  условия — прежнее равенство).
- `LEFT JOIN` не фильтрует строки (не INNER): символы `report_form` без
  резолвнутого `report_forms` (теоретически отсутствуют — проверено SQL) не
  теряются.
- Спека `query/symbol-search` описывает поиск «по имени» без перечисления
  полей-источников; расширение укладывается в текущие требования, при
  необходимости — дельта-спека через OpenSpec change.

## Риски

- LIKE-запросы по `report_forms.form_name/form_class` без trigram-индекса —
  таблица 1260 строк, seq scan незначителен; при желании — индекс
  `ON report_forms (LOWER(form_name))` (опционально).
- Возможные ложные совпадения form_name с именами других сущностей —
  митигируется: fallback срабатывает только при явном `type=report_form`,
  смешения с другими типами нет.

## Связанные артефакты

- Прогон: `Tests/CODEBASE_QUERY_TEST_RESULTS_2026-10-11.md` (TC-142, FAIL)
- Кейс: `Tests/TEST_CASES.md` TC-142
- Аналогия в коде: `internal/query/query.go:261-275` (`form` → `signature`)
