# Design: Fallback поиска report_form-символов по form_name/form_class

## Context

Мотивация — в `proposal.md` (Why). Текущее состояние кода, определяющее подход:

- `buildSymbolLookupCondition` (`internal/query/query.go:261-275`) — чистый
  string-builder: exact-режим для типов, кроме `form`, вырождается в
  `LOWER(s.symbol_name) = LOWER($n)`; `form` получает fallback на
  `s.signature` (поле лежит в `symbols`, JOIN не нужен); поиск без типа —
  guarded-ветка `(s.symbol_type = 'form' AND s.signature ...)`.
- `SearchSymbol` (`query.go:297-357`) джойнит только `files`; тип-фильтр
  проходит через `normalizeSymbolTypeFilter` → список канонических типов,
  `primaryType = canonicalTypes[0]`. Единственный production-вызывающий
  `buildSymbolLookupCondition`.
- `form_name`/`form_class` физически лежат в `report_forms`; `symbol_name`
  для report_form = только `report_name` (оба пути индексации: TPR
  `indexer.go:1022-1029`, RPT `indexer.go:1143-1150`). `entity_id`
  report_form-символов заполняется обоими путями (TPR `1062-1065`,
  RPT `1199-1202`) — SQL-проверкой подтверждено 1260/1260.
- `SearchReportForm` (`query.go:430-449`) ищет по `rf.report_name` OR
  `rf.form_name` (без `form_class`); DFM-аналог `SearchDFMForm` ищет по
  form_name + form_class + caption.
- Алиасы типов (`model/symbol_types.go:48-56`): ни один алиас не разворачивается
  в список, содержащий `report_form`, поэтому `primaryType == "report_form"`
  ⇔ фильтр ровно `{"report_form"}`.

## Goals / Non-Goals

**Goals:**

- `symbol` с явным `type=report_form` находит отчётные формы по всем трём
  именам (`report_name`, `form_name`, `form_class`) — точные сценарии из
  дельты `query/symbol-search`.
- `query report-form` / MCP `codebase_query_report_form` ищет и по
  `form_class` — симметрия с `symbol` и с DFM-поиском.
- Условие lookup и добавление JOIN управляются из одной точки — исключить
  рассинхрон «условие ссылается на `rf.*`, а JOIN не добавлен» (и наоборот).
- Нулевая стоимость для остальных запросов: JOIN добавляется только при
  явном `type=report_form`.

**Non-Goals:**

- Не меняем индексацию и схему БД (`form_name`/`form_class` не проецируются
  в `symbols`; переиндексация не требуется).
- Не расширяем fallback на поиск без типа (расходится с `form`-паттерном —
  см. Decisions).
- Не трогаем fallback других типов и не добавляем fallback по `caption`
  для отчётов.
- Не чиним case-sensitivity exact-поиска для `type=form` (существующее
  поведение `s.symbol_name = $1` без LOWER — отдельная тема).

## Decisions

### D1. Query-time fallback через LEFT JOIN, а не проекция в symbols

`SearchSymbol` при `primaryType == "report_form"` добавляет
`LEFT JOIN report_forms rf ON s.symbol_type = 'report_form' AND s.entity_id = rf.id`;
условие — `(LOWER(s.symbol_name) = LOWER($n) OR LOWER(rf.form_name) = LOWER($n) OR LOWER(rf.form_class) = LOWER($n))`,
в like-режиме ILIKE по трём полям.

Альтернативы:

- *Проекция при индексации* (дописывать form_name/form_class в `signature`
  или отдельными строками `symbols`): требует переиндексации всех TPR/RPT,
  ломает семантику `signature` (сейчас — тип отчёта) и меняет вывод
  инструмента. Отвергнуто.
- *Композиция на уровне querysvc* (вызвать `SearchReportForm` и смержить):
  дублирование пагинации/сортировки, два SQL-обращения. JOIN проще и
  соответствует существующему стилю (`relationSearchBaseQuery`).

LEFT JOIN, а не INNER: символы `report_form` без резолвнутой записи
`report_forms` (теоретически отсутствуют) не теряются — для них остаётся
ветка `symbol_name`. Fan-out исключён: `report_forms.id` — PK, один символ
на запись отчёта → `DISTINCT` не нужен.

### D2. Общий предикат для условия и JOIN

`buildSymbolLookupCondition` и `SearchSymbol` проверяют необходимость
`report_forms`-fallback через один helper (например,
`needsReportFormsLookup(primaryType string) bool` → true только для
`"report_form"`). Ранний exact-return (`query.go:263`) исключает
`report_form` наравне с `form`. Это устраняет главный риск рассинхрона:
string-builder не «знает» про JOIN, поэтому единственная точка решения —
обязательное условие корректности SQL.

### D3. Fallback только при явном типе (отклонение от form-паттерна)

Для `form` fallback на signature работает и без типа (поле в `symbols`,
JOIN не нужен). Для report_form поля лежат в другой таблице: безтиповое
расширение означало бы LEFT JOIN `report_forms` в каждый поиск `symbol`.
Потенциальная неожиданность: like-поиск без типа начал бы возвращать
отчёты по совпадению `form_name`. Ограничиваем fallback явным
`type=report_form` — дешёво и предсказуемо; фиксируем как осознанное
отклонение от аналогии с `form`.

### D4. Exact-режим — LOWER, а не `=`

Ветви для `rf.*` сравниваются `LOWER(...) = LOWER($n)` — консистентно с
exact-режимом остальных типов (query.go:264). Отмечаем: для `form`
существующий exact-fallback case-sensitive (`s.symbol_name = $1 OR
s.signature = $1`) — новый код следует более правильномуLower-паттерну,
менять form-ветку не будем (Non-Goal).

### D5. `form_class` в SearchReportForm — одна строка

`buildNameLookupCondition([]string{"rf.report_name", "rf.form_name"}, ...)`
→ добавить `"rf.form_class"` (query.go:432). Без этого после D1 возникла бы
инверсия: `symbol` ищет по `form_class`, а выделенный инструмент — нет.

## Risks / Trade-offs

- [Рассинхрон условия и JOIN при будущих правках string-builder'а] →
  общий предикат (D2) + юнит-тест, что при type=report_form SQL содержит
  JOIN, а без типа — не содержит.
- [LIKE по `report_forms.form_name/form_class` без индекса] → таблица
  ~1260 строк, seq scan в LEFT JOIN при явном тип-фильтре незначителен;
  опциональный индекс `ON report_forms (LOWER(form_name))` — вне скоупа.
- [Ложные срабатывания: form_name совпал с именем другого вида сущности] →
  fallback активен только при явном `type=report_form`, смешения типов нет.
- [Семантическое отклонение от form-паттерна (без типа fallback нет)] →
  задокументировано в D3 и в дельте `query/symbol-search` сценарием
  «Без фильтра типа fallback не применяется».

## Migration Plan

Изменение query-only: без миграций БД и переиндексации. Деплой — обычная
сборка бинарника; откат — откат бинарника. Верификация на живой БД:
TC-142 (`FormRepBCHPeriod` + type=report_form → 6), регрессии
(`BCH_UCH_Period` + type → 1; `FormRepBCHPeriod` без типа → 0;
`report_form {"name":"TFormRepBCHPeriod"}` → найдены).
