# Proposal: add-view-usage-rule

## Why

В кодовой базе FA представления (VIEW) — клиентская технология: часть view
фильтруется по текущему пользователю через `tUserResSel` (fa-administrator:
`Batch`, `Node`, `Instrument`), index-хинты `M_*_INDEX` через view не
применяются, вложенные view разворачиваются планировщиком непрозрачно.
Использование view в серверном коде — риск корректности и производительности,
но сегодня review его не видит. Замер на индексе FA (2026-11-14, все ветки):
49 обращений к view в 30 SQL-файлах (47 select + 2 update); после свёртки
параллельных ветвей продуктов (`fa-cards`/`fa-cards-110109` и т.п.) — 33
уникальных `(путь, строка, имя)`. Лидер — `tfund` (27 ссылок в 9 файлах),
далее `tinststaff`, `titermgroup`, `batch`, `account`, `balance`. Все они
проходят review молча. Хуже: поскольку создателя-таблицы для этих имён нет, на них сейчас
сыплются ложные severity-1 `tableExistsInMetadata` («таблица отсутствует»),
маскирующие реальную причину — обращение к view.

## What Changes

- Новое правило review `viewUsageInServerCode` (severity 1, deploy stopper):
  обращения к представлениям в SQL-файле дают finding «Использование
  представления в серверном коде».
- Множество имён view лениво выводится из индекса (фрагменты
  `create [or replace] view` в `query_fragments`) при первом обращении
  правила; материализация в схеме БД и переиндексация не требуются.
- Правило `tableExistsInMetadata` подавляется для имён, распознанных как
  view: такое обращение атрибутируется новому правилу, а не даёт ложный
  finding об отсутствии таблицы.
- Правило доступно через CLI `review` и MCP `codebase_review_sql` без новых
  флагов; входит в `DefaultEnabledRules`.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/deploy-stoppers`: добавляется требование «Запрет представлений в
  серверном коде» (правило `viewUsageInServerCode`, severity 1); требование
  «Существование таблицы в метаданных» дополняется исключением для имён,
  распознанных как представления.

## Impact

- `internal/review/`: `types.go` (RuleID, поля кэша view в Runner), `catalog.go`
  (регистрация, severity 1), `review_lookup.go` (ленивый кэш `isViewTable`
  по образцу `isMemoryTable`), `review_rules.go` (реализация правила +
  подавление в `tableExistsInMetadata`), `runner.go` (сброс кэша на файл).
- `README.md`: ручной список правил «Deploy stoppers» — добавить
  `viewUsageInServerCode`. Списки правил в CLI (`--rules`, cmd/review.go:245)
  и MCP (описание `codebase_review_sql`, registry.go:373) генерируются
  динамически из `review.RuleListString()` и правок не требуют.
- Тесты: unit (кэш, извлечение имён, подавление), integration на реальном
  индексе (30 файлов-нарушителей).
- Не затрагивает: схему БД (без bump `CurrentSchemaVersion`), индексацию,
  переиндексацию не требует.
