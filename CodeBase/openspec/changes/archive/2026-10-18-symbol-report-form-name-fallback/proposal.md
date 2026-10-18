# Proposal: Fallback поиска report_form-символов по form_name/form_class

## Why

Unified-индекс `symbols` асимметричен: инструмент `symbol` с явным
`type=report_form` находит отчётную форму только по `report_name`
(`BCH_UCH_Period`), но не по имени DFM-формы (`FormRepBCHPeriod` /
`TFormRepBCHPeriod`), хотя выделенный инструмент `report_form` по
`form_name` ищет. Прогон тест-кейсов 2026-10-11 зафиксировал это как FAIL
TC-142 (`symbol {"name":"FormRepBCHPeriod","type":"report_form"}` → count=0,
при том что 6 отчётов с `form_name=FormRepBCHPeriod` проиндексированы).
Причина: `symbol_name` для report_form = только `report_name`
(indexer.go, пути TPR и RPT), а `form_name`/`form_class` лежат в таблице
`report_forms` и в unified-индекс не проецируются; lookup-условие для
`type=form` уже имеет fallback на `s.signature` (DFM-класс), для
`report_form` аналога нет.

## What Changes

- `symbol` (CLI `query symbol`, MCP `codebase_query_symbol`) при явном
  фильтре `type=report_form` (канонический тип) ищет по трём полям:
  `symbol_name` (= `report_name`), `report_forms.form_name`,
  `report_forms.form_class`; exact-режим — сравнение без учёта регистра,
  like-режим — ILIKE по всем трём. Для этого `SearchSymbol` добавляет
  `LEFT JOIN report_forms rf ON s.symbol_type='report_form' AND
  s.entity_id = rf.id` — только при явном фильтре типа.
- Поиск без `type` не меняется: fallback активен только при явном
  `type=report_form` (JOIN в общий поиск не добавляется).
- Выделенный инструмент `report_form` (CLI `query report-form`, MCP
  `codebase_query_report_form`) расширяет поля поиска с
  `report_name`/`form_name` до `report_name`/`form_name`/`form_class` —
  устраняется инверсия асимметрии (иначе `symbol` нашёл бы по
  `form_class`, а `report_form` — нет; DFM-аналог `form` ищет по
  form_name, form_class, caption).
- Условие lookup и добавление JOIN управляются общим предикатом
  (единая точка решения), чтобы string-builder `buildSymbolLookupCondition`
  и SQL `SearchSymbol` не могли разойтись.

Не меняется: схема БД, индексация, поведение остальных типов символов,
поиск по `report_name` (первая ветка условия — прежнее равенство).

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `query/symbol-search`: поиск при явном `type=report_form` расширяется
  fallback-полями `form_name`/`form_class` из `report_forms` (только при
  явном типе; без типа — без fallback).
- `query/entity-queries`: требование «Поиск report entities» — `query
  report-form` / `codebase_query_report_form` дополнительно ищет по
  `form_class`.

## Impact

- Код: `internal/query/query.go` — `buildSymbolLookupCondition`,
  `SearchSymbol` (LEFT JOIN), `SearchReportForm` (список полей);
  `internal/query/query_test.go` — юнит-тесты условия;
  `internal/query/query_symbol_types_integration_test.go` — интеграционный
  тест (символ report_form находится по всем трём именам; JOIN присутствует
  в SQL при type=report_form и отсутствует без него).
- API: CLI и MCP-инструменты наследуют поведение через `internal/querysvc`;
  контракт ответов (поля, envelope) не меняется.
- Зависимости: нет (без миграций БД и переиндексации — `entity_id`
  report_form-символов уже заполняется обоими путями индексации, TPR и RPT).
- Верификация: TC-142 FAIL→PASS; контроль регрессии — `BCH_UCH_Period`
  с type=report_form → count=1; `FormRepBCHPeriod` без type → count=0.

## Связанные артефакты

- Исходный анализ: `Modifications/symbol-report-form-name-fallback-c9a2f1.md`
- Прогон: `Tests/CODEBASE_QUERY_TEST_RESULTS_2026-10-11.md` (TC-142, FAIL)
- Кейс: `Tests/TEST_CASES.md` TC-142
