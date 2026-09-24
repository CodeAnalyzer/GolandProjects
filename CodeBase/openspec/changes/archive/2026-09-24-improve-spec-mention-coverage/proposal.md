# Improve Spec Mention Coverage

## Why

Парсер спек извлекает и резолвит не все упоминания кода. Замер по 2200 spec.md FA-продуктов показал ~7-9 тыс. потерянных или неверно классифицированных упоминаний: отчётные формы `.tpr`/`.rpt` (2210 упоминаний в 607 файлах) дропаются полностью; события `OnAfter*`/`OnBefore*` (~979) классифицируются как процедуры и не связываются с event-контрактами; GIVEN-строки сценариев не сканируются вовсе; `.pas`-пути резолвятся только в `pas_methods` и теряют связь с формой; строчные процы (`r8938_prc`, `rpt_f1417u_proc`) получают kind=unknown и никогда не резолвятся; префикс `pAPI_*` (1662) даёт ложный kind=table. При этом сущности в индексе есть (1256 report_forms, event-контракты в api_contracts) — пробел чисто в извлечении и классификации упоминаний.

## What Changes

- Новый kind упоминания `report`: токены `*.tpr`/`*.rpt` (бэктики и пути) → резолв в `report_forms` по `report_name`.
- Новый kind упоминания `event`: идентификаторы `On(After|Before)[A-Z]*` и пути `Event/*.xml` → резолв в `api_contracts` (контракты событий).
- GIVEN-строки сценариев сканируются на inline-упоминания наравне с WHEN/THEN.
- Резолв `.pas`-упоминаний двухфазный: `pas_methods`, при промахе — фолбэк в `dfm_forms` по имени юнита (форма).
- Second-chance резолв для `kind=unknown`: попытка сопоставления с `sql_procedures` до отбрасывания (строчные процы `rpt_*_proc` и т.п.).
- Префикс `pAPI_*` выведен из табличной эвристики в отдельный kind `api_table` с резолвом через `api_contract_tables` в контракты-владельцы (сегодня ребро уходит на произвольную usage-запись `sql_tables`).

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `indexing/openspec-parsing`: требования «Извлечение упоминаний кода» и «Пост-обработка — резолв упоминаний в references_code» дополняются видами упоминаний (report, event), сканированием GIVEN, двухфазным резолвом `.pas`, second-chance резолвом unknown и исключением `pAPI_*` из table-классификации.

## Impact

- `internal/parser/openspecmd/mentions.go` — новые регекспы (report-пути, event-идентификаторы), исключение `pAPI_*` из `reTableName`-классификации.
- `internal/indexer/indexer_spec.go` — сканирование GIVEN в `insertSpecMentions`.
- `internal/indexer/indexer_postprocess_spec.go` — новые lookup'ы (report_forms, event→api_contracts), фолбэк `.pas`→`dfm_forms`, second-chance unknown→`sql_procedures`.
- `internal/store/db_lookup_spec_mentions.go` — batch-lookup `report_forms` по имени, `api_contract_tables` по имени таблицы (мультикарта в контракты).
- Таблица `spec_code_mentions`: новые значения `mention_kind` (`report`, `event`, `api_table`) — совместимо, enum в текстовой колонке.
- MCP-инструмент `spec_by_code` и queries по `references_code` — рост числа связей без изменения контрактов.
