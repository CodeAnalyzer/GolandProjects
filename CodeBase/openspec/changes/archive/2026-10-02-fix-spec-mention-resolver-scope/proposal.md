# Fix: скоупированный резолв упоминаний спек — генерируемые копии и чужой продукт не должны перехватывать покрытие

## Why

Пакетные name-based lookup'ы (`FindLatestSQLProcedureIDsByNames`, `FindLatestSQLTableIDsByNames`, `FindDFMFormIDsByNames`, `FindSMFInstrumentIDsByNames`, `FindPASMethodIDsByNames`, `FindAPIContractIDsByNames`, `FindReportFormIDsByNames` и родственные) выбирают сущность по `MAX(id)`/`id DESC` глобально: без привязки к продукту спеки и без отсечения генерируемых копий (`*/UPLOAD/*`, `*.t01`). Когда одноимённая процедура существует в каноническом исходнике и в копии, связь `references_code` указывает на копию — канонический файл остаётся «непокрытым» в отчётах (BUG-spec-mention-resolver-maxid-unscoped-20260930: только в fa-contracts ~1300 строк шума), а продуктовый контекст review (`BatchLookupProcedureProductIDs`) может браться от копии чужого продукта и искажать скоупированный lookup типов колонок. Это тот же класс дефекта, что и исправленный в `fix-datatype-cross-product-lookup`, но с дополнительным измерением — генерируемые копии внутри того же продукта.

## What Changes

- Новое поле `files.is_generated BOOLEAN NOT NULL DEFAULT FALSE`: детект в месте сохранения файла (`isGeneratedFile(relPath, ext)`: сегмент пути `UPLOAD` без учёта регистра, расширение `t01`), backfill UPDATE в миграции схемы для уже проиндексированных файлов.
- Приоритетный резолв упоминаний спек: `не-генерируемый источник → тот же продукт, что и спека → стабильный tie-break (id DESC)` вместо глобального `MAX(id)`/`id DESC`. Продукт упоминания берётся из `spec_code_mentions.file_id → files.ds_product_id`; батч-группировка имён по продукту на стороне Go.
- Та же приоритезация в `BatchLookupProcedureProductIDs` и `BatchLookupProcedureParams` (продуктовый контекст и параметры процедур для review больше не берутся от генерируемых копий).
- `FindAPIContractIDsByTableNames`: контракты-копии (`is_generated`) отфильтровываются, если среди владельцев есть хотя бы один не-копия.
- Удаление мёртвого кода: `FindLatestSQLProcedureIDByName`, `FindLatestSQLTableIDByName` (без вызывающих, повторяют дефектный паттерн).
- Вне скоупа: хранение пути упоминания (`mention_path`) для резолва по точному пути; фильтрация копий в ad-hoc отчётах непокрытого кода.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `indexing/file-walking`: новое требование о маркировке генерируемых копий (`is_generated`) при сохранении файла + миграционный backfill.
- `indexing/openspec-parsing`: требование «Пост-обработка — резолв упоминаний в references_code» дополняется приоритетным порядком резолва (не-копия → свой продукт → стабильный tie-break).
- `infrastructure/database-schema`: новое требование о контракте пакетного name-based lookup (порядок предпочтения кандидатов) для семейства lookup-функций store, включая продуктовые карты review.

## Impact

- `internal/fswalk` — признак генерируемой копии в `FileInfo` (детект по rel_path/ext).
- `internal/indexer` — `saveFileCtx` пишет `is_generated`; `indexer_postprocess_spec.go` — группировка mentions по продукту, передача контекста в lookup'ы.
- `internal/store` — `db_schema.go` (миграция + backfill), `db_lookup_sql.go`, `db_lookup_spec_mentions.go` (приоритетный ORDER BY, новые сигнатуры с productID), удаление двух мёртвых функций.
- Потребители связей `references_code` (`specsvc` покрытие, `spec-by-code`, `spec-search`) — лечатся автоматически переориентацией `target_id` на канон; переиндексация (init) желательна для обновления флага у существующих файлов, backfill покрывает инкрементальный update.
- Тесты: integration-тесты lookup-контракта по образцу `db_lookup_sql_integration_test.go` (прецедент `fix-datatype-cross-product-lookup`).
