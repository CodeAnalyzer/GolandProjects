## Why

Резолвер спек-упоминаний падает на каждом kind=method: `find pas_methods by names: pq: column e.file_id does not exist`. Generic-lookup (`lookupEntityIDsByNames`) предполагает прямую колонку `file_id` у таблицы сущности — у `pas_methods` (как и у `pas_classes`, `pas_fields`) её нет, файл достигается только через цепочку `unit_id → pas_units → files`. Хуже: у этих трёх таблиц нет и FK, поэтому каскадное удаление файла (`DELETE FROM files`) убивает только `pas_units`, оставляя классы/методы/поля сиротами — утечка строк при любом инкрементальном update (подтверждено экспериментально: удаление одного `.pas`-файла оставляет orphan-метод). Текущая БД чиста лишь потому, что последние прогоны были полными пересборками (TRUNCATE).

## What Changes

- Таблицы `pas_classes`, `pas_methods`, `pas_fields` получают колонку `file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE` (денормализация прямой привязки к файлу по образцу всех остальных таблиц сущностей).
- Добавляются индексы `idx_pas_classes_file_id`, `idx_pas_methods_file_id`, `idx_pas_fields_file_id` — без них каждый deleted file даёт seq scan по ~900k строк при каскаде.
- Идемпотентная миграция существующих БД: `ADD COLUMN IF NOT EXISTS` → очистка сирот (строки с разрушенной цепочкой unit/class) → backfill из `pas_units` без переиндексации → `SET NOT NULL` → FK-констрейнт.
- Версия схемы поднимается до `codebase_schema_v2`: новый бинарник на старой БД даёт внятную ошибку «schema update required» вместо невнятных ошибок COPY IN.
- Batch insert (COPY IN) `pas_classes/pas_methods/pas_fields` заполняет `file_id` из контекста индексируемого файла; `model.PASClass/PASMethod/PASField` получают поле `FileID`.
- Автоматически чинится резолвер спек-упоминаний kind=method (запрос `e.file_id` становится валидным) — код резолвера не меняется; все `references_code → pas_method` рёбра восстанавливаются.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `infrastructure/database-schema`: новое требование о прямой привязке PAS-сущностей к файлу — колонка `file_id` с FK CASCADE на трёх таблицах, индексы, идемпотентная миграция с backfill, заполнение при batch insert. Поведенческие следствия для остальных спек (резолв method-упоминаний, каскадное удаление PAS-строк) — восстановление соответствия уже существующим требованиям, их текст не меняется.

## Impact

- `internal/store/db_schema.go` — CREATE TABLE трёх таблиц, миграционные statements, named-миграция `pas_entities_file_id_v1`, `CurrentSchemaVersion → codebase_schema_v2`, индексы.
- `internal/store/db_insert_pas.go` — CopyIn колонка `file_id` для трёх batch insert.
- `internal/model/model.go` — поле `FileID` в `PASClass`, `PASMethod`, `PASField`.
- PAS-обработка индексатора (`internal/indexer`) — заполнение `FileID` из `fileID` per-file транзакции.
- Не меняется: запросы `internal/query` (JOIN через `pas_units` остаётся валидным), резолвер `lookupEntityIDsByNames` и `postProcessSpecCodeMentions` (заработают без правок), `ResetCodebaseTables` (TRUNCATE CASCADE покрывает новые FK).
- Вне скоупа: cleanup висячих `relations` после каскадных удалений (консистентно с остальными типами сущностей, механизм и сегодня отсутствует); баг счётчика `files_indexed=0` в `scan_runs`.
