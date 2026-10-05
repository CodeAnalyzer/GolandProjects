## 1. Схема и миграция

- [x] 1.1 В `internal/store/db_schema.go` обновить CREATE TABLE `pas_classes`, `pas_methods`, `pas_fields`: колонка `file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE` inline — проверить `schema_integration_test` (таблицы создаются на пустой БД)
- [x] 1.2 Добавить идемпотентные миграционные statements по порядку из design.md (Decision 3): `ADD COLUMN IF NOT EXISTS` ×3 → DELETE сирот ×3 → backfill ×3 → `SET NOT NULL` ×3 → условный `ADD CONSTRAINT ... FK CASCADE` через `DO $$` ×3 — проверить повторный `InitSchema` на мигрированной БД (no-op)
- [x] 1.3 Добавить индексы `idx_pas_classes_file_id`, `idx_pas_methods_file_id`, `idx_pas_fields_file_id` (`CREATE INDEX IF NOT EXISTS`) в общий список индексов
- [x] 1.4 Записать named-маркер `pas_entities_file_id_v1` через `applyMigrationTx` и поднять `CurrentSchemaVersion` до `codebase_schema_v2` — проверить: свежая БД получает оба маркера, старая БД без v2 даёт `ErrSchemaUpdateRequired` от `CheckSchemaVersion`
- [x] 1.5 Интеграционный тест миграции: вставить PAS-сущности старым путём (без file_id), создать сироту (метод с битым unit_id, поле с NULL class_id), прогнать `InitSchemaCtx` — проверить: сироты удалены, `file_id` заполнен у всех строк, повторный прогон не меняет данные

## 2. Модель и вставка

- [x] 2.1 Добавить поле `FileID int64` в `model.PASClass`, `model.PASMethod`, `model.PASField` (`internal/model/model.go`) — проверить компиляцию `go build ./...`
- [x] 2.2 В `internal/store/db_insert_pas.go` дополнить списки колонок CopyIn (`"file_id"`) и значения в `insertPASClassesBatch` / `insertPASMethodsBatch` / `insertPASFieldsBatch`
- [x] 2.3 В PAS-обработке индексатора заполнить `FileID` из `fileID` per-file транзакции для классов/методов/полей — проверить интеграционный тест индексации `.pas`-файла: строки в БД содержат корректный `file_id`
- [x] 2.4 Расширить `files_integration_test` (`TestDeleteFilesByPathsExcept_KeepsNewIDAndCascades` или новый тест): после удаления файла с PAS-сущностями таблицы `pas_classes/pas_methods/pas_fields` не содержат строк этого `file_id` (каскад работает, сирот нет)

## 3. Резолвер спек-упоминаний

- [x] 3.1 Проверить/дополнить тест резолва: упоминание kind=method с существующим методом → ребро `references_code → pas_method` создаётся (hit-path, indexer_helpers_test.go:232; интеграционный вариант в indexer_postprocess_spec_mentions_integration_test.go)
- [x] 3.2 Проверить регрессию fallback: `.pas`-упоминание имени юнита без метода → ребро к `dfm_forms` (существующий тест `RPPortfolio_f`)

## 4. Верификация

- [x] 4.1 `go build ./...` и `go test ./internal/store/... ./internal/indexer/...` — зелёные
- [x] 4.2 Прогон `codebase update` на FA: лог без `column e.file_id does not exist`; SQL-проверка сирот (`pas_methods` без валидного `file_id`/юнита = 0 строк); резолв method-упоминаний появился в `spec_code_mentions`/coverage
