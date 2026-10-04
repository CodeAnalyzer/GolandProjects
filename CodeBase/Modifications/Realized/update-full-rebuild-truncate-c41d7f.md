# `codebase update --modified=false`: полная пересборка индекса через TRUNCATE + init-пайплайн

При `codebase update --modified=false` вместо перепрохода всех файлов по
update-ветке (per-file каскадное удаление старых сущностей) выполнить сброс
таблиц, заполняемых парсерами кодовой базы (`InitSchema` + `TRUNCATE`), и
запустить штатный init-пайплайн. RTI- и TRC-сессии сохраняются. Семантика
флага становится честной: «пересобрать индекс кодовой базы с нуля».

---

## Контекст проблемы

Замеры на FA (145 341 файлов, 2026-10-02):

| # | Путь | Механика | Результат |
|---|------|----------|-----------|
| 1 | `update --modified=false` (scan 27, build 1545) | per-file: `saveFileCtx` (новый `file_id`) + `DeleteFilesByPathsExcept` → каскадные DELETE старых строк по ~40 таблицам сущностей (sql_tables 1.17M, sql_columns 2.5M, query_fragments 1.7M, symbols, relations, ...) | 134 738 файлов за **4ч39м** |
| 2 | `update --modified=false` (повторный прогон, build 1549) | то же | **47 тыс. файлов за ~2.5ч** — прерван как непрактичный |
| 3 | `codebase init` на чистой БД (build 1549) | только INSERT, без per-file DELETE | 145 341 файлов за **1ч35м** (22:04:31–23:40:05), errors=0 |

Причина: update-ветка платит каскадными DELETE за каждый файл; init на чистой
БД этих расходов не имеет. Ручной обход (переименовать/дропнуть БД + `init`)
работает, но **теряет RTI/TRC-сессии** и историю сканов.

Текущий код (`internal/indexer/runner.go`): при `onlyModified=false` pre-filter
не устанавливается (фикс из change `fix-sql-tables-update-pollution`), все
файлы читаются и хешируются, но каждый идёт по update-ветке
«вставить нового + каскадно удалить старого».

---

## Предлагаемое решение

`update --modified=false` = «пересобрать индекс кодовой базы с нуля,
сохранив RTI/TRC-сессии»:

1. `InitSchema` (идемпотентен: `CREATE TABLE IF NOT EXISTS` + ALTER-патчи).
2. `TRUNCATE` всех таблиц, заполняемых парсерами кодовой базы (список ниже) —
   одним statement с `CASCADE`.
3. Запуск штатного init-пайплайна (walk без pre-filter → парсинг → batch
   insert → пост-обработка → LSA), т.е. делегация в логику `InitCtx`.

> Примечание про `VACUUM FULL`: после `TRUNCATE` он не требуется — TRUNCATE
> возвращает место на диске немедленно (создаёт новые relfile-ы). `VACUUM FULL`
> имеет смысл только для сохраняемых RTI/TRC-таблиц, если они разрослись, —
> оставить как отдельную ручную операцию, в пайплайн не включать.

### Список таблиц для TRUNCATE (по `internal/store/db_schema.go`)

- **Файлы/символы/прогоны**: `files`, `symbols`, `ds_products`, `scan_runs`, `stats_snapshot`
- **SQL**: `sql_procedures`, `sql_tables`, `sql_columns`, `sql_column_definitions`, `sql_index_definitions`, `sql_index_definition_fields`
- **PAS/H/JS/SMF/DFM**: `pas_units`, `pas_classes`, `pas_methods`, `pas_fields`, `h_files_defines`, `js_functions`, `js_constants`, `smf_instruments`, `dfm_forms`, `dfm_components`
- **Отчёты**: `report_forms`, `report_fields`, `report_params`, `vb_functions`
- **Фрагменты/include**: `query_fragments`, `include_directives`
- **API XML**: `api_business_objects`, `api_contracts`, `api_contract_params`, `api_contract_tables`, `api_contract_table_fields`, `api_business_object_params`, `api_business_object_tables`, `api_business_object_table_fields`, `api_business_object_table_indexes`, `api_business_object_table_index_fields`, `api_contract_return_values`, `api_contract_contexts`, `api_macro_invocations`
- **Связи/retcode**: `relations`, `ds_return_codes`
- **Спеки**: `spec_configs`, `spec_capabilities`, `spec_requirements`, `spec_scenarios`, `spec_usecases`, `spec_usecase_steps`, `spec_changes`, `spec_change_delta`, `spec_code_mentions`, `spec_vocab`, `spec_embeddings`
- **Sidecar LSA**: файлы `spec_lsa_model.bin` / `spec_lsa_state.json` удаляются — пересоздаются пайплайном при ретрейне

Плюс sidecar-файлы LSA (`spec_lsa_model.bin`, `spec_lsa_state.json`) — удалить
перед запуском пайплайна (пересоздаются ретрейном).

### Сохраняется (не трогается)

- **RTI-парсер**: `rti_sessions`, `rti_calls`, `rti_params`, `rti_checkpoints`, `rti_blog_blocks`, `rti_blog_tables`, `rti_client_events`
- **TRC-парсер**: `trc_sessions`, `trc_events`
- **Служебные**: `schema_migrations`

Если у RTI/TRC-таблиц появятся FK на очищаемые таблицы — `CASCADE` их зацепит;
сейчас связей нет, в реализации проверить `information_schema` и зафиксировать
список тестом.

---

## Затронутые файлы

- `internal/store/db_schema.go` (или новый `db_reset.go`) — `ResetCodebaseTables(ctx)`: один `TRUNCATE TABLE <список> CASCADE` + возвращаемое количество очищенных таблиц
- `internal/indexer/runner.go` — в `UpdateCtx` при `onlyModified == false`: `ResetCodebaseTables` → делегация в init-пайплайн (walk/parse/postprocess/LSA); update-ветка остаётся только для `onlyModified == true`
- `cmd/update.go` — справка флага: `"scan only modified files"` → уточнение `false = full rebuild of codebase index (TRUNCATE codebase tables; RTI/TRC sessions are kept)`
- `internal/store` — тест `ResetCodebaseTables` (testutil): таблицы пусты, RTI/TRC-строки живут

## Проверка

```
go build ./...
go test ./internal/store/... ./internal/indexer/...
# На FA: очистить RTI/TRC-сессию не нужно — проверить, что после
# codebase update --modified=false сессии rti_sessions/trc_sessions на месте,
# sql_tables/sql_columns соответствуют свежему init, время ≈ init (~1.5ч)
```

---

## Не меняется

- Семантика `--modified=true` (дефолт): инкрементальный update с pre-filter
- Фикс pre-filter из change `fix-sql-tables-update-pollution` (`runner.go:161` — pre-filter только при `onlyModified`); без него `--modified=false` ветка вообще не достигалась бы
- RTI/TRC storage (`rti_prune`/`trc_prune` не затрагиваются)

## Риски / Trade-offs

- [TRUNCATE необратим] → операция явная (`--modified=false`); вывод предупреждения в начале прогона; RTI/TRC-данные не затрагиваются
- [Индеск пуст на время пересборки (~1.5ч)] → то же поведение, что у `init` на чистой БД; запросы в это время вернут пустые результаты
- [История scan_runs теряется] → приемлемо (при rename+init терялась тоже); при необходимости — исключить `scan_runs`/`stats_snapshot` из TRUNCATE (решить на реализации, FK `files.scan_run_id` всё равно потребует CASCADE)
- [Дубли, если делегация в init-пайплайн выполнится без TRUNCATE] → TRUNCATE и делегация в одной транзакции-последовательности; интеграционный тест на «сущности ровно в одном экземпляре»
