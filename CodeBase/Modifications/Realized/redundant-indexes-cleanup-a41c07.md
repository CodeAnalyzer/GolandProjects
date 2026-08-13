# Удаление избыточных индексов в db_schema.go

Удаление 5 избыточных/мёртвых индексов и замена одного функционального индекса на составной. Продолжение чистки, начатой в `prune-optimization-2150f2.md` (там вычищены standalone-индексы `trc_events` и `rti_calls`).

## Контекст проблемы

Анализ всех CREATE INDEX в `internal/store/db_schema.go` (строки 569-684) против фактических паттернов запросов в `internal/query`, `internal/store`, `internal/rti`, `internal/review` выявил:

1. **Избыточные по leftmost-prefix** (покрываются существующими составными индексами):
   - `idx_symbols_symbol_name_lower` — `symbols(LOWER(symbol_name))`, покрыт составным `idx_symbols_symbol_name_type_lower` на `(LOWER(symbol_name), symbol_type)`. Запрос `query.go:261` (`LOWER(s.symbol_name) = LOWER($n)`, опционально + `symbol_type`) полностью обслуживается составным.
   - `idx_query_fragments_file_id` — `query_fragments(file_id)`, покрыт составным `idx_query_fragments_file_line` на `(file_id, line_number)`. Все запросы фильтруют по `file_id`; `query.go:551` делает `ORDER BY qf.file_id, qf.line_number` — составной ложится идеально.
   - `idx_rti_client_events_session_id` — `rti_client_events(session_id)`, покрыт составным `idx_rti_client_events_session_kind` на `(session_id, kind)`. Все запросы в `internal/rti/store.go` фильтруют по `session_id` (часто + `kind`). Тот же паттерн, что уже вычищен для `rti_calls`/`trc_events`, — здесь пропущен.

2. **Мёртвые (подтверждено `pg_stat_user_indexes`, `idx_scan = 0`)**:
   - `idx_api_contracts_name_kind` — `api_contracts(contract_name, contract_kind)`. Все lookup'и используют `LOWER(contract_name) = LOWER(...)` / `= ANY(...)` (`api_store.go:214,256,284,357`) или `ILIKE $1` (`api_query.go`) — plain btree неприменим.
   - `idx_relations_relation_type` — `relations(relation_type)`. Запросов с фильтром только по `relation_type` в коде нет (все запросы дополнительно ограничивают `source_type`/`target_type` и покрываются составными). Ad-hoc сценарий "только relation_type" через `codebase_query_relations` на практике не используется (0 сканов); деградация до seq scan для этого редкого пути — осознанный trade-off ради удешевления массовой вставки в `relations` при индексации.

3. **Замена для улучшения покрытия**:
   - `idx_api_contracts_contract_name_lower` — `api_contracts(LOWER(contract_name))`, активно используется (334 скана), но запросы `FindLatestAPIContractIDByNameAndKind` / `FindAPIContractIDsByNameAndKind` фильтруют `contract_kind` постфактум. Заменяется составным функциональным `(LOWER(contract_name), LOWER(contract_kind))`: name-only запросы работают по leftmost prefix, name+kind — полное совпадение без post-filter.

## Затронутые файлы

- `internal/store/db_schema.go` — единственный изменяемый файл

## План (3 шага)

### Шаг 1: Дроп избыточных standalone-индексов (prefix coverage)

В `internal/store/db_schema.go`:

- Убрать `CREATE INDEX` для:
  - `idx_symbols_symbol_name_lower` (строка 574)
  - `idx_query_fragments_file_id` (строка 623)
  - `idx_rti_client_events_session_id` (строка 662)
- Добавить `DROP INDEX IF EXISTS` для существующих БД (по образцу строк 667-671, 679-684):
  ```sql
  DROP INDEX IF EXISTS idx_symbols_symbol_name_lower;
  DROP INDEX IF EXISTS idx_query_fragments_file_id;
  DROP INDEX IF EXISTS idx_rti_client_events_session_id;
  ```

### Шаг 2: Дроп мёртвых индексов (подтверждено pg_stat)

В `internal/store/db_schema.go`:

- Убрать `CREATE INDEX` для:
  - `idx_api_contracts_name_kind` (строка 632)
  - `idx_relations_relation_type` (строка 650)
- Добавить `DROP INDEX IF EXISTS`:
  ```sql
  DROP INDEX IF EXISTS idx_api_contracts_name_kind;
  DROP INDEX IF EXISTS idx_relations_relation_type;
  ```

### Шаг 3: Замена lower-индекса api_contracts на составной

В `internal/store/db_schema.go`:

- Убрать `CREATE INDEX idx_api_contracts_contract_name_lower` (строка 633).
- Добавить:
  ```sql
  DROP INDEX IF EXISTS idx_api_contracts_contract_name_lower;
  CREATE INDEX IF NOT EXISTS idx_api_contracts_lower_name_kind
      ON api_contracts(LOWER(contract_name), LOWER(contract_kind));
  ```

**Важно:** порядок внутри шага 3 — сначала DROP, потом CREATE (составной покрывает запросы старого; окно между DROP и CREATE в рамках одного `InitSchema` прогона некритично, т.к. вызывается при старте, а не под нагрузкой).

## Проверка

1. `go build ./...` — чисто.
2. `go vet ./internal/store/...` — чисто.
3. `go test ./internal/store/... -count=1` — PASS (существующие тесты не должны зависеть от удалённых индексов).
4. На тестовой БД: запустить приложение (InitSchema отработает идемпотентно), затем:
   ```sql
   SELECT indexname FROM pg_indexes WHERE tablename IN
     ('symbols', 'query_fragments', 'rti_client_events', 'api_contracts', 'relations')
   ORDER BY tablename, indexname;
   ```
   Проверить отсутствие дропнутых и наличие `idx_api_contracts_lower_name_kind`.
5. Smoke-запросы через CLI:
   - `codebase query symbol <name>` — использует `(LOWER(symbol_name), symbol_type)`.
   - `codebase query sql-fragment <text>` — использует `(file_id, line_number)` + trgm.
   - `codebase query api-contract <name>` — использует новый `(LOWER(contract_name), LOWER(contract_kind))`.
   - `codebase rti client-tree --session <id>` — использует `(session_id, kind)`.

## Откат

Все изменения идемпотентны и обратимы: дропнутые индексы можно пересоздать теми же `CREATE INDEX IF NOT EXISTS` (они сохранены в git-истории). Данные не затрагиваются.

## Ожидаемый результат

| Метрика | Было | Стало |
|---|---|---|
| Индексов на `symbols` | 4 | 3 |
| Индексов на `query_fragments` | 5 | 4 |
| Индексов на `rti_client_events` | 4 | 3 |
| Индексов на `api_contracts` | 5 | 4 |
| Индексов на `relations` | 5 | 4 |
| Итого индексов в схеме | ~80 | ~75 |
| name+kind lookup `api_contracts` | index + post-filter | полное index-совпадение |
| Стоимость INSERT в `relations` (массовая при индексации) | 5 индексов/строку | 4 индекса/строку |
