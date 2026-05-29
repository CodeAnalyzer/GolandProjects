# Plan: SQL Review (CLI + MCP)

В этом плане добавляется команда `review` и MCP tool `codebase_review_sql` для проверок одного переданного SQL-файла по 4 правилам с использованием уже проиндексированных данных CodeBase.

## Scope (Iteration 1)

- Проверяется только один переданный SQL-файл (`full path`).
- Правила:
  - `foreignTablesUsing` (severity `3`)
  - `foreignPTablesUsing` (severity `3`)
  - `foreignProcedureUsing` (severity `3`)
  - `datatype` (severity `3`, минимально необходимый охват)
- `t-` классификация — по префиксу имени таблицы.
- Для `foreignPTablesUsing`: `p-` классификация по префиксу, но `pAPI`-таблицы исключаются из проверки.
- `pAPI`-таблицы определяются по наличию имени в `api_business_object_tables` или `api_contract_tables`.
- Если у целевой сущности нет `ds_product_id`, замечание не создаётся.

## Architecture changes

1. Добавить новый пакет `internal/review`:
   - доменные типы: `RuleID`, `Severity`, `Finding`, `ReviewResult`, `ReviewOptions`;
   - сервис `Runner` с методом `RunSQLFile(path string, opts ReviewOptions)`.
2. Вынести доступ к данным через локальный репозиторий/интерфейс внутри `internal/review`:
   - получить `file_id` и `current ds_product_id` по входному пути;
   - получить таблицы/процедуры, используемые в этом файле;
   - получить owner `ds_product_id` целевой сущности через её `file_id`.
3. Не менять существующий контракт query-команд; review идёт отдельным use-case.

## Rule implementation details

### 1) foreignTablesUsing
- Для входного файла собрать используемые таблицы.
- Оставить только таблицы с префиксом `t` (case-insensitive, после нормализации имени).
- Для каждой таблицы определить `target ds_product_id`.
- Если `target != current`, создать finding `foreignTablesUsing`.

### 2) foreignPTablesUsing
- Аналогично `foreignTablesUsing`, но фильтр префикса `p`.
- Перед сравнением продуктов исключать таблицы, найденные в `api_business_object_tables` или `api_contract_tables` (общие API-таблицы).

### 3) foreignProcedureUsing
- Для входного файла собрать вызываемые процедуры.
- Определить `target ds_product_id` для процедуры.
- Если `target != current`, создать finding `foreignProcedureUsing`.

### 4) datatype (MVP)
- Реализовать минимальный набор проверок несоответствия типов на основе доступного индекса:
  - для конструкций, где можно однозначно сопоставить source/target колонку;
  - использовать `sql_column_definitions` (последний тип колонки).
- На первом шаге покрыть только безопасно-детектируемые кейсы (без агрессивных эвристик).
- Матчинг типов: нормализация + базовые правила «потеря точности / неэквивалентность».

## CLI integration

1. Добавить новую команду `codebase review` в `cmd/`:
   - аргумент: `file path` (обязательный);
   - флаги (MVP): `--json`, `--rules`, `--min-severity`.
2. Подключить вызов `internal/review.Runner`.
3. Вывод:
   - текстовый режим: компактный список findings;
   - JSON режим: envelope в стиле CLI (`success`, `count`, `items`, `meta`).
4. Ошибки классифицировать аналогично текущему CLI-подходу.

## MCP integration

1. В `internal/mcp/registry.go` зарегистрировать tool:
   - name: `codebase_review_sql` (underscore, без точек);
   - args: `file_path` (required), `rules` (optional), `min_severity` (optional).
2. Handler вызывает тот же `internal/review.Runner`.
3. MCP-ответ: чистые доменные данные (без CLI envelope), например:
   - `analyzed_file`, `summary`, `findings`.

## Tests and validation

1. Unit-тесты `internal/review`:
   - owner-resolution через `files.ds_product_id`;
   - `foreign*` правила;
   - фильтр неизвестного owner (пропуск без finding);
   - базовые datatype-сценарии.
2. Smoke для CLI команды `review` (json/text).
3. Smoke для MCP tool `codebase_review_sql`.
4. После каждой итерации: `go test` по затронутым пакетам, затем `go build ./...`.

## Delivery sequence

1. Каркас `internal/review` + модели + тестовые фикстуры.
2. Реализовать `foreignTablesUsing`, `foreignPTablesUsing`, `foreignProcedureUsing`.
3. Подключить CLI `review`.
4. Добавить `datatype` MVP.
5. Подключить MCP `codebase_review_sql`.
6. Финальная проверка тестами и сборкой.
