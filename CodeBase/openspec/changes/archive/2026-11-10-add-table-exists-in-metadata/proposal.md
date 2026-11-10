## Why

Review уже проверяет существование процедур (`execNotExistsProc`) и индексов (`indexExistsInDB`), но не проверяет существование самой таблицы. Скрипт, использующий таблицу, для которой нет создающего DDL (патча для t-таблицы или TmpTbl-скрипта/DSArchitect-описания для p-таблицы), падает при деплое или в рантайме — это должно блокировать деплой, а не обнаруживаться на стенде. Пилотный прогон по индексу подтвердил реальный дефект: `ContractMassLoad.sql` использует `pAPI_Account_MassClose`, тогда как в метаданных объявлена `pAPI_Acc_MassClose`.

## What Changes

- Новое правило review `tableExistsInMetadata` (severity 1, deploy stopper): для каждой таблицы, используемой скриптом в DML-контексте, проверяется наличие в индексе скрипта/метаданных, создающих эту таблицу.
- «Создана таблица» считается по объединению источников индекса:
  - `sql_tables` с `context='create'` (патчи постоянных t-таблиц и TmpTbl-скрипты p-таблиц);
  - `api_contract_tables` и `api_business_object_tables` (p-таблицы, объявленные в DSArchitect XML);
  - `sql_column_definitions` с `definition_kind='select_into'` (создание через SELECT INTO);
  - таблицы с `context='create'` в самом анализируемом файле (защита от устаревшего индекса).
- Проверяются только реальные обращения к таблицам: `context IN ('select','insert','update','delete','log')`; `unknown`-контекст игнорируется. Таблицы с префиксом `#`/`##` не проверяются.
- Не-табличный шум отсекается дополнительно: курсоры, объявленные в самом файле (включая использование через `FETCH ... FROM` и `WHERE CURRENT OF`), триггерные pseudo-таблицы `inserted`/`deleted`, системные каталоги (`sys*`, `master`, `information_schema`), placeholder-токены генераторов, ключевые слова SQL и Diasoft-макросы.
- Добавляется batch-lookup в store (`BatchLookupTableCreators` → `map[string]bool`) с предзагрузкой в `Runner`, чтобы не было N+1 запросов.
- Имя таблицы сравнивается без учёта регистра; finding содержит имя таблицы, файл и строку использования.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/deploy-stoppers`: добавляется требование «Существование таблицы в метаданных» с новым правилом `tableExistsInMetadata`, severity 1.

## Impact

- `internal/review/types.go` — новый `RuleID` `tableExistsInMetadata`.
- `internal/review/catalog.go` — регистрация правила и его `Build`.
- `internal/review/review_rules.go` — реализация проверки.
- `internal/review/review_lookup.go` / `internal/review/runner.go` — предзагрузка кэша создателей таблиц.
- `internal/store/db_lookup_sql.go` — новый batch-lookup по `sql_tables`, `api_contract_tables`, `api_business_object_tables`, `sql_column_definitions`.
- `openspec/specs/review/deploy-stoppers/spec.md` — новое требование после архивации.
- CLI/MCP: правило становится доступно в `review` и `codebase_review_sql` через существующий каталог правил; новых флагов нет.
- Ограничение: корректность зависит от полноты индекса (ядровые продукты, например `fa-fmcore`, должны быть проиндексированы); непроиндексированные продукты могут давать ложные срабатывания.
- Гейт включения: пилотный замер без харденинга (fa-contracts, локальный индекс) дал 160 имён без источника создания и 1.9% файлов с кандидатом на severity-1 finding; перед включением правила по умолчанию выполняется повторный замер с полным набором фильтров (задача 4.5, результат фиксируется в design.md).
