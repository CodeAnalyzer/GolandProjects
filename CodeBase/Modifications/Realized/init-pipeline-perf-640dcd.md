# Оптимизация пайплайна `codebase init` — устранение N+1 запросов

План поэтапной оптимизации: 7 фаз, каждая устраняет N+1 запросы в конкретном участке пайплайна. После каждой фазы — сборка + тесты + явный акцепт пользователя.

---

## Фаза 1: `postProcessCallbackEventRelations` — batch-resolve event контрактов

**Проблема:** Для каждого callback_event контракта выполняется отдельный `FindLatestAPIContractIDByNameKindAndOwnerModule` (N round-trips).

**Файлы:**
- `internal/store/api_store.go` — новый метод `FindLatestAPIContractIDsByNamesAndKinds(names, kinds []string) (map[string]map[string]int64, error)` — один SQL-запрос с `WHERE contract_name = ANY($1) AND contract_kind = ANY($2)`, возвращает `map[name][kind]id`
- `internal/indexer/indexer_postprocess_callbacks.go` — заменить цикл поштучных `FindLatestAPIContractIDByNameKindAndOwnerModule` на: загрузка всех event-контрактов одним запросом (уже есть `FindAPIContractsByKind("event")`) → построение in-memory map `map[name+module]id` → lookup из map

**Паттерн:** Аналогично `FindLatestSQLProcedureIDsByNames` в `db_lookup_sql.go:26`

---

## Фаза 2: `postProcessPASPending` — batch-resolve классов и DFM-форм

**Проблема:** Три цикла с поштучными запросами `FindLatestPASClassIDByName`, `FindLatestDFMFormIDByClassName`, `FindLatestDFMComponentIDByFormAndName`. При этом `FindLatestPASClassIDByName` вызывается дважды для одного и того же имени (methods + fields).

**Файлы:**
- `internal/store/db_lookup_pas.go` — новый метод `FindLatestPASClassIDsByNames(classNames []string) (map[string]int64, error)` — один SQL-запрос с `WHERE class_name = ANY($1)`
- `internal/store/db_lookup_dfm.go` — новые методы:
  - `FindLatestDFMFormIDsByClassNames(classNames []string) (map[string]int64, error)` — `WHERE form_class = ANY($1)`
  - `FindLatestDFMComponentIDsByFormAndNames(formID int64, names []string) (map[string]int64, error)` — `WHERE form_id = $1 AND component_name = ANY($2)`
- `internal/indexer/indexer_postprocess_pas.go` — переписать `postProcessPASPending`:
  1. Собрать уникальные className из pendingClasses + pendingMethods + pendingFields
  2. Один batch-запрос `FindLatestPASClassIDsByNames` → cache map
  3. Один batch-запрос `FindLatestDFMFormIDsByClassNames` → cache map
  4. Для fieldCandidates: группировать по FormID, batch-resolve компонентов

---

## Фаза 3: `buildJSProcedureCallRelations` — batch-resolve callee процедур

**Проблема:** Для каждого JS procedure call — отдельный `FindLatestSQLProcedureIDByName`.

**Файлы:**
- `internal/indexer/indexer_relations.go` — переписать `buildJSProcedureCallRelations`:
  1. Собрать уникальные `procName` из `calls`
  2. Один вызов `FindLatestSQLProcedureIDsByNames` (уже существует)
  3. Резолвить из map
- Аналогично для `buildT01GeneratedSubscriberRelations` в `indexer_sql_pas.go:686-733` — тоже использует поштучный `FindLatestSQLProcedureIDByName`

---

## Фаза 4: `buildQueryFragmentRelations` — batch-resolve procedure calls из фрагментов

**Проблема:** В цикле по query fragments для каждого `extractProcedureCallsFromQuery` вызывается `FindLatestSQLProcedureIDByName` поштучно.

**Файлы:**
- `internal/indexer/indexer_relations.go` — в функции `buildQueryFragmentRelations` (или её helper для procedure calls):
  1. Первый проход: собрать все уникальные procName из всех fragments
  2. Один вызов `FindLatestSQLProcedureIDsByNames`
  3. Второй проход: резолвить из map

---

## Фаза 5: `indexAPIMacros` — batch-resolve API контрактов

**Проблема:** Для каждого macro invocation — 1-2 запроса `FindLatestAPIContractIDByNameAndKind`.

**Файлы:**
- `internal/store/api_store.go` — новый метод `FindLatestAPIContractIDsByNamesAndKinds(pairs []ContractNameKind) (map[string]map[string]int64, error)` — один SQL-запрос
- `internal/indexer/indexer.go` — переписать `indexAPIMacros` (строки 1167-1218):
  1. Собрать уникальные `(targetName, kind)` пары из всех invocations
  2. Один batch-запрос
  3. Резолвить из map в switch по `MacroType`

---

## Фаза 6: `FindDFMComponentIDsByForm` — группировка по FormID

**Проблема:** В `parseDFMFile` для каждого компонента вызывается `FindDFMComponentIDsByForm(component.FormID)` — N запросов вместо ~N_forms.

**Файлы:**
- `internal/indexer/indexer.go:447-452` — переписать цикл:
  1. Сгруппировать `componentsBatch` по `FormID`
  2. Для каждой уникальной FormID — один вызов `FindDFMComponentIDsByForm`
  3. Резолвить componentID из map

---

## Фаза 7: Параллельная пост-обработка

**Проблема:** Три независимых пост-обработки выполняются последовательно в `runner.go:102-104`.

**Файлы:**
- `internal/indexer/runner.go` — заменить три последовательных вызова на параллельные через `sync.WaitGroup` (3 горутины). Каждая пишет в свой `localStats`, затем merge в `collector`.
- `internal/indexer/runner.go` — то же для `Update` (строки 181-183).

**Риск:** `postProcessPASPending` и `postProcessSQLProcedureCallRelations` оба пишут в БД через `saveRelations` / `Update*` — убедиться, что они не конфликтуют по таблицам (они работают с разными relations/types, так что конфликта быть не должно).

---

## Проверка после каждой фазы

```bash
cd d:\GITHUB\GolandProjects\CodeBase
go build ./...
go test ./internal/indexer/... ./internal/store/...
```
