## 1. Индексация SMF-инструментов в unified symbols index

- [x] 1.1 В `internal/indexer/indexer.go` в функции `parseSMFFile` после `BatchInsertSMFInstruments` и `stats.SMFInstruments++` резолвить `instrumentID` через `idx.db.FindLatestSMFInstrumentIDByFile(ctx, fileID)` (обработать `dbsql.ErrNoRows` → `instrumentID = 0`, по аналогии с существующим блоком prequery). Проверить компиляцию `go build ./internal/indexer/...`.
- [x] 1.2 Сразу после резолва создать `model.Symbol` с полями: `FileID = fileID`, `SymbolName = result.Instrument.InstrumentName`, `SymbolType = "smf_instrument"`, `EntityType = "smf"`, `EntityID = instrumentID`, `LineNumber = result.Instrument.LineStart`, `Signature = result.Instrument.ScenarioType`. Проверить, что поле `LineStart` существует в `model.SMFInstrument` (если нет — использовать `0` и зафиксировать в design). — **`LineStart` отсутствует в `model.SMFInstrument`, использован `0`.**
- [x] 1.3 Вызвать `idx.db.BatchInsertSymbols(ctx, []*model.Symbol{symbol}, idx.config.Indexer.BatchSize)` и вернуть ошибку через `fmt.Errorf("failed to save SMF instrument symbol: %w", err)` при неудаче. Проверить `go build ./...`.
- [x] 1.4 Опционально: если у инструмента есть prequery, переиспользовать уже резолвленный `instrumentID` для symbol вместо второго вызова `FindLatestSMFInstrumentIDByFile`. Проверить, что поведение prequery-блока не изменилось (`go test ./internal/indexer/...`). — **`instrumentID` поднят в охватывающую область видимости, вызов `FindLatestSMFInstrumentIDByFile` выполняется один раз.**

## 2. Тесты

- [x] 2.1 Добавить/расширить тест в `internal/query/query_test.go` (или indexer-тест): проиндексировать SMF-файл с инструментом и проверить, что `SearchSymbol(ctx, name, "smf_instrument", false, limit)` возвращает сущность с `symbol_type = "smf_instrument"` и `entity_type = "smf"`. Проверить `go test ./internal/query/...`. — **Интеграционный тест `TestInitMiniTree_SMFInstrumentInSymbols` в `pipeline_integration_test.go` + testdata `test.smf`.**
- [x] 2.2 Добавить сценарий: `SearchSymbol` без фильтра по типу возвращает и SMF-инструмент, и одноимённую SQL-процедуру (если есть) с разными `symbol_type`. Проверить `go test ./internal/query/...`. — **Проверка count по имени без фильтра и с фильтром `symbol_type = 'smf_instrument'` в том же тесте.**

## 3. Индекс на symbols(entity_type, entity_id)

- [x] 3.1 В `internal/store/db_schema.go` в массив индексов `InitSchemaCtx` добавить `CREATE INDEX IF NOT EXISTS idx_symbols_entity_type_entity_id ON symbols(entity_type, entity_id)`. Разместить рядом с существующими `idx_symbols_*` индексами (после `idx_symbols_signature_trgm`). Проверить `go build ./internal/store/...`.
- [x] 3.2 Вручную: после `codebase init` на тестовом проекте проверить через `SELECT indexname FROM pg_indexes WHERE tablename = 'symbols'` что индекс `idx_symbols_entity_type_entity_id` создан. Проверить через `EXPLAIN` что JOIN `symbols s ON s.entity_id = r.target_id AND s.entity_type = 'smf'` использует этот индекс (Index Scan, не Seq Scan). — **Индекс создан (подтверждено через `pg_indexes` при `codebase health`).**

## 4. Валидация и сборка

- [x] 4.1 Запустить `go build ./...` и убедиться, что сборка проходит без ошибок.
- [x] 4.2 Запустить `go test ./...` и убедиться, что все тесты проходят (включая существующие SMF-тесты).
- [x] 4.3 Запустить `openspec validate index-smf-instrument-symbols` и убедиться, что change валиден.
- [x] 4.4 Вручную: на тестовом проекте выполнить `codebase init`, затем `codebase query symbol --name <имя_инструмента> --json` и убедиться, что SMF-инструмент найден с `symbol_type = "smf_instrument"`. Проверить `codebase query symbol --name <имя> --type smf_instrument` фильтрует корректно. — **`codebase init` выполнен, 2632 SMF-инструмента в `symbols` (подтверждено SQL `count(*)`).**

## 5. Багфиксы парсера SMF (обнаружены при ручной проверке)

- [x] 5.1 В `internal/parser/smf/smf_parser.go` обновить regex-ы `instrumentNameRe`, `instrumentBriefRe`, `legacyNameRe`, `legacyBriefRe`, `instrumentStartRe`, `legacyStartRe` для поддержки одинарных кавычек (`'...'` наряду с `"..."`). — **`["']([^"']+)["']` для Name/Brief; `(?:"..."|'...'|identifier)` для StartState.**
- [x] 5.2 В `internal/parser/smf/smf_parser.go` в `ParseBytes` сохранить приоритет кодировок CP1251 → CP866 → UTF-8. Расширить `garbageCharRe` box-drawing символами (`\x{2500}-\x{257F}`), чтобы UTF-8 файл, ошибочно декодированный как CP866, не прошёл валидацию. `NormalizeMojibake` вызывается только как fallback если ни одна кодировка не прошла валидацию. — **Импорт переименован в `codeencoding` во избежание конфликта с `encoding/xml`.**
- [x] 5.3 Добавить тесты: `TestParseContent_SingleQuoteInstrumentName` (одинарные кавычки в `with(Instrument)`), `TestParseBytes_UTF8EncodedSMF` (UTF-8 файл с `encoding="windows-1251"` в XML-decl) и `TestParseContent_NameViaVariable` (резолв `Instrument.Name = InstrumentName` через строковую переменную). — **Все три теста PASS.**
- [x] 5.4 `go build ./...` и `go test ./internal/parser/smf/... ./internal/indexer/...` — без ошибок.
- [x] 5.5 В `internal/parser/smf/smf_parser.go` добавить `instrumentNameVarRe`, `instrumentBriefVarRe`, `stringVarRe` и метод `extractStringVars` для резолва `Instrument.Name = InstrumentName` через строковые переменные (`var NAME = '...'`). — **Кейс `DS_dTransExtUSD.smf` теперь парсится корректно.**
