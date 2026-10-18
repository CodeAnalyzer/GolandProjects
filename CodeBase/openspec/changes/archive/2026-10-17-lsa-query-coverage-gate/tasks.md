## 1. Метрика лексического покрытия

- [x] 1.1 Добавить в `internal/specfts/tokenizer.go` функции `QueryLexicalCoverage(query string) (recognized, total int)` и `UnrecognizedWords(query string) []string`: «сырые» единицы — буквы/цифры/`_`, минимум одна буква, длина ≥ 2 руны; распознана, если `Tokenize(unit)` непуст или lower/стем входит в `stopWords`/`stopStems`; `total == 0` → coverage 0. Проверка: `go build ./...`
- [x] 1.2 Покрыть метрику табличными unit-тестами: TC-037 = 2/4, TC-119 = 1/3, чистый мусор = 0/2, «массовое создание плановых событий» = 4/4, «расчёт аннуитетного платежа» = 3/3, «расчёт для платежа» = 3/3 (стоп-слово), `asdkjhasd аннуитетного платежа` = 2/3, техимя `API_Card` = 1/1, пустая строка = 0/0. Проверка: `go test ./internal/specfts/...`

## 2. Конфигурация

- [x] 2.1 Добавить `LSAMinQueryCoverage *float64` в `SpecConfig` и `DescLSAConfig`, константу `DefaultLSAMinQueryCoverage = 0.6`, геттеры `MinQueryCoverage()` (nil → 0.6, явный 0 сохраняется). Проверка: `go build ./...` + unit-тест геттеров.
- [x] 2.2 Добавить валидацию в `ensureDefaults` рядом с `lsa_min_cosine`: значения `< 0` или `> 1` → ошибка с именем ключа `spec.lsa_min_query_coverage` / `desc_lsa.lsa_min_query_coverage`; отсутствие ключа и явный `0` ошибкой не считаются. Проверка: `go test ./internal/config/...`
- [x] 2.3 Добавить закомментированный `lsa_min_query_coverage = 0.6` в секции `[spec]` и `[desc_lsa]` файла `codebase.toml` с пояснением про `0 = отключено`. Проверка: `codebase health` с текущим конфигом.

## 3. Общий тип диагностики

- [x] 3.1 Добавить `specfts.SemanticDiagnostics` с полями `filtered_out`, `threshold`, `coverage`, `coverage_threshold`, `reason`, `oov_terms`, `hint`; `SpecSearchMeta` объявить алиасом типа. Проверка: `go build ./...` + JSON-тест, что `coverage: 0.0` не теряется из-за `omitempty`.
- [x] 3.2 Зафиксировать правило заполнения: `filtered_out` только для `reason=threshold` (и `oov`), для `query_coverage` не заполняется; `semantic_meta` отсутствует при непустом semantic-слое без OOV. Проверка: unit-тест построения метаданных.

## 4. Гейт и диагностика в desc-search

- [x] 4.1 Ввести `DescriptionSearchOutput{Items, HasMore, SemanticMeta}` и перевести `SearchDescriptions` на возврат `(*DescriptionSearchOutput, error)`; обновить `cmd/query_desc.go` и `internal/mcp/registry.go`. Проверка: `go build ./...`
- [x] 4.2 В `searchDescriptionsSemantic` вычислить покрытие до `descSemanticCache.get`; при `coverage < cfg.DescLSA.MinQueryCoverage()` вернуть пустой semantic-слой с `SemanticMeta` (`Reason=query_coverage`, `Coverage`, `CoverageThreshold`, `OOVTerms=UnrecognizedWords`, `Hint`), не загружая модель. Проверка: integration-тест — `loadCount` не растёт, exact-хиты не затронуты.
- [x] 4.3 Добавить desc-диагностику по образцу `buildSemanticMeta`: `reason=threshold` с `filtered_out` и `threshold`, `reason=oov` с OOV-стемами и подсказкой; при непустом semantic-слое без OOV метаданные отсутствуют. Проверка: unit/integration-тесты.
- [x] 4.4 CLI: добавить `semantic_meta` в `queryResponseMeta` (`meta.semantic_meta`), сохранив `items`/`count`/`meta.has_more`. MCP: `codebase_query_desc_search` возвращает объект `{items, has_more, semantic_meta}` вместо массива (**BREAKING**). Проверка: contract-тесты CLI и MCP.
- [x] 4.5 Integration-сценарии: TC-037 (`count=0`, `reason=query_coverage`), порог `0` (semantic возвращается), стоп-слова не подавляют запрос, MCP-ответ — объект. Проверка: `go test ./internal/query/... ./cmd/... ./internal/mcp/...`

## 5. Гейт и диагностика в spec-search

- [x] 5.1 В `searchSpecSemantic` после `specSemanticCache.get` и до `ProjectQuery` применить гейт: пустая секция, `Reason=query_coverage`, `OOVTerms=UnrecognizedWords`, `Coverage`, `CoverageThreshold`, `Hint`; `FilteredOut` не заполнять. Существующую диагностику недоступной модели не менять (проверяется раньше). Проверка: integration-тесты на TC-119, полностью нераспознанный запрос, покрытие ровно 0.6, `0` отключает гейт.
- [x] 5.2 Проверить независимость секций: `[spec]` и `[desc_lsa]` с разными значениями дают разное поведение на одинаковом запросе. Проверка: integration-тест.

## 6. Регрессия и контрольный прогон

- [x] 6.1 Прогнать пакетные тесты и линтеры: `go build ./...`, `go vet ./...`, `go test ./internal/specfts/... ./internal/config/... ./internal/query/... ./internal/specsvc/... ./cmd/... ./internal/mcp/...`. Проверка: все зелёные.
- [x] 6.2 Живой контроль: `desc-search "asdkjhasd несуществующий запрос zzz"` → `count=0`, `semantic_meta.reason=query_coverage`; `spec search "asdkjhasd zzzzz несуществующее"` → `semantic=[]`, `reason=query_coverage`; позитивные TC-032…036 и TC-113…118 без регрессий. Результаты зафиксировать в описании change или отдельном отчёте.
- [x] 6.3 Проверить exact-слой на смешанном запросе: exact-хиты возвращаются независимо от гейта (сценарий «Смешанный запрос с нераспознанными словами»). Проверка: ручной CLI-прогон + integration-тест.

## 7. Релиз

- [x] 7.1 Поднять `buildNumber` в `cmd/root.go` до 1607, пересобрать `CodeBase.exe`, проверить `CodeBase.exe health`. Проверка: health ok, версия/build `1607` в выводе.
- [x] 7.2 Выполнить `openspec validate "lsa-query-coverage-gate" --strict` и `openspec validate --specs`. Проверка: 0 ошибок.

## Контрольный прогон (build 1607, 2026-10-17)

- `go build ./...`, `go vet ./...`, `go test ./...` — зелёные.
- Integration-тесты (`-tags integration`): `QueryCoverage` в `internal/query` и
  `internal/specsvc` — PASS (включая `loadCount=0` при гейте и exact-хит под гейтом).
- `desc-search "asdkjhasd несуществующий запрос zzz"` → `count=0`,
  `reason=query_coverage`, `coverage=0.5`, `coverage_threshold=0.6`,
  `oov_terms=[asdkjhasd zzz]`.
- `spec search "asdkjhasd zzzzz несуществующее" --layer semantic` → `semantic=[]`,
  `exact=[]`, `reason=query_coverage`, `coverage=1/3`, `filtered_out` отсутствует.
- Позитивные: TC-032 → `API_ObjectEvent_MassCreate` (service/exact); TC-036 →
  `RPT_Security_FindListOfferByID`, `CST_Sec_CheckOfferDelete`, `API_GSO_MassModify`;
  TC-113 → exact 5 / semantic 5, top `Массовое создание объектных событий` (tsvector);
  TC-114 → semantic 5, top `consumer-pay-schedule/annuity-calc`; TC-115 →
  exact 5 / semantic 4.
- Exact-слой под гейтом: живой смешанный запрос даёт 0 exact, потому что
  `plainto_tsquery` — AND по всем терминам и в корпусе нет документа со всеми
  «мусорными» словами; integration-тест с документом, содержащим
  `asdkjhasd zzz классификатор`, подтвердил, что exact-хит возвращается при
  сработавшем гейте.
- `CodeBase.exe health` → ok, версия/build `1607`.
