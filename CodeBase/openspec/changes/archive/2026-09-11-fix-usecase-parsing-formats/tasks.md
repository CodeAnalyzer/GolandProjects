## 1. ClassifyPath: распознавание specs/usecases/

- [x] 1.1 Добавить в `ClassifyPath` (openspecmd.go) проверку `len(rest) >= 2 && strings.EqualFold(rest[0], "specs") && isUsecaseDir(rest[1]) && strings.HasSuffix(baseLower, ".md")` → возвращать `KindUsecase` с `SourceDir = rest[1]`. Проверить: unit-тест ClassifyPath для пути `openspec/specs/usecases/scheta-i-zhurnaly/BP-01.md` → `KindUsecase`, `SourceDir = "usecases"`
- [x] 1.2 Добавить проверку для `INDEX.md` и `CAPABILITY-INDEX.md` внутри `specs/usecases/` → `KindUsecaseIndex` (не индексируются как usecase). Проверить: ClassifyPath для `openspec/specs/usecases/CAPABILITY-INDEX.md` → `KindUsecaseIndex`

## 2. ParseUsecaseFile: inline-метаданные

- [x] 2.1 Добавить regex `reInlineMeta = regexp.MustCompile(`^\*\*(.+?)\*\*:\s*(.+)$`)` в usecase.go. Добавить маппинг ключей (case-insensitive): `описание`→description, `пользователи и системы`/`роли и системы`/`актёры`/`акторы`→actors, `бизнес-ценность`/`ценность`→business_value, `точка старта`→architecture, `источник`→(извлечение pageId). Применять в цикле парсинга до проверок H2/таблиц, только когда `flow == flowNone && sectionTarget == nil`. Проверить: unit-тест для файла fa-custody `BP-01 Приём клиента на обслуживание.md` → actors и business_value заполнены
- [x] 2.2 Добавить regex `reURLPageID = regexp.MustCompile(`[?&]pageId=(\d+)`)`. Применять после `reConfluenceID` как fallback. Проверить: unit-тест для строки `**Источник**: ...pageId=467512912` → `PageID = 467512912`

## 3. ParseUsecaseFile: H3/H4-секции

- [x] 3.1 Добавить `reUsecaseH3` и `reUsecaseH4` regex. Вызывать `setSection` для H3 и H4 (не только H2). Для H3 `### Сценарий ...` — устанавливать `flow = flowMain` (сценарий = основной поток). Проверить: unit-тест для файла fa-financialasset `REQ-001-SC-001 — ....md` → `#### Предусловия` распознано, preconditions заполнены
- [x] 3.2 Добавить `"ветвления"` в switch `setSection` → `flow = flowAlt`. Проверить: unit-тест для файла fa-custody `BP-01.md` → блоки WHEN/ELSE из `## Ветвления` попали в steps с `flow_kind = "alternative"`
- [x] 3.3 Добавить `"спеки-компоненты"`, `"спеки-компоненты (запчасти)"`, `"запчасти"` в switch `setSection` → `setTarget(nil)`, установить флаг `inSpecComponents = true`. Буллеты в этой секции парсятся существующим `ExtractSpecReferences` в `parseUsecaseMD`. Проверить: unit-тест → mentions с `mention_kind = "spec_ref"` созданы для ссылок `specs/depo-contract/spec.md`

## 4. ParseUsecaseFile: шаги **Шаг N**. и табличные шаги

- [x] 4.1 Добавить `reBoldStep = regexp.MustCompile(`^\*\*Шаг\s+(\d+)\*\*[.)]?\s*(.+)$`)`. Если matches и `flow != flowNone` — добавлять шаг с `StepOrder` из regex. Проверить: unit-тест для `**Шаг 1**. Загрузка документа выплаты` → step с order=1, text="Загрузка документа выплаты"
- [x] 4.2 Расширить условие табличного парсинга: `uc.Kind == "business-process" || uc.Kind == "usecase"`. Для usecase-формата таблицы 4-колоночные: шаг = `cols[2]` (Действие) + `cols[3]` (Результат) через " — ". Колонка `cols[0]` (Шаг) — как step_order если число. Проверить: unit-тест для файла fa-custody `BP-01.md` → строки таблицы `| 0 | Оператор | ... | ... |` → steps с `flow_kind = "main"`

## 5. ExecuteSpecUsecase: параметр product и поиск по pageId

- [x] 5.1 Изменить сигнатуру `ExecuteSpecUsecase(ctx, db, name, product string)` в specsvc.go. При `name == "" && product != ""` — возвращать список usecase'ов продукта (`SpecUsecaseListResult` с массивом `[]SpecUsecaseListItem`). При `name != ""` — текущее поведение + опциональный фильтр по product. Проверить: unit-тест `ExecuteSpecUsecase(ctx, db, "", "fa-financialasset")` → список с source_dir и usecase_kind
- [x] 5.2 Добавить поиск по pageId: если `name` — число, сначала `WHERE page_id = $1::bigint`, затем fallback на `usecase_name`. Проверить: unit-тест `ExecuteSpecUsecase(ctx, db, "467512912", "")` → найден usecase с page_id=467512912
- [x] 5.3 Добавить типы `SpecUsecaseListResult` и `SpecUsecaseListItem` в specsvc.go (usecase_name, title, source_dir, usecase_kind, product). Проверить: компиляция

## 6. MCP registry и CLI: параметр product

- [x] 6.1 Обновить схему инструмента `codebase_query_spec_usecase` в registry.go: добавить опциональный параметр `product` (stringProp). Обновить handler: извлекать `product` через `optionalString`, передавать в `ExecuteSpecUsecase`. Проверить: MCP-вызов `codebase_query_spec_usecase` с `product = "fa-financialasset"` (без name) → список usecase'ов
- [x] 6.2 Добавить флаг `--product` в `querySpecUsecaseCmd` в cmd/query_spec.go. Сделать `--name` необязательным (убрать `MarkFlagRequired`). Добавить `MarkFlagsMutuallyExclusive` если нужно. Проверить: `codebase query spec usecase --product fa-financialasset` → JSON-список

## 7. Интеграционные тесты и реиндексация

- [x] 7.1 Добавить интеграционный тест в specsvc_integration_test.go: индексация файла `usecases/fot/REQ-001-SC-001 — ....md` → проверка заполненных description, actors, business_value, preconditions, postconditions, steps, page_id. Проверить: тест проходит
- [x] 7.2 Добавить интеграционный тест: индексация файла `specs/usecases/scheta-i-zhurnaly/BP-01 Приём клиента на обслуживание.md` → проверка заполненных actors, business_value, steps (из таблицы), alternative-шагов (из ветвлений), spec_code_mentions (из запчастей), page_id. Проверить: тест проходит
- [x] 7.3 Добавить интеграционный тест: `ExecuteSpecUsecase` с `product = "fa-financialasset"` → список из 33+ usecase'ов с source_dir="usecases". Проверить: тест проходит
- [x] 7.4 Выполнить реиндексацию продуктов fa-financialasset, fa-generalledger, fa-custody через `codebase update`. Проверить: `codebase stats` показывает ненулевое количество usecase_steps для этих продуктов; `codebase query spec usecase --name "REQ-001-SC-001 — ..."` возвращает заполненные поля. (Требует running PostgreSQL + FA repos — ручной шаг) продуктов fa-financialasset, fa-generalledger, fa-custody через `codebase update`. Проверить: `codebase stats` показывает ненулевое количество usecase_steps для этих продуктов; `codebase query spec usecase --name "REQ-001-SC-001 — ..."` возвращает заполненные поля
