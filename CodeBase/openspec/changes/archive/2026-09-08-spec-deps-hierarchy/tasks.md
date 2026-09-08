## 1. Маркеры парсера

- [x] 1.1 Изменить регэксп `reLinkedDomains` в `internal/indexer/indexer_postprocess_spec_deps.go`: `:\s*` → `:?\s*` (опциональное двоеточие). Проверить: `go build ./internal/indexer/...`
- [x] 1.2 Добавить регэксп `reSubdomains = regexp.MustCompile(\`(?i)поддомен(?:у|ам|а|ов)?\s*:?\s*(.+)\`)` в блок `var ()`. Проверить: `go build ./internal/indexer/...`
- [x] 1.3 Добавить маркер 4 ("поддомену/поддоменам") в `extractCapabilityDeps` — извлечение slug'ов через `reBacktickSlugs` и `resolveSlug`, `confidence: "notes"`. Перенумеровать маркеры 4→5 (cci) и 5→6 (ExtractSpecReferences). Проверить: `go build ./internal/indexer/...`

## 2. Загрузка иерархии

- [x] 2.1 Добавить `ParentID int64` поле в `model.SpecCapability` в `internal/model/`. Проверить: `go build ./internal/model/...`
- [x] 2.2 Добавить `COALESCE(parent_id, 0)` в SELECT запроса `LoadAllSpecCapabilitiesForDeps` в `internal/store/db_lookup_spec_deps.go` и добавить `&c.ParentID` в `rows.Scan`. Проверить: `go build ./internal/store/...`

## 3. Раскрытие иерархии

- [x] 3.1 В `postProcessSpecDependencies` после построения `slugToID` построить два map'а: `childrenOf map[int64][]int64` (parent_id → []child_id) и `parentOf map[int64]int64` (child_id → parent_id) из загруженных caps. Проверить: `go build ./internal/indexer/...`
- [x] 3.2 Реализовать функцию `expandHierarchyDeps(relations []*model.Relation, childrenOf map[int64][]int64, parentOf map[int64]int64) []*model.Relation` — для каждой прямой зависимости добавляет неявные: parent → children (вниз) и child → parent (вверх) с `confidence: "hierarchy"`. Проверить: `go build ./internal/indexer/...`
- [x] 3.3 В `postProcessSpecDependencies` вызвать `expandHierarchyDeps` после `extractCapabilityDeps` и до дедупликации через `depSeen`. Проверить: `go build ./internal/indexer/...`

## 4. Тесты

- [x] 4.1 Добавить unit-тест `TestExtractCapabilityDeps_LinkedDomainsNoColon` — проверка что "Связан с доменами \`slug\`" без двоеточия создаёт relation. Проверить: `go test ./internal/indexer/... -run TestExtractCapabilityDeps_LinkedDomainsNoColon -count=1`
- [x] 4.2 Добавить unit-тест `TestExtractCapabilityDeps_Subdomains` — проверка что "поддомену: \`slug1\`, \`slug2\`" создаёт relations. Проверить: `go test ./internal/indexer/... -run TestExtractCapabilityDeps_Subdomains -count=1`
- [x] 4.3 Добавить unit-тест `TestExpandHierarchyDeps_ParentToChildren` — упоминание родителя создаёт неявные связи на детей. Проверить: `go test ./internal/indexer/... -run TestExpandHierarchyDeps -count=1`
- [x] 4.4 Добавить unit-тест `TestExpandHierarchyDeps_ChildToParent` — упоминание ребёнка создаёт неявную связь на родителя. Проверить: `go test ./internal/indexer/... -run TestExpandHierarchyDeps_ChildToParent -count=1`
- [x] 4.5 Добавить unit-тест `TestExpandHierarchyDeps_Dedup` — прямая связь не дублируется иерархической. Проверить: `go test ./internal/indexer/... -run TestExpandHierarchyDeps_Dedup -count=1`

## 5. Интеграционная проверка

- [x] 5.1 Выполнить `go build ./...` — чистая сборка
- [x] 5.2 Выполнить `go test ./internal/indexer/... -count=1` — все тесты PASS
- [x] 5.3 Выполнить `go vet ./internal/indexer/...` — чисто
- [x] 5.4 После переиндексации выполнить `codebase query spec deps --name consumer-cession --direction depends_on --json` — убедиться что зависимости появились: 3 внешних домена (consumer-credit, api-credit, client-ui-consumer) + 5 поддоменов (consumer-cession/portfolio-management, consumer-cession/nominal-calculation, consumer-cession/purchase, consumer-cession/sale, consumer-cession/purchased-contracts)
