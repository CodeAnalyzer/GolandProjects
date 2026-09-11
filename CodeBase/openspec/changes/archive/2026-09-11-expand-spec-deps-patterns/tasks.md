## 1. Новые regex-паттерны

- [x] 1.1 Добавить `reDomainsPlural` для маркера "Связи с доменами" (plain text slugs в круглых скобках). Проверить: regex находит "Связи с доменами: card-transaction (обороты), card-commission (комиссии)" и извлекает `card-transaction`, `card-commission`
- [x] 1.2 Добавить `reOtherDomains` для маркера "Связи с другими доменами" (slugs в круглых скобках, возможно несколько через запятую). Проверить: regex находит "Связи с другими доменами: процессинговый центр (card-proccenter, card-race-export)" и извлекает `card-proccenter`, `card-race-export`
- [x] 1.3 Добавить `reOtherSpecs` для маркера "Связи с другими spec" (plain text slugs перед скобками). Проверить: regex находит "Связи с другими spec: chart-accounts (контроль остатка), memorial-orders (создание МО)" и извлекает `chart-accounts`, `memorial-orders`
- [x] 1.4 Добавить `reLinkedNoDomains` для маркера "Связан с" без слова "доменами" (backtick-wrapped slugs). Проверить: regex находит "Связан с `consumer-credit` — графики" и извлекает `consumer-credit`
- [x] 1.5 Добавить `reDomainAngle` для маркера "Связь с доменом «Name» (slug)" (slug в круглых скобках после названия в кавычках «»). Проверить: regex находит "Связь с доменом «Договоры обеспечения» (contract-coverage)" и извлекает `contract-coverage`
- [x] 1.6 Добавить `reLinkSpec` для маркера "Связь с spec `slug`" (backtick-wrapped slug). Проверить: regex находит "Связь с spec `consumer-credit`" и извлекает `consumer-credit`
- [x] 1.7 Расширить `reSeeAlso` для распознавания "см. `slug`" без обязательных скобок и "См. spec `slug`". Проверить: regex находит "см. `exchange-rates`" без скобок и "(см. `slug`)" со скобками

## 2. Интеграция в extractCapabilityDeps

- [x] 2.1 Добавить блок извлечения для `reDomainsPlural` в `extractCapabilityDeps`: для каждого match извлекать slugs из круглых скобок через `reParenSlugs`, резолвить через `resolveSlug`, создавать relations с `confidence: "notes"`. Проверить: unit-тест с текстом "Связи с доменами: card-transaction (обороты)" создаёт relation
- [x] 2.2 Добавить блок извлечения для `reOtherDomains` в `extractCapabilityDeps`: для каждого match извлекать slugs из круглых скобок (включая несколько через запятую), резолвить через `resolveSlug`. Проверить: unit-тест с текстом "Связи с другими доменами: центр (card-proccenter, card-race-export)" создаёт 2 relations
- [x] 2.3 Добавить блок извлечения для `reOtherSpecs` в `extractCapabilityDeps`: для каждого match извлекать slugs как слова перед открывающей скобкой через `rePlainTextSlug`, резолвить через `resolveSlug`. Проверить: unit-тест с текстом "Связи с другими spec: chart-accounts (контроль), memorial-orders (создание)" создаёт 2 relations
- [x] 2.4 Добавить блок извлечения для `reLinkedNoDomains` в `extractCapabilityDeps`: для каждого match извлекать slugs из backtick-обёрток через `reBacktickSlugs`, резолвить через `resolveSlug`. Проверить: unit-тест с текстом "Связан с `consumer-credit` — графики" создаёт relation
- [x] 2.5 Добавить блок извлечения для `reDomainAngle` в `extractCapabilityDeps`: для каждого match извлекать slug из круглых скобок, резолвить через `resolveSlug`. Проверить: unit-тест с текстом "Связь с доменом «Договоры» (contract-coverage)" создаёт relation
- [x] 2.6 Добавить блок извлечения для `reLinkSpec` в `extractCapabilityDeps`: для каждого match извлекать slug из backtick-обёртки, резолвить через `resolveSlug`. Проверить: unit-тест с текстом "Связь с spec `consumer-credit`" создаёт relation
- [x] 2.7 Обновить блок `reSeeAlso` в `extractCapabilityDeps` для использования расширенного regex. Проверить: unit-тест с текстом "См. spec `exchange-rates`" без скобок создаёт relation с `confidence: "inline"`

## 3. Unit-тесты

- [x] 3.1 Добавить тест-кейс для "Связи с доменами: slug (desc)" в `indexer_postprocess_spec_deps_test.go`. Проверить: тест проходит, relation создан с `confidence: "notes"`
- [x] 3.2 Добавить тест-кейс для "Связи с другими доменами: desc (slug, slug)". Проверить: тест проходит, 2 relations созданы
- [x] 3.3 Добавить тест-кейс для "Связи с другими spec: slug (desc)". Проверить: тест проходит, relations созданы для каждого slug
- [x] 3.4 Добавить тест-кейс для "Связан с `slug`" без "доменами". Проверить: тест проходит, relation создан
- [x] 3.5 Добавить тест-кейс для "Связь с доменом «Name» (slug)". Проверить: тест проходит, relation создан
- [x] 3.6 Добавить тест-кейс для "Связь с spec `slug`". Проверить: тест проходит, relation создан
- [x] 3.7 Добавить тест-кейс для "см. `slug`" без скобок и "См. spec `slug`". Проверить: тест проходит, relation создан с `confidence: "inline"`
- [x] 3.8 Добавить тест-кейс с реальным текстом `operations/spec.md` из fa-generalledger (8 связей). Проверить: тест проходит, 8 relations созданы
- [x] 3.9 Добавить тест-кейс с реальным текстом `card-transaction/spec.md` из fa-cards ("Связи с другими доменами"). Проверить: тест проходит, relations созданы для всех slugs в скобках
- [x] 3.10 Запустить `go test ./internal/indexer/ -run TestPostProcessSpecDeps -v` и проверить, что все существующие тесты проходят (нет регрессии)

## 4. Валидация и переиндексация

- [x] 4.1 Запустить `go build ./...` и проверить, что компиляция успешна
- [x] 4.2 Запустить `go test ./internal/indexer/...` и проверить, что все тесты проходят
- [x] 4.3 Выполнить переиндексацию продукта fa-generalledger и проверить через `codebase_query_spec_deps` что `operations` теперь возвращает зависимости (ранее `null`)
- [x] 4.4 Выполнить переиндексацию продукта fa-warranty и проверить через `codebase_query_spec_deps` что `object-coverage` теперь возвращает зависимости из "Связь с доменом «Name» (slug)"
- [x] 4.5 Выполнить переиндексацию продукта fa-cards и проверить, что `card-transaction` возвращает дополнительные зависимости из "Связи с другими доменами" (в дополнение к существующим markdown-ссылкам)
- [x] 4.6 Проверить, что существующие зависимости в fa-contracts не изменились (например, `consumer-pay-schedule` → `credit-vacation` через markdown-ссылку остаётся)
