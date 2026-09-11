## ADDED Requirements

### Requirement: Извлечение depends_on_capability из паттерна "Связи с доменами"

Система SHALL извлекать relations `depends_on_capability` из текста spec-файла при обнаружении маркера "Связи с доменами" (множественное число, в отличие от существующего "Связан с доменами"). Slug'и извлекаются из круглых скобок в формате `description (slug, slug)` после двоеточия. Каждому резолвнутому slug'у создаётся relation с `confidence: "notes"`.

#### Scenario: Связи с доменами со slugs в скобках

- **GIVEN** spec-файл capability `card-registers` с текстом в Notes: "Связи с доменами: card-transaction (обороты транзакций), card-commission (комиссии), card-loyalty (кэшбэк/бонусы)"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `card-registers` к `card-transaction`, `card-commission`, `card-loyalty` с `confidence: "notes"`

#### Scenario: Связи с доменами с несколькими slugs в одних скобках

- **GIVEN** spec-файл capability `card-transaction` с текстом в Notes: "Связи с другими доменами: процессинговый центр (card-proccenter, card-race-export) — загрузка рейсов из ПЦ"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `card-transaction` к `card-proccenter` и `card-race-export` с `confidence: "notes"`

### Requirement: Извлечение depends_on_capability из паттерна "Связи с другими spec"

Система SHALL извлекать relations `depends_on_capability` из текста spec-файла при обнаружении маркера "Связи с другими spec" (plain text, без backticks). Slug'и извлекаются как первые слова перед круглыми скобками с описанием в формате `slug (description)`. Каждому резолвнутому slug'у создаётся relation с `confidence: "notes"`.

#### Scenario: Связи с другими spec со slugs без backticks

- **GIVEN** spec-файл capability `operations` с текстом в Notes: "Связи с другими spec: chart-accounts (контроль остатка при вводе, красное сальдо), memorial-orders (создание МО по проводкам CreateMOByOper), balances (расчёт остатков из tOperPart)"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `operations` к `chart-accounts`, `memorial-orders`, `balances` с `confidence: "notes"`

#### Scenario: Связи с другими spec с запятыми внутри описания

- **GIVEN** spec-файл capability `infrastructure` с текстом: "Связи с другими spec: chart-accounts (плана счетов, USEOLDNUMBER/STRATEGY_CHECKREST, скрипты Указаний ЦБ), oper-dates (DAYS_RESET, DATE_302P)"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `infrastructure` к `chart-accounts` и `oper-dates` с `confidence: "notes"`

### Requirement: Извлечение depends_on_capability из паттерна "Связан с `slug`" без слова "доменами"

Система SHALL извлекать relations `depends_on_capability` из текста spec-файла при обнаружении маркера "Связан с" с последующими backtick-wrapped slug'ами, даже если слово "доменами" отсутствует. Каждому резолвнутому slug'у создаётся relation с `confidence: "notes"`.

#### Scenario: Связан с без слова доменами

- **GIVEN** spec-файл capability `consumer-pay-schedule` с текстом в Notes: "Связан с `consumer-credit` — графики привязаны к кредитным договорам"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** создана relation `depends_on_capability` от `consumer-pay-schedule` к `consumer-credit` с `confidence: "notes"`

#### Scenario: Связан с несколькими slugs без слова доменами

- **GIVEN** spec-файл capability `verification` с текстом: "Связан с `contract-coverage`, `object-coverage`, `assessment`, `elements`, `links`"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `verification` к `contract-coverage`, `object-coverage`, `assessment`, `elements`, `links` с `confidence: "notes"`

### Requirement: Извлечение depends_on_capability из паттерна "Связь с доменом «Name» (slug)"

Система SHALL извлекать relations `depends_on_capability` из текста spec-файла при обнаружении маркера "Связь с доменом" с названием домена в кавычках «» и slug'ом в круглых скобках. Slug извлекается из круглых скобок. Создаётся relation с `confidence: "notes"`.

#### Scenario: Связь с доменом со slug в скобках

- **GIVEN** spec-файл capability `object-coverage` с текстом в Notes: "Связь с доменом «Договоры обеспечения» (contract-coverage): объект привязывается к договору через DealID"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** создана relation `depends_on_capability` от `object-coverage` к `contract-coverage` с `confidence: "notes"`

#### Scenario: Несколько связей с доменами в одном Notes

- **GIVEN** spec-файл capability `assessment` с текстом: "Связь с доменом «Объекты обеспечения» (object-coverage): экспертные оценки... Связь с доменом «Договоры обеспечения» (contract-coverage): процентные ставки..."
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `assessment` к `object-coverage` и `contract-coverage` с `confidence: "notes"`

### Requirement: Извлечение depends_on_capability из паттерна "Связь с spec `slug`"

Система SHALL извлекать relations `depends_on_capability` из текста spec-файла при обнаружении маркера "Связь с spec" с последующим backtick-wrapped slug'ом. Создаётся relation с `confidence: "notes"`.

#### Scenario: Связь с spec с backtick-wrapped slug

- **GIVEN** spec-файл capability `change-interest-baserate` с текстом в Notes: "Связь с spec `consumer-credit`: договоры из `tContractCredit` — основная сущность домена consumer-credit"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** создана relation `depends_on_capability` от `change-interest-baserate` к `consumer-credit` с `confidence: "notes"`

### Requirement: Извлечение depends_on_capability из расширенного паттерна "см. `slug`"

Система SHALL извлекать relations `depends_on_capability` из текста spec-файла при обнаружении маркера "см." с последующим backtick-wrapped slug'ом, как в скобках "(см. `slug`)", так и без скобок "см. `slug`". Также распознаётся вариант "См. spec `slug`". Создаётся relation с `confidence: "inline"`.

#### Scenario: см. без скобок

- **GIVEN** spec-файл capability `operations` с текстом в Notes: "См. spec `exchange-rates` (курсы)"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** создана relation `depends_on_capability` от `operations` к `exchange-rates` с `confidence: "inline"`

#### Scenario: см. в скобках с путём

- **GIVEN** spec-файл capability `object-coverage/verification` с текстом: "(см. `contract-coverage/verification`). Общий механизм повторного ввода описан в `verification`"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** создана relation `depends_on_capability` от `object-coverage/verification` к `contract-coverage/verification` с `confidence: "inline"`

## MODIFIED Requirements

### Requirement: Опциональное двоеточие в маркере "Связан с доменами"

Система SHALL распознавать маркер "Связан с доменами" как с двоеточием, так и без него. Slug'и извлекаются из backtick-обёрток и резолвятся через `resolveSlug` с sibling-префиксом. Также система SHALL распознавать сокращённую форму "Связан с" без слова "доменами" с backtick-wrapped slug'ами.

#### Scenario: Связан с доменами без двоеточия

- **GIVEN** spec-файл capability `consumer-cession` с текстом в Notes: "Связан с доменами `consumer-credit`, `api-credit`, `client-ui-consumer`"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `consumer-cession` к `consumer-credit`, `api-credit`, `client-ui-consumer` с `confidence: "notes"`

#### Scenario: Связан с доменами с двоеточием

- **GIVEN** spec-файл capability `my-domain` с текстом: "Связан с доменами: `dep-a`, `dep-b`"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `my-domain` к `dep-a` и `dep-b`

#### Scenario: Связан с без слова доменами

- **GIVEN** spec-файл capability `consumer-pay-schedule` с текстом: "Связан с `consumer-credit` — графики привязаны к кредитным договорам"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** создана relation `depends_on_capability` от `consumer-pay-schedule` к `consumer-credit` с `confidence: "notes"`
