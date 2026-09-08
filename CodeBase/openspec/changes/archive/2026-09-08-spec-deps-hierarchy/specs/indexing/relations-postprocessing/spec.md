## ADDED Requirements

### Requirement: Извлечение depends_on_capability из паттерна "поддомену/поддоменам"

Система SHALL извлекать relations `depends_on_capability` из текста spec-файла при обнаружении паттерна "поддомену" или "поддоменам" с последующим списком slug'ов в backtick-обёртке. Двоеточие после слова опционально. Каждому резолвнутому slug'у создаётся relation с `confidence: "notes"`.

#### Scenario: Поддомены без двоеточия

- **GIVEN** spec-файл capability `consumer-cession` с текстом в Notes: "Подробные спецификации по каждому поддомену: `portfolio-management`, `nominal-calculation`, `purchase`"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `consumer-cession` к `consumer-cession/portfolio-management`, `consumer-cession/nominal-calculation`, `consumer-cession/purchase` с `confidence: "notes"`

#### Scenario: Поддомены с двоеточием

- **GIVEN** spec-файл capability `my-domain` с текстом: "Поддоменам: `child-a`, `child-b`"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `my-domain` к `my-domain/child-a` и `my-domain/child-b`

### Requirement: Опциональное двоеточие в маркере "Связан с доменами"

Система SHALL распознавать маркер "Связан с доменами" как с двоеточием, так и без него. Slug'и извлекаются из backtick-обёрток и резолвятся через `resolveSlug` с sibling-префиксом.

#### Scenario: Связан с доменами без двоеточия

- **GIVEN** spec-файл capability `consumer-cession` с текстом в Notes: "Связан с доменами `consumer-credit`, `api-credit`, `client-ui-consumer`"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `consumer-cession` к `consumer-credit`, `api-credit`, `client-ui-consumer` с `confidence: "notes"`

#### Scenario: Связан с доменами с двоеточием

- **GIVEN** spec-файл capability `my-domain` с текстом: "Связан с доменами: `dep-a`, `dep-b`"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `my-domain` к `dep-a` и `dep-b`

### Requirement: Раскрытие иерархии capabilities в depends_on_capability

Система SHALL после извлечения прямых зависимостей раскрывать неявные связи по иерархии `parent_id` в обоих направлениях. При упоминании родительского домена создаются неявные зависимости на всех его детей. При упоминании дочернего домена создаётся неявная зависимость на его родителя. Неявные связи получают `confidence: "hierarchy"` и дедуплицируются с прямыми.

#### Scenario: Упоминание родителя → зависимости на детей

- **GIVEN** capability `accrual-core` (parent) имеет детей `accrual-core/base-algorithms` и `accrual-core/accrual-engine`
- **AND** spec-файл capability `my-feature` содержит markdown-ссылку на `accrual-core`
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** создана прямая relation `depends_on_capability` от `my-feature` к `accrual-core` с `confidence: "explicit"`
- **AND** созданы неявные relations `depends_on_capability` от `my-feature` к `accrual-core/base-algorithms` и `accrual-core/accrual-engine` с `confidence: "hierarchy"`

#### Scenario: Упоминание ребёнка → зависимость на родителя

- **GIVEN** capability `accrual-core` (parent) имеет ребёнка `accrual-core/base-algorithms`
- **AND** spec-файл capability `my-feature` содержит markdown-ссылку на `accrual-core/base-algorithms`
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** создана прямая relation `depends_on_capability` от `my-feature` к `accrual-core/base-algorithms` с `confidence: "explicit"`
- **AND** создана неявная relation `depends_on_capability` от `my-feature` к `accrual-core` с `confidence: "hierarchy"`

#### Scenario: Дедупликация прямых и иерархических связей

- **GIVEN** capability `accrual-core` имеет ребёнка `accrual-core/base-algorithms`
- **AND** spec-файл capability `my-feature` явно упоминает оба slug'а: `accrual-core` и `accrual-core/base-algorithms`
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** для каждого target_id существует ровно одна relation (прямая имеет приоритет над иерархической)
