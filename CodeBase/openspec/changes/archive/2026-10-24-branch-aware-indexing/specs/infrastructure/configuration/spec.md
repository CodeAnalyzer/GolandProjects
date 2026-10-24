# Дельта: Настройки индексатора — ветка по умолчанию

## ADDED Requirements

### Requirement: Ветка по умолчанию индексатора

Система SHALL поддерживать параметр `default_branch` в секции `[indexer]`
конфигурации `codebase.toml` (значение по умолчанию — `develop`). Параметр
задаёт имя git-ветки, помечаемой `is_default` при discovery источников, и
ветку fallback для будущих lookup'ов сущностей. Отсутствие параметра SHALL
эквивалентно `default_branch = "develop"`.

#### Scenario: Явное значение

- **GIVEN** конфигурация с `[indexer] default_branch = "develop"` и продукт с линиями `develop` и `fa-contracts-11-01-7`
- **WHEN** выполняется discovery источников
- **THEN** линия `develop` помечена `is_default = true`

#### Scenario: Параметр отсутствует

- **GIVEN** конфигурация без `default_branch` в `[indexer]`
- **WHEN** читается конфигурация
- **THEN** используется значение по умолчанию `develop`
