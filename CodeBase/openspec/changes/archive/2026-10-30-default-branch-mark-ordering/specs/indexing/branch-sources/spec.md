# Delta: indexing/branch-sources

## MODIFIED Requirements

### Requirement: Ветка по умолчанию

Система SHALL поддерживать параметр `indexer.default_branch` (по умолчанию
`develop`) и помечать `is_default` строку `ds_products_branch`, чей
`branch_name` равен значению параметра. Маркировка SHALL выполняться после
обработки всех каталогов продуктов в текущем прогоне discovery и не зависит
от порядка обхода каталогов. У продукта MAY быть не более одной
default-ветки; если default-ветка не проиндексирована, это не ошибка.

#### Scenario: Develop помечен default

- **GIVEN** продукт `fa-contracts` с линиями `develop` и `fa-contracts-11-01-7`, `default_branch = "develop"`
- **WHEN** выполняется discovery
- **THEN** линия `develop` имеет `is_default = true`, линия `fa-contracts-11-01-7` — `false`

#### Scenario: Клиентский каталог сортируется раньше develop-каталога

- **GIVEN** продукт `fa-contracts-ext` с линиями: клиентская `prj/CHECK_SAVE_PTI` в каталоге `fa-contracts-ext-check-save-pti` и `develop` в каталоге `fa-contracts-ext-develop`, сортируемом после клиентского
- **WHEN** выполняется первый `codebase update` на пустой БД
- **THEN** линия `develop` имеет `is_default = true`, клиентская линия — `false`

#### Scenario: Повторный прогон сохраняет метку

- **GIVEN** индекс после первого прогона, где `develop` помечен `is_default`
- **WHEN** выполняется повторный `codebase update` (upsert строк всех линий продукта, включая сброс `is_default` на path-конфликте)
- **THEN** линия `develop` остаётся `is_default = true`, прочие линии — `false`
