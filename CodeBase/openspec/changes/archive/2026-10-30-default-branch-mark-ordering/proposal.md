# Proposal: default-branch-mark-ordering

## Why

Маркировка default-ветки в discovery выполняется при обработке **первого**
каталога продукта (`markedProducts`, `internal/indexer/source_registry.go`),
а каталоги обходятся в побайтовом порядке `ReadDir`. Если develop-чекаут
сортируется позже другого каталога того же продукта, метка либо ставится до
того, как строка develop создана (первый скан), либо ставится и тут же
сбрасывается upsert-ом самой develop-строки: `UpsertDSProductBranch` на
`ON CONFLICT (path)` делает `is_default = FALSE`
(`internal/store/db_products.go`). Повторной маркировки в прогоне нет.

Подтверждено на рабочем ПК: у `fa-contracts-ext` 12 линий, develop присутствует
(2838 файлов), но `is_default = false` у всех — порядок обхода
`fa-contracts-ext-ZAPK` (Z=0x5A) → `fa-contracts-ext-check-save-pti` (c) →
`fa-contracts-ext-develop` (d) воспроизводится по id строк 6, 7, 8. Следствие:
ось `is_default` в `chosenBranchQuery` (дедуп/ранжирование результатов)
деградирует до тай-брейка по id.

## What Changes

- **Перенос маркировки**: `SetDefaultBranch` вызывается **после** цикла
  обработки всех каталогов `root_path`, по одному разу на продукт — все
  upsert-ы линий (включая сброс `is_default = FALSE` на path-конфликте) уже
  отработали, метку никто не затирает. Чувствительность к порядку каталогов
  исчезает.
- Политика не меняется: имя default-ветки — по-прежнему
  `indexer.default_branch` (`develop`), отсутствие такой линии у продукта —
  не ошибка, не более одной default-линии на продукт.
- Существующий сброс `is_default = FALSE` в `ON CONFLICT` сохраняется: он
  нужен при смене ветки в каталоге (прежняя default-линия теряет флаг).
- Integration-тест с точным сценарием дефекта: клиентский каталог продукта
  сортируется раньше develop-каталога, два прогона discovery (первый скан и
  установившийся), develop помечен в обоих.

## Capabilities

### New Capabilities

- (нет)

### Modified Capabilities

- `indexing/branch-sources`: MODIFIED «Ветка по умолчанию» — маркировка
  выполняется после обработки всех каталогов продукта и не зависит от
  порядка обхода; новые сценарии «клиентский каталог раньше develop» и
  «повторный прогон сохраняет метку».

## Impact

- **Код**: `internal/indexer/source_registry.go` (`discoverGitSources`:
  накопление `productID`, перенос `SetDefaultBranch` за цикл; правка ~10
  строк). `internal/store/db_products.go` — без изменений.
- **Поведение**: видимое только исправление — у продуктов, чей develop-каталог
  сортируется не первым, `is_default` выставляется корректно; на существующих
  БД достаточно очередного `codebase update`, миграций нет.
- **Тесты**: новый integration-тест (`-tags integration`) рядом с
  `source_discovery_integration_test.go`; регресс — полный `go test ./...`.
- Тема «версионные vs клиентские линии» (union-семантика, профиль dsinfo,
  missing-ветки) — вне этого change, отдельный дизайн.
