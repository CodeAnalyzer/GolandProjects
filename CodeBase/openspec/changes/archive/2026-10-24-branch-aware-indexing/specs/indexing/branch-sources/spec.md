# Веточные источники индексации

## Purpose

Индексация нескольких релизных линий продуктов FA (develop + SP-ветки) из
отдельных git-checkout'ов: определение продукта и ветки из git-состояния
каталога, хранение линий в `ds_products_branch`, привязка файлов к ветке.
Основа мультиветочной модели из `Modifications/dsinfo-product-branches-6f2a1c.md`.

## ADDED Requirements

### Requirement: Git-детекция источника

Система SHALL определять источник индексации по git-состоянию каталога:
продукт — basename URL remote `origin`, ветка — `git rev-parse --abbrev-ref
HEAD`, commit — `git rev-parse HEAD`. Детекция MUST выполняться через
`git -C <dir> ...` и поддерживать worktree (`.git` — файл). Для detached HEAD
система SHALL использовать `git describe --tags --always` или короткий SHA.
Каталог без `.git` или без remote SHALL обрабатываться по прежней эвристике
определения продукта по пути.

#### Scenario: Worktree релизной линии

- **GIVEN** каталог `FA/fa-contracts-11017`, являющийся worktree с веткой `FA-Contracts-11-01-7-SP` и remote `origin` на `fa-contracts.git`
- **WHEN** выполняется git-детекция источника
- **THEN** продукт = `fa-contracts`, ветка = `FA-Contracts-11-01-7-SP`, зафиксирован commit HEAD

#### Scenario: Detached HEAD

- **GIVEN** git-каталог в состоянии detached HEAD на теге `FA-Contracts-11-01-7-26081201`
- **WHEN** выполняется git-детекция источника
- **THEN** имя ветки детерминированно получено через `git describe --tags --always` (или короткий SHA)

#### Scenario: Каталог без git

- **GIVEN** каталог `FA/DCP` без `.git`
- **WHEN** выполняется discovery источников
- **THEN** каталог обрабатывается по прежней эвристике продукта по пути, запись в `ds_products_branch` не создаётся

### Requirement: Хранение релизных линий

Система SHALL хранить релизные линии в таблице `ds_products_branch`:
`ds_product_id` (FK `ds_products` ON DELETE CASCADE), сырое имя ветки
`branch_name`, нормализованный `branch_key`, `path` каталога, `commit_sha`,
`is_default`, `indexed_at`. Пара `(ds_product_id, branch_key)` и `path`
MUST быть уникальны. `files.ds_product_id` сохраняется как денормализация;
новый код использует `files.ds_product_branch_id`.

#### Scenario: Upsert линии

- **GIVEN** продукт `fa-contracts` и каталог на ветке `FA-Contracts-11-01-7-SP`
- **WHEN** выполняется discovery при `codebase update`
- **THEN** создана или обновлена единственная строка `ds_products_branch` с `branch_key = 'fa-contracts-11-01-7'` и актуальным `commit_sha`

#### Scenario: Два каталога одной линии

- **GIVEN** две записи `ds_products_branch` с одинаковыми `(ds_product_id, branch_key)`, но разными `path`
- **WHEN** выполняется вставка
- **THEN** уникальный индекс отклоняет дубликат

### Requirement: Discovery источников в init/update

Система SHALL при каждом `init`/`update` перед обходом файлов проходить
непосредственных детей `root_path`: для каждого git-каталога выполнять
git-детекцию, upsert `ds_products` (по basename remote) и `ds_products_branch`
(по продукту + `branch_key`), и строить карту «префикс пути → источник».
Количество вызовов git CLI SHALL быть конечным (по одному набору вызовов на
каталог, с кэшем на время скана).

#### Scenario: Появление каталога ветки

- **GIVEN** индекс с одним источником `fa-contracts` (develop) и новый каталог `fa-contracts-11017` на `FA-Contracts-11-01-7-SP`
- **WHEN** выполняется `codebase update`
- **THEN** в `ds_products_branch` две линии продукта `fa-contracts`: `develop` и `fa-contracts-11-01-7`

#### Scenario: Отсутствие git в PATH

- **GIVEN** среда без установленного git и дерево с git-каталогами
- **WHEN** выполняется `codebase update`
- **THEN** выводится понятная ошибка либо git-каталоги обрабатываются по эвристике пути без аварийного завершения

### Requirement: Привязка файлов к ветке

Система SHALL при сохранении файла присваивать ему `ds_product_branch_id`
источника с самым длинным подходящим префиксом пути. Для файлов вне
git-источников `ds_product_branch_id` остаётся NULL, а продукт определяется
прежней эвристикой. Система SHALL выполнять backfill: при обнаружении
источника обновлять `files.ds_product_id` и `files.ds_product_branch_id`
существующих строк по префиксу пути источника (включая неизменённые по
mtime+size файлы).

#### Scenario: Файл из каталога ветки

- **GIVEN** файл `FA/fa-contracts-11017/SOURCE/Contract.pas` и источник с префиксом `FA/fa-contracts-11017`
- **WHEN** файл индексируется
- **THEN** файл получает `ds_product_id` продукта `fa-contracts` и `ds_product_branch_id` линии `fa-contracts-11-01-7`

#### Scenario: Backfill неизменённых файлов

- **GIVEN** индекс, собранный до появления веточной схемы, и неизменённый по mtime+size файл в каталоге git-источника
- **WHEN** выполняется `codebase update` с discovery источника
- **THEN** существующая строка файла получает корректные `ds_product_id` и `ds_product_branch_id` без переиндексации содержимого

### Requirement: Нормализация имён веток

Система SHALL вычислять `branch_key` единой функцией нормализации, общей для
git-имён веток и dsinfo-парсера манифеста `DiasoftSystemInfo.csv`: lowercase;
снять суффикс `-SP`; снять суффикс `-HF<цифры>`; символы `.`/`_` заменить на
`-`. Нормализация git-ветки `FA-Contracts-11-01-7-SP` и манифест-строки
`FA-Contracts-11-01-7\FA-Contracts-11-01-7-HF008` SHALL давать одинаковый
`branch_key` `fa-contracts-11-01-7`.

#### Scenario: SP-ветка и HF-манифест совпадают

- **GIVEN** git-ветка `FA-Contracts-11-01-7-SP` и строка манифеста `FA-Contracts-11-01-7\FA-Contracts-11-01-7-HF008`
- **WHEN** вычисляются `branch_key` обеих
- **THEN** оба равны `fa-contracts-11-01-7`

#### Scenario: Служебные ветки не совпадают с манифестом автоматически

- **GIVEN** ветка `sup/11-01-7`
- **WHEN** вычисляется `branch_key`
- **THEN** ключ не совпадает ни с одной манифест-строкой (`sup/11-01-7` не матчится автоматически)

### Requirement: Ветка по умолчанию

Система SHALL поддерживать параметр `indexer.default_branch` (по умолчанию
`develop`) и помечать `is_default` строку `ds_products_branch`, чей
`branch_name` равен значению параметра. У продукта MAY быть не более одной
default-ветки; если default-ветка не проиндексирована, это не ошибка.

#### Scenario: Develop помечен default

- **GIVEN** продукт `fa-contracts` с линиями `develop` и `fa-contracts-11-01-7`, `default_branch = "develop"`
- **WHEN** выполняется discovery
- **THEN** линия `develop` имеет `is_default = true`, линия `fa-contracts-11-01-7` — `false`

### Requirement: Смена ветки и удаление каталога

Система SHALL при смене ветки в существующем каталоге источника обновить
`branch_name`/`branch_key`/`commit_sha` строки `ds_products_branch` и
переиндексировать изменившиеся файлы; прежняя линия при этом теряет файлы
(для параллельного хранения нужны разные каталоги). При исчезновении каталога
источника система SHALL удалить его файлы и связанные сущности каскадно при
следующем `update`; пустая строка `ds_products_branch` удаляется или
помечается неактуальной.

#### Scenario: Переключение каталога на другую ветку

- **GIVEN** каталог источника на линии `fa-contracts-11-01-6`, переключаемый на `FA-Contracts-11-01-7-SP`
- **WHEN** выполняется `codebase update`
- **THEN** строка `ds_products_branch` каталога ссылается на линию `fa-contracts-11-01-7`, файлы старой линии удалены, изменившиеся файлы переиндексированы

#### Scenario: Удаление каталога ветки

- **GIVEN** индекс с источником `fa-contracts-11016` и удалённый каталог
- **WHEN** выполняется `codebase update`
- **THEN** файлы источника и его сущности удалены из индекса каскадно

## Related code

- `internal/indexer/source_git.go` — новый: `DetectSource(dir)` (remote/branch/commit, worktree-safe)
- `internal/branchkey/` — новый общий пакет нормализации (refactor из `internal/dsinfo/normalize.go`)
- `internal/store/db_schema.go` — `ds_products_branch`, `files.ds_product_branch_id`, индексы
- `internal/store/db_products.go` — upsert линий
- `internal/indexer/runner.go` — discovery детей `root_path`, префиксная карта, backfill
- `internal/indexer/indexer.go` — `saveFileCtx`, прежняя эвристика как fallback
- `Modifications/dsinfo-product-branches-6f2a1c.md` — исходный дизайн (решения 1–3)

## Notes

- Корневая причина старой модели — «одно каноническое тело на имя в продукте»;
  ветка добавляет вторую ось, где обе копии легитимны (см. исходный док,
  «Корневая причина»). Приоритет выбора копии — срез `branch-aware-lookup`.
- Гранулярность — релизная линия (`-SP`), не хотфикс: head линии может быть
  новее сборки банка; апгрейд до точных тегов остаётся возможным позже
  (`PacketName`/`HotfixName` из dsinfo-профиля сохранены).
