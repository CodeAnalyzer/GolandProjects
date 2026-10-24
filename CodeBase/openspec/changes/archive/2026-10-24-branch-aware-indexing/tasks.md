# Tasks: branch-aware-indexing

## 1. Общий пакет нормализации

- [x] 1.1 Создать пакет `internal/branchkey`: перенести правила нормализации из `internal/dsinfo/normalize.go` (lowercase, снять `-SP`, снять `-HF<цифры>`, `.`/`_` → `-`, разбор манифест-строки); перевести `internal/dsinfo` на общий пакет; проверить: существующие юнит-тесты `internal/dsinfo` зелёные без правок ожиданий, добавлен тест «git-ветка `-SP` и манифест `-HF008` дают одинаковый branch_key»
- [x] 1.2 Юнит-тесты `internal/branchkey`: `FA-Contracts-11-01-7-SP` → `fa-contracts-11-01-7`; `sup/11-01-7` не матчится автоматически; `.`/`_`, регистр, повторные суффиксы

## 2. Схема БД

- [x] 2.1 Добавить в `internal/store/db_schema.go` таблицу `ds_products_branch` (unique `(ds_product_id, branch_key)`, unique `path`, index `ds_product_id`, `is_default`, `indexed_at`), колонку `files.ds_product_branch_id` (nullable, FK ON DELETE SET NULL) и индексы — идемпотентно; расширить `schema_integration_test`
- [x] 2.2 Добавить `ds_products_branch` в TRUNCATE-список `internal/store/db_reset.go` и обновить `db_reset_integration_test` (каскад от таблицы, колонка files сбрасывается)

## 3. Git-детекция источника

- [x] 3.1 Создать `internal/indexer/source_git.go`: `DetectSource(dir)` — `git -C` remote/branch/commit, worktree-safe, detached HEAD через `describe --tags --always`, кэш по каталогу на время скана, явная ошибка при недоступном git; юнит-тесты: обычный репозиторий, worktree (`.git`-файл), detached HEAD, нет remote, нет git в PATH

## 4. Discovery и backfill

- [x] 4.1 Реализовать в `internal/store/db_products.go` upsert линий: `UpsertDSProductBranch(productID, branchName, branchKey, path, commitSHA)` + `SetDefaultBranch(productID, branchName)` + удаление строк без файлов; интеграционные тесты upsert/уникальность/is_default
- [x] 4.2 Встроить discovery в `internal/indexer/runner.go` (`init` и `update`): непосредственные дети `root_path` с `.git` → DetectSource → upsert продуктов/линий; префиксная карта источников передаётся в обработку файлов; не-git каталоги — прежний путь; интеграционный тест: индекс с develop + появление каталога `fa-contracts-11017`-подобного (фикстура) даёт две линии
- [x] 4.3 Реализовать backfill в `saveFileCtx`/runner: `UPDATE files SET ds_product_id, ds_product_branch_id WHERE path = $prefix OR path LIKE $prefix || '/%'` при обнаружении источника; интеграционный тест: существующие неизменённые строки получают branch_id без переиндексации, старые псевдопродукты исправляются
- [x] 4.4 Привязка файлов: самый длинный подходящий префикс из карты источников → `ds_product_branch_id`; вне источников — NULL + прежняя эвристика; тест на вложенных путях и нескольких источниках

## 5. Ветка по умолчанию (конфигурация)

- [x] 5.1 Добавить `default_branch` в `internal/config` ([indexer], default `develop`) + комментарий в `codebase.toml`; пометка/пересчёт `is_default` при discovery; тест: смена параметра перекладывает флаг

## 6. LSA-корпуса

- [x] 6.1 Единый helper выбора актуальной копии документа (default-ветка → свежая не-default → дедуп по хешу) в `internal/store`; применить в выборке корпусов spec-LSA (`specfts`) и desc-LSA; тесты: документ в двух ветках — один в корпусе; только в SP — включён; fingerprint не меняется от побайтового дубля

## 7. Финальная верификация

- [x] 7.1 `go test ./...` (unit+integration по профилям store/indexer/specfts/cmd) зелёные; пересборка `CodeBase.exe` (build bump)
- [x] 7.2 Smoke на реальном дереве FA (fa-contracts develop + fa-contracts-11016/11017 worktree): `codebase update` → в `ds_products_branch` три линии fa-contracts, псевдопродуктов нет, backfill отработал, LSA не переобучился от дублей; `openspec validate branch-aware-indexing --strict`
- [x] 7.3 README: раскладка каталогов/worktree, `default_branch`, ограничение «один каталог — одна ветка», предупреждение о порядке срезов (update с ветками — только после среза B для запросов)
- [x] 7.4 Бамп buildNumber до 1636 в `cmd/root.go` (константа `buildNumber`) и пересборка `CodeBase.exe`
