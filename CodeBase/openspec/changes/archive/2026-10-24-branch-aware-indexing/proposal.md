# branch-aware-indexing: мультиветочная индексация источников FA

## Why

Анализ инцидентов требует сопоставления трейсов с кодом той релизной линии,
которая установлена в банке, а у продуктов линии разные и меняются
одновременно. В рабочем дереве FA уже развёрнуты `fa-contracts-11016` и
`fa-contracts-11017` (`git worktree` на `FA-Contracts-11-01-6-SP` /
`FA-Contracts-11-01-7-SP`), но текущая эвристика определения продукта по имени
каталога (`extractCanonicalDSProductFromPath`, `tokens[:3]`) превращает их в
псевдопродукты `fa-contracts-11016`/`fa-contracts-11017`, а следующий
`codebase update` смешает тела процедур develop и SP-веток в одном индексе.
Изменение — базовый срез мультиветочной модели из
`Modifications/dsinfo-product-branches-6f2a1c.md`: продукт и ветка
определяются из git-состояния каталога, а не из имени каталога.

## What Changes

- **Git-детекция источника**: для каждого непосредственного ребёнка
  `root_path`, содержащего `.git` (каталог или файл worktree), при
  `init`/`update` определяются продукт (basename remote `origin`), ветка
  (`git rev-parse --abbrev-ref HEAD`) и commit; не-git каталоги и вложенные
  раскладки обрабатываются по прежней эвристике.
- **Новая таблица `ds_products_branch`**: релизные линии продуктов
  (`branch_name`, нормализованный `branch_key`, `path`, `commit_sha`,
  `is_default`); уникальность `(ds_product_id, branch_key)` и `path`.
- **Привязка файлов к ветке**: колонка `files.ds_product_branch_id`
  (nullable) + backfill существующих строк по префиксу каталога источника.
- **Единая нормализация имён веток**: правила «lowercase, снять `-SP`, снять
  `-HF<цифры>`, `.`/`_` → `-`» выносятся из `internal/dsinfo` в общий пакет;
  git-сторона и dsinfo-парсер считают `branch_key` одной функцией.
- **Ветка по умолчанию**: параметр `indexer.default_branch` (по умолчанию
  `develop`); ветка с совпадающим именем помечается `is_default`.
- **LSA-корпус при мультиветочности**: spec-LSA и desc-LSA обучаются на
  копиях сущностей из default-веток; сущности, встречающиеся только в других
  ветках, включаются оттуда; побайтовые дубли схлопываются — корпус не
  раздувается параллельными ветками.
- **Не меняется**: модель «одно каноническое тело на имя» в пределах ветки,
  поведение lookup/запросов (приоритет ветки в выборе сущностей — отдельный
  срез `branch-aware-lookup`), CLI/MCP-контракты, схема существующих таблиц
  (кроме аддитивной колонки `files`).

## Capabilities

### New Capabilities

- `indexing/branch-sources`: git-детекция источников, хранение релизных
  линий в `ds_products_branch`, discovery источников в `init`/`update`,
  привязка и backfill файлов к ветке, нормализация имён веток, ветка по
  умолчанию, поведение при смене ветки в каталоге.

### Modified Capabilities

- `infrastructure/configuration`: новое требование — параметр
  `indexer.default_branch`.
- `query/spec-search`: новое требование — состав LSA-корпуса при
  мультиветочной индексации (default-ветки, дедуп).
- `query/description-search`: новое требование — состав корпуса описаний при
  мультиветочной индексации.

## Impact

- **Код**: новые `internal/indexer/source_git.go` (детекция), общий пакет
  нормализации (refactor `internal/dsinfo/normalize.go`); правки
  `internal/store/db_schema.go` (таблица + колонка + индексы, идемпотентно),
  `internal/store/db_products.go` (upsert ветки), `internal/indexer/runner.go`
  (discovery + backfill + префиксная карта), `internal/indexer/indexer.go`
  (`saveFileCtx`: `ds_product_branch_id` для файлов из git-источников),
  `internal/specfts/` + `internal/store` (выборка LSA-корпуса),
  `internal/config/config.go` (`default_branch`).
- **Схема БД**: новая `ds_products_branch` (3 индекса), новая nullable-колонка
  `files.ds_product_branch_id` + индекс; `db_reset.go` — таблица в
  TRUNCATE-список. Существующие данные не мигрируются — backfill выполняется
  при следующем `init`/`update`.
- **Зависимости**: `git` CLI становится зависимостью индексатора (один вызов
  на источник, не на файл); при отсутствии git у каталога — fallback на
  эвристику.
- **Совместимость**: аддитивно; без git-каталогов поведение идентично
  текущему. Обновление индекса с каталогами веток до этого изменения —
  небезопасно (создаёт псевдопродукты), после — корректно.
