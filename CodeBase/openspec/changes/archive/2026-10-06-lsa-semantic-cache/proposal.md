# Proposal: lsa-semantic-cache

## Why

Семантический слой поиска сейчас выполняет всю дорогую работу при каждом вызове заново: gob-декод LSA-моделей (`spec_lsa_model.bin`, `desc_lsa_model.bin`), чтение кэша `desc_lsa_embeddings.bin` (~110 МБ, ~0.2 c) и — для spec-поиска — полный pull `spec_embeddings` из БД через `float8[]`-как-текст (~0.25 c на 2 356 строк). Для долгоживущего MCP-сервера это повторная трата на каждый tool-вызов; для одноразовых CLI-команд spec-поиск и вовсе лишён файлового кэша, который уже есть у desc-поиска.

## What Changes

- Кэш в памяти для LSA-моделей и эмбеддингов обоих корпусов (spec, desc), ключуемый по generation; инвалидация — по дешёвому EXISTS-запросу поколения при каждом вызове (проверка уже выполняется).
- Новый sidecar-файл `spec_lsa_embeddings.bin` — бинарный кэш эмбеддингов spec-корпуса по образцу `desc_lsa_embeddings.bin` (header с поколением + float32 LE блок + поле product для фильтрации в памяти).
- Общий binary-cache helper в `internal/specfts` (параметризация row-типа), `internal/query/desc_embeddings_cache.go` переиспользует его.
- `--modified=false` (full rebuild) удаляет `spec_lsa_embeddings.bin` вместе с остальными sidecar-файлами.

## Capabilities

### New Capabilities

_(нет)_

### Modified Capabilities

- `query/spec-search`: semantic-слой spec-поиска получает файловый кэш эмбеддингов (`spec_lsa_embeddings.bin`, инвалидация по поколению, атомарная публикация) и переиспользование загруженной модели/эмбеддингов между вызовами одного процесса.
- `query/description-search`: semantic-слой desc-поиска переиспользует загруженную модель и эмбеддинги между вызовами одного процесса (модель и кэш-файл читаются один раз на поколение, а не на каждый вызов).
- `indexing/file-walking`: полная пересборка (`--modified=false`) удаляет `spec_lsa_embeddings.bin` вместе с остальными LSA-sidecar-файлами.

## Impact

- Код: `internal/specfts` (общий binary-cache helper), `internal/query/desc_query.go` + `desc_embeddings_cache.go` (memory-кэш), `internal/specsvc/specsvc.go` (memory-кэш + файловый кэш spec-эмбеддингов), `internal/indexer/runner.go` (`removeLSASidecars` — новый файл), `internal/config` (путь sidecar, по аналогии с desc).
- Память: MCP query-профиль удерживает до ~110 МБ (54 556 × 512 × float32) после первого semantic-вызова; кэш ленивый — профили rti/trc/review не платят.
- Совместимость: схема БД не меняется; внешнее поведение (результаты, graceful degradation при отсутствии моделей) сохраняется; меняются только тайминги.
- `--profile` не трогаем: ленивая загрузка и так исключает чтение sidecar-файлов в непрофильных сценариях.
