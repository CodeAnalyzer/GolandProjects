# Tasks: lsa-semantic-cache

## 1. Общий binary-cache helper (specfts)

- [x] 1.1 Вынести формат кэша `desc_lsa_embeddings.bin` (JSON-header {version, generation, dim, count} + строки метаданных + float32 LE блок, атомарная публикация temp+rename) в generic-хелпер `internal/specfts` (параметр — тип строки метаданных); юнит-тесты write→read, промах по generation, промах по version, повреждённый файл (`go test ./internal/specfts/`)
- [x] 1.2 Перевести `internal/query/desc_embeddings_cache.go` на общий хелпер без изменения формата; существующий файл кэша остаётся валидным; `go test ./internal/query/` зелёный

## 2. Кэш в памяти desc-semantic

- [x] 2.1 Добавить holder (RWMutex + singleflight, слот {generation, model, rows}) в `internal/query`; `searchDescriptionsSemantic` использует holder: хит → без `LoadLSAModel`/чтения файла; промах → существующий путь загрузки + запись в holder; юнит-тест «две загрузки — одно чтение» (мок/счётчик)
- [x] 2.2 Юнит-тест смены поколения: holder с gen G1, БД-модель G2 → замещение слота; и fallback без модели (exact-only) при повторных вызовах (`go test ./internal/query/`)

## 3. Файловый кэш spec-эмбеддингов + память

- [x] 3.1 Определить строку метаданных spec-кэша (capability_id, name, title, purpose, lines, snippet, product, dim) и путь `spec_lsa_embeddings.bin` (по образцу `config.DescLSAStatePath`); загрузчик из БД с фильтрами generation + embed_level='spec' (существующий SQL), фильтр по продукту — в памяти; юнит-тесты фильтра продукта
- [x] 3.2 Встроить файловый кэш в `specsvc.searchSpecSemantic`: хит по generation → из файла; промах/повреждён → БД + атомарная перезапись; интеграционный тест: первый вызов создаёт файл, повторный — читает (mtime/счётчик), результаты идентичны (`go test ./internal/specsvc/`)
- [x] 3.3 Добавить holder в памяти для spec (модель + rows, тот же протокол, что в п.2); тест «повторный вызов не перечитывает модель и кэш» (`go test ./internal/specsvc/`)

## 4. Индексатор: удаление sidecar при пересборке

- [x] 4.1 Добавить `spec_lsa_embeddings.bin` в список `removeLSASidecars` (`internal/indexer/runner.go:377`); тест/проверка: после `update --modified=false` файл отсутствует, последующий semantic-поиск пересобирает кэш без ошибки

## 5. Интеграционная проверка и производительность

- [x] 5.1 Интеграционный прогон на живом индексе FA: `codebase query spec-search` и `desc-search` (semantic layer) — результаты идентичны до/после изменения (golden-сравнение выдачи)
- [x] 5.2 Замер таймингов: холодный первый вызов и повторный (spec: БД-путь vs файловый кэш; desc: повторный вызов без перечитывания ~110 МБ) — повторный вызов semantic-слоя на порядок быстрее холодного; зафиксировать цифры в PR/коммите
- [x] 5.3 `go vet ./...`, `go build ./...`, полный `go test ./...` зелёные
