# Tasks: improve-lsa-stemming-stats

## 1. Токенизатор: Snowball вместо списка окончаний

- [x] 1.1 Добавить зависимость `github.com/kljensen/snowball` (`go get`), проверить `go mod tidy` и сборку `go build ./...`
- [x] 1.2 Заменить тело `stemRussian` в `internal/specfts/tokenizer.go` на вызов Snowball (Russian) через обёртку-единую точку (design D2); нормализация ё→е остаётся до стемминга; проверить unit-тестом: формы «автоматический/автоматическим/автоматического/автоматическому» → один стем «автоматическ», мусорные стемы («автоматическо», «автоматическу») не порождаются
- [x] 1.3 Перевести проверки длины на руны (`utf8.RuneCountInString`): аббревиатуры ≤ 4 символов не стеммятся (тест: «США» → токен «сша», не «сш»; «ЦБ», «НДС» остаются целыми), минимальная длина стема — 2 символа (тест: одно-буквенный огрызок отбрасывается)
- [x] 1.4 Предвычислить стемы записей стоп-словаря тем же Snowball и фильтровать токены по ним вместе с проверками по исходной форме; проверить тестом: «которого»/«которому» отфильтрованы при записи «который» в стоп-словаре
- [x] 1.5 Bump `TokenizerVersion` до 3 в `internal/specfts/lsa.go` (комментарий версии: Snowball + рунные длины + стоп-стемы); проверить, что fingerprint-тест (если есть) фиксирует смену
- [x] 1.6 Обновить ожидания существующих тестов токенизатора и словаря под новые стемы: `tokenizer_test.go` (TestTokenize_RussianStemming, TestStemRussian_SimpleCases и др.), `lsa_test.go` (TestTokenizeToStemCounts_Multiplicity, TestTokenize_YoNormalization), `vocab_tfidf_test.go`; `go test ./internal/specfts/...` зелёный

## 2. Регрессия LSA

- [x] 2.1 Обновить golden-тест LSA в `internal/indexer` на синтетическом корпусе `testdata/lsacorpus` под новую токенизацию: прогнать, осознанно перегенерировать ожидания, повторный прогон зелёный
- [x] 2.2 Прогнать `Tests/lsaaudit` на реальной БД (или вендорской копии): доля OOV-стеммов запросов не выросла, осколки-префиксы вида «автоматическо»/«автоматическу» отсутствуют в активном словаре

## 3. Метрики stats: активное поколение LSA

- [x] 3.1 Изменить `DB.GetStats` в `internal/store/db_stats.go`: параметр `lsaGeneration string`; `spec_vocab_terms`/`spec_embeddings` — `COUNT(*) FILTER (WHERE generation = $1)` при непустом поколении, иначе полный `COUNT(*)`; новое поле `spec_lsa_generations` = `COUNT(DISTINCT generation)` из `spec_vocab`; интеграционный тест (`db_lsa_integration_test.go`/schema-тесты): два поколения 11 000 + 11 200 терминов → активное = 11 000, generations = 2; пустое поколение → фолбэк 22 200
- [x] 3.2 В `internal/systemsvc/runtime.go` (`ExecuteStats`) после `config.Get()` читать `config.SpecLSAStatePath()` → `specfts.LoadLSAState` → `Generation` и передавать в `GetStats`; ошибки чтения state (отсутствие/повреждение) → пустой generation, команда не падает; unit-тест фолбэка
- [x] 3.3 Добавить поле `SpecLSAGenerations` (json `spec_lsa_generations`) в `Stats` (`internal/store` / `cmd/stats.go`), проверить smoke: `codebase stats --json` содержит `spec_vocab_terms` активного поколения и `spec_lsa_generations` на БД с двумя поколениями
- [x] 3.4 Обновить тесты/stats-сценарии MCP (`codebase_stats` возвращает те же поля без envelope) — проверка через существующий тест registry

## 4. Финальная проверка

- [x] 4.1 `go build ./...` и `go vet ./...` без замечаний
- [x] 4.2 `go test ./internal/specfts/... ./internal/store/... ./internal/systemsvc/... ./internal/indexer/...` — все зелёные
- [x] 4.3 `openspec validate improve-lsa-stemming-stats` — без ошибок
