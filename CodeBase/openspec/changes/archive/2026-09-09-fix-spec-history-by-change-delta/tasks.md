## 1. Структуры данных

- [x] 1.1 Добавить поля `CapabilityName string`, `SkipSpecs bool`, `DeltaSource string` в `SpecHistoryEntry` (`internal/specsvc/specsvc.go:136-143`). Теги: `capability_name` (без omitempty — нужно в режиме по change), `skip_specs,omitempty`, `delta_source,omitempty`. Проверить компиляцию `go build ./internal/specsvc/...`.
- [x] 1.2 Убедиться, что `SpecHistoryResult.Changes` (строка 132) остаётся без `omitempty` — пустой массив `[]` информативнее `null`. Проверить тег `json:"changes"` без omitempty.

## 2. SQL: delta-секции в executeSpecHistoryByChange

- [x] 2.1 В `executeSpecHistoryByChange` (`specsvc.go:714-744`) добавить второй SQL-запрос с `LEFT JOIN spec_change_delta d ON d.change_id = sc.id AND LOWER(d.capability_slug) = LOWER(c.capability_name)` и `COALESCE(r.confidence, '')` в SELECT (см. design.md D3). Запрос возвращает: change_name, status, capability_name, section, requirement_name, body_text, confidence.
- [x] 2.2 В цикле scan заполнить `SpecHistoryEntry`: `CapabilityName`, `ChangeName`, `Status`, `Section`, `ReqName`, `BodyText`. `DeltaSource` = confidence (default `"delta"` если пусто и есть delta-секция), `SkipSpecs = (DeltaSource == "proposal")`. Инициализировать `result.Changes = make([]SpecHistoryEntry, 0)` перед циклом. Проверить `go build ./internal/specsvc/...`.
- [x] 2.3 Существующий запрос для `Capabilities` (summary, без delta-join) оставить как есть — он уже корректен. Убедиться, что `result.Capabilities` инициализируется `make([]SpecHistoryCapability, 0)` (уже есть, строка 718).

## 3. Тесты

- [x] 3.1 Добавить тест `TestExecuteSpecHistoryByChange_Delta` — change с delta-секциями: проверить, что `Changes` содержит entry с `capability_name`, `section = "ADDED"`, непустым `requirement_name` и `body_text`, `SkipSpecs = false`. Тест должен проходить.
- [x] 3.2 Добавить тест `TestExecuteSpecHistoryByChange_SkipSpecs` — change с `skip_specs` (confidence = "proposal", нет delta): проверить entry с `SkipSpecs = true`, `DeltaSource = "proposal"`, пустыми `section`/`requirement_name`. Тест должен проходить.
- [x] 3.3 Добавить тест `TestExecuteSpecHistoryByChange_EmptyChanges` — change без delta и без skip_specs: проверить `Changes` = `[]` (не nil) в JSON. Тест должен проходить.
- [x] 3.4 Запустить `go test ./internal/specsvc/...` — все тесты должны проходить.

## 4. Валидация и сборка

- [x] 4.1 Запустить `go build ./...` — сборка должна проходить без ошибок.
- [x] 4.2 Запустить `go vet ./internal/specsvc/...` — без предупреждений.
- [x] 4.3 Запустить `openspec validate fix-spec-history-by-change-delta` — change должен валидироваться без ошибок.
