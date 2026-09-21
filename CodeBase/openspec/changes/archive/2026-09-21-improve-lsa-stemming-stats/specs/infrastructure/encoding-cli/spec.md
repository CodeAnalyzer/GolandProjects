# Delta: infrastructure/encoding-cli

## MODIFIED Requirements

### Requirement: Stats через systemsvc

Система SHALL предоставлять команду `stats` для агрегированной статистики индекса (через `systemsvc.ExecuteStats(db)`, общий execution-слой для CLI `cmd/stats.go` и MCP-инструмента `codebase_stats`). Реализация читает конфиг через `config.Get()` (возвращает `ErrConfigNotLoaded`, если не загружен) и `db.GetStats(ctx)`. Метрики полнотекстового слоя спек MUST быть generation-осведомлёнными: `spec_vocab_terms` и `spec_embeddings` считаются по активному поколению LSA-модели (generation из sidecar-файла состояния модели, разрешаемого через конфигурацию); статистика также содержит `spec_lsa_generations` — число поколений, удерживаемых в БД. Если файл состояния модели недоступен или не содержит generation, счётчики `spec_vocab_terms`/`spec_embeddings` возвращаются без фильтра по поколению (полное число строк), а `spec_lsa_generations` — по фактическому числу различимых поколений.

#### Scenario: Stats из CLI

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `codebase stats --json`
- **THEN** возвращена статистика в JSON envelope через `systemsvc.ExecuteStats`

#### Scenario: Stats через MCP

- **GIVEN** запущенный MCP-сервер и проиндексированный проект
- **WHEN** вызывается `codebase_stats`
- **THEN** возвращена та же статистика (без envelope) через тот же `systemsvc.ExecuteStats`

#### Scenario: Счётчики LSA по активному поколению

- **GIVEN** в БД удерживаются два поколения LSA (активное 11 000 терминов и предыдущее 11 200 терминов), state-файл модели указывает на активное поколение
- **WHEN** выполняется `codebase stats --json`
- **THEN** `spec_vocab_terms` = 11 000 (термины активного поколения, не сумма 22 200), `spec_embeddings` = числу векторов активного поколения, `spec_lsa_generations` = 2

#### Scenario: Фолбэк при недоступном state

- **GIVEN** в БД есть строки `spec_vocab`/`spec_embeddings`, но sidecar-файл состояния LSA-модели отсутствует или повреждён
- **WHEN** выполняется `codebase stats --json`
- **THEN** статистика возвращается успешно: `spec_vocab_terms`/`spec_embeddings` — полное число строк без фильтра по поколению, `spec_lsa_generations` — фактическое число поколений; ошибка чтения state не заваливает команду
