## MODIFIED Requirements

### Requirement: Полная пересборка индекса (--modified=false)

Система SHALL при `codebase update --modified=false` выполнять полную пересборку индекса кодовой базы вместо перепрохода по update-ветке: сброс таблиц, заполняемых парсерами кодовой базы (`ResetCodebaseTables`, см. `infrastructure/database-schema`), удаление LSA-sidecar файлов (`spec_lsa_model.bin`, `spec_lsa_state.json`, `spec_lsa_embeddings.bin`) и запуск штатного init-пайплайна (walk без pre-filter → парсинг → batch insert → постобработка → пересчёт LSA-модели). Прогресс-строка прогона помечается меткой `rebuild`. RTI- и TRC-сессии, история `scan_runs` и `schema_migrations` сохраняются. Прерванная (например, Ctrl+C) пересборка возобновляется повторным запуском `codebase update --modified=false` — полным рестартом пересборки. Инкрементальный run (`--modified=true`) после прерванной пересборки восстанавливает сущности уже обработанных файлов (pre-filter пропускает их по fingerprint), но НЕ восстанавливает relations-граф: pending-ссылки постпроцессоров накапливаются в памяти при парсинге и не переживают перезапуск процесса, поэтому для доигрывания пересборки инкрементальный run использовать нельзя.

#### Scenario: Полная пересборка с сохранением RTI/TRC и истории сканов

- **GIVEN** проиндексированный проект, в БД есть RTI- и TRC-сессии и история `scan_runs`
- **WHEN** выполняется `codebase update --modified=false`
- **THEN** таблицы кодовой базы сброшены и заполнены заново init-пайплайном
- **AND** сессии `rti_sessions`/`trc_sessions` и строки `rti_*`/`trc_*` остались на месте
- **AND** строки истории `scan_runs` сохранились, добавлена новая запись прогона

#### Scenario: Метка прогресса rebuild

- **GIVEN** выполняется `codebase update --modified=false`
- **WHEN** прогресс-репортер печатает строку прогресса
- **THEN** строка содержит метку `rebuild`, а не `init` или `update`

#### Scenario: Прерванная пересборка возобновляется повторным rebuild

- **GIVEN** пересборка `codebase update --modified=false` прервана после обработки части файлов (индекс частично заполнен)
- **WHEN** выполняется повторный `codebase update --modified=false`
- **THEN** пересборка стартует заново с чистого состояния и завершается полностью
- **AND** итоговый индекс эквивалентен непрерванной пересборке: сущности и relations-граф в единственном экземпляре

#### Scenario: Кэш эмбеддингов удаляется при пересборке

- **GIVEN** рядом с конфигом существуют sidecar-файлы `spec_lsa_model.bin`, `spec_lsa_state.json` и `spec_lsa_embeddings.bin`
- **WHEN** выполняется `codebase update --modified=false`
- **THEN** все три файла удалены перед запуском init-пайплайна
- **AND** последующий semantic-поиск пересобирает кэш из свежего индекса
