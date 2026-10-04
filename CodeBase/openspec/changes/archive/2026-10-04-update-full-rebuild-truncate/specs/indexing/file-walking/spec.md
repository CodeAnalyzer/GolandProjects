# Дельта: indexing/file-walking

## MODIFIED Requirements

### Requirement: Pre-filter по fingerprint (mtime + size)

Система SHALL при инкрементальном обновлении (`codebase update`, флаг `--modified=true` — значение по умолчанию) пропускать чтение и хеширование файлов, у которых `size` и `mtime` совпадают с предыдущей индексацией. Карта известных fingerprint-ов (`map[path]FileFingerprint{Size, ModTime}`) загружается из индекса и устанавливается в walker через `SetPreFilter` перед обходом. Сравнение `mtime` выполняется с допуском 1мс (`modTimeMatch`), чтобы компенсировать потерю точности между PostgreSQL `TIMESTAMPTZ` (микросекунды) и `os.Stat` на Windows (100нс). Pre-filter применяется только при наличии предыдущего состояния (Init не использует pre-filter). Число пропущенных файлов отражается в метрике `PreFilteredFiles` `ScanStats` и печатается в выводе `codebase update` (`Pre-filtered: N`).

Pre-filter устанавливается в walker только при `--modified=true`. При `--modified=false` pre-filter НЕ устанавливается: все файлы читаются и хешируются, но прогон выполняется как полная пересборка индекса через init-пайплайн (см. требование «Полная пересборка индекса (--modified=false)»), а не по per-file update-ветке. Флаг `--modified=false` SHALL отключать как mtime+size pre-filter, так и hash-сравнение (`onlyModified`).

#### Scenario: Файл не изменился — пропущен pre-filter-ом

- **GIVEN** ранее проиндексированный файл `Form.pas` с тем же размером и `mtime`, что в индексе
- **WHEN** выполняется `codebase update` (или `codebase update --modified=true`) с установленным pre-filter
- **THEN** файл не читается с диска и не хешируется
- **AND** метрика `PreFilteredFiles` увеличивается на 1
- **AND** файл не передаётся в indexer

#### Scenario: Файл изменился по mtime — читается и хешируется

- **GIVEN** ранее проиндексированный файл `Proc.sql`, у которого изменилось содержимое (а значит и `mtime`/`size`)
- **WHEN** выполняется `codebase update`
- **THEN** fingerprint не совпадает, файл читается и хешируется
- **AND** файл передаётся в indexer для повторного парсинга

#### Scenario: Init не использует pre-filter

- **GIVEN** первичная индексация проекта, предыдущего состояния в индексе нет
- **WHEN** выполняется `codebase init`
- **THEN** pre-filter отключён, все подходящие файлы читаются и хешируются

#### Scenario: --modified=false — полный перепрох без pre-filter

- **GIVEN** ранее проиндексированный проект, в индексе есть файлы с неизменными `size` и `mtime`
- **WHEN** выполняется `codebase update --modified=false`
- **THEN** pre-filter не устанавливается, все подходящие файлы читаются и хешируются независимо от совпадения fingerprint и hash
- **AND** прогон выполняется как полная пересборка индекса (сброс таблиц кодовой базы + init-пайплайн), а не по per-file update-ветке
- **AND** счётчик `PreFilteredFiles` не растёт за счёт пропуска по fingerprint

## ADDED Requirements

### Requirement: Полная пересборка индекса (--modified=false)

Система SHALL при `codebase update --modified=false` выполнять полную пересборку индекса кодовой базы вместо перепрохода по update-ветке: сброс таблиц, заполняемых парсерами кодовой базы (`ResetCodebaseTables`, см. `infrastructure/database-schema`), удаление LSA-sidecar файлов (`spec_lsa_model.bin`, `spec_lsa_state.json`) и запуск штатного init-пайплайна (walk без pre-filter → парсинг → batch insert → постобработка → пересчёт LSA-модели). Прогресс-строка прогона помечается меткой `rebuild`. RTI- и TRC-сессии, история `scan_runs` и `schema_migrations` сохраняются. Прерванная (например, Ctrl+C) пересборка возобновляется повторным запуском `codebase update --modified=false` — полным рестартом пересборки. Инкрементальный run (`--modified=true`) после прерванной пересборки восстанавливает сущности уже обработанных файлов (pre-filter пропускает их по fingerprint), но НЕ восстанавливает relations-граф: pending-ссылки постпроцессоров накапливаются в памяти при парсинге и не переживают перезапуск процесса, поэтому для доигрывания пересборки инкрементальный run использовать нельзя.

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
