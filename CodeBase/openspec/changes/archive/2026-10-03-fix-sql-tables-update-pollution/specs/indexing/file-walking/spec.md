## MODIFIED Requirements

### Requirement: Pre-filter по fingerprint (mtime + size)

Система SHALL при инкрементальном обновлении (`codebase update`, флаг `--modified=true` — значение по умолчанию) пропускать чтение и хеширование файлов, у которых `size` и `mtime` совпадают с предыдущей индексацией. Карта известных fingerprint-ов (`map[path]FileFingerprint{Size, ModTime}`) загружается из индекса и устанавливается в walker через `SetPreFilter` перед обходом. Сравнение `mtime` выполняется с допуском 1мс (`modTimeMatch`), чтобы компенсировать потерю точности между PostgreSQL `TIMESTAMPTZ` (микросекунды) и `os.Stat` на Windows (100нс). Pre-filter применяется только при наличии предыдущего состояния (Init не использует pre-filter). Число пропущенных файлов отражается в метрике `PreFilteredFiles` `ScanStats` и печатается в выводе `codebase update` (`Pre-filtered: N`).

Pre-filter устанавливается в walker только при `--modified=true`. При `--modified=false` pre-filter НЕ устанавливается: все файлы читаются, хешируются и перепроходятся заново (полная переиндексация изменённых позиций — каждый файл сохраняется новой записью с каскадным удалением старых сущностей). Флаг `--modified=false` SHALL отключать как mtime+size pre-filter, так и hash-сравнение (`onlyModified`).

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
- **AND** каждый файл перепроходится по ветке обновления (новая запись файла, старые сущности удалены каскадно)
- **AND** счётчик `PreFilteredFiles` не растёт за счёт пропуска по fingerprint
