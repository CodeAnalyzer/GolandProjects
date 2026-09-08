## Purpose

Навигационные запросы по спекам поверх общего графа индекса: от кода к требованиям (обратные references_code), граф зависимостей capability, usecase-слой с involved capabilities, покрытие кода спеками и история изменения capability по changes. Все запросы доступны как MCP-инструменты `codebase_query_spec_*` и зеркальные CLI-подкоманды `codebase query spec …`.

## ADDED Requirements

### Requirement: Спеки по коду (spec_by_code)

Система SHALL по имени код-сущности (SQL-процедура, API-контракт, таблица, PAS-метод, DFM-форма, JS-функция, SMF-инструмент, отчёт) находить спеки, ссылающиеся на неё: обратный проход по `relations` `references_code` до сущности-источника (capability/requirement/scenario/usecase) с текстом-фрагментом строки упоминания, продуктом и границами строк. Результат группируется по capability; внутри — требования и сценарии с указанием, из какой части спеки пришла ссылка (Related code / текст требования / текст сценария).

#### Scenario: Требования по процедуре
- **GIVEN** спека fa-custody с требованием, чей сценарий WHEN упоминает `API_DepoAccount_MassInsert`, и проиндексированная процедура
- **WHEN** вызывается `codebase_query_spec_by_code` с `name = "API_DepoAccount_MassInsert"`
- **THEN** возвращены capability и требование с текстом строки упоминания и границами строк

#### Scenario: Код-сущность не упоминается в спеках
- **GIVEN** процедура `SomeInternalProc`, не встречающаяся ни в одном тексте спек
- **WHEN** вызывается spec_by_code
- **THEN** возвращён пустой результат (`count = 0`), не ошибка

### Requirement: Зависимости capability (spec_deps)

Система SHALL по slug capability (с опциональным фильтром продукта) возвращать её зависимости: прямые и транзитивные `depends_on_capability` в заданном направлении (depends_on | depended_by, с глубиной), вложенность по `parent_id` (родители и дети). Каждое ребро содержит confidence (explicit|notes|inline), источник (заголовок секции и строка маркера) и продукт. Транзитивные зависимости ограничиваются глубиной (по умолчанию 2) с защитой от циклов.

#### Scenario: Прямые и транзитивные зависимости
- **GIVEN** capability `card-limits` зависит от `card-service`, который зависит от `limits`
- **WHEN** вызывается `codebase_query_spec_deps` с `slug = "card-limits"` и `direction = depends_on`, depth 2
- **THEN** возвращены оба уровня зависимостей с пометкой уровня и confidence каждого ребра

#### Scenario: Кто зависит от capability
- **GIVEN** capability `reserve-references`, на которую ссылаются спеки reserve-elements
- **WHEN** вызывается spec_deps с `direction = depended_by`
- **THEN** возвращены все зависящие capability с источниками ссылок

### Requirement: Usecase-слой (spec_usecase)

Система SHALL по имени/идентификатору usecase (имя файла, `REQ-001-SC-001`, pageId) возвращать его шаги с потоками (main|alternative), актёров и связанные capability (usecase_involves); по фильтру продукта — список usecase слоя с их видами (`source_dir`). Для продуктов с `usecase_layout = none` запросы возвращают пустой результат с пояснением профиля, а не ошибку.

#### Scenario: Сценарий с шагами и involved capabilities
- **GIVEN** проиндексированный `scenarios/scenario-sms-disable.md` с основным и альтернативным потоками и ссылками `../specs/card-service/spec.md`
- **WHEN** вызывается `codebase_query_spec_usecase` с `name = "scenario-sms-disable"`
- **THEN** возвращены актёры, шаги обоих потоков в порядке, involved capability `card-service`

#### Scenario: Продукт без usecase-слоя
- **GIVEN** продукт с профилем `usecase_layout = none`
- **WHEN** вызывается spec_usecase с фильтром этого продукта
- **THEN** возвращён пустой список с пометкой, что usecase-слой у продукта отсутствует

### Requirement: Покрытие кода спеками (spec_coverage)

Система SHALL по продукту или capability возвращать метрики покрытия: для capability с явными метриками в Notes — сохранённые значения; иначе — вычисляемые как отношение разрешённых `references_code` к числу сущностей кода соответствующего типа. Нерезолвнутые записи `spec_code_mentions` доступны отдельным запросом как «пробелы покрытия» (имя, kind, источник, строка) для ревизии спек.

#### Scenario: Вычисляемое покрытие по API-контрактам
- **GIVEN** продукт с 69 API-сервисами и спеками, разрешившими 55 упоминаний контрактов
- **WHEN** вызывается `codebase_query_spec_coverage` с фильтром продукта
- **THEN** возвращено покрытие 55/69 с разбивкой по capability

#### Scenario: Пробелы покрытия
- **GIVEN** спеки с 12 нерезолвнутыми mention-записями
- **WHEN** вызывается spec_coverage с `mode = gaps`
- **THEN** возвращён список из 12 записей с именами, kind и источниками для ревизии

### Requirement: История capability по changes (spec_history)

Система SHALL по slug capability возвращать хронологию изменений: все `spec_changes` (активные и архивные) с рёбрами `change_modifies`, для каждого — статус, артефакты и delta-секции (ADDED/MODIFIED/REMOVED) с именами и телами затронутых требований. Обратный запрос — по change его затронутые capability. Это даёт ответ «какие требования менялись и в каких change» без чтения файлов changes вручную.

#### Scenario: Хронология требований capability
- **GIVEN** capability `card-limits`, затронутая тремя архивными change (один с MODIFIED-требованием «Лимиты по операциям»)
- **WHEN** вызывается `codebase_query_spec_history` с `slug = "card-limits"`
- **THEN** возвращены три change в хронологическом порядке; у второго — delta-секция MODIFIED с телом требования

#### Scenario: Change со skip_specs
- **GIVEN** change с `skip_specs: true`, чей proposal упоминал `openspec/specs/CORE/data`
- **WHEN** вызывается spec_history по этому change
- **THEN** возвращена связь с capability `CORE/data` с пометкой, что связь извлечена из proposal (delta-требований нет)
