## MODIFIED Requirements

### Requirement: Спеки по коду (spec_by_code)

Система SHALL по имени код-сущности (SQL-процедура, API-контракт, таблица, PAS-метод, DFM-форма, JS-функция, SMF-инструмент, отчёт) находить спеки, ссылающиеся на неё: обратный проход по `relations` `references_code` до сущности-источника (capability/requirement/scenario/usecase) с непустым текстом-фрагментом соответствующего источника, продуктом, путём к spec-файлу упоминания и границами строк. Результат — плоский список упоминаний, упорядоченный по имени capability и номеру строки; каждая запись указывает, из какой части спеки пришла ссылка (Related code / текст требования / текст сценария / текст usecase). Каждая запись результата SHALL содержать поле `file` с относительным путём spec-файла, в котором найдено упоминание код-сущности. Формирование фрагмента MUST выбирать текст по фактическому `source_type`: capability, requirement, scenario или usecase; пустые поля другой сущности не должны подавлять доступный контекст источника.

#### Scenario: Требования по процедуре

- **GIVEN** спека fa-custody с требованием, чей сценарий WHEN упоминает `API_DepoAccount_MassInsert`, и проиндексированная процедура
- **WHEN** вызывается `codebase_query_spec_by_code` с `name = "API_DepoAccount_MassInsert"`
- **THEN** возвращены capability и требование с непустым текстом строки упоминания, границами строк и путём к spec-файлу в поле `file`

#### Scenario: Код-сущность не упоминается в спеках

- **GIVEN** процедура `SomeInternalProc`, не встречающаяся ни в одном тексте спек
- **WHEN** вызывается spec_by_code
- **THEN** возвращён пустой результат (`count = 0`), не ошибка

#### Scenario: Путь файла для упоминания из usecase

- **GIVEN** usecase-файл `scenarios/scenario-sms-disable.md` с упоминанием `API_SmsDisable` и связанная с ним capability `card-service`
- **WHEN** вызывается `codebase_query_spec_by_code` с `name = "API_SmsDisable"`
- **THEN** поле `file` содержит путь к usecase-файлу, в котором найдено упоминание (не путь к spec.md capability)
- **AND** текст-фрагмент содержит контекст из usecase, а не пустую строку

#### Scenario: Контекст для каждого типа источника

- **GIVEN** одно кодовое имя упомянуто в capability, requirement, scenario и usecase с заполненными текстовыми полями
- **WHEN** выполняется spec_by_code по этому имени
- **THEN** каждая возвращенная запись содержит непустой фрагмент текста именно своей сущности-источника

### Requirement: Зависимости capability (spec_deps)

Система SHALL по slug capability (с опциональным фильтром продукта) возвращать её зависимости: прямые и транзитивные `depends_on_capability` в заданном направлении (depends_on | depended_by, с глубиной), вложенность по `parent_id` (родители и дети). Поддерживаются алиасы направлений: `outgoing` нормализуется к `depends_on`, `incoming` — к `depended_by`; канонические значения и алиасы равнозначны для CLI и MCP. Каждое ребро содержит confidence (explicit|notes|inline), источник (заголовок секции и строка маркера) и продукт. Транзитивные зависимости ограничиваются глубиной (по умолчанию 2) с защитой от циклов.

#### Scenario: Прямые и транзитивные зависимости

- **GIVEN** capability `card-limits` зависит от `card-service`, который зависит от `limits`
- **WHEN** вызывается `codebase_query_spec_deps` с `slug = "card-limits"` и `direction = depends_on`, depth 2
- **THEN** возвращены оба уровня зависимостей с пометкой уровня и confidence каждого ребра

#### Scenario: Кто зависит от capability

- **GIVEN** capability `reserve-references`, на которую ссылаются спеки reserve-elements
- **WHEN** вызывается spec_deps с `direction = depended_by`
- **THEN** возвращены все зависящие capability с источниками ссылок

#### Scenario: Алиас направления нормализуется

- **GIVEN** capability `card-limits` с прямой зависимостью `depends_on` от `card-service`
- **WHEN** вызывается spec_deps с `direction = outgoing` (CLI-флаг или MCP-параметр)
- **THEN** направление трактуется как `depends_on`
- **AND** результат идентичен вызову с `direction = depends_on`
