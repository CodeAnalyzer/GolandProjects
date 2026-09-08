## Purpose

Индексация OpenSpec-артефактов финпродуктов (`openspec/` в дереве исходников: спецификации, use-case-слои, changes, config.yaml) как источника сущностей `spec_*` общего индекса CodeBase: детект корня и продукта, структурный парсинг markdown, извлечение упоминаний кода, построение зависимостей и dual-write в symbols. Спеки встраиваются в существующий граф (files/symbols/relations) без собственной таблицы связей.

## ADDED Requirements

### Requirement: Детект openspec-корня и продукта

Система SHALL при индексации markdown/yaml-файлов детектировать openspec-корни: директории `openspec/` (регистр имени не значим — встречается `OpenSpec/`), содержащие `config.yaml` с `schema: spec-driven`. Артефакты вне openspec-корней не парсятся как спеки. Продукт-владелец определяется по каноническому имени `fa-*` из пути (существующий механизм `extractCanonicalDSProductFromPath`) и привязывается ко всем сущностям спек через `ds_product_id`.

#### Scenario: Корень в нижнем регистре
- **GIVEN** дерево проекта с `openspec/config.yaml` (`schema: spec-driven`) и `openspec/specs/card-limits/spec.md`
- **WHEN** выполняется `codebase init`
- **THEN** директория распознана как openspec-корень, `spec.md` отпарсен в сущности спек с привязкой к продукту

#### Scenario: Корень с именем OpenSpec
- **GIVEN** дерево проекта с `OpenSpec/specs/factoring-agreement/spec.md` и `OpenSpec/config.yaml`
- **WHEN** выполняется `codebase init`
- **THEN** директория распознана как openspec-корень несмотря на регистр имени

#### Scenario: Markdown вне openspec-корня
- **GIVEN** файл `docs/README.md` вне какой-либо openspec-директории
- **WHEN** выполняется индексация
- **THEN** файл сохранён в `files`, но сущности спек из него не создаются

### Requirement: Парсинг capability из spec.md

Система SHALL парсить каждый `specs/**/spec.md` в запись `spec_capabilities`: slug = полный путь директории относительно `openspec/specs/` (правило, продиктованное вложенными деревьями вида `FORMS/0409120/usage`), `title` — первый H1 (может отсутствовать), `purpose` — секция `## Purpose`, `notes` и `related_code` — сырой текст секций `## Notes` / `## Related code`. Парсер MUST NOT опираться на наличие H1 или заголовка `## Requirements` (встречаются спеки без обоих): якорями служат только `### Requirement:` / `#### Scenario:` и позиция файла в дереве. Промежуточные директории без `spec.md` становятся capability-контейнерами с `parent_id`-иерархией.

#### Scenario: Вложенная таксономия с полным путём
- **GIVEN** `openspec/specs/FORMS/0409120/usage/spec.md`
- **WHEN** выполняется парсинг
- **THEN** создана capability со slug `FORMS/0409120/usage`, контейнерные capability `FORMS` и `FORMS/0409120` связаны через `parent_id`

#### Scenario: Спека без H1 и без заголовка Requirements
- **GIVEN** `spec.md`, начинающийся сразу с `## Purpose` и содержащий `### Requirement:` без родительского `## Requirements`
- **WHEN** выполняется парсинг
- **THEN** capability, требования и сценарии распознаны; отсутствие H1 не считается ошибкой

### Requirement: Парсинг требований и сценариев

Система SHALL парсить блоки `### Requirement:` в `spec_requirements` (имя, SHALL-формулировка `body_text`, порядок, границы строк) и вложенные `#### Scenario:` в `spec_scenarios` с раздельными колонками `given_text` / `when_text` / `then_text`. Ключевые слова блоков GIVEN/WHEN/THEN распознаются как в жирном начертании (`**WHEN**`), так и в простом; строки AND присоединяются к последнему распознанному блоку. Нормативные ключевые слова в теле требований могут быть английскими (SHALL/MUST) или русскими (ДОЛЖНА/ДОЛЖЕН) — на распознавание структуры это не влияет, язык фиксируется в профиле продукта.

#### Scenario: Сценарий с жирным WHEN/THEN
- **GIVEN** сценарий со строками `- **WHEN** выполняется ...` и `- **THEN** Система ...`
- **WHEN** выполняется парсинг
- **THEN** строки распределены в `when_text` и `then_text` сценария

#### Scenario: AND после THEN относится к then_text
- **GIVEN** сценарий со строками THEN и следующей за ней `- **AND** формируется событие ...`
- **WHEN** выполняется парсинг
- **THEN** AND-строка дописана в `then_text` через перевод строки

#### Scenario: Русские нормативные формулировки
- **GIVEN** требование, начинающееся с «Система ДОЛЖНА обеспечивать ...»
- **WHEN** выполняется парсинг
- **THEN** требование сохранено с `body_text`; структура (Requirement/Scenario) распознана как для английских нормативов

### Requirement: Парсинг usecase-слоя

Система SHALL индексировать usecase-слой продукта в `spec_usecases` + `spec_usecase_steps`, поддерживая три фактических формата: `scenarios/*.md` (актёры, пред/постусловия, архитектура, потоки шагов), `usecases/**/*.md` (файлы вида `REQ-001-SC-001 — <имя>.md` с медиа-каталогом) и `business-processes/**/*.md` (выгрузки Confluence с pageId). Формат распознаётся по позиции директории в openspec-корне и фиксируется в `spec_usecases.source_dir` и профиле продукта (`usecase_layout`); Confluence pageId сохраняется в `spec_usecases.page_id`. Нумерованные шаги потоков парсятся в `spec_usecase_steps` с `flow_kind` main|alternative. Продукты без usecase-слоя не создают записей — запросы к usecase-инструментам честно возвращают пустой результат.

#### Scenario: Сценарии с актёрами и потоками
- **GIVEN** `openspec/scenarios/scenario-sms-disable.md` с секциями «Пользователи», «Основной поток» и «Альтернативный поток»
- **WHEN** выполняется парсинг
- **THEN** usecase создан с актёрами, шаги основного и альтернативного потоков — в `spec_usecase_steps` с соответствующим `flow_kind`

#### Scenario: Бизнес-процессы с pageId
- **GIVEN** `openspec/business-processes/Депозитарий._Счета_и_журналы/1._Бизнес-процесс_Прием_клиента_на_обслуживание.md` с пометкой pageId 403104219
- **WHEN** выполняется парсинг
- **THEN** usecase создан с `source_dir = business-processes` и `page_id = 403104219`

#### Scenario: Продукт без usecase-слоя
- **GIVEN** продукт с `openspec/specs/`, но без `scenarios/`, `usecases/`, `business-processes/`
- **WHEN** выполняется индексация
- **THEN** записи `spec_usecases` не создаются, профиль фиксирует `usecase_layout = none`

### Requirement: Парсинг changes-слоя

Система SHALL индексировать `changes/` (активные и `archive/`) в `spec_changes`: имя change, статус active|archived, вид артефакта (proposal/design/tasks/delta-specs). Delta-спеки (`## ADDED/MODIFIED/REMOVED Requirements`) парсятся в `spec_change_delta` с признаком секции, именем требования и телом. Для change с маркером `skip_specs: true` (структурной связи со спецификациями нет) система SHALL извлекать затронутые capability из текста `proposal.md` по упоминаниям путей `openspec/specs/<…>`.

#### Scenario: Архивированный change с delta-требованиями
- **GIVEN** `changes/archive/2026-08-27-card-limits/` с `proposal.md`, `tasks.md` и `specs/card-limits/spec.md` (секция `## ADDED Requirements`)
- **WHEN** выполняется парсинг
- **THEN** создан `spec_changes` со статусом archived; delta-требования сохранены в `spec_change_delta` с section ADDED; change связан с capability `card-limits`

#### Scenario: Change со skip_specs
- **GIVEN** активный change с `skip_specs: true` в `.openspec.yaml` и `proposal.md`, упоминающим `openspec/specs/CORE/data`
- **WHEN** выполняется парсинг
- **THEN** change создан и связан с capability `CORE/data` по извлечённому упоминанию, delta-записи не создаются

### Requirement: Профиль продукта

Система SHALL строить профиль продукта (`spec_configs`, один на openspec-корень) детекцией фактической структуры при индексации, а не из ручного конфигура: `usecase_layout` (scenarios|usecases|business-processes|none), `id_style` (dir|full_path — по наличию вложенных директорий в `specs/`), `has_changes`, `has_adr`, `has_audit`, `normative_lang` (en|ru — по нормативным ключевым словам в требованиях), `cross_ref_style` (explicit|notes|inline|mixed — по фактическим маркерам), а также сохранять текст секции `context:` из `config.yaml`.

#### Scenario: Профиль таксономического продукта
- **GIVEN** продукт с вложенными `specs/FORMS/0409718/settings/`, архивом changes и `business-processes/`
- **WHEN** выполняется индексация
- **THEN** профиль зафиксирован: `id_style = full_path`, `has_changes = true`, `usecase_layout = business-processes`

### Requirement: Извлечение упоминаний кода

Система SHALL извлекать упоминания код-сущностей из текстов спек (Related code с подсекциями `### Серверные процедуры` / `### API-процедуры` / `### События` / `### Клиент (Delphi)` / `### Отчёты` и т.п., inline-тексты требований и сценариев, delta-тексты changes) в staging-таблицу `spec_code_mentions` с привязкой к сущности-источнику (capability|requirement|scenario|usecase), номером строки и эвристическим `mention_kind`: `API_*` → api_contract; `FCD_*` → фасад (sql_procedure); `t[A-Z]*` / `p[A-Z]*` → sql_table; пути `*.smf` → smf_instrument; `*.dfm` → dfm_form; `*.pas` → pas_method; `*.sql` → sql_procedure; прочие идентификаторы → sql_procedure с fallback unknown.

#### Scenario: Подсекция Related code
- **GIVEN** capability со секцией `## Related code`, подсекцией `### API-процедуры` и буллетом `` `API_Depo/Server/DepoAccount/API_DepoAccount_MassInsert.sql` — массовое добавление ...``
- **WHEN** выполняется извлечение
- **THEN** в `spec_code_mentions` создана запись: имя `API_DepoAccount_MassInsert`, kind api_contract, источник — capability, строка буллета

#### Scenario: Inline-упоминание в сценарии
- **GIVEN** сценарий со строкой WHEN, содержащей `` процедуру `API_DpAcc_MassUpdateAccount` ``
- **WHEN** выполняется извлечение
- **THEN** создана запись mention с источником — scenario и номером строки

### Requirement: Пост-обработка — резолв упоминаний в references_code

Система SHALL резолвить записи `spec_code_mentions` в глобальной пост-обработке индексации (по образцу существующих callback/retcode-постпроцессоров): упоминание сопоставляется с сущностями кода (sql_procedures, api_contracts, sql_tables, dfm_forms, pas_methods, js_functions, smf_instruments, report_forms) по имени; разрешённые упоминания пишутся в `relations` с `relation_type = references_code`. Резолв двухфазен и НЕ зависит от порядка индексации файлов: спека может ссылаться на код, проиндексированный позже. Нерезолвнутые упоминания остаются в `spec_code_mentions` как измеримые пробелы покрытия и доступны запросам.

#### Scenario: Упоминание резолвится в процедуру
- **GIVEN** спека упоминает `API_DpAcc_MassUpdateAccount`; соответствующая SQL-процедура проиндексирована (до или после спеки)
- **WHEN** выполняется пост-обработка
- **THEN** в `relations` создано ребро `references_code` от сущности спеки к `sql_procedures` с указанием строки

#### Scenario: Упоминание без цели остаётся пробелом
- **GIVEN** спека упоминает `API_DpAcc_FindListGroupAccByID`, но файл процедуры отсутствует в дереве
- **WHEN** выполняется пост-обработка
- **THEN** ребро не создаётся, запись остаётся в `spec_code_mentions` и доступна запросу покрытия

### Requirement: Зависимости между capability

Система SHALL извлекать зависимости capability в `relations` с `relation_type = depends_on_capability`, распознавая пять фактических маркеров cross-refs: markdown-ссылки на `../specs/<name>/spec.md` (включая формы без сегмента `specs/`, как `../project/spec.md`); маркеры `«Связан с доменами:»` / `«Связи с другими spec:»` в Notes; inline-backtick-ссылки (`см. спецификацию \`slug\``, «описаны в `<path>`», «(см. `<slug>`)») и перечни «Смежные capability домена» в Notes/Purpose. Способ записи фиксируется в `relations.confidence`: explicit (markdown-ссылка), notes (текстовый маркер), inline (backtick-упоминание). Внутренняя иерархия (вложенность директорий) выражается `parent_id` и в relations НЕ дублируется.

#### Scenario: Markdown-ссылка в Related code
- **GIVEN** capability со ссылкой `../specs/card-limits/spec.md` в `## Related code`
- **WHEN** выполняется пост-обработка
- **THEN** создано ребро `depends_on_capability` с confidence explicit к capability `card-limits`

#### Scenario: Inline-ссылка на соседний аспект
- **GIVEN** требование capability `FORMS/0409120/usage` со строкой «(см. `calc-flow`)»
- **WHEN** выполняется пост-обработка
- **THEN** создано ребро `depends_on_capability` с confidence inline к sibling-capability `FORMS/0409120/calc-flow` (резолв по общему префиксу пути)

#### Scenario: Абсолютная внешняя ссылка по хвосту пути
- **GIVEN** Notes со ссылкой `cci:9://file:///D:/GITHUB/.../specs/consumer-credit:0:0-0:0`
- **WHEN** выполняется пост-обработка
- **THEN** хвост пути резолвится к локальной capability `consumer-credit` того же продукта; ребро с confidence explicit

### Requirement: Dual-write в symbols

Система SHALL дублировать каждую сущность спек (capability, requirement, scenario, usecase, change) в unified `symbols` с соответствующим `symbol_type`, `symbol_name` (slug/имя требования/имя usecase/имя change), `entity_type`, `entity_id`, `file_id` и `line_number` — по образцу существующих сущностей. Спеки со slug, совпадающим у разных продуктов, дают несколько записей в symbols; запросы показывают все совпадения с указанием продукта.

#### Scenario: Поиск capability через существующий query symbol
- **GIVEN** проиндексированные спеки, содержащие capability `depo-account` продукта fa-custody
- **WHEN** выполняется `codebase query symbol --name depo-account --type spec_capability`
- **THEN** найдена запись с указанием продукта и файла без новых механизмов поиска

#### Scenario: Совпадающие slug у разных продуктов
- **GIVEN** capability `contract-coverage` существует у двух продуктов
- **WHEN** выполняется запрос symbol по имени
- **THEN** возвращены обе записи, различимые по продукту

### Requirement: Инкрементальность и каскадное обновление

Система SHALL обновлять сущности спек при повторной индексации через существующий механизм `DeleteFilesByPathsExcept` + `ON DELETE CASCADE`: изменённый `spec.md` получает новый `file_id`, старые сущности спек (capability, требования, сценарии, mentions, symbols, references_code) удаляются каскадно, новые создаются заново. Usecase-файлы и файлы changes обновляются независимо, по собственным путям.

#### Scenario: Изменение spec.md
- **GIVEN** ранее проиндексированный `spec.md` с 3 требованиями, содержимое изменено (одно требование удалено, одно добавлено)
- **WHEN** выполняется `codebase update`
- **THEN** старая capability и её требования удалены каскадно, создана новая с актуальным составом требований, relations references_code пересобраны из свежих mentions
