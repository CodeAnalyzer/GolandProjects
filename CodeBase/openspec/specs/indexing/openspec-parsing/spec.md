# OpenSpec Parsing

## Purpose

Индексация OpenSpec-артефактов финпродуктов (`openspec/` в дереве исходников: спецификации, use-case-слои, changes, config.yaml) как источника сущностей `spec_*` общего индекса CodeBase: детект корня и продукта, структурный парсинг markdown, извлечение упоминаний кода, построение зависимостей и dual-write в symbols. Спеки встраиваются в существующий граф (files/symbols/relations) без собственной таблицы связей.

## Requirements

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

Система SHALL парсить блоки `### Requirement:` в `spec_requirements` (имя, SHALL-формулировка `body_text`, порядок, фактические границы строк исходного Markdown) и вложенные `#### Scenario:` в `spec_scenarios` с раздельными колонками `given_text` / `when_text` / `then_text` и фактическими границами строк блока. Ключевые слова блоков GIVEN/WHEN/THEN распознаются как в жирном начертании (`**WHEN**`), так и в простом; строки AND присоединяются к последнему распознанному блоку. Нормативные ключевые слова в теле требований могут быть английскими (SHALL/MUST) или русскими (ДОЛЖНА/ДОЛЖЕН) — на распознавание структуры это не влияет, язык фиксируется в профиле продукта. `line_end` сценария MUST указывать на последнюю строку его исходного блока перед следующим `#### Scenario:`, `### Requirement:`, заголовком H2 или концом файла, включая находящиеся внутри блока пустые, комментарные и иные Markdown-строки.

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

#### Scenario: Фактическая граница сценария

- **GIVEN** между GWT-строками сценария и следующим структурным заголовком присутствуют пустая строка и Markdown-комментарий
- **WHEN** выполняется парсинг
- **THEN** `line_end` сценария равен номеру последней строки его исходного блока, а не вычисленному количеству распознанных GWT-строк

### Requirement: Парсинг usecase-слоя

Система SHALL индексировать usecase-слой продукта в `spec_usecases` + `spec_usecase_steps`, поддерживая четыре фактических формата: `scenarios/*.md` (актёры, пред/постусловия, архитектура, потоки шагов с H2-секциями и нумерованными шагами), `usecases/**/*.md` (файлы вида `REQ-001-SC-001 — <имя>.md` с H3/H4-секциями, inline-метаданными `**Ключ**: Значение` и шагами `**Шаг N**. текст`), `business-processes/**/*.md` (выгрузки Confluence с pageId и табличными шагами) и `specs/usecases/**/*.md` (бизнес-процессы внутри `specs/` с inline-метаданными, табличными шагами, ветвлениями WHEN/ELSE и секцией «Спеки-компоненты (запчасти)» для трассируемости шаг→capability). Формат распознаётся по позиции директории в openspec-корне и фиксируется в `spec_usecases.source_dir` и профиле продукта (`usecase_layout`); Confluence pageId сохраняется в `spec_usecases.page_id` — извлекается как из строки `Confluence pageId NNN`, так и из URL `pageId=NNN`. Нумерованные шаги (`1. текст`), шаги `**Шаг N**. текст` и табличные шаги (`| Шаг | ... |`) парсятся в `spec_usecase_steps` с `flow_kind` main|alternative для всех форматов. Inline-метаданные `**Ключ**: Значение` SHALL извлекаться в соответствующие поля (`**Описание**` → description, `**Пользователи и системы**` / `**Роли и системы**` → actors, `**Бизнес-ценность**` → business_value, `**Точка старта**` → architecture). Секция «Ветвления» SHALL парситься в alternative-шаги; секция «Спеки-компоненты (запчасти)» SHALL извлекаться в `spec_code_mentions` с `mention_kind = "spec_ref"` для построения `usecase_involves` relations. Продукты без usecase-слоя не создают записей — запросы к usecase-инструментам честно возвращают пустой результат.

#### Scenario: Сценарии с актёрами и потоками

- **GIVEN** `openspec/scenarios/scenario-sms-disable.md` с секциями «Пользователи», «Основной поток» и «Альтернативный поток»
- **WHEN** выполняется парсинг
- **THEN** usecase создан с актёрами, шаги основного и альтернативного потоков — в `spec_usecase_steps` с соответствующим `flow_kind`

#### Scenario: Бизнес-процессы с pageId

- **GIVEN** `openspec/business-processes/Депозитарий._Счета_и_журналы/1._Бизнес-процесс_Прием_клиента_на_обслуживание.md` с пометкой pageId 403104219
- **WHEN** выполняется парсинг
- **THEN** usecase создан с `source_dir = business-processes` и `page_id = 403104219`

#### Scenario: Продукт без usecase-слоя

- **GIVEN** продукт с `openspec/specs/`, но без `scenarios/`, `usecases/`, `business-processes/`, `specs/usecases/`
- **WHEN** выполняется индексация
- **THEN** записи `spec_usecases` не создаются, профиль фиксирует `usecase_layout = none`

#### Scenario: Usecases с H3/H4-секциями и inline-метаданными

- **GIVEN** `openspec/usecases/fot/REQ-001-SC-001 — Расчет ставки налога....md` с H2 `## Требование REQ-001`, H3 `### Сценарий REQ-001/SC-001`, H4 `#### Предусловия`, `#### Шаги`, `#### Постусловия` и inline-метаданными `**Описание**:`, `**Пользователи и системы**:`, `**Бизнес-ценность**:`
- **WHEN** выполняется парсинг
- **THEN** usecase создан с заполненными description, actors, business_value, preconditions, postconditions
- **AND** шаги `**Шаг 1**. Загрузка документа...`, `**Шаг 2**. Формирование...` — в `spec_usecase_steps` с `flow_kind = main` в порядке
- **AND** pageId извлечён из URL `pageId=467512912` в поле `page_id`

#### Scenario: Бизнес-процессы внутри specs/usecases с таблицами и ветвлениями

- **GIVEN** `openspec/specs/usecases/scheta-i-zhurnaly/BP-01 Приём клиента на обслуживание.md` с inline-метаданными `**Роли и системы**:`, `**Бизнес-ценность**:`, таблицей шагов `| Шаг | Пользователь/система | Действие | Ожидаемый результат |`, секцией `## Ветвления` с блоками WHEN/ELSE и секцией `## Спеки-компоненты (запчасти)` со ссылками `specs/depo-contract/spec.md — шаг 3`
- **WHEN** выполняется парсинг
- **THEN** usecase создан с `source_dir = usecases`, заполненными actors, business_value
- **AND** строки таблицы шагов — в `spec_usecase_steps` с `flow_kind = main`
- **AND** блоки WHEN/ELSE из секции «Ветвления» — в `spec_usecase_steps` с `flow_kind = alternative`
- **AND** ссылки на capabilities из секции «Спеки-компоненты» — в `spec_code_mentions` с `mention_kind = "spec_ref"` для построения `usecase_involves` relations
- **AND** pageId извлечён из строки `Confluence pageId 403104219`

#### Scenario: ClassifyPath распознаёт specs/usecases

- **GIVEN** файл `openspec/specs/usecases/kolichestvennyy-uchet/BP-01 ....md` внутри `specs/`
- **WHEN** выполняется ClassifyPath
- **THEN** файл классифицирован как `KindUsecase` с `SourceDir = "usecases"` (не `KindOther`)

#### Scenario: PageId из URL при отсутствии слова Confluence

- **GIVEN** usecase-файл со строкой `**Источник**: ... https://conf.diasoft.ru/pages/viewpage.action?pageId=467512912`
- **WHEN** выполняется парсинг
- **THEN** `page_id = 467512912` извлечён из URL-параметра `pageId=`

### Requirement: Парсинг changes-слоя

Система SHALL индексировать `changes/` (активные и `archive/`) в `spec_changes`: имя change, статус active|archived, вид артефакта (proposal/design/tasks/delta-specs). Delta-спеки (`## ADDED/MODIFIED/REMOVED Requirements`) парсятся в `spec_change_delta` с признаком секции, именем требования, телом и фактическими границами строк. Для change с маркером `skip_specs: true` (структурной связи со спецификациями нет) система SHALL извлекать затронутые capability из текста `proposal.md` по упоминаниям путей `openspec/specs/<…>`. Для delta-требования со сценариями `line_end` MUST охватывать как минимум весь сохранённый body до первого `#### Scenario:` и не может быть меньше номера последней строки body.

#### Scenario: Архивированный change с delta-требованиями

- **GIVEN** `changes/archive/2026-08-27-card-limits/` с `proposal.md`, `tasks.md` и `specs/card-limits/spec.md` (секция `## ADDED Requirements`)
- **WHEN** выполняется парсинг
- **THEN** создан `spec_changes` со статусом archived; delta-требования сохранены в `spec_change_delta` с section ADDED; change связан с capability `card-limits`

#### Scenario: Change со skip_specs

- **GIVEN** активный change с `skip_specs: true` в `.openspec.yaml` и `proposal.md`, упоминающим `openspec/specs/CORE/data`
- **WHEN** выполняется парсинг
- **THEN** change создан и связан с capability `CORE/data` по извлечённому упоминанию, delta-записи не создаются

#### Scenario: Граница delta-требования перед сценарием

- **GIVEN** delta-требование содержит несколько строк body, после которых начинается `#### Scenario:`
- **WHEN** выполняется парсинг delta-спеки
- **THEN** `line_start` указывает на заголовок требования, а `line_end` — на последнюю строку сохранённого body перед сценарием

### Requirement: Профиль продукта

Система SHALL строить профиль продукта (`spec_configs`, один на openspec-корень) детекцией фактической структуры при индексации, а не из ручного конфигура: `usecase_layout` (scenarios|usecases|business-processes|none), `id_style` (dir|full_path — по наличию вложенных директорий в `specs/`), `has_changes`, `has_adr`, `has_audit`, `normative_lang` (en|ru — по нормативным ключевым словам в требованиях), `cross_ref_style` (explicit|notes|inline|mixed — по фактическим маркерам), а также сохранять текст секции `context:` из `config.yaml`.

#### Scenario: Профиль таксономического продукта

- **GIVEN** продукт с вложенными `specs/FORMS/0409718/settings/`, архивом changes и `business-processes/`
- **WHEN** выполняется индексация
- **THEN** профиль зафиксирован: `id_style = full_path`, `has_changes = true`, `usecase_layout = business-processes`

### Requirement: Извлечение упоминаний кода

Система SHALL извлекать упоминания код-сущностей из текстов спек (Related code с подсекциями `### Серверные процедуры` / `### API-процедуры` / `### События` / `### Клиент (Delphi)` / `### Отчёты` и т.п., inline-тексты требований, GIVEN/WHEN/THEN-строки сценариев, delta-тексты changes) в staging-таблицу `spec_code_mentions` с привязкой к сущности-источнику (capability|requirement|scenario|usecase), номером строки и эвристическим `mention_kind`: `API_*` → api_contract; `FCD_*` → фасад (sql_procedure); `t[A-Z]*` / `p[A-Z]*` → sql_table, за исключением префикса `pAPI_` — контрактные таблицы API → api_table; пути `*.smf` → smf_instrument; `*.dfm` → dfm_form; `*.pas` → pas_method; `*.sql` → sql_procedure; `*.tpr` / `*.rpt` → report (report_forms); идентификаторы `On(After|Before)[A-Z]*` и пути `.../Event/<имя>.xml` → event (api_contract события); прочие идентификаторы → sql_procedure с fallback unknown. Inline-режим (тексты требований и сценариев) SHALL дополнительно распознавать report-пути и event-идентификаторы вне бэктиков как однозначные маркеры.

#### Scenario: Подсекция Related code

- **GIVEN** capability со секцией `## Related code`, подсекцией `### API-процедуры` и буллетом `` `API_Depo/Server/DepoAccount/API_DepoAccount_MassInsert.sql` — массовое добавление ...``
- **WHEN** выполняется извлечение
- **THEN** в `spec_code_mentions` создана запись: имя `API_DepoAccount_MassInsert`, kind api_contract, источник — capability, строка буллета

#### Scenario: Inline-упоминание в сценарии

- **GIVEN** сценарий со строкой WHEN, содержащей `` процедуру `API_DpAcc_MassUpdateAccount` ``
- **WHEN** выполняется извлечение
- **THEN** создана запись mention с источником — scenario и номером строки

#### Scenario: Отчётная форма в Related code

- **GIVEN** capability со секцией `## Related code`, подсекцией `### Отчёты` и буллетом `` `ReservePortfolio/Reporting/rbr.tpr` ``
- **WHEN** выполняется извлечение
- **THEN** в `spec_code_mentions` создана запись: имя `rbr`, kind report, источник — capability

#### Scenario: Отчётный путь в сценарии вне бэктиков

- **GIVEN** сценарий со строкой THEN, содержащей «формируется отчет на основании отчетной формы form651.tpr»
- **WHEN** выполняется извлечение
- **THEN** создана запись mention: имя `form651`, kind report, источник — scenario

#### Scenario: Событийный идентификатор в THEN

- **GIVEN** сценарий со строкой THEN, содержащей «публикуется событие OnAfterPrtf_UpdateNorm»
- **WHEN** выполняется извлечение
- **THEN** создана запись mention: имя `OnAfterPrtf_UpdateNorm`, kind event, источник — scenario

#### Scenario: Путь к event-контракту в Related code

- **GIVEN** Related code с буллетом `` `API_Reserv/DSArchitectData/BObject/RsvPortfolio/Event/OnAfterPrtf_UpdateNorm.xml` — событие изменения нормы ``
- **WHEN** выполняется извлечение
- **THEN** создана запись mention: имя `OnAfterPrtf_UpdateNorm`, kind event (путь сведён к последнему сегменту без расширения, сегмент `Event` определяет kind)

#### Scenario: GIVEN-строка сканируется

- **GIVEN** сценарий со строкой GIVEN, содержащей `` расчёт по схеме `Rpt_Sheme_KRS_754P.sql` ``, и без повторения этого имени в WHEN/THEN
- **WHEN** выполняется извлечение
- **THEN** создана запись mention: имя `Rpt_Sheme_KRS_754P`, kind procedure, источник — scenario

#### Scenario: Контрактная таблица pAPI классифицируется как api_table

- **GIVEN** сценарий со строкой WHEN, содержащей `` передает в `pAPI_Accrual_ObjDate` дату расчета ``
- **WHEN** выполняется извлечение
- **THEN** создана запись mention: имя `pAPI_Accrual_ObjDate`, kind api_table (контрактная таблица API, не табличная эвристика `t`/`p`)

### Requirement: Пост-обработка — резолв упоминаний в references_code

Система SHALL резолвить записи `spec_code_mentions` в глобальной пост-обработке индексации (по образцу существующих callback/retcode-постпроцессоров): упоминание сопоставляется с сущностями кода по имени с учётом `mention_kind` — procedure → sql_procedures, table → sql_tables, form → dfm_forms, smf → smf_instruments, method → pas_methods, api → api_contracts, report → report_forms, event → api_contracts, api_table → api_contracts через `api_contract_tables` (таблица сопоставляется всем DISTINCT контрактам-владельцам, и service, и callback_event); разрешённые упоминания пишутся в `relations` с `relation_type = references_code`. Упоминания kind method при промахе в `pas_methods` SHALL дополнительно сопоставляться с `dfm_forms` по имени юнита (упоминание формы через `.pas`-путь); при хите создается ребро к `dfm_form`. Упоминания kind unknown SHALL проходить second-chance сопоставление с `sql_procedures`; при хите создается ребро к `sql_procedure` (покрывает процы в нижнем регистре: `r8938_prc`, `rpt_f1417u_proc`). Резолв двухфазен и НЕ зависит от порядка индексации файлов: спека может ссылаться на код, проиндексированный позже. Нерезолвнутые упоминания остаются в `spec_code_mentions` как измеримые пробелы покрытия и доступны запросам.

#### Scenario: Упоминание резолвится в процедуру

- **GIVEN** спека упоминает `API_DpAcc_MassUpdateAccount`; соответствующая SQL-процедура проиндексирована (до или после спеки)
- **WHEN** выполняется пост-обработка
- **THEN** в `relations` создано ребро `references_code` от сущности спеки к `sql_procedures` с указанием строки

#### Scenario: Отчет резолвится в report_form

- **GIVEN** спека упоминает `form651` с kind report; отчетная форма `form651.tpr` проиндексирована
- **WHEN** выполняется пост-обработка
- **THEN** в `relations` создано ребро `references_code` от сущности спеки к `report_forms`

#### Scenario: Событие резолвится в api_contract

- **GIVEN** спека упоминает `OnAfterPerson_Update` с kind event; событийный контракт проиндексирован
- **WHEN** выполняется пост-обработка
- **THEN** в `relations` создано ребро `references_code` от сущности спеки к `api_contracts`

#### Scenario: Контрактная таблица резолвится в контракты-владельцы

- **GIVEN** спека упоминает `pAPI_Accrual_ObjDate` с kind api_table; таблица декларирована в контрактах `CON_Accrual_Process` и `CON_AfterAccrual_Process`
- **WHEN** выполняется пост-обработка
- **THEN** в `relations` созданы рёбра `references_code` от сущности спеки к обоим контрактам в `api_contracts` (по одному на DISTINCT-владельца)

#### Scenario: .pas-упоминание фолбэком в форму

- **GIVEN** спека содержит буллет `` `ReservePortfolio/CLIENT/ReservePortfolio/RPPortfolio_f.pas` ``; метод с именем `RPPortfolio_f` в `pas_methods` отсутствует, форма `RPPortfolio_f` есть в `dfm_forms`
- **WHEN** выполняется пост-обработка
- **THEN** в `relations` создано ребро `references_code` к `dfm_forms`

#### Scenario: Unknown-упоминание second-chance в процедуру

- **GIVEN** спека упоминает `` `r8938_prc` ``, классифицированный как unknown (нижний регистр); SQL-процедура `r8938_prc` проиндексирована
- **WHEN** выполняется пост-обработка
- **THEN** в `relations` создано ребро `references_code` к `sql_procedures`

#### Scenario: Unknown-упоминание без цели остаётся пробелом

- **GIVEN** спека упоминает `` `f123_proc` `` (unknown), процедуры с таким именем в дереве нет
- **WHEN** выполняется пост-обработка
- **THEN** ребро не создаётся, запись остаётся в `spec_code_mentions` и доступна запросу покрытия

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
