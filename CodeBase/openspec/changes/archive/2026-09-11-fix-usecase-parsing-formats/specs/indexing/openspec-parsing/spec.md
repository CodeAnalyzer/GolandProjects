## MODIFIED Requirements

### Requirement: Парсинг usecase-слоя

Система SHALL индексировать usecase-слой продукта в `spec_usecases` + `spec_usecase_steps`, поддерживая четыре фактических формата: `scenarios/*.md` (актёры, пред/постусловия, архитектура, потоки шагов с H2-секциями и нумерованными шагами), `usecases/**/*.md` (файлы вида `REQ-001-SC-001 — <имя>.md` с H3/H4-секциями, inline-метаданными `**Ключ**: Значение` и шагами `**Шаг N**. текст`), `business-processes/**/*.md` (выгрузки Confluence с pageId и табличными шагами) и `specs/usecases/**/*.md` (бизнес-процессы внутри `specs/` с inline-метаданными, табличными шагами, ветвлениями WHEN/ELSE и секцией «Спеки-компоненты (запчасти)» для трассируемости шаг→capability). Формат распознаётся по позиции директории в openspec-корне и фиксируется в `spec_usecases.source_dir` и профиле продукта (`usecase_layout`); Confluence pageId сохраняется в `spec_usecases.page_id` — извлекается как из строки `Confluence pageId NNN`, так и из URL `pageId=NNN`. Нумерованные шаги (`1. текст`), шаги `**Шаг N**. текст` и табличные шаги (`| Шаг | ... |`) парсятся в `spec_usecase_steps` с `flow_kind` main|alternative для всех форматов. Inline-метаданные `**Ключ**: Значение` SHALL извлекаться в соответствующие поля (`**Описание**` → description, `**Пользователи и системы**` / `**Роли и системы**` → actors, `**Бизнес-ценность**` → business_value, `**Точка старта**` → architecture, `**Источник**` → source-строка с извлечением pageId). Секция `## Ветвления` с блоками WHEN/ELSE SHALL парситься в `spec_usecase_steps` с `flow_kind = alternative`. Секция `## Спеки-компоненты (запчасти)` SHALL извлекаться в `spec_code_mentions` с `mention_kind = "spec_ref"` для построения `usecase_involves` relations в постобработке. Продукты без usecase-слоя не создают записей — запросы к usecase-инструментам честно возвращают пустой результат.

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
