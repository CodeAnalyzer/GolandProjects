## MODIFIED Requirements

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
