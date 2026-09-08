## MODIFIED Requirements

### Requirement: Фильтрация по шаблонам

Система SHALL поддерживать настройку `include_patterns` и `exclude_patterns` в секции `[indexer]` конфигурационного файла для гибкого управления составом индексируемых файлов. Шаблоны по умолчанию: `*.sql`, `*.h`, `*.pas`, `*.inc`, `*.js`, `*.smf`, `*.dfm`, `*.tpr`, `*.rpt`, `*.xml`, `*.md`, `*.yaml`. Для markdown-файлов, не относящихся к openspec-корням, действует тот же механизм include/exclude: владелец конфигурации может исключить их явным паттерном (например `docs/*`), не отключая индексацию спек.

#### Scenario: Include patterns по умолчанию
- **GIVEN** конфигурация без явного указания `include_patterns`
- **WHEN** выполняется инициализация индекса
- **THEN** используются шаблоны по умолчанию: `*.sql`, `*.h`, `*.pas`, `*.inc`, `*.js`, `*.smf`, `*.dfm`, `*.tpr`, `*.rpt`, `*.xml`, `*.md`, `*.yaml`

#### Scenario: Опциональные .t01
- **GIVEN** конфигурация с `*.t01` в `include_patterns`
- **WHEN** выполняется индексация
- **THEN** препроцессированные `.t01` файлы индексируются как SQL-like layer

#### Scenario: Markdown вне openspec исключается паттерном
- **GIVEN** конфигурация с `docs/*` в `exclude_patterns` и деревом, содержащим openspec-спеки и посторонние `docs/*.md`
- **WHEN** выполняется индексация
- **THEN** markdown-спеки openspec-корней индексируются, файлы `docs/*.md` пропущены

### Requirement: Порядок постобработки

Система SHALL выполнять постобработку после завершения индексации всех файлов. Сначала удаляются все `subscribes_to_event` relations (`DeleteSubscribesToEventRelations`), затем запускаются параллельные постпроцессоры: PAS-DFM links, SQL procedure call relations, callback event relations, retcode constants, fragment relations и spec-постпроцессор (резолв `spec_code_mentions` → `references_code`, построение `depends_on_capability`, иерархия capability, `change_modifies`; поведение — см. `indexing/openspec-parsing`). Если индексируемых спек нет, spec-постпроцессор завершается без изменений. Пересчёт LSA-модели полнотекстового слоя (см. `query/spec-search`) выполняется после завершения всех постпроцессоров, по порогу изменений.

#### Scenario: Постобработка после update
- **GIVEN** завершённая индексация изменённых файлов
- **WHEN** выполняется `runPostProcessingParallel`
- **THEN** сначала удалены все `subscribes_to_event` relations
- **AND** затем постпроцессоры, включая spec-постпроцессор, запущены параллельно

#### Scenario: Спек в дереве отсутствует
- **GIVEN** индексация проекта без openspec-корней
- **WHEN** выполняется постобработка
- **THEN** spec-постпроцессор не создаёт записей и не мешает прочим постпроцессорам
