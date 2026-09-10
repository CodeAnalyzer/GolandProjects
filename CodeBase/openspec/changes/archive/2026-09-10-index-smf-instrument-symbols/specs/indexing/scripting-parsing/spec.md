## MODIFIED Requirements

### Requirement: Извлечение SMF-инструментов

Система SHALL извлекать из SMF-файлов модели Ф.О. (инструменты) с их именем, типом сценария (`instrument_model`, `mass_operation`, и др.), brief (краткое название) и встроенным JavaScript, и SHALL создавать для каждого инструмента запись в unified-индексе `symbols` с `symbol_type = "smf_instrument"`, `entity_type = "smf"`, `entity_id` = ID инструмента в `smf_instruments`, `line_number` и `signature` = `scenario_type`, чтобы инструмент был доступен через `query symbol`.

#### Scenario: SMF-инструмент

- **GIVEN** SMF-файл с инструментом `CreditMassOperation` типа `mass_operation`
- **WHEN** выполняется индексация файла
- **THEN** инструмент `CreditMassOperation` сохранён в `smf_instruments` с типом `mass_operation`
- **AND** доступен через `query smf-instrument --name CreditMassOperation`
- **AND** доступен через `query symbol --name CreditMassOperation` с `symbol_type = "smf_instrument"` и `entity_type = "smf"`

#### Scenario: Поиск SMF по типу

- **GIVEN** проиндексированные SMF-файлы с разными типами сценариев
- **WHEN** выполняется `query smf-type --type mass_operation`
- **THEN** возвращены все инструменты с типом `mass_operation`

#### Scenario: Поиск SMF-инструмента через unified symbols index

- **GIVEN** проиндексированный SMF-файл с инструментом `CreditMassOperation`
- **WHEN** выполняется `query symbol --name CreditMassOperation --type smf_instrument`
- **THEN** возвращена сущность с `symbol_type = "smf_instrument"`, `entity_type = "smf"`, указанием файла и строки
