## MODIFIED Requirements

### Requirement: Детали SQL-процедуры

Система SHALL предоставлять команду `query procedure` для получения деталей хранимой процедуры по точному имени: путь к файлу, диапазон строк, список параметров с типами и направлением. Результат SHALL быть представлен как массив из нуля или одного элемента. Если процедура с указанным именем не найдена, система SHALL вернуть пустой результат (`count: 0`, пустой массив) без ошибки.

#### Scenario: Детали существующей процедуры

- **GIVEN** проиндексированный проект с процедурой `API_RuleDoc_MassCreateDocument`
- **WHEN** выполняется `query procedure --name API_RuleDoc_MassCreateDocument`
- **THEN** возвращены имя, файл, `line_start`/`line_end` и параметры процедуры

#### Scenario: Процедура не найдена

- **GIVEN** проиндексированный проект без процедуры `API_NonExistent`
- **WHEN** выполняется `query procedure --name API_NonExistent --json`
- **THEN** возвращён успешный ответ с `"count": 0` и `"items": []`
- **AND** запрос не завершается ошибкой `sql: no rows in result set`
