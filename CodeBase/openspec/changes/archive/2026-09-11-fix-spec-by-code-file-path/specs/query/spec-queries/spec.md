## MODIFIED Requirements

### Requirement: Спеки по коду (spec_by_code)

Система SHALL по имени код-сущности (SQL-процедура, API-контракт, таблица, PAS-метод, DFM-форма, JS-функция, SMF-инструмент, отчёт) находить спеки, ссылающиеся на неё: обратный проход по `relations` `references_code` до сущности-источника (capability/requirement/scenario/usecase) с текстом-фрагментом строки упоминания, продуктом, путём к spec-файлу упоминания и границами строк. Результат группируется по capability; внутри — требования и сценарии с указанием, из какой части спеки пришла ссылка (Related code / текст требования / текст сценария). Каждая запись результата SHALL содержать поле `file` с относительным путём spec-файла, в котором найдено упоминание код-сущности.

#### Scenario: Требования по процедуре

- **GIVEN** спека fa-custody с требованием, чей сценарий WHEN упоминает `API_DepoAccount_MassInsert`, и проиндексированная процедура
- **WHEN** вызывается `codebase_query_spec_by_code` с `name = "API_DepoAccount_MassInsert"`
- **THEN** возвращены capability и требование с текстом строки упоминания, границами строк и путём к spec-файлу в поле `file`

#### Scenario: Код-сущность не упоминается в спеках

- **GIVEN** процедура `SomeInternalProc`, не встречающаяся ни в одном тексте спек
- **WHEN** вызывается spec_by_code
- **THEN** возвращён пустой результат (`count = 0`), не ошибка

#### Scenario: Путь файла для упоминания из usecase

- **GIVEN** usecase-файл `scenarios/scenario-sms-disable.md` с упоминанием `API_SmsDisable` и связанная с ним capability `card-service`
- **WHEN** вызывается `codebase_query_spec_by_code` с `name = "API_SmsDisable"`
- **THEN** поле `file` содержит путь к usecase-файлу, в котором найдено упоминание (не путь к spec.md capability)
