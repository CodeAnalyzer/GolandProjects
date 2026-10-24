# Tasks: dsinfo-parser

## 1. Схема БД

- [x] 1.1 Добавить в `internal/store/db_schema.go` таблицы `dsinfo`, `dsinfo_products`, `dsinfo_modules`, `dsinfo_procedures`, `dsinfo_state` и индексы из design D2 (идемпотентно, `CREATE TABLE IF NOT EXISTS`); проверить: `schema_integration_test` расширен новыми таблицами/индексами и проходит
- [x] 1.2 Добавить `dsinfo` в TRUNCATE-список `internal/store/db_reset.go` (каскад от `dsinfo`), обновить `db_reset_integration_test` и убедиться, что reset очищает профили каскадно

## 2. Парсер internal/dsinfo

- [x] 2.1 Создать пакет `internal/dsinfo`: типы `Profile`, `Product`, `Module`, `Procedure` (поля по design D2), константу `ParserVersion = 1`; проверить сборкой пакета
- [x] 2.2 Реализовать сниффинг+разбор в один проход (state machine по секциям, `bufio.Scanner` + WIN-1251 через `internal/encoding`, заголовок секции = строка без `;` < 60 символов, признак валидности = маркеры «Продукты Digital Q» И «Модули» с заголовком `FileName;`); проверить: разбор `Modifications/DiasoftSystemInfo.csv` даёт 62 продукта, ~200 модулей, 62 649 процедур, метаданные `RUMOS072VT/testdb2/sql2019_testv`
- [x] 2.3 Реализовать нормализацию манифеста (правило «до первого числа», `branch_key` = product + 3 числовых токена, `hotfix_name` по `-HF\d+`); проверить юнит-тестами на строках `FA-Contracts-11-01-7\...-HF008`, `FA-Contracts-Ext-9-01-553-PKG-IFRS9_TOYOTA\...`, `FA-Contracts-Ext-7-02-2-7-02-2`, `FA-UserProc-7-02-06-SP6\`, `FA-FinancialAsset-Ext-8-03-33-SP\...`
- [x] 2.4 Юнит-тесты парсера на фрагментах реального CSV: манифест с HF/PKG, модуль с пакетом в HotfixName при пустом PacketName, процедуры с state 1/16/32 и пустым пакетом, вторая секция «Модули» («Установленные пакеты») не попадает в модули, отсутствие секций метаданных не ломает разбор, посторонний CSV отклоняется с ошибкой

## 3. Хранилище internal/store/ds_info_store.go

- [x] 3.1 Реализовать `FindProfileByHash`, `InsertProfile` (профиль + батчи дочерних строк по ~500 в одной транзакции, `ON CONFLICT`-устойчивость к гонке: конфликт → перечитать существующий id), `ListProfiles`, `GetProfile`, `DeleteProfile`, `GetActiveProfile`/`SetActiveProfile` (upsert в `dsinfo_state`); проверить сборкой
- [x] 3.2 Интеграционные тесты store: parse-вставка профиля с дочерними строками и счётчиками; повторная вставка того же hash → reuse без дубля; удаление → каскадная очистка трёх дочерних таблиц; `ON DELETE SET NULL` сбрасывает `dsinfo_state.active_dsinfo_id` при удалении активного профиля; фильтры процедур по `module/packet/object_name ILIKE` с limit

## 4. Сервис internal/dsinfosvc

- [x] 4.1 Реализовать `ExecuteParse` (hash → reuse | insert, результат: id, reused, счётчики, таблица продуктов), `ExecuteList` (убывание `parsed_at`, пометка активного), `ExecuteShow` (метаданные + манифест + модули; процедуры только с фильтром `Module/Packet/Search` + Limit), `ExecuteDelete` (сброс активного с фактом в ответе), `ExecuteUse` (установка/чтение активного, ошибка на неизвестный id); проверить сборкой
- [x] 4.2 Интеграционные тесты сервиса: повторный parse того же файла → тот же id + `reused`; `show` без фильтров не возвращает процедуры; `show --packet` возвращает ограниченный набор; `use` + `delete` активного → активный NULL, ответ сообщает о сбросе; `use 999` → ошибка без изменения активного

## 5. CLI cmd/dsinfo.go

- [x] 5.1 Создать группу команд `dsinfo parse/list/show/delete/use` с флагами `--json` (persistent), `--name`, `--module/--packet/--search/--limit`, `--limit` у list; зарегистрировать в `root.go`; человекочитаемый вывод: parse — id + счётчики + таблица продуктов, list — строка на профиль с `*` у активного, show — секции, use — активный профиль; проверить `go build ./...`
- [x] 5.2 Тест CLI (`cmd`): флаги parse/show регистрируются, `--json` обёртки соответствуют контракту (`success/format_version/command/count/items`); smoke вручную: parse реального CSV, list, show с `--packet`, use, delete

## 6. MCP-инструменты

- [x] 6.1 Добавить в `internal/mcp` инструменты `dsinfo_parse/list/show/delete/use` (параметры и результаты как в CLI, limit по умолчанию 100/макс 1000, пагинация для крупных show); проверить unit-тестами registry: инструменты присутствуют во всех профилях сервера
- [x] 6.2 Smoke MCP: вызов `dsinfo_parse` на реальном файле, `dsinfo_show` с фильтром через тестовый JSON-RPC прогон или ручную сессию

## 7. Документация и сборка

- [x] 7.1 Обновить `README.md`: раздел dsinfo (формат входного файла, сниффинг, команды CLI/MCP, схема хранения, активный профиль, ограничения — без интеграции в анализ); обновить таблицы MCP-инструментов; оговорить, что «профиль dsinfo» ≠ флаг `--profile` MCP (подмножество инструментов)
- [x] 7.2 Финальная верификация: `go test ./...` (unit+integration по профилям rti/trc/store/cmd/mcp) зелёные; пересборка `CodeBase.exe`; smoke на `Modifications/DiasoftSystemInfo.csv`: parse → list → show --packet → use → show активного → delete активного → list; `openspec validate dsinfo-parser --strict`
