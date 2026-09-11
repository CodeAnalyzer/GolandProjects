## Context

MCP-инструмент `codebase_query_spec_history` объявляет параметр `product` в InputSchema (`internal/mcp/registry.go`, строка 1105), но handler (строки 1109–1119) не читает его из args и не передаёт в `ExecuteSpecHistory`. Функция `ExecuteSpecHistory` (`internal/specsvc/specsvc.go`, строка 828) уже принимает product как variadic-аргумент `changeName[1]` и SQL-запросы уже фильтруют по продукту через `AND ($2 = '' OR dp.product_name = $2)` (строки 864, 900, 934). CLI-команда `codebase query spec history` (`cmd/query_spec.go`, строка 122) уже передаёт `specHistoryProduct`. Таким образом, инфраструктура полностью готова — единственный gap в MCP-handler.

## Goals / Non-Goals

**Goals:**

- MCP-handler `codebase_query_spec_history` читает опциональный `product` из args и передаёт его в `ExecuteSpecHistory`.
- Спека `query/spec-queries` документирует параметр `product` для `spec_history`.

**Non-Goals:**

- Аудит других spec-инструментов на расхождения MCP↔CLI — не является целью данного change.
- Изменение SQL-запросов — они уже поддерживают product-фильтр.
- Изменение CLI — `--product` уже работает.
- Сделать `product` обязательным — он остаётся опциональным.

## Decisions

**Передача product через variadic-аргумент, а не через отдельный параметр функции.**

`ExecuteSpecHistory` уже имеет сигнатуру `func ExecuteSpecHistory(ctx, db, capabilityName string, changeName ...string)`, где `changeName[1]` интерпретируется как product. Альтернатива — добавить явный параметр `product string` в сигнатуру. Это потребует изменения всех вызовов (MCP, CLI, тесты) и сломает совместимость. Текущий variadic-подход работает и уже используется CLI. Решение: оставить сигнатуру как есть, MCP-handler просто передаёт `product` третьим variadic-аргументом.

**MCP-handler читает product через `optionalString`.**

Параметр `product` в MCP schema объявлен как опциональный (не в `required`). Handler использует `optionalString(args, "product")` — тот же паттерн, что и `spec_coverage` handler для `name` и `kind`. Если `product` не передан, `optionalString` возвращает `""`, что в SQL трактуется как «без фильтра» (`$2 = '' OR ...`).

## Risks / Trade-offs

- **[Risk] Variadic-аргумент неочевиден для читателя кода** → Mitigation: комментарий в handler, указывающий что третий аргумент — product. Существующий комментарий в `ExecuteSpecHistory` уже документирует это поведение.
- **[Risk] Передача product в wrong position** → Mitigation: CLI уже использует тот же порядок (`specHistoryName, specHistoryChange, specHistoryProduct`), тесты покрывают этот путь.
