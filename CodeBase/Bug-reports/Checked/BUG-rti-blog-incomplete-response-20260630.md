# Bug Report: `codebase_rti_blog` возвращает неполные данные — пустые `blog_blocks`, `checkpoints`, `params`

## Summary

MCP-инструмент `codebase_rti_blog` при вызове с `session_id` возвращает только базовые поля вызова (`enter_line`, `elapsed_ms`), не заполняя `blog_blocks`, `checkpoints`, `blog_tables`. При вызове с `file_path` дополнительно некорректно заполняется `ret_val_context` и `ret_val` у процедуры (из M_LOG-записей вместо реального Return).

Пример вызова с `session_id`:

```json
{
  "calls": [
    {
      "enter_line": 15,
      "elapsed_ms": 90
    }
  ],
  "count": 1,
  "procedure": "UndoConsSale_PurchPortfolio"
}
```

Ожидаемый ответ должен содержать `blog_blocks` и `checkpoints`.

---

## Environment

- Tool: `codebase_rti_blog` (MCP)
- RTI-файл: `D:\GITHUB\GolandProjects\CodeBase\Logs\TS-DIASOFT-EXT5_diasoft_db-mssql-test1_test1_fa_1_SRV11.rti`
- Процедура: `UndoConsSale_PurchPortfolio`

---

## Reproduction Steps

### Баг 1 — `session_id` путь (пустые вложенные данные)

1. Спарсить RTI-файл через `codebase_rti_parse` → получить `session_id`.
2. Вызвать `codebase_rti_blog` с `session_id=<X>` и `procedure="UndoConsSale_PurchPortfolio"`.
3. Убедиться, что в ответе `blog_blocks`, `checkpoints`, `blog_tables` отсутствуют.

### Баг 2 — `file_path` путь (неверный `ret_val_context`)

1. Вызвать `codebase_rti_blog` с `file_path="...TS-DIASOFT-EXT5_...rti"` и `procedure="UndoConsSale_PurchPortfolio"`.
2. Убедиться, что `ret_val_context` у процедуры равен `"Откатили постановку обеспечения на внебаланс"` вместо правильного пустого значения.
3. RTI-файл содержит несколько `Trace.Server.BusinessLog` записей без `Enter`/`Exit`-префикса (M_LOG-записи) — например, строки 79, 128 файла:

```
10.06.2026 15:21:05.803	INFO	Trace.Server.BusinessLog			58	692575		0	164
@@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 39

RetVal = 0#Вернули флаги договора
BLogParam:@ContractFlag : DSINT_KEY                      = 1073741824
```

---

## Expected Result

**Баг 1:** `blog_blocks` должны содержать блоки бизнес-лога, сохранённые при `codebase_rti_parse`.

**Баг 2:** M_LOG-запись (строка `@@TranCount...` без `Enter`/`Exit`) должна игнорироваться при установке `ret_val` / `ret_val_context` у родительской процедуры.

---

## Actual Result

**Баг 1:** `blog_blocks`, `checkpoints`, `blog_tables` пустые (json omitempty → поля отсутствуют в ответе).

**Баг 2:** `RetVal = 28545#Откатили постановку обеспечения на внебаланс` (из M_LOG-записи строка 130) устанавливается как `ret_val_context` у `UndoConsSale_PurchPortfolio`. Реальный `Return 28545` (строка 140) уже не изменяет контекст, т.к. `RetVal` ненулевой.

---

## Root Cause Analysis

### Баг 1 — `LoadCalls` не загружает вложенные данные из БД

Файл: `internal/rti/store.go`, функция `LoadCalls` (строка ~515).

`LoadCalls` делает SELECT только из таблицы `rti_calls`. Таблицы `rti_blog_blocks`, `rti_blog_tables`, `rti_params`, `rti_checkpoints` не читаются. Соответственно, поля `RTICall.BLogBlocks`, `BLogTables`, `Params`, `Checkpoints` остаются `nil`.

В `loadRTIFromArgs` (registry.go, строка ~947) при `session_id > 0` вызывается только `rti.LoadCalls(db, sessionID)` без дополнительных загрузчиков.

### Баг 2 — M_LOG записи без `Enter`/`Exit`-префикса неверно обрабатываются парсером

Файл: `internal/rti/parser.go`, функция `parseContent`.

RTI-формат предусматривает два вида BusinessLog-записей:

| Вид | Строка после заголовка | Смысл |
|-----|------------------------|-------|
| `M_BUSINESSLOG_BLOCK_BEGIN(name)` | `Enter @@TranCount...` | Начало именованного блока |
| `M_BUSINESSLOG_BLOCK_END(name)` | `Exit @@TranCount...` | Конец именованного блока |
| `M_LOG(msg)` / `M_LOG_PARAM(...)` | `@@TranCount...` (без префикса) | Разовый лог-чекпоинт, не блок |

Текущий парсер:
1. При `reBLogHeader` устанавливает `pendingBLog = true`, `pendingBLogIsEnter = nil`.
2. Проверяет только `reBLogEnter` (`^Enter\s+@@TranCount`) и `reBLogExit` (`^Exit\s+@@TranCount`) — M_LOG-строка (`@@TranCount` без префикса) не матчится.
3. `pendingBLogIsEnter` остаётся `nil`.
4. При следующем `RetVal = X#msg` условие `pendingBLog && pendingBLogIsEnter != nil` → FALSE, и RetVal обрабатывается **в нормальной ветке**, устанавливая `top.call.RetVal` и `top.call.RetValContext` у текущей процедуры.
5. `pendingBLog` при этом **не сбрасывается** (остаётся `true`).

---

## Impact

- `codebase_rti_blog` с `session_id` всегда возвращает пустые `blog_blocks` — инструмент фактически нефункционален при работе с сохранёнными сессиями.
- При `file_path`-режиме неверно заполняется `ret_val_context` у процедур, содержащих `M_LOG`-записи — затрудняет диагностику ошибок через `codebase_rti_errors`.

---

## Suggested Fix

### Баг 1

В `internal/rti/store.go` добавить функции загрузки:
- `loadBLogBlocks(db, sessionID) (map[int64][]RTIBLogBlock, error)` — SELECT из `rti_blog_blocks` по `session_id`
- `loadBLogTables(db, sessionID) (map[int64][]RTIBLogTable, error)` — SELECT из `rti_blog_tables` по `session_id`

Вызвать их в `LoadCalls` и прикрепить к соответствующим `RTICall` по `call_id`.

### Баг 2

В `internal/rti/parser.go`, в ветке `reRetVal`, добавить перед существующим `if pendingBLog && pendingBLogIsEnter != nil`:

```go
// M_LOG entry: pendingBLog active but no Enter/Exit prefix seen.
// This is a plain log message (not a block begin/end) — skip it.
if pendingBLog && pendingBLogIsEnter == nil {
    pendingBLog = false
    continue
}
```

### Тесты

- `TestParseContent_MLog_DoesNotSetCallRetVal` — `Trace.Server.BusinessLog` без `Enter`/`Exit`, затем `Return X` → `RetVal` берётся из `Return`, `RetValContext` пустой.
- `TestLoadCalls_BLogBlocksPopulated` (интеграционный) — после `SaveSession` → `LoadCalls` → `BLogBlocks` непустой.
