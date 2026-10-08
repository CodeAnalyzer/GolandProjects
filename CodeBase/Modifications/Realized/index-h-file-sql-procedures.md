# Индексация SQL-процедур из H-скриптов: оставшиеся дельты

**СТАТУС: РЕАЛИЗОВАНО 2026-10-08** (change
`openspec/changes/archive/2026-10-08-index-h-sql-outgoing-relations/`,
дельты слиты в specs `indexing/h-report-parsing` и `indexing/sql-parsing`).

Итоги на индексе FA (перестрой 2026-10-08, errors=0): исходящие связи
`.h`-процедур 0 → 790; `.h`-сущности: 64 процедуры / 288 таблиц /
10 275 фрагментов; джанк из макросов удалён (таблицы −39 782,
фрагменты −18 655, процедуры −2); `Ins_Check_ExistsLinkObject` имеет
calls_procedure `FCD_INS_INSP_FindListObjIDByID` + табличные relations.

**Follow-up (не реализовано):** сама define-строка до define-ветки
успевает приклеиться к накапливаемому стейтменту — ~22k фрагментов
содержат текст define-строки в `query_text`; поведение существовало
до изменения (см. design.md архивированного change).

---

Базовая функциональность реализована 2026-06-22 (commit `adab7b4`
«Исправлен парсер H-файлов (кейс когда в H-файле создается процедура)»).
Документ перезаписан 2026-10-06 и покрывает только то, что осталось
нереализованным.

---

## Уже реализовано (контекст)

- **Dual-parse `.h`**: `parseHFile` (`internal/indexer/indexer.go:426`) после
  h-парсера прогоняет контент через SQL-парсер и сохраняет процедуры в
  `sql_procedures` + `symbols` (`EntityType='sql'`, `file_id` → `.h`-файл).
- **Фильтр джанка из макросов**: процедуры, попавшие внутрь многострочных
  `#define`, отбрасываются пост-фактум `isLineInsideMacroDefinition`
  (`internal/indexer/indexer.go:524`) по номеру строки.
- **Дескрипшены**: `EnsureDescriptionSearchVectors` (`indexer.go:503`) —
  `.h`-процедуры доступны в `query desc-search` / MCP
  `codebase_query_desc_search`.
- **Callee-сторона графа вызовов**: вызовы `.h`-процедур из `.sql` резолвятся
  (`FindLatestSQLProcedureIDsByNames` по `sql_procedures`) — `.h`-процедуры
  видны как цели `calls_procedure`.
- **Поиск**: `query procedure` находит `.h`-процедуры. Проверено на живом
  индексе FA (2026-10-06): `Ins_Check_ExistsLinkObject` из
  `fa-contracts/Insurance/SERVER/Insurance/InsurancePolicy_Proc.h:3-45`.

## Нереализованное

### 1. Исходящие связи `.h`-процедур (главная дельта)

Из результата SQL-парсинга `.h` используются только `Procedures`.
`result.Calls`, `result.Tables`, `result.Fragments` игнорируются:

- `addPendingSQLCalls` вызывается только в `.sql`-пайплайне
  (`internal/indexer/indexer_sql_pas.go:369`) → исходящие `calls_procedure`
  от `.h`-процедур не строятся;
- `buildSQLProcedureTableRelations` — тоже только для `.sql`
  (`indexer_sql_pas.go:371`) → `selects_from`/`inserts_into`/`updates`/… из
  тел `.h`-процедур отсутствуют (таблицы для `.h` не вставляются в
  `sql_tables`, значит резолвить target-ID не по чему);
- `query_fragments` из `.h` не сохраняются (нет батча, нет parent-binding
  к процедурам по образцу `indexer_sql_pas.go:327-350`).

**Подтверждено на живом индексе FA (2026-10-06)**:
`Ins_Check_ExistsLinkObject` вызывает `FCD_INS_INSP_FindListObjIDByID`
(`InsurancePolicy_Proc.h:30`) и пишет в `pAPI_InsurancePolicy_ID`,
`pIns_Identificator`, читает `pIns_Policy_Search`, `pAPI_INSP_LinkObject` —
исходящих relations у процедуры **0**; в `callers` цели `.h`-источник
не входит.

**Фикс** — в `parseHFile` после вставки процедур повторить схему
`.sql`-пайплайна: резолв `procedureIDs` (`FindSQLProcedureIDsByFile`),
вставка `tablesBatch`/`columnsBatch`/`fragmentsBatch` (с parent-binding),
`addPendingSQLCalls(fileID, path, procBatch, procedureIDs, sqlResult.Calls)`,
`buildSQLProcedureTableRelations(...)`. `Defines` из результата SQL-парсера
по-прежнему игнорировать (их даёт h-парсер — иначе дубли в `h_files_defines`).

### 2. Pre-check перед SQL-парсингом `.h` (perf)

SQL-парсер сейчас запускается **безусловно для всех** `.h` (по FA — 1954
файла), хотя маркеры `DCL_PROC_BEGIN(`/`__BEGIN_PROCEDURE__(` содержат ~99
файлов (5%), реальных процедур ~40. Фикс: дешёвый маркерный pre-check
(с учётом строк внутри `#define`-веток — там маркеры встречаются в телах
макросов) и запуск SQL-парсера только при наличии маркеров. Экономит полный
SQL-парс на ~95% `.h` при каждом init/update.

Согласовать с п.1: файлы без маркеров не дают ни процедур, ни calls, так что
pre-check безопасен для полноты связей.

### 3. Потребление `\`-продолжений в `#define`-ветках SQL-парсера

Define-ветки (`internal/parser/sql/sql_parser.go`, regex-ветки ~49-54,
перехват ~1141) захватывают только **первую** строку макроса;
continuation-строки с завершающим `\` утекают в основной цикл. Для `.h` это
даёт джанк-процедуры из тел макросов (`DIAG_PROCNAME`, `proc_name`) — сейчас
они гасятся фильтром из «Уже реализовано», но парсер продолжает отдавать
ложные сущности. Фикс: в define-ветках потреблять строки, пока очередная
строка заканчивается `\`.

- **Проверить при реализации**: утекают ли continuation-строки многострочных
  макросов в `query_fragments` при парсинге `.sql` (замер по индексу FA).
- После фикса `isLineInsideMacroDefinition` становится технически избыточным
  (оставить как защиту от регресса или убрать — решить при реализации).

## Затронутые файлы

- `internal/indexer/indexer.go` — `parseHFile`: pre-check + calls/tables/
  fragments + parent-binding + relations
- `internal/parser/sql/sql_parser.go` — потребление `\`-продолжений
- `internal/indexer/indexer_h_test.go`, `internal/parser/sql/sql_parser_test.go`
  — тесты/фикстуры

## Миграция и совместимость

- Схема БД не меняется; новые сущности и связи появятся только после полного
  перестроения: `codebase init` или `codebase update --modified=false`
  (pre-filter по mtime+size не перепарсивает неизменённые `.h`).
- Инкрементальный `update` обслуживает изменённые `.h` автоматически.
- Публичные контракты CLI/MCP не меняются — просто появляется больше данных.

## Тесты

- Relations: `.h` с `exec` другой процедуры и `insert` в таблицу → появляются
  `calls_procedure` + `inserts_into`; `callers` находит `.h`-процедуру как
  caller.
- Pre-check: `.h` без маркеров → SQL-парсер не вызывается; `.h` с маркером
  внутри `#define`-тела → не парсится как SQL.
- Парсер: многострочный `#define` потребляется целиком; `DCL_PROC_BEGIN` в
  теле макроса не даёт процедур и фрагментов; однострочные define не ломаются.
- Индексер: `.h` с процедурой → процедуры + фрагменты + связи, дублей defines
  нет; `.h` без процедур — поведение не изменилось.

## Ожидаемый результат

| Метрика | Сейчас | После |
|---|---|---|
| Исходящие `calls_procedure` от `.h`-процедур | 0 | все вызовы из тел |
| Табличные relations из тел `.h`-процедур | 0 | `selects_from`/`inserts_into`/… |
| `query_fragments` из `.h` | 0 | с parent-binding к процедурам |
| SQL-парс на `.h` без маркеров | 100% файлов (1954 по FA) | пропускаются (~95%) |
| Джанк-процедуры из макросов на выходе парсера | есть (гасятся фильтром) | отсутствуют в принципе |
