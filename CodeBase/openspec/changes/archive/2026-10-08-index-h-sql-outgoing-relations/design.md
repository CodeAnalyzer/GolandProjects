## Context

`parseHFile` (internal/indexer/indexer.go:426) после H-парсера прогоняет контент через SQL-парсер, но использует только `sqlResult.Procedures` (с пост-фильтром `isLineInsideMacroDefinition`); `Calls`/`Tables`/`Fragments`/`Defines` отбрасываются. Полный пайплайн для SQL-файлов (internal/indexer/indexer_sql_pas.go) включает: вставку таблиц/колонок/индексов/фрагментов, резолв `procedureIDs`/`tableIDs`, parent-binding фрагментов к процедурам (:327-350), `addPendingSQLCalls` (:369), `buildSQLProcedureTableRelations` (:371), `buildQueryFragmentRelations`. SQL-парсер (internal/parser/sql/sql_parser.go) в define-ветках (~1141-1180) потребляет только первую строку `#define`; continuation-строки с `\` утекают в основной цикл. По индексу FA: 1954 `.h`-файла, маркеры процедур содержат ~99 (5%), реальных процедур ~40. Исходящий план — Modifications/index-h-file-sql-procedures.md.

## Goals / Non-Goals

**Goals:**

- Исходящие `calls_procedure` и табличные relations от `.h`-процедур, `query_fragments` с parent-binding — по той же схеме, что `.sql`.
- Pre-check маркеров: SQL-парсер запускается только для `.h` с `DCL_PROC_BEGIN(`/`__BEGIN_PROCEDURE__(`.
- Парсер: define-ветки потребляют `\`-продолжения целиком — джанк из тел макросов не возникает в принципе.
- Порядок работ: фикс парсера → pre-check → связи (связи строятся на чистом выводе парсера).

**Non-Goals:**

- Не меняем схему БД, публичные контракты CLI/MCP.
- Не подставляем содержимое include-файлов при парсинге (`.h` парсится как есть).
- Не строим глобальный (межфайловый) резолв таблиц для табличных relations — семантика остаётся внутрифайловой, как у `.sql`.

## Decisions

- **D1. Порядок дельт: парсер первым.** После фикса `\`-продолжений `isLineInsideMacroDefinition` технически избыточен, но оставляем его как дешёвую защиту от регресса (O(строк до define), не влияет на производительность). Решение об удалении — после регрессионного замера.
- **D2. Pre-check — пере-включительный `strings.Contains` по двум маркерам**, без анализа define-контекста. Маркер внутри `#define`-тела делает pre-check истинным (лишний парс ~5% файлов), но не влияет на корректность: джанк гасится парсером (после D1-фикса) и фильтром. Альтернатива (контекстный pre-check с учётом `\`-продолжений) отклонена — усложнение ради <1% файлов.
- **D3. Репликация схемы `.sql`-пайплайна в `parseHFile`**: вставка процедур → `EnsureDescriptionSearchVectors` → вставка таблиц/колонок → резолв `FindSQLProcedureIDsByFile`/`FindSQLTableIDsByFileAndLine` → parent-binding фрагментов → вставка symbols/fragments → `addPendingSQLCalls` → `buildSQLProcedureTableRelations` → `buildQueryFragmentRelations` → `saveRelations`. Реюз существующих хелперов без копирования логики. `Defines` из SQL-результата игнорируются (H-парсер уже дал их — иначе дубли в `h_files_defines`).
- **D4. Таблицы из `.h` в `sql_tables` — без дедупликации.** `sql_tables` ключуется по `(file_id, name, context, line)`; та же таблица из разных файлов уже сейчас даёт несколько строк (referenced-таблицы из `FROM`/`INSERT` попадают туда из каждого `.sql`). Строки из `.h` — та же семантика.
- **D5. Регрессионный baseline до фикса парсера.** Перед правкой define-веток замерить по живому индексу FA, сколько continuation-строк многострочных макросов сейчас утекает в `query_fragments` при парсинге `.sql` (сравнение фрагментов до/после).

## Risks / Trade-offs

- [Правка define-веток меняет поведение и для `.sql`] → D5-baseline + прогон тестов `sql_parser_test.go` + сравнение полного перестроения индекса FA до/после (counters: procedures, tables, fragments).
- [Новые строки в `sql_tables`/`symbols` из `.h` раздуют счётчики] → это ожидаемое появление реальных данных; зафиксировать в tasks проверку метрик после перестроения.
- [Табличные relations `.h`-процедур — только к таблицам того же файла] → осознанное ограничение (та же семантика, что у `.sql`); задокументировано в spec (Non-Goals/design).
- [Полное перестроение обязательно для новых данных] → инкрементальный `update` подхватывает изменённые `.h` автоматически; миграция схемы не требуется.

## Migration Plan

### Baseline (замер 2026-10-08, индекс FA)

- Глобально: `query_fragments` = 1 707 159; `sql_tables` = 1 128 462; `sql_procedures` = 40 449.
- Утечка continuation-строк: 41 073 фрагмента внутри `\`-цепочек многострочных `#define` (911 425 фрагментов в 23 822 файлах с defines), из них 28 439 — первые continuation-строки; 10 038 файлов с утечкой; 28 439 цепочек, 562 409 continuation-строк.
- Методика: SQL-джойн `query_fragments`×`h_files_defines` (`q.line = d.line+1`) → 83 870 пар; файловый скан (одноразовый Go-скрипт) отсеял 66% ложных (define без `\`); полные цепочки сверенены с выгрузкой фрагментов файлов с defines.

### After (перестрой 2026-10-08, errors=0)

- Счётчики: `query_fragments` = 1 698 779 (нетто −8 380 = −18 655 джанк-continuation + 10 275 новых `.h`); `sql_tables` = 1 088 680 (−39 782 ≈ джанк-таблицы из continuation-строк, +288 `.h`); `sql_procedures` = 40 447 (−2 джанк-процедуры из `.sql`-макросов).
- Прокси джанка: `tables_at_define+1` = 4 легитимных (было ~40k); `frag_at_define+1` = 62 158 (легитимные + новые `.h`).
- `.h`: 64 процедуры, 288 таблиц, 10 275 фрагментов, 790 исходящих связей (было 0); `Ins_Check_ExistsLinkObject` → `FCD_INS_INSP_FindListObjIDByID` + табличные relations — подтверждено.
- Follow-up (вне скоупа): сама define-строка до define-ветки успевает приклеиться к накапливаемому стейтменту (`appendStatementLine` идёт раньше define-веток) — ~22k фрагментов содержат текст define-строки в `query_text` как завершение предыдущего стейтмента; поведение существовало до изменения и continuation-фиксом не затрагивается.

1. Merge → `go build ./...` + `go vet` + тесты.
2. Полное перестроение индекса FA: `codebase init` (или `update --modified=false`).
3. Проверка метрик: исходящие связи `.h`-процедур (`query callers` на цели из тел `.h`-процедур), отсутствие джанка, счётчики. Rollback — обычный git revert + повторное `init`; схема БД не менялась.
