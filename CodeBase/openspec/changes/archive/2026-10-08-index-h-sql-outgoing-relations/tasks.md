## 1. Baseline (регрессионный замер)

- [x] 1.1 Замерить по живому индексу FA, сколько continuation-строк многострочных `#define` сейчас утекает в `query_fragments` при парсинге `.sql` (baseline для сравнения после фикса) — зафиксировать цифры (procedures, tables, fragments) до правок

Baseline (индекс FA, 2026-10-08): `query_fragments` = 1 707 159, `sql_tables` = 1 128 462, `sql_procedures` = 40 449. Утечка: 41 073 фрагмента лежат внутри `\`-цепочек многострочных `#define` (из 911 425 фрагментов в 23 822 файлах с defines), в т.ч. 28 439 — первые continuation-строки (пар «фрагмент на define+1» = 83 870, из них реальные — 34%); файлов с утечкой ≥1: 10 038; многострочных define-цепочек: 28 439 (562 409 continuation-строк). После фикса парсера ожидается: фрагменты −41 073, таблицы/процедуры — без просадки.

## 2. Парсер: потребление `\`-продолжений в define-ветках

- [x] 2.1 В `internal/parser/sql/sql_parser.go` (define-ветки ~1141-1180: `macroDefineRe`, `emptyMacroDefineRe`, `defineRe`, `emptyDefineRe`, `constDefineRe`, `constCommentRe`) потреблять строки, пока очередная строка заканчивается `\`
- [x] 2.2 Тесты в `internal/parser/sql/sql_parser_test.go`: многострочный `#define` потребляется целиком; `DCL_PROC_BEGIN` в теле макроса не даёт процедур и фрагментов; однострочные define не ломаются — `go test ./internal/parser/sql/`
- [x] 2.3 Сравнение с baseline 1.1: полный перестрой индекса FA, счётчики не просели, утёкшие continuation-фрагменты исчезли

## 3. Pre-check маркеров в `parseHFile`

- [x] 3.1 Дешёвый pre-check `strings.Contains` по `DCL_PROC_BEGIN(`/`__BEGIN_PROCEDURE__(` в `parseHFile` (internal/indexer/indexer.go) — SQL-парсер запускается только при наличии маркера
- [x] 3.2 Тесты в `internal/indexer/indexer_h_test.go`: `.h` без маркеров → SQL-парсер не вызывается (defines/includes индексируются как обычно); маркер внутри `#define`-тела → парс не порождает сущностей — `go test ./internal/indexer/`

## 4. Исходящие связи `.h`-процедур

- [x] 4.1 В `parseHFile` реплицировать схему `.sql`-пайплайна (по design D3): вставка таблиц/колонок, резолв `procedureIDs`/`tableIDs`, parent-binding фрагментов к процедурам, вставка symbols/fragments, `addPendingSQLCalls`, `buildSQLProcedureTableRelations`, `buildQueryFragmentRelations`, `saveRelations`; `Defines` из SQL-результата игнорировать
- [x] 4.2 Тесты: `.h` с `exec` другой процедуры и `insert` в таблицу → появляются `calls_procedure` + `inserts_into`, `callers` находит `.h`-процедуру; фрагменты с parent-binding; дублей defines нет — `go test ./internal/indexer/`
- [x] 4.3 `go build ./...` + `go vet ./internal/...` проходят

## 5. Приёмка на индексе FA

- [x] 5.1 Полное перестроение (`codebase init` или `update --modified=false`) *выполняет пользователь*
- [x] 5.2 Проверка живых примеров: `Ins_Check_ExistsLinkObject` (fa-contracts/Insurance/.../InsurancePolicy_Proc.h) имеет исходящие связи (`FCD_INS_INSP_FindListObjIDByID`, таблицы `pIns_Identificator`, `pIns_Policy_Search` и др.); `query callers` находит `.h`-источник
- [x] 5.3 Сверка метрик с таблицей «Ожидаемый результат» из Modifications/index-h-file-sql-procedures.md: исходящие связи от `.h`-процедур > 0, табличные relations > 0, фрагменты > 0, SQL-парс на `.h` без маркеров пропускается (~95%), джанк-процедуры из макросов отсутствуют

Итоги приёмки (перестрой 2026-10-08, errors=0): исходящие связи `.h`-процедур 790 (было 0); `.h`-сущности 64 процедуры / 288 таблиц / 10 275 фрагментов; `Ins_Check_ExistsLinkObject` → calls_procedure `FCD_INS_INSP_FindListObjIDByID` + selects_from `pIns_Policy_Search`/`pAPI_INSP_LinkObject` + inserts/deletes `pIns_Identificator`/`pAPI_InsurancePolicy_ID` + executes_query×5; джанк-таблицы на continuation-строках: `tables_at_define+1` = 4 легитимных (до фикса ~40k); джанк-процедуры −2; фрагменты нетто −8 380 = −18 655 джанк-continuation + 10 275 новых `.h`.
