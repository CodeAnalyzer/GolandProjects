# Задачи: fix-postgre-label-goto-batch-scope

## 1. Реализация батч-скопинга

- [x] 1.1 В `checkPostgreLabelGotoLevel` (internal/review/review_rules.go) добавить счётчик батча: строка `go` на `cleanLine` (после зачистки комментариев и маскирования строк) — проверка `^go(\s+\d+)?$` по `strings.TrimSpace(strings.ToLower(cleanLine))` (TrimSpace снимает `\r` от CRLF; `TrimRight(" \t")` не годится) — инкрементирует батч; `gotoRef` и записи меток получают `batchID`; в коде оставить комментарий, что поддержка `go N` — намеренное расширение относительно SQL-парсера (`sql_parser.go` проверяет строгое `EqualFold(trimmed, "go")`). Проверка: `go build ./...` и `go test ./internal/review -run TestCheckPostgreLabelGotoLevel` (существующие кейсы без изменений).
- [x] 1.2 Сопоставление `goto` ↔ метка дополнить условием равенства `batchID` (finding при `label.batchID == goto.batchID && labelLine <= gotoLine`); в переборе позиций несовпадение батча — продолжение перебора, `break` — только по первому подошедшему своему батчу; goto без метки в своём батче — пропуск. Проверка: `go test ./internal/review -run TestCheckPostgreLabelGotoLevel`.

## 2. Тесты

- [x] 2.1 Новые table-driven кейсы в `internal/review/review_rules_test.go`: две процедуры с `go`-разделителем и одноимёнными метками (Trunc_Inst-стиль) → 0 findings; `go N` как граница батча; goto с меткой только в чужом батче → 0; `GO` в верхнем регистре разделяет; CRLF-кейсы — `"L:\r\nselect 1\r\ngo\r\ngoto L\r\n"` → 0 findings (граница с хвостовым `\r` распознаётся) и `"L:\r\ngoto L\r\n"` → 1 finding (CRLF не ломает TP-путь). Проверка: `go test ./internal/review -run TestCheckPostgreLabelGotoLevel`.
- [x] 2.2 Кейсы взаимодействия с комментариями в `internal/review/review_block_comment_test.go`: `-- go` между меткой и goto — finding сохраняется; `go` внутри блочного комментария — батч не разделяет. Проверка: `go test ./internal/review -run BlockCommentHeader`.
- [x] 2.3 Полная регрессия пакета. Проверка: `go test ./internal/review` (все тесты зелёные).

## 3. Приёмка на реальных данных

- [x] 3.1 Бампнуть buildNumber с `"1719"` до `"1720"` в `CodeBase\cmd\root.go`, пересобрать CodeBase.exe и прогнать одиночный review: `Trunc_Inst.sql` → 0 findings (был 1 на line=103); `archiver.sql` → 2 findings (line=215, 406 — настоящие обратные переходы). Проверка: вывод `CodeBase.exe review <файл> --rules postgreLabelGotoLevel`.
- [x] 3.2 Корпусный прогон fa-generalledger: список файлов с `goto` (формировать через `cmd /c "rg -l --glob *.sql -i \bgoto\b D:\GITHUB\GolandProjects\FA\fa-generalledger > Tests\Temp\gl_goto_files.txt"` — перенаправление обязательно **внутри** кавычек cmd: иначе `>` разбирает PowerShell и пишет UTF-16, ломая `--files`), затем `CodeBase.exe review --files <list> --rules postgreLabelGotoLevel --json` → ожидается 2 findings (оба в archiver.sql), против 39 в базлайне. Результаты и дифф зафиксировать в отчёте приёмки.

## 4. Документация

- [x] 4.1 Уточнить строку правила `postgreLabelGotoLevel` в README.md (раздел review): «обратный переход `GOTO` к метке выше перехода в том же батче» (текущая формулировка «использование `GOTO` и меток» после правки неточна). Проверка: чтение README, `openspec validate fix-postgre-label-goto-batch-scope --strict`.

## 5. Макросы-завершители как граница батча (по code-review R1, решение — Вариант A)

- [x] 5.1 В `checkPostgreLabelGotoLevel` добавить `reProcEndMacroSeparator` — `^(arc_proc_end|dcl_proc_end|proc_end)\s*\(` по `TrimSpace(ToLower(cleanLine))`; граница батча: `go`/`go N` **или** вызов макроса-завершителя. Скобка обязательна (отличает `PROC_END(имя)` от метки `PROC_END:`). Проверка: `go build ./...`, `go vet ./internal/review`, `go test ./internal/review`.
- [x] 5.2 Тесты: `ARC_PROC_END` archiver-стиль (2 процедуры без `go`) → 0; обратный переход в макро-батче → 1; `DCL_PROC_END( P1 )` с пробелами → 0; `PROC_END(P1)` → 0; метка `PROC_END:` — не разделитель → 1. Проверка: `go test ./internal/review -run TestCheckPostgreLabelGotoLevel`.
- [x] 5.3 Бампнуть buildNumber `"1720"` → `"1721"`, пересобрать: `Trunc_Inst.sql` → 0; `archiver.sql` → **0** (было 2 — переклассифицированы R1 в остаточные кросс-процедурные FP). Проверка: `CodeBase.exe review <файл> --rules postgreLabelGotoLevel --json`.
- [x] 5.4 Корпусный прогон fa-generalledger (тот же список 2946 файлов) → **0** findings (39 в базлайне); обновить отчёт приёмки: переклассификация archiver.sql:215/406, дифф по макро-границе. Проверка: `CodeBase.exe review --files Tests\Temp\gl_goto_files.txt --rules postgreLabelGotoLevel --json`.
- [x] 5.5 Синхронизировать артефакты change с расширением (proposal What Changes/Impact, design D6 + Risks, дельта: формулировка границы + сценарии «Макрос-завершитель…», «Метка PROC_END…», R2-правка Context). Проверка: `openspec validate fix-postgre-label-goto-batch-scope --strict`.
