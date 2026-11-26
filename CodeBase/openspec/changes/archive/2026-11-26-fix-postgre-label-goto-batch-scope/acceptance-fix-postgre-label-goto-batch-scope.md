# Отчёт приёмки: fix-postgre-label-goto-batch-scope

Сборка: CodeBase 0.9.5 build **1721** (1719 → 1720 — батч-скопинг по `go`; 1720 → 1721 — макросы-завершители как граница, по code-review R1). Дата: 2026-11-26.

## Одиночные прогоны (build 1721)

| Файл | Базлайн | 1720 (`go`-скопинг) | 1721 (+макросы) | Комментарий |
|---|---|---|---|---|
| `GeneralLedger\SERVER\SCRIPT_UTIL\CleaningData\Trunc_Inst.sql` | 1 (line=103) | 0 | **0** | FP устранён `go`-границей: метка@55 из proc 1 невидима для goto@103 proc 2 |
| `GeneralLedger\SERVER\Rest\archiver.sql` | 2 (line=215, 406) | 2 | **0** | По code-review R1: **переклассифицированы из TP в остаточные кросс-процедурные FP** — границы 6 процедур файла заданы `ARC_PROC_END(proc)` (archive.h:162, раскрытие `go … go`), `#include` не раскрывается replaceMacros, литерального `go` между процедурами 1–3 нет (первый — строка 427). Единственная `EXIT_PROC:` в батче `Arc_ClrRest` — @419, **ниже** gotos 215/406 → реальные переходы прямые (вперёд), finding не должен формироваться |

## Корпусный прогон fa-generalledger

Метод тот же, что в базлайне: `rg -l --glob *.sql -i \bgoto\b` (redirect внутри кавычек `cmd /c`) → `CodeBase.exe review --files <list> --rules postgreLabelGotoLevel --json`.

- Файлов в списке: **2946** (совпадает с базлайном).
- Findings: **39 → 0** (итог build 1721; промежуточно 39 → 2 на build 1720).
- Дифф: −37 кросс-процедурных FP через `go`-границы (все 15 FP-файлов базлайна чистые) и −2 остаточных FP в archiver.sql через макро-границу; **новых findings не появилось** (разбиение батчей не может создать label-above-goto пару — метки могут только уходить в более ранние батчи).

Итог: все 39 findings базлайна — кросс-процедурные FP (классификация 37/39 на этапе explore была неполной: archiver.sql:215/406 — тот же класс, скрытый include-макросами). Ожидание proposal (39 → 0) — **выполнено**.

## Тесты

- `TestCheckPostgreLabelGotoLevel` — 19 кейсов: 8 на `go`-скопинг (Trunc-стиль, чужая метка сверху, `go N`×2, `GO`, CRLF×2) + 5 на макросы (ARC_PROC_END archiver-стиль → 0; обратный переход в макро-батче → 1; `DCL_PROC_END( P1 )` → 0; `PROC_END(P1)` → 0; метка `PROC_END:` — не разделитель → 1) + 6 исходных.
- `TestBlockCommentHeaderScanners` — 3 новых кейса (`-- go`, inline `/* go */`, многострочный блок).
- Полная регрессия `go test ./internal/review` — ok; `go vet` — чисто.

## Обоснование макросов-разделителей (R1, Вариант A)

- `ARC_PROC_END(proc)` — archive.h:162 (6 копий в fa-generalledger): `go \ X_ANYMODE … \ grant … \ go`; в корпусе — 6 вызовов, все в archiver.sql.
- `DCL_PROC_END( NAME )` — MACROS.H:762 (5 копий в fa-generalledger, десятки по FA): хвост `… end \ go \ X_ANYMODE(NAME) \ go \ grant … \ go`; в fa-generalledger вызовов нет (20 файлов в других продуктах FA).
- `PROC_END(name)` — archive.h:168, раскрытие идентично ARC; 8 вызовов в fa-administrator/fa-payments, в fa-generalledger — 0 (только **метка** `PROC_END:` в Exponent_ShiftNumber.sql:101, отсюда обязательная скобка в шаблоне).
- Вызовы без скобок — 0 вхождений по всему FA; `#define`-строки не матчатся (якорь `^`).

## Сырые артефакты

- `Tests\Temp\gl_goto_files.txt` — список 2946 файлов (байты `44 3A 5C 47`, без UTF-16 BOM).
- `Tests\Temp\gl_goto_review_1720.json` — промежуточный прогон (count=2, archiver.sql:215/406).
- `Tests\Temp\gl_goto_review_1721.json` — итог (count=0).
- `Tests\Temp\trunc_1721.json`, `Tests\Temp\archiver_1721.json` — одиночные прогоны build 1721.
