# Code-review реализации change `fix-review-block-comment-order`

Дата: 2026-11-20.
Объект ревью: рабочий дифф (`internal/review/review_rules.go`, `cmd/root.go`,
новый `internal/review/review_block_comment_test.go`), его соответствие
артефактам change (proposal/design/tasks/дельты, обновлённые по итогам
change-review) и отсутствие регрессов.

## Итог

Реализация **соответствует change, замечаний, блокирующих архивацию, нет**.
Все 12 сканеров исправлены одним и тем же минимальным reorder'ом, замечания
предыдущего ревью (F1–F3) учтены и в коде, и в артефактах. Тесты пакета
`internal/review` и `cmd` зелёные, `go vet`/`go build` чистые, e2E-приёмка на
реальном файле FA выполнена, `openspec validate --strict` — valid.

## Состав изменений (по git diff)

| Файл | Изменение |
|---|---|
| `cmd/root.go:29` | `buildNumber` 1710 → 1711 |
| `internal/review/review_rules.go` | 12 идентичных по форме reorder'ов (см. ниже) |
| `internal/review/review_block_comment_test.go` | новый: 1 dedicated-тест + table-driven на 27 под-кейсов |
| `CodeBase.exe` | пересобран (task 4.3) |

Посторонних изменений в диффе нет — затронуты только заявленные файлы.

## Корректность reorder'а по каждому сканеру

Единый паттерн (для 11 сканеров с префиксной проверкой): блок
`if inBlockComment { ... line = line[idx+2:] ... }` перенесён выше
`trimmed := strings.TrimSpace(line)` / `if strings.HasPrefix(trimmed, "--")`;
`trimmed` вычисляется **после** мутации строки — замечание F2 из change-review
учтено. Порядок теперь 1:1 соответствует эталону `checkTableHintIsRight`
(:3901–3913), как зафиксировано в design D2.

| Сканер | Место (после фикса) | Носитель мутации | Проверено |
|---|---|---|---|
| checkIndexWrong | review_rules.go:273→277 | локальная `line` | дифф + тест + e2e |
| checkUpdateOnlyVar | :492→497 | `line` | дифф + тест |
| checkPTableSpid | :680→689 | `line` | дифф + тест |
| checkForceOrder2Tbl | :832→842 | `line` | дифф + тест |
| checkSaveTran | :965→973 | `line` | дифф + тест |
| checkUseDrop | :1030→1041 | `line` | дифф + тест |
| checkMathOperations | :1114→1125 | `line` | дифф + тест |
| checkExistsWithAndInIf | :1200→1205 | **`lines[i]`** (сохранён) | дифф + тест |
| checkIndexExistsInDB | :1312→1320 | `line` (macro-content) | дифф + тест |
| checkVarcharSize | :4687→4698 | `line` | дифф + тест |
| checkPostgreLabelGotoLevel | :4805→4814 | `line`, усечение по `--` после закрытия | дифф + тест |
| checkUseFuncInIndCol | :6268→6277 | `line` (macro-content) | дифф + тест |

`checkPostgreLabelGotoLevel` исправлен по F1: усечение по первому `--`
перенесено после закрытия блока; семантика inline-усечения остатка сохранена
(design D1, Non-Goals, риск в design — всё отражено).

Семантические границы сдвига (проверены по коду):

- строка `-- ...` вне блока — поведение идентично прежнему (префиксный skip);
- строка закрытия `-----*/` в блоке — теперь закрывает, остаток `""` ведёт себя
  как пустая строка (в statement-сканерах попадает в буфер как обычная пустая
  строка — как и до фикса для пустых строк);
- строка `-- text */` в блоке — теперь закрывает блок (до фикса зависала);
- открывающая `/*---...` — обработка не изменилась (префикс не `--`);
- в `checkPostgreLabelGotoLevel` строки вида `x /* ---` и `/*----` дают ту же
  последовательность состояний, что и до фикса (усечение → open-loop).

## Регрессионные тесты

`review_block_comment_test.go`:

- `TestCheckIndexWrong_DashedHeaderBlockComment` — сценарий 1:1 из дельты
  deploy-stoppers: dashed-header, `delete ... M_ROWLOCK_INDEX(XPK...)`,
  `where SPID = @@spid and BankProductID = @BankProductID`, моки `indexCandCache`
  (`XPK (SPID, AutoID)` / `XIE1 (SPID, BankProductID, AmountMin)`); проверки
  count=1, Line=4, Object=`pbpconscoolingoff.xpkpbpconscoolingoff`, сообщение
  содержит `XIE1pBPConsCoolingOff`. Соответствует task 3.1 и сценарию спеки.
- `TestBlockCommentHeaderScanners` — 27 под-кейсов, по правилу: dashed-header
  (положительный) и line-comment (негативный) для 11 правил; плюс
  «незакрытый `/*` скрывает код» (saveTran, useFuncInIndCol, postgreLabelGotoLevel),
  «одиночный `/* ... */` в строке не ломает анализ» (saveTran),
  «активная метка + `-- goto MyLabel`» (postgreLabelGotoLevel, task 3.3).
  Mock-seam'ы: `indexCandCache` (indexExistsInDB), `indexFieldsCache`
  (useFuncInIndCol), `newRuleTestExec` для macro-content сканеров. Хarness
  `validateRuleCases`/`assertRuleFindings` переиспользован; ожидания
  wantLine/wantObject сверены с кодом правил (Object `goto MyLabel` —
  review_rules.go:4904; Line — строка `goto`, :4903).

Покрытие против tasks 3.1–3.3 — полное. Отдельного негативного кейса `--`
для `indexWrong` в новом файле нет, но позитив+негатив для правила уже есть в
`TestCheckIndexWrong_Coverage`/`_VariableInConditionIsNotColumn` — требование
«положительный и отрицательный кейсы» соблюдено.

## Верификация (выполнена в ходе ревью)

| Проверка | Команда | Результат |
|---|---|---|
| Новые тесты | `go test -count=1 -v ./internal/review -run 'BlockCommentHeader\|DashedHeader'` | 28/28 PASS |
| Пакет целиком | `go test -count=1 ./internal/review` | ok (без кеша) |
| Пакет cmd | `go test -count=1 ./cmd` | ok |
| Статический контроль | `go vet ./...` + `go build ./...` | чисто |
| Сборка | `CodeBase.exe` | banner `0.9.5 build 1711` |
| E2E-приёмка (task 4.3) | `CodeBase.exe review ...BPConsCoolingOff_Transfer.sql --rules indexWrong --json` | `count: 3`; **строка 40**, severity 1, `object: pbpconscoolingoff.xpkpbpconscoolingoff`, рекомендация `XIE1pBPConsCoolingOff` |
| OpenSpec | `openspec validate fix-review-block-comment-order --strict` | valid |

## Дополнительные findings e2e (строки 79 и 121) — легитимны

Ожидаемый эффект из design Risks («вырастет число findings»): шапка больше не
скрывает файл, и правило увидело ещё два оператора, проверенных по исходнику:

- **строка 79** — `update tBPConsCoolingOff ... from pBPConsCoolingOff
  M_NOLOCK_INDEX(XPK...) where p.SPID = @@spid and p.BankProductID =
  @BankProductID` (FA-файл :79–87): равенства `{SPID, BankProductID}` — префикс
  XIE1 (2) длиннее XPK (1);
- **строка 121** — `select @MaxID = max(AutoID) from pBPConsCoolingOff
  M_NOLOCK_INDEX(XPK...) where SPID = @@spid and BankProductID = @BankProductID
  and BPConsCoolingOffID = -1` (:121–125): XIE1 и XIE2 покрывают условия лучше
  XPK.

Ложных срабатываний среди новых findings нет.

## Соответствие артефактам change

| Требование артефакта | Статус |
|---|---|
| Reorder в 12 сканерах, `trimmed` после мутации (design D1/D2, tasks 1.1–2.2) | Выполнено, включая `lines[i]`-мутацию в checkExistsWithAndInIf |
| Семантика `postgreLabelGotoLevel` — усечение после закрытия блока (design D1, Non-Goals) | Выполнено |
| Поведение незакрытого блока / `--` вне блока не меняется (proposal) | Подтверждено негативными кейсами тестов |
| Регрессионные тесты incl. e2e-кейс (design D3, tasks 3.1–3.4) | Выполнено |
| `buildNumber` 1710→1711 (design D4, task 4.1) | Выполнено (cmd/root.go:29) |
| Схема БД / CLI / MCP не меняются (proposal Impact) | Подтверждено составом диффа |
| Артефакты после change-review (F1–F3) обновлены | proposal/design/дельты согласованы с реализацией: 12 сканеров, сценарий GOTO в дельте postgre-requirements, уточнён сценарий useDrop (`create procedure` в GIVEN) |

## Замечания (не блокируют)

1. **Незначительное, тесты**: в table-driven нет позитивного кейса «код после
   шапки + `*/` с остатком» вида `-----*/ select ...` (строка закрывает блок и
   содержит код в остатке). Именно этот случай проговаривался в design D1 как
   критичный для пересчёта `trimmed`. Покрыт косвенно (`trimmed` пересчитывается
   и без остатка), при желании добавить один под-кейс `saveTran`.
2. **Наблюдение**: `CodeBase.exe` закоммичен в репозитории как бинарник и
   обновлён в диффе — это практика проекта (task 4.3), но бинарник в git —
   кандидат на вынос из-под версионирования в будущем.
3. **Напоминание процесса**: после archive — синхронизировать дельты в основные
   спеки и зафиксировать summary в project memory (follow-up'ы уже есть в
   change-review: FP-класс в сканерах без отслеживания блочных комментариев,
   унификация helper'ов).

## Вердикт

Реализация соответствует спроектированному change, замечаний F1–F3 из
change-review предыдущего этапа устранены, регрессов не выявлено
(полный прогон пакета + vet + build + e2e). Change готов к archive.
