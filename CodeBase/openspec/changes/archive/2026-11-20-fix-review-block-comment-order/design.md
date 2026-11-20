## Context

В `internal/review/review_rules.go` 11 построчных сканеров дублируют один и тот же цикл обработки комментариев. В них порядок такой:

1. `trimmed := strings.TrimSpace(line)`
2. `if strings.HasPrefix(trimmed, "--") { continue }`
3. `if inBlockComment { ... искать "*/" ... }`

Если шапка открыта `/*---`, а закрыта строкой `-------------------------------------------------------------------*/`, то на шаге 2 строка закрытия принимается за строчный комментарий, `continue` выполняется раньше шага 3, `inBlockComment` остаётся `true`, и весь последующий файл игнорируется.

12-й сканер, `checkPostgreLabelGotoLevel`, содержит тот же класс ошибки в более сильной форме: до закрытия блока строка безусловно усекается по первому `strings.Index(line, "--")`. На строке закрытия `-----*/` усечение даёт пустую строку, `*/` уже не находится, и комментарий остаётся открытым.

Два сканера уже написаны в правильном порядке и служат эталоном:

- `checkTableHintIsRight` — блок `inBlockComment` до `TrimSpace`/`--` (`review_rules.go:3900`), текстуально ближе всего к 11 циклам.
- `checkUseEqColumn` — тоже корректен (`review_rules.go:3254`), но дополнительно пропускает пустой остаток после `*/`; для исправления этот нюанс не обязателен.

Затронуты 12 правил:

| capability | rule | функция |
|---|---|---|
| deploy-stoppers | indexWrong | `checkIndexWrong` |
| deploy-stoppers | updateOnlyVar | `checkUpdateOnlyVar` |
| deploy-stoppers | pTableSpid | `checkPTableSpid` |
| deploy-stoppers | forceOrder2Tbl | `checkForceOrder2Tbl` |
| deploy-stoppers | saveTran | `checkSaveTran` |
| deploy-stoppers | useDrop | `checkUseDrop` |
| deploy-stoppers | mathOperations | `checkMathOperations` |
| deploy-stoppers | existsWithAndInIf | `checkExistsWithAndInIf` |
| deploy-stoppers | indexExistsInDB | `checkIndexExistsInDB` |
| deploy-stoppers | varcharSize | `checkVarcharSize` |
| postgre-requirements | useFuncInIndCol | `checkUseFuncInIndCol` |
| postgre-requirements | postgreLabelGotoLevel | `checkPostgreLabelGotoLevel` |

## Goals / Non-Goals

**Goals:**

- Во всех 12 сканерах закрытие блочного комментария обрабатывается раньше проверки/усечения по `--`.
- Строка закрытия, начинающаяся с `--`, корректно сбрасывает `inBlockComment`.
- Поведение для незакрытого блока и для обычных `--`-комментариев не меняется.
- Есть регрессионные тесты, включая реальный кейс `BPConsCoolingOff_Transfer.sql`.

**Non-Goals:**

- Не менять логику `indexWrong` (prefix-match, PK-join guard, area-scope).
- Не извлекать общий helper обработки комментариев для всех сканеров.
- Не исправлять другие места, где нет отслеживания блочного комментария (например, `collectExecCallLines`), если они не входят в список 12.
- Не унифицировать inline-усечение по `--` в `checkPostgreLabelGotoLevel` с префиксной проверкой остальных сканеров: семантика правила сохраняется.
- Не менять схему БД, CLI-контракт и MCP-контракт.

## Decisions

### D1. Локальный reorder в 12 циклах, без общего helper

В каждом из 12 циклов блок обработки `inBlockComment` переносится выше вычисления `trimmed` и проверки/усечения по `--`; после закрытия блока текст строки (остаток после `*/`) участвует в дальнейшем анализе как обычно.

Критично для реализации: `trimmed` (или эквивалентная нормализация) MUST вычисляться заново после мутации `line = line[idx+2:]`. Иначе строка вида `-----*/ select ...` после закрытия блока всё равно будет отброшена префиксной проверкой `--` по старому значению.

Для `checkPostgreLabelGotoLevel` сохраняется его семантика: усечение по первому `--` в строке выполняется после закрытия блока и применяется к остатку после `*/`.

Почему не общий helper:

- циклы различаются: часть мутирует локальную `line`, `checkExistsWithAndInIf` мутирует `lines[i]`; есть дополнительные состояния (`isInsertStmt`, `*`-хинты, macro-expanded контент); `checkPostgreLabelGotoLevel` использует усечение по любому `--`, а не префиксную проверку;
- извлечение helper'а расширяет дифф и риск изменить нюансы 12 правил;
- стоимость исправления — 3 строки на цикл, эффект — восстановление анализа всего файла после шапки.

Альтернатива (helper `stripComments`) отклонена как более дорогая и рискованная для bug-fix.

### D2. Эталон — checkTableHintIsRight

Порядок приводится к виду `checkTableHintIsRight`: сначала закрытие блока, затем `TrimSpace` + `--`; `trimmed` вычисляется из уже мутированной строки. Это сохраняет единый стиль в пакете и не вводит новую абстракцию. `checkUseEqColumn` тоже корректен, но его дополнительный пропуск пустого остатка после `*/` не обязателен для этих 12 сканеров.

### D3. Тесты

- Unit-регрессия `checkIndexWrong`: шапка `/*---` … `---*/`, затем `delete ... M_ROWLOCK_INDEX(XPK...)` с `where SPID = @@spid and BankProductID = @BankProductID`; моки кандидатов `XPK (SPID, AutoID)` и `XIE1 (SPID, BankProductID, AmountMin)`; ожидается finding на строке `delete` с рекомендацией `XIE1`.
- Table-driven регрессия по остальным 11 сканерам: тот же dashed-header + минимальный триггер правила; для правил с внешними lookup'ами использовать существующие mock-seam'ы (`indexCandCache` и аналоги). Для `postgreLabelGotoLevel` — `MyLabel:` и `goto MyLabel` после шапки (finding формируется, когда метка расположена выше перехода); отдельно проверить, что `-- goto MyLabel` при активной метке по-прежнему игнорируется.
- Негативные кейсы: незакрытый `/*` скрывает код; обычный `--` скрывает код; одиночный `/* ... */` в строке не ломает анализ.
- E2E-приёмка на реальном файле `FA/fa-contracts/Consumer/SERVER/Consumer/BPConsCoolingOff_Transfer.sql`: `indexWrong` на строке 40, `Object = pbpconscoolingoff.xpkpbpconscoolingoff`, сообщение содержит `XIE1pBPConsCoolingOff`.

### D4. Build number

`cmd/root.go:29` — `1710` → `1711`, по принятому в проекте правилу нумерации сборок.

## Risks / Trade-offs

- [После фикса сканеры увидят код, который раньше полностью скрывался; вырастет число findings в существующих файлах] → ожидаемый эффект; приёмка фиксирует конкретный кейс, а существующие тесты правил проверяют отсутствие регрессий.
- [В `checkExistsWithAndInIf` цикл мутирует `lines[i]`, а не локальную `line`; при reorder легко потерять мутацию] → переносить только порядок блоков, не меняя носитель (`lines[i]` остаётся).
- [Строка вида `*/ select ...` — после закрытия блока остаётся значимый код] → сохраняется текущее поведение: остаток после `*/` передаётся дальше в анализ.
- [В `checkPostgreLabelGotoLevel` легко потерять усечение по `--` внутри строки, если просто поменять блоки местами] → усечение остаётся после закрытия блока и применяется к остатку после `*/`; это зафиксировано в D1 и тесте.
- [Сканеры без отслеживания блочных комментариев (`checkUseSelectAll`, `checkTruncTbl`, `checkInsertRowLock` и др.) могут давать FP по тексту внутри `/* */`] → вне scope этого change; зафиксировано кандидатом-последователем.
- [Разные ветки/продукты дают разные определения индексов] → не влияет: баг комментариев не зависит от scope, а тест `indexWrong` использует моки кандидатов.
