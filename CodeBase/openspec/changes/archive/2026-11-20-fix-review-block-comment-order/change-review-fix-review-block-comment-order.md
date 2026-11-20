# Ревью change `fix-review-block-comment-order`

Дата: 2026-11-20.
Объект ревью: артефакты `proposal.md`, `design.md`, `tasks.md`, дельта-спеки
`specs/review/deploy-stoppers/spec.md`, `specs/review/postgre-requirements/spec.md`
и код, который планируется изменить (`internal/review/review_rules.go`,
`cmd/root.go`, тесты пакета `internal/review`).

## Итог

Change **готов к реализации с одной существенной поправкой**: инвентаризация
затронутых сканеров неполна. Помимо заявленных 11 сканеров, тот же класс бага
(закрытие блочного комментария обрабатывается позже `--`) есть в
`checkPostgreLabelGotoLevel` — правило `postgreLabelGotoLevel` той же capability
`review/postgre-requirements`. Рекомендуется включить его в change (стоимость —
тот же reorder, эффект — закрытие всей capability) либо явно зафиксировать
отложенность в design/Non-Goals. Остальные находки — уточнения формулировок
и заметки для реализации, блокирующими не являются.

`openspec validate fix-review-block-comment-order --strict` — **valid**
(перепроверено в ходе ревью).

## Проверка заявлений артефактов по коду

| Заявление артефакта | Проверка по коду | Статус |
|---|---|---|
| Баг: в 11 сканерах `--` проверяется раньше закрытия `*/` | Подтверждено построчным чтением: `checkIndexWrong` (review_rules.go:268→273), `checkUpdateOnlyVar` (:488→493), `checkPTableSpid` (:675+, смещения по grep идентичны), `checkForceOrder2Tbl` (:827+), `checkSaveTran` (:963→968), `checkUseDrop` (:1025+), `checkMathOperations` (:1110+), `checkExistsWithAndInIf` (:1195→1200), `checkIndexExistsInDB` (:1306+), `checkVarcharSize` (:4688→4693), `checkUseFuncInIndCol` (:6267→6272) | Подтверждено |
| Эталон: `checkUseEqColumn` / `checkTableHintIsRight` уже корректны | `checkUseEqColumn` — блок `inBlockComment` на :3255–3265 до `trimmed`/`--` на :3279–3282; `checkTableHintIsRight` — :3901–3908 до :3910–3913 | Подтверждено |
| 10 правил — deploy-stoppers, `useFuncInIndCol` — postgre-requirements | catalog.go: SeverityDeployStopper у всех 10 (catalog.go:152–214, 285–291), `RuleUseFuncInIndCol` — SeverityPostgreReq (catalog.go:376–378) | Подтверждено |
| Severity 1 / 2 в сценариях спек | types.go:70–72: `SeverityDeployStopper = 1`, `SeverityPostgreReq = 2` | Подтверждено |
| `Object = pbpconscoolingoff.xpkpbpconscoolingoff` | review_rules.go:462 — `fmt.Sprintf("%s.%s", tableName, indexName)` после `normalizeIdentifier` | Подтверждено |
| `buildNumber` 1710 | cmd/root.go:29 — `buildNumber = "1710"` | Подтверждено |
| Кейс FA: шапка `/*---` (стр. 8), закрытие `-----*/` (стр. 20), `delete` (стр. 40) | FA/fa-contracts/Consumer/SERVER/Consumer/BPConsCoolingOff_Transfer.sql:8, 20, 40 | Подтверждено |
| Mock-seam `indexCandCache` доступен тестам | runner.go:33; используется в review_rule_coverage_test.go:236, 382, 426, 673 | Подтверждено |
| `-run`-паттерны задач существуют | Имена тестов матчатся: `TestCheckIndexWrong_*`, `TestAnalyzeStatementForUpdateOnlyVar_*`, `TestAnalyzeStatementForPTableSpid_*`, `TestCheckSaveTran_Coverage`, `TestCheckUseDrop_Coverage`, `TestCheckMathOperations_Coverage`, `TestCheckExistsWithAndInIf_Coverage`, `TestCheckIndexExistsInDB_Coverage`, `TestCheckVarcharSize_Coverage`, `TestCheckUseFuncInIndCol_Coverage` | Подтверждено |
| Команда приёмки `--rules indexWrong --json` валидна | Флаг `--rules` объявлен (cmd/review.go:488), `parseReviewRules` принимает `indexWrong` | Подтверждено |
| Существующие тесты не зависят от бага | В фикстурах тестов пакета нет закрытий блочного комментария строкой на `--`; единственный близкий кейс (review_rules_test.go:2153–2165) закрывается чистой строкой `*/` — reorder его не затрагивает | Подтверждено |
| Helper `stripLineComments` корректен, его пользователи вне списка | review_helpers.go:1111–1138 — закрытие блока внутри цикла раньше `--`; правила `nullComparison`, `emptyReturn`, `rawTransactionControl`, `diffTypesComparison` не затронуты | Подтверждено |

## Находки

### F1. Пропущен 12-й сканер с тем же багом — `checkPostgreLabelGotoLevel` (существенная)

`checkPostgreLabelGotoLevel` (review_rules.go:4785, правило `postgreLabelGotoLevel`,
`SeverityPostgreReq`, catalog.go:299–305, требование «Использование GOTO и меток»
в основной спеке postgre-requirements:79) содержит тот же дефект в более сильной
форме:

```go
// review_rules.go:4805-4816
if idx := strings.Index(line, "--"); idx >= 0 {
    line = line[:idx]          // усечение по ЛЮБОМУ "--" в строке, не только префиксу
}
if inBlockComment {
    if idx := strings.Index(line, "*/"); idx >= 0 { ... }
```

На строке закрытия `-------------------------------------------------------------------*/`
строка усекается до `""` ещё до поиска `*/`: комментарий не закрывается, весь код
после шапки скрывается от правила — тот же класс FN, что и у 11 заявленных.
Открывающая строка `/*---...` продолжает работать лишь случайно (после усечения
остаётся `/*`, и блок открывается).

Design Non-Goal «не исправлять другие места, где нет отслеживания блочного
комментария» сюда не относится: здесь отслеживание **есть**, но порядок неверен.

**Рекомендация.** Включить сканер в change как 12-й: в design-таблицу, в дельту
`postgre-requirements` (расширить формулировку требования или добавить сценарий
с `goto`/`label` после шапки), в tasks (группа 2). Исправление — тот же reorder:
блок закрытия `inBlockComment` выше усечения `--`; при этом важно сохранить саму
семантику усечения по любому `--` внутри строки (это inline-обработка правила).
Стоимость сопоставима с остальными, а capability postgre-requirements закрывается
целиком. Альтернатива — явно перенести в Non-Goals с отдельным candidate-change,
но по критерию «цена/эффект» включение выгоднее.

### F2. Уточнение к D1/D2: `trimmed` должен вычисляться после мутации строки (замечание к реализации)

В 11 сканерах `trimmed := strings.TrimSpace(line)` вычисляется до закрытия блока.
При переносе блока закрытия выше необходимо, чтобы `trimmed` вычислялся **после**
мутации `line = line[idx+2:]` (как в эталоне `checkTableHintIsRight`:
мутация :3904 → `trimmed` :3910). Иначе строка вида `-----*/ select ...` после
закрытия всё равно будет отброшена префиксной проверкой `--` по старому значению.
Формулировка задач 1.1–1.5 («перенести обработку закрытия выше TrimSpace/проверки»)
это подразумевает, но стоит проговорить явно в design D1, чтобы при реализации
не выполнить перенос только частично.

### F3. Различие эталонов: пропуск пустого остатка после `*/` (нюанс, действия не требует)

`checkUseEqColumn` после закрытия блока делает `if strings.TrimSpace(line) == "" { continue }`
(:3259–3261), `checkTableHintIsRight` — нет. Для 11 сканеров проще повторять
`checkTableHintIsRight`: пустой остаток строки закрытия ведёт себя как пустая
строка (попадает в `stmtBuffer` при открытом операторе — так же, как существующие
пустые строки). Поведение корректно, но design D2 называет два эталона сразу;
при реализации достаточно следовать одному — `checkTableHintIsRight`, чей блок
закрытия уже текстуально совпадает с блоками в 11 сканерах.

### F4. `checkVarcharSize` имеет два пути анализа (нюанс, учтён неявно)

Правило проверяет и `parsed.Procedures` (параметры процедур, review_rules.go:4658),
и сырой `DECLARE` в теле (:4677+). Баг затрагивает только второй путь; сценарий
дельты «Код после шапки виден остальным сканерам» использует `DECLARE @Var VARCHAR`
— корректная форма триггера. Замечание только для полноты: рост находок после
фикса для `varcharSize` возможен и по параметрам процедур нет — параметры уже
видны парсером и сейчас.

### F5. Дельты оформлены как ADDED-требования (мнение: допустимо)

Изменение вводит новое сквозное требование о порядке обработки комментариев вместо
MODIFIED одиннадцати поимённых требований («Использование SAVE TRAN» и т.д.).
Для сквозного механизма это разумно: MODIFIED потребовал бы 11 почти одинаковых
правок. Требование сформулировано с перечислением правил, SHALL/MUST присутствуют,
сценарии behavior-oriented, названия с существующими требованиями не конфликтуют.
Если принимать F1 — учесть `postgreLabelGotoLevel` в перечне сканеров дельты
postgre-requirements.

## Полная инвентаризация сканеров (по итогам ревью)

| Сканер | Правило | Обработка комментариев | Статус в change |
|---|---|---|---|
| checkIndexWrong, checkUpdateOnlyVar, checkPTableSpid, checkForceOrder2Tbl, checkSaveTran, checkUseDrop, checkMathOperations, checkExistsWithAndInIf, checkIndexExistsInDB, checkVarcharSize | deploy-stoppers | `--` раньше закрытия `*/` — баг | В списке (10) |
| checkUseFuncInIndCol | useFuncInIndCol | `--` раньше закрытия `*/` — баг | В списке (11) |
| **checkPostgreLabelGotoLevel** | postgreLabelGotoLevel | усечение по `--` раньше закрытия `*/` — баг | **Пропущен (F1)** |
| checkUseEqColumn (:3255), checkTableHintIsRight (:3901) | — | корректный порядок | Эталоны |
| checkNullComparison, checkEmptyReturn, checkRawTransactionControl, checkDiffTypesComparison | — | `stripLineComments` (корректно) | Вне списка, верно |
| findInsertWithoutColumns (review_parser.go:1976) | columnInsert | посимвольно, блок раньше `--` — корректно | Вне списка, верно |
| checkAnsiInJoin (:2999 `maskBlockCommentsKeepLines`) | ansiInJoin | маскирование блочных комментариев | Вне списка, верно |
| checkUseSelectAll (:2507), checkTruncTbl (:2542), checkInsertRowLock (:3142) и др. | — | нет отслеживания блочных комментариев (FP-класс: текст внутри `/* */` может совпасть с regex правила) | Вне scope — см. Non-Goals; кандидат-последователь |

## Кандидаты-последователи (вне scope, для памяти)

1. **FP-класс в сканерах без отслеживания блочных комментариев**:
   `checkUseSelectAll`, `checkTruncTbl`, `checkInsertRowLock` — текст внутри
   шапки `/* ... */` анализируется как код. Возможное решение — подход
   `maskBlockCommentsKeepLines` (как в checkAnsiInJoin:2999).
2. **Унификация обработки комментариев**: после фикса останутся минимум три
   идиома (локальные reorder-циклы, `stripLineComments`, `maskBlockCommentsKeepLines`).
   Общий helper осознанно отклонён в D1 для bug-fix; возможен отдельный
   рефакторинг-change позже.
3. AGENTS.md проекта CodeBase указывает `version 0.9.2, build 1539` — фактически
   `0.9.5`/`1710` (cmd/root.go:28–29); документ отстаёт, к change отношения
   не имеет.

## Вердикт

- Диагностика проблемы, механизм фикса (локальный reorder по эталону
  `checkTableHintIsRight`), план сборки 1710→1711 и приёмка на
  `BPConsCoolingOff_Transfer.sql` — корректны и подтверждены кодом.
- Перед apply устранить **F1** (включить `checkPostgreLabelGotoLevel` или явно
  задеферить) и учесть **F2** в формулировке design D1. F3–F5 — на усмотрение
  автора, реализацию не блокируют.
