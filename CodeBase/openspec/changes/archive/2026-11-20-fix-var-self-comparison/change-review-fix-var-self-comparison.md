# Ревью change: fix-var-self-comparison

- **Дата:** 2026-11-20
- **Ревьюер:** opencode (L3/pro)
- **Вердикт:** change в целом готов к реализации; требуется корректировка приёмки по `BPConsCoolingOff_Transfer.sql` (находка P1) и уточнение design D2 (находка P2). См. «Рекомендации».

## Методика

Ревью выполнено на фактическом коде и данных, без правок кода CodeBase:

1. Прочитаны все артефакты change (`proposal.md`, `design.md`, `tasks.md`, `specs/review/deploy-stoppers/spec.md`, `.openspec.yaml`) и основная спека `openspec/specs/review/deploy-stoppers/spec.md`.
2. Прочитан затрагиваемый код: `analyzeConditionForEqColumn`/`checkUseEqColumn` (`review_rules.go:3236-3412`), `extractUnqualifiedConditionColumns`/`collectColumnsFromConditionExpression` (`review_helpers.go:375-570`), `checkIndexWrong`/`analyzeStatementForIndexWrong` (`review_rules.go:250-468`), `catalog.go:124-130`, `cmd/root.go:29`, `findConditionStart` (`review_parser.go:1372-1415`).
3. Прочитаны эталонные FA-файлы: `BPConsConditionInsert.sql:1705-1734`, `BPConsCoolingOff_Transfer.sql:1-64`.
4. Выполнены CLI-прогоны на реальных файлах (build 1709):
   - `codebase review BPConsConditionInsert.sql --json` (полный набор правил и `--rules useEqColumn,indexWrong`);
   - `codebase review BPConsCoolingOff_Transfer.sql --json --rules useEqColumn,indexWrong`.
5. Выполнены временные отладочные Go-тесты в `internal/review` (созданы и удалены по завершении; git-статус чист): репликация сканера `checkIndexWrong` на expanded-контенте, прямой SQL к `sql_index_definitions`/`files`, полный путь `RunSQLFileCtx` на реальной БД.
6. `openspec validate fix-var-self-comparison --strict` — **valid**.

## Верификация заявлений change

| # | Заявление | Проверка | Статус |
|---|-----------|----------|--------|
| 1 | Опечатка `@BankProductID = @BankProductID` в DELETE на стр. 1723 | Чтение файла: строки 1720-1724 подтверждены дословно | Подтверждено |
| 2 | «До»: ConditionInsert — 33 findings (23 procParamDefValue, 8 datatype, 1 maxProcParam, 1 indexWrong@1720), useEqColumn 0 | CLI-прогон полного набора: `total: 33`, `by_rule` в точности совпадает; indexWrong@1720 c рекомендацией XIE1 | Подтверждено |
| 3 | `reEqOperands` уже захватывает `@`-операнды; `@`-skip на `review_rules.go:3370` | Код: `review_rules.go:49` (регэксп), `:3370` (skip) | Подтверждено |
| 4 | `extractUnqualifiedConditionColumns` считает идентификатор внутри `@X` колонкой | Отладочный тест на where-фрагменте stmt@1720: `unqualified(where) = {spid, bankproductid}` — `bankproductid` приходит именно из `@BankProductID` | Подтверждено |
| 5 | Скоринг: для cols `{spid, bankproductid}` XIE1(SPID,BankProductID,AmountMin)=2 > XPK(SPID,AutoID)=1; после фикса cols `{spid}` → 1=1 → finding исчезает | Прямой SQL к БД: кандидаты XPK/XIE1/XIE2 в ветках 54/55/56 продукта 141; `calculateIndexPrefixMatch` (`review_helpers.go:248`) | Подтверждено |
| 6 | `findConditionStart` ограничен `where`/`on`/`having` → EXEC-аргументы (`exec ... @BankProductID = @BankProductID` на стр. 1705/1710/1728) не анализируются | `review_parser.go:1372-1381` | Подтверждено |
| 7 | Битовые `&`/`|`/`^` гасят всё условие (`containsBitwiseOperator` до регэкспов) | `review_rules.go:3348-3354` — if-условия 1718-1719 с `&` не мешают WHERE-буферу 1722-1723 | Подтверждено |
| 8 | Описание `RuleUseEqColumn` не соответствует семантике | `catalog.go:126` — «Сравнение по столбцу без индекса» (не «с самим собой»); задача 3.1 оправдана | Подтверждено |
| 9 | bump buildNumber 1709 → 1710 | `cmd/root.go:29` — `buildNumber = "1709"`; CLI: «0.9.5 build 1709» | Подтверждено |
| 10 | Дельта-спека: требования MODIFIED существуют, формат корректен | Основная спека: «Сравнение столбца с самим собой» (стр. 266), «Неправильное использование индекса для p-таблиц» (стр. 292) — названия совпадают; `--strict` валиден | Подтверждено |
| 11 | «После»: indexWrong@1720 исчезнет, useEqColumn@1723 появится | Логический вывод из №4+№5 (не прогонялось — код не менялся) | Обосновано |
| 12 | В предикатах FA 3 живых самосравнения (Contracts 1, GeneralLedger 2) | Не проверялось; косвенно подтвердится задачей 4.3 (батч: +1 ровно на 1723) | Принято на веру |

## Находки

### P1 (существенная): приёмка по `BPConsCoolingOff_Transfer.sql` опирается на ложную посылку

**Факт:** CLI даёт 0 findings для Transfer.sql — но **не потому, что форма условия корректна**.

**Причина (вскрыта трассировкой сканера):** в `checkIndexWrong` (`review_rules.go:268-271`) проверка строки на `--`-префикс стоит **до** ветки закрытия блочного комментария (`:273-280`). Закрывающая строка шапки процедуры `------...---*/` (Transfer.sql:20) начинается с `--`, ловится первым фильтром и выполянет `continue`, не сбросив `inBlockComment`. В результате **весь файл после строки 20 не сканируется правилом indexWrong**. Трассировка дословной копии сканера показала: ни одно утверждение Transfer.sql не доходит до анализа (в отличие от ConditionInsert, где блочной шапки нет и stmt@1720 анализируется).

**Следствия для change:**

1. **D4/задача 4.2 формально пройдут** (0 indexWrong до и после), но эталон «корректная форма `BankProductID = @BankProductID` не даёт indexWrong» реальным файлом **не проверяется** — файл просто мёртв для правила.
2. **Синтетический тест 2.3** (та же форма DELETE в изоляции даёт indexWrong на XIE1) и **4.2** (реальный файл — не даёт) выглядят противоречием; теперь оно объяснимо, но в tasks/design это противоречие не разъяснено.
3. Если сканер когда-нибудь починят, на Transfer.sql появится **легитимный** indexWrong@40 (по симуляции: cols `{spid, bankproductid}` → XIE1=2 > XPK=1) — ровно как ожидает 2.3.
4. Класс системный: шапка `/* ... ---*/` — конвенция Diasoft, т.е. indexWrong (и `checkUpdateOnlyVar` — тот же порядок проверок, `review_rules.go:488-500`) фактически отключены для большей части файлов FA. `checkUseEqColumn` порядком **не поражён** (там блочный комментарий обрабатывается первым, `review_rules.go:3254-3277`) — поэтому useEqColumn-часть приёмки 4.2 корректна.

**Рекомендации:**

- В рамках этого change: переформулировать задачу 4.2 (и D4-эталон) — зафиксировать, что для Transfer.sql проверяется только useEqColumn=0 (сканер useEqColumn жив), а индексная корректность формы покрывается синтетическим тестом 2.3; реальный файл не проходит indexWrong из-за независимого бага сканера.
- Завести **отдельный change** на фикс порядка `--`/блочного комментария в `checkIndexWrong` (+ `checkUpdateOnlyVar`): цена минимальна (перестановка двух блоков), эффект высокий (возврат покрытия indexWrong на файлах с конвенциональной шапкой). В его приёмку включить Transfer.sql: ожидается indexWrong@40 на XIE1.

### P2 (средняя): механизм отсечения `@@spid = @@spid` не описан в design

`reEqOperands = (?i)(@?\w+(?:\.\w+)?)...` матчит `@@spid` как операнд `@spid` — **матч начинается со второй `@`**. Простая реализация «leftRaw == rightRaw и оба с `@`» даст ложное срабатывание на `@@spid = @@spid`. D2 формулирует требование («ровно один `@`»), но не описывает механизм; техника пред-символьной проверки описана только в D3 для helper'а. Тест задачи 1.1 (`@@spid = @@spid` → false) этот риск ловит, однако design стоит дополнить явной фразой: «символ непосредственно перед началом матча операнда не должен быть `@`» — как в D3. Иначе высок риск первой реализации с FP.

### P3 (мелкие замечания)

1. **tasks 2.1, формулировка теста:** `where SPID = @@spid and @BankProductID = @BankProductID` даёт «только `spid`». Верно, но стоит добавить в тест и правый операнд с `@` перед оператором (`@BankProductID = BankProductID`, обратный порядок) — фильтр по индексу матча обязан покрывать и его (сейчас этот порядок тоже даёт FP-колонку).
2. **proposal Impact:** «≈42 тыс. в fa-contracts» для EXEC-аргументов — не проверялось, цифра из исследования автора; на выводы не влияет.
3. **design D2, ключ дедупликации** `var:<lower>`: корректно расходится с колонковым `seen`; граничный кейс `where @X = @X and A.ID = A.ID` даст 2 finding'а (var + column) — это ожидаемо, но полезно зафиксировать табличным тестом в 1.3.
4. **AGENTS.md CodeBase** указывает версию «0.9.2, build 1539» — фактическая 0.9.5/1709; к change не относится, отмечено попутно.
5. `--files` для батча (задача 4.3) CLI поддерживает — проверено по `codebase review --help`.

## Соответствие артефактов правилам OpenSpec

| Артефакт | Проверка | Статус |
|----------|----------|--------|
| `proposal.md` | Секции Why/What Changes/Capabilities/Impact; Modified Capability `review/deploy-stoppers` совпадает с путём в `openspec/specs/` | Соответствует |
| `design.md` | Context/Goals-Non-Goals/Decisions/Risks/Migration/Open Questions; ссылки на код точны (`review_rules.go:3370`, `review_helpers.go:48` проверены) | Соответствует (см. P2) |
| `tasks.md` | Группы `## 1.`–`## 4.`, чек-боксы, проверки с конкретными командами и числами | Соответствует (см. P1 по 4.2) |
| `specs/.../spec.md` | `## MODIFIED Requirements` (мн.ч.); названия требований совпадают с основными; полный текст воспроизведён вместе с прежними сценариями; новые сценарии GIVEN/WHEN/THEN; SHALL/SHALL NOT в телах | Соответствует |
| `.openspec.yaml` | schema: spec-driven | Ок |
| `openspec validate --strict` | | **Valid** |

## Оценка решений дизайна

- **D1 (расширение useEqColumn, а не новое правило)** — поддерживаю: тот же контекст WHERE/ON/HAVING, та же severity 1, минимальная поверхность.
- **D2 (порядок проверок)** — верен по сути; требует уточнения механизма отсечения `@@` (P2).
- **D3 (фильтр по индексу матча, а не lookbehind)** — поддерживаю: RE2 без lookbehind, фильтр закрывает и `@name`, и `@@name`, не ломает соседние совпадения.
- **D4 (приёмочные эталоны)** — пункт по ConditionInsert подтверждён полностью (33→33, состав и строки); пункт по Transfer.sql — см. P1.

## Итог

Change корректно диагностирует реальный дефект (FA-баг подтверждён), предложения по коду минимальны и проверяемы, дельта-спека оформлена по правилам. К реализации допускать после устранения P1 (переформулировка приёмки 4.2/D4 или осознанная фиксация причины в задаче) и P2 (уточнение D2). Дополнительно рекомендован change-последователь: фикс порядка `--`/блочного комментария в сканерах `checkIndexWrong`/`checkUpdateOnlyVar`.
