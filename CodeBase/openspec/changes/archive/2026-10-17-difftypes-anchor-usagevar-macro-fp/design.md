## Context

См. proposal.md — Why. Ключевые факты текущей реализации:

- SQL-парсер (`internal/parser/sql/sql_parser.go:812-874`) собирает строки оператора в буфер и flush'ит его на пустой строке (1044-1048), control-flow (1032-1038), `go`, границах процедур. Standalone-макросы `M_*` границами не являются → `M_DELETE_PTABLE(...)` становится первой строкой фрагмента, а INSERT…SELECT приклеивается к нему; `select @RowCount = ...` склеивается с последующими `M_BUSINESSLOG_PARAM(...)`.
- `checkDiffTypesComparison` (`internal/review/review_rules.go:5852`) для фрагментного пути берёт `Line: fragment.LineNumber` (5949) — без поиска позиции совпадения. (Построчный IF-путь, 5958-6010, уже якорится корректно — его не трогаем.)
- `checkUsageVarInSameSelect` (5009-5092): dotall-регекс `reVarAssignInSelect = (?is)^(@\w+)\s*=\s*(.+)$` при отсутствии top-level FROM проглатывает хвост фрагмента в expression; `containsVarReference` (`review_helpers.go:2334`) матчит `@var\b` подстрокой, в т.ч. внутри `@@rowcount` и внутри склеенных макросов.
- `removeComments` — blank-preserving (сохраняет `\n` и длину строк) после change `tablefullscan-derived-table` → построчная адресация `fragment.QueryText` валидно отображается на файл.
- У `containsVarReference` и `findAssignmentLineOffset` ровно по одному потребителю — `checkUsageVarInSameSelect`; правки helpers не задевают другие правила.

## Goals / Non-Goals

**Goals:**

- Якорь `diffTypesComparison` — строка фактического сравнения внутри фрагмента.
- `usageVarInSameSelect` — иммунитет к standalone-макросам и системным `@@`-переменным.
- Нулевая регрессия остальных finding'ов на контрольном корпусе FA.

**Non-Goals:**

- Не меняем фрагментацию SQL-парсера (макросы продолжают склеиваться с операторами) — это затронуло бы `query_fragments`, отношения и все правила разом; при необходимости отдельный change.
- Не меняем `truncateAtStatementBoundary`, `splitTopLevelCSV`, `parseSelectAssignStatement` — макросы вырезаем раньше и адресно.
- Не расширяем список boundary-ключевых слов для макросов (риск задеть inline-макросы `M_NOLOCK_INDEX(...)`, `M_FORCEORDER` и т.п.).

## Decisions

### D1. Реанкор diffTypesComparison: offset совпадения в QueryText

Для сработавшего comparison ищем в `fragment.QueryText` (построчно, case-insensitive) первую строку, содержащую оба операнда (trimmed `left` и `right`); `Line = fragment.LineNumber + номер_строки_совпадения`. Fallback: операнды на разных строках или не найдены → `fragment.LineNumber` (текущее поведение).

Альтернатива — реанкор по образцу `findStatementStartForTableHintExists` (min-position + insert), как в `tableFullScan`: отклонена, т.к. даёт строку начала оператора (insert), а не строку сравнения; для diffTypes цель — именно сравнение.

Почему строка, а не точная колонка: rule-контракт (`Finding.Line`) — строкочная адресация; достаточно строки.

### D2. Вырезание standalone-макросов в checkUsageVarInSameSelect

До `truncateAtStatementBoundary`/`parseSelectAssignStatement` вырезаем из `fragment.QueryText` строки-макросы: построчный regexp `(?i)^\s*M_[A-Z0-9_]+\s*\(.*\)\s*$` (строка целиком — вызов макроса). Замена на пустую строку с сохранением `\n` (нумерация строк для `findAssignmentLineOffset` не плывёт).

Ограничение per-line: макросы с многострочными аргументами не покрываются — в кодовой базе FA `M_BUSINESSLOG_PARAM`/`M_LOG_TABLE`/`M_DELETE_PTABLE` однострочны (проверяется контрольным прогоном). Inline-макросы не матчатся: `M_FORCEORDER`/`M_ISOLAT` без скобок, `M_NOLOCK_INDEX(...)` всегда на строке с другим содержимым.

Альтернатива — учить парсер flush'ить на standalone-макросах: отклонена (см. Non-Goals).

### D3. @@-guard в containsVarReference

RE2 не поддерживает lookbehind → вместо `(?<!@)@var\b` ищем все `FindAllStringIndex` вхождений `@var\b` и отбрасываем те, где предыдущий байт — `@`. Единственный потребитель — `checkUsageVarInSameSelect`, побочных эффектов на другие правила нет.

### D4. Контрольный прогон

Базлайн: build 1600 на `FA/fa-contracts/Consumer/SERVER/Accrual` (~470 файлов). Ожидаемый дифф после правок: line-shift `diffTypesComparison` на строку сравнения (MassInsert: 228 → 242), исчезновение FP `usageVarInSameSelect` (MassInsert: 506), ничего больше.

## Risks / Trade-offs

- [Strip-регекс зацепит значимую строку] → паттерн anchored `^...$` с обязательными скобками; контрольный прогон на Accrual с диффом по всем правилам.
- [Совпадение операндов comparison найдено не на той строке (дубль текста в UNION-ветках)] → берём первое вхождение; якорь остаётся внутри корректного оператора, детализация «какая из веток» несущественна для severity-2.
- [Многострочные макросы] → не покрываются (см. D2); при обнаружении в корпусе — расширить на следующий flush-контекст отдельной задачей.
- [Дифф больше ожидаемого] → каждый новый исчезнувший/появившийся finding разбирается вручную до archive.

## Migration Plan

1. Правки + тесты → `go build/vet/test ./...`.
2. Контрольный прогон (D4), сверка диффа.
3. build number 1601, пересборка `CodeBase.exe`.

Откат — единый коммит, механика правил независима от схемы БД и индекса.
