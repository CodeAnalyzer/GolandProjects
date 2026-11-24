## Why

Правило `statementsWithJoinsRequireAliases` даёт ложное срабатывание на неявном алиасе столбца в списке выборки: в `SELECT u.Brief meUser` (алиас без ключевого слова `AS`) правило удаляет квалифицированную ссылку `u.Brief` и трактует оставшийся идентификатор `meUser` как неквалифицированную ссылку на столбец. Паттерн `expr alias` массовый (в `fa-payments` — более 1000 совпадений в 233 файлах), поэтому шум в отчётах review существенный. Заодно устранён дрейф требования: спека описывает проверку «алиасы у таблиц», тогда как фактическое поведение — квалификация ссылок на столбцы.

## What Changes

- Правило `statementsWithJoinsRequireAliases` перестаёт считать неявный алиас столбца (`expr alias` без `AS`) ссылкой на столбец в списке выборки.
- Срезание неявного алиаса выполняется с жёстким guard'ом по символу перед пробелом: алиасом считается только хвостовой идентификатор, перед которым стоит «конец значения» (`[A-Za-z0-9_)\]]`, закрывающая кавычка) на верхнем уровне и вне строковых литералов; операторы/продолжение выражения (`a + b`) алиасом не признаются.
- Учитываются модификаторы списка выборки `DISTINCT`/`ALL`/`TOP n [PERCENT] [WITH TIES]`: они не должны приводить к ошибочному срезанию реальной колонки (`SELECT TOP 10 col`).
- Поведение для UPDATE SET, WHERE и ON-условий не меняется (неявных алиасов там нет).
- `truncateAtStatementBoundary` становится CASE-aware (отслеживает `caseDepth`, как `findTopLevelKeywordPosition`): запросы с top-level `CASE…END` в SELECT перестают обрезаться до `from` и начинают анализироваться правилом.
- Закрываются смежные FP-классы, вскрытые расширением: assignment-style алиасы (`alias = expr`), строки препроцессора (`#ifdef`/`#else`/`#endif`) и аргументы `M_DELETE_PTABLE(...)`, квалификация с пробелом вокруг точки (`alias. column`), ORDER BY по выходным алиасам (ORDER BY не сканируется).
- Второй потребитель CASE-aware `truncateAtStatementBoundary` — `usageVarInSameSelect`: комментарии срезаются перед разбором assignment'ов, чтобы `--@var` в конце выражения не считался чтением переменной.
- Требование «JOIN без алиасов таблиц» в `review/postgre-requirements` приведено к фактическому поведению: проверяется квалификация ссылок на столбцы, а не наличие алиасов у самих таблиц; исправлен неверный сценарий и добавлены сценарии на неявный алиас и guard.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/postgre-requirements`: уточняется требование о неквалифицированных ссылках в запросах с JOIN — неявный алиас столбца (`expr alias`) MUST NOT считаться ссылкой; фиксируется guard и реальная семантика проверки (квалификация столбцов, а не алиасы таблиц).

## Impact

- Код: `internal/review/review_rules.go` (`checkStatementsWithJoinsRequireAliases`, блок списка выборки), новый helper срезания неявного алиаса в `internal/review/review_parser.go`, CASE-aware `truncateAtStatementBoundary`; `internal/review/review_rules_test.go` и `review_parser_test.go` (позитивные/негативные/регрессионные тесты).
- Поведение review: меньше findings `statementsWithJoinsRequireAliases` на неявных и assignment-style алиасах, препроцессоре, p-табличных макросах и ORDER BY; `usageVarInSameSelect` не считает `@var` внутри комментария. Замер по `fa-payments/Payments/SERVER`: `statementsWithJoinsRequireAliases` 138 → 87 (новых 0), `usageVarInSameSelect` 0 → 0 (новых 0).
- Спека: `openspec/specs/review/postgre-requirements/spec.md` (через дельту).
- БД/индексация/API/MCP: без изменений.
