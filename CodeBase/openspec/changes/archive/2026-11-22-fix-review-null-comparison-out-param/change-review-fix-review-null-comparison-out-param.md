# Ревью change `fix-review-null-comparison-out-param`

Дата: 2026-11-22.
Объект ревью: артефакты `proposal.md`, `tasks.md`, дельта-спека
`specs/review/postgre-requirements/spec.md`, `.openspec.yaml`; код, который
планируется изменить (`internal/review/types.go`, `internal/review/review_rules.go`,
`internal/review/review_rules_test.go`, `cmd/root.go`); реальные данные FA
(`Card_OffB_CheckClient.sql` и выборка из 4 файлов приёмки).

## Итог

Change **готов к реализации**. Блокирующих проблем нет: диагноз подтверждён
кодом, предложенная регулярка эмпирически проверена на всём корпусе FA
(после фикса — 0 оставшихся срабатываний, регрессий на истинных сравнениях не
найдено), номера строк в proposal/tasks точны, `openspec validate --strict` —
**valid**.

Замечания носят характер улучшения тестов и формулировок (F1–F3), их можно
внести до или во время apply; F4–F7 — информационные.

## 1. Валидация и согласованность артефактов

- `openspec validate fix-review-null-comparison-out-param --strict` — **valid**.
- `proposal.md` содержит все обязательные секции (`## Why`, `## What Changes`,
  `## Capabilities` с `### New Capabilities`/`### Modified Capabilities`,
  `## Impact`); путь capability `review/postgre-requirements` совпадает с
  каталогом основной спеки.
- Дельта: `## MODIFIED Requirements` (уровень 2), `### Requirement: Сравнение с
  NULL через =/<>` (уровень 3) — заголовок **точно** совпадает с основным
  требованием (`openspec/specs/review/postgre-requirements/spec.md:9`),
  существующий сценарий «Сравнение с NULL через =» воспроизведён, добавлены 4
  новых сценария GIVEN/WHEN/THEN, английский `SHALL` в теле есть.
- Отсутствие `design.md` приемлемо: изменение локальное (одна регулярка + тесты),
  cross-cutting-решений, зависимостей и миграций нет; валидатор это допускает.
- Числа в proposal проверены: `= null out` — **2363** вхождения в **823** файлах
  (2361 в 822 `.sql` + 2 в 1 `.h`, `fa-administrator/.../DealProtCheck.h:353-354`).
  Точность подтверждена.

## 2. Проверка заявлений артефактов по коду

| Заявление артефакта | Проверка по коду | Статус |
|---|---|---|
| `nullParamDefaultRe` (`types.go:142`) знает `output`, но не `out` | `@\w+\s+\w+(?:\s*\([^)]*\))?\s*=\s*null\s*(?:output\s*)?,?\s*$` — группа `(?:output\s*)?` не покрывает `out` | ✔ |
| Сканер пропускает объявления до `nullComparisonBinaryRe` | `review_rules.go:4105` — skip при `nullParamDefaultRe`, `:4115` — детекция `nullComparisonBinaryRe` | ✔ |
| `nullSelectAssignRe` не спасает ситуацию | `types.go:147` требует `@Var = null` без типа; хвост `out` мешает | ✔ |
| Severity 2 | `SeverityPostgreReq = 2` (`types.go:71`), `RuleNullComparison` → `SeverityPostgreReq` (`catalog.go:215-217`) | ✔ |
| Строки 9–10 и 2 findings сейчас | `Card_OffB_CheckClient.sql:9-10`, бинарь build 1713 → `count: 2` | ✔ |
| buildNumber сейчас 1713 | `cmd/root.go:29` `buildNumber = "1713"` | ✔ |
| `out` уже признаётся синонимом `output` в других местах review | `review_helpers.go:2142` (`case "output", "out", ...`) | ✔ (см. F7) |
| Заданные тест-кейсы существуют | `TestCheckNullComparison_NoFalsePositive` (`review_rules_test.go:3331`), кейсы `output` — `:3347-3349` | ✔ |

Поведение фильтра `--min-severity` проверено: при явном `--rules nullComparison`
finding severity 2 выводится и без `--min-severity` (фильтр `finding.Severity <=
minSeverity`, `runner.go:240`), поэтому команды в tasks 2.1/2.2 корректны.

## 3. Эмпирическая проверка регулярки

Смоделировал новый скан на Go (тот же `stripLineComments` из
`review_helpers.go:1112`, RE2) на всех 2361 строках `= null out` из `.sql`-файлов
FA (файл строк — `Tests/Temp/regexcheck`):

| Метрика | Старая регулярка | Новая регулярка |
|---|---|---|
| Пропущено объявлений | 0 | **2357** |
| Осталось findings | 2361 | **0** |

Оставшиеся 4 строки — целиком закомментированные (`-- @x ... = null out`); они
отсекаются проверкой `strings.TrimSpace(stripped) == ""` (`review_rules.go:4100`)
и finding не дают. Плюс 2 строки в `DealProtCheck.h:353-354` также матчатся новой
регуляркой.

Точечные кейсы (новый шаблон): `= NULL OUT,`, `= null Out ,`, `= null out ,`,
`= null output,`, `varchar(20) = Null ,` — пропускаются; `@p INT = null outs`,
`WHERE x = NULL`, `WHERE NULL = x`, `UPDATE t SET c = NULL OUTPUT inserted.c`,
`select @x = NULL OUTPUT inserted.c`, `@p INT = null readonly` — **не**
пропускаются. Регрессий/ложных отрицаний не обнаружено.

Приёмка «до» (бинарь build 1713): `Card_OffB_CheckClient.sql` — 2 findings
(строки 9–10); образцы из tasks 2.2 — `SETPRTY_Generate.sql` 1,
`SecSwift_LongLine.sql` 1, `ValueByBlockStr.sql` 3, `CBRF_Maket_Unload.sql` 1.
Все пути из tasks существуют.

По корпусу expected reduction ≈ **2359** findings (2357 `.sql` + 2 `.h`);
4 закомментированные строки findings не дают и сейчас.

## 4. Находки

### F1. Тесты не покрывают `OUT` в верхнем регистре и хвостовой комментарий (мелкое)

Task 1.2 добавляет `= null out,`, `= null out`, `= NULL out,` — ни один кейс не
проверяет `OUT`/`Out` и строку с хвостовым `--`-комментарием, хотя именно
`(?i)` и `stripLineComments` обеспечивают эти сценарии. Сценарий дельты
«...в верхнем регистре» фактически использует `NULL out,` (в верхнем регистре
только `NULL`), т.е. название сценария не соответствует примеру.

**Рекомендация.** Добавить в `NoFalsePositive`:
`@ProtocolID DSIDENTIFIER = NULL OUT,`, `@x DSMONEY = null Out ,`,
`@Message DSCOMMENT = null out, -- passthrough`; в дельте либо переименовать
сценарий, либо привести пример с `OUT`.

### F2. Приёмка 2.2 не доказывает сохранение true-positive (среднее)

В 4 перечисленных файлах нет ни одного истинного сравнения `= NULL` в теле
(только объявления с `= NULL`/`= null out` и `@DateUFEBS40 = NULLDATE`, который
корректно не матчится из-за `\b`). Фраза «истинные сравнения (если есть)
по-прежнему детектируются» на этой выборке проверяема только формально.

**Рекомендация.** Либо добавить в приёмку файл с настоящим `WHERE x = NULL` в
теле, либо явно сослаться на unit-кейсы `TestCheckNullComparison_Detects`
(`review_rules_test.go:3288-3298`) как доказательство отсутствия FN.

### F3. Проверка task 1.1 ограничена `go build` (мелкое)

После правки регулярки недостаточно `go build ./...`; нужен прогон пакета.

**Рекомендация.** В 1.1 добавить `go test ./internal/review/ -run
TestCheckNullComparison` (или полный `go test ./internal/review/`).

### F4. Формулировка «2363 guaranteed-FP» слегка завышена (info)

2363 — это вхождения `= null out`, а findings дают 2359 строк (4 строки целиком
закомментированы). На решение не влияет; при желании уточнить «2359 findings
(2363 вхождения, из них 4 — в комментариях)». Также в proposal сказано «таблица
тестов `nullParamDefault`» — фактический тест называется
`TestCheckNullComparison_NoFalsePositive`.

### F5. Ограничение линейного якоря `$` (info, принято)

Пропуск срабатывает только если объявление заканчивает строку. Варианты
`= null out)`/`= null out;` или несколько параметров на одной строке останутся FP.
Проверка корпуса FA: таких вхождений нет (`out` за пределами `,`/пробелов/`--`
встречается только в комментариях), поэтому ограничение приемлемо. Можно
зафиксировать его одной строкой в комментарии над регуляркой.

### F6. Эквивалентная запись альтернативы (info)

`out(?:put)?` работает, но читается менее очевидно, чем `(?:output|out)`.
Функционально эквивалентно (RE2, `output` матчится через опциональный `put`),
эмпирически проверено — менять не обязательно.

### F7. Согласованность с парсером направлений (info, плюс)

`review_helpers.go:2142` уже считает `out` синонимом `output` при разборе
направления параметра; правка приводит `nullParamDefaultRe` в соответствие с
этой логикой. Хороший аргумент, его стоит упомянуть в комментарии к регулярке.

## 5. Вердикт

- Диагностика FP (`out` не покрыт `nullParamDefaultRe`), механизм (расширение
  группы направления), build 1713→1714 и приёмка на реальном
  `Card_OffB_CheckClient.sql` — корректны и подтверждены кодом и данными.
- Предложенная регулярка полностью устраняет класс FP в корпусе FA (2357
  объявлений в `.sql` + 2 в `.h`, 0 оставшихся) и не даёт ложных отрицаний на
  проверенных кейсах, включая `OUTPUT`-клаузы `UPDATE`/`SELECT`.
- Перед/во время apply рекомендуется закрыть **F1** (тесты/название сценария) и
  **F2** (доказательство сохранения true-positive), **F3** — по желанию.
  Блокирующих замечаний нет — change можно переводить в реализацию.
