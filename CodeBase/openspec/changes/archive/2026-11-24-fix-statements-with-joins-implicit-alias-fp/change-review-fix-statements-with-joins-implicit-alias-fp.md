# Change review: fix-statements-with-joins-implicit-alias-fp

- **Объект ревью:** `openspec/changes/fix-statements-with-joins-implicit-alias-fp/`
- **Тип:** ревью планируемого изменения (proposal / design / tasks / delta + анализ кода под правку)
- **Дата:** 2026-11-24
- **Статус:** APPROVED — можно приступать к реализации; замечания ниже non-blocking (носят характер уточнений и защиты приёмки).

## Вердикт

Изменение корректно по постановке и реализуемо. Ложное срабатывание (FP) подтверждено на реальном
эталоне; предложенный алгоритм (вариант A: guard по символу перед пробелом + снятие модификаторов
выборки) проверен прототипом и даёт ожидаемый результат на всех сценариях спеки. Замечания — по
измеримости приёмки и по полноте артефактов, блокеров нет.

## Что проверено

- Артефакты: `proposal.md`, `design.md`, `tasks.md`, дельта `specs/review/postgre-requirements/spec.md`.
- `openspec validate fix-statements-with-joins-implicit-alias-fp --strict` → **valid**.
- `openspec show ... --deltas-only` → 1 дельта `MODIFIED`, требование совпадает с основной спекой, 8 сценариев распознаны.
- Код под правку: `checkStatementsWithJoinsRequireAliases` (`internal/review/review_rules.go:6166`),
  `findAllUnqualifiedColumnRefs` (`internal/review/review_parser.go:2347`), существующие
  `extractColumnAliasName` (`:2989`), `reParserTopN` (`:55`), `findColLineOffsetInPart` (`:2333`),
  фильтр severity (`internal/review/runner.go:240`), регистрация правила (`internal/review/catalog.go:369`).
- Реальные замеры на эталоне `FA/fa-payments/Payments/SERVER/NonBalVal_f/NonBalVal_SelectHistory.sql`.

## Подтверждение проблемы (FP) на реальных данных

Свежая сборка из HEAD, прогон по правилу:

```
CodeBase.exe review NonBalVal_SelectHistory.sql --rules statementsWithJoinsRequireAliases --json
```

Результат: `1` finding, `severity 2`, `line 58`, `object = "meUser"`, `current_product_id = 148`.
Строка 58 источника — `u.Brief meUser` в запросе с соединением через запятую
(`from hDealTransact dt, pAPI_User_FindLst u`). Причина ровно как в design: явного `AS` нет,
`findAllUnqualifiedColumnRefs` удаляет `u.Brief` (qualified ref), а `meUser` остаётся.
`extractTablesFromFromClause` для этого запроса возвращает 2 таблицы — правило срабатывает.

Вывод: мотивация proposal фактически верна.

## Замечания

### R1 (High, принять) — методика приёмки: `--min-severity` скрывает finding severity 2

Фильтр в `runner.go:240` — `if finding.Severity <= minSeverity`, то есть **меньшее число = важнее**
(по умолчанию `minSeverity = 3`). `statementsWithJoinsRequireAliases` имеет `severity 2`, поэтому
прогон с `--min-severity 1` даёт `2 <= 1 == false` и **прячет** finding. Это легко принять за
«FP исчез».

- Факт: `--min-severity 1` → `count: 0`; прогон по умолчанию (3) → `count: 1` (объект `meUser`).
- Риск для tasks 4.2/4.3: если гонять до/после с `--min-severity 1`, «до» тоже покажет 0 и сравнение будет ложным.
- **Решение:** в tasks 4.2/4.3 явно зафиксировать использование дефолтного `min-severity` (или `>= 3`), не `1`. Добавить фразу в `tasks.md` (например, в 4.3: «использовать min-severity по умолчанию — severity правила = 2»).

### R2 (Medium, принять) — в design не учтены существующие helper'ы, пересекающиеся по смыслу

В `review_parser.go` уже есть `extractColumnAliasName` (`:2989`, разбирает `col AS alias` / `col alias`)
и `reParserTopN` (`:55`, `^\s*top\s+\d+\s+`). План вводит новый `stripImplicitSelectAlias`, их не упоминая.

- Почему новый helper оправдан: guard у `extractColumnAliasName` слабее (в `a + b` он вернёт `b` как
  алиас — это дало бы FN), а `reParserTopN` не покрывает `TOP (@n) PERCENT WITH TIES` из теста 1.2.
- **Решение:** добавить в `design.md` (D1/D3) абзац «почему не переиспользуем существующее»: guard
  по символу перед пробелом строже; `reParserTopN` расширяем/не переиспользуем из-за `(@n)/PERCENT/WITH TIES`.
  Это снимает риск дублирования и «изобретения» второго похожего парсера алиасов.

### R3 (Low, принять) — дрейф в тексте требования: не упомянут UPDATE SET

Тело нового требования перечисляет «список выборки, `WHERE` и `ON`», но правило также проверяет
правую часть `UPDATE ... SET` (существующий тест `_UpdateWithJoin_Finding`). Раз change позиционируется
как «устранение дрейфа», отсутствие `UPDATE SET` — новый мелкий дрейф.

- **Решение:** добавить `UPDATE SET` в перечисление тела требования (и, при желании, один сценарий).
  Либо явно вынести в Non-Goals с обоснованием. Рекомендую первое — дешевле и точнее.

### R4 (Low, принять частично) — заголовок требования по-прежнему про «алиасы таблиц»

`### Requirement: JOIN без алиасов таблиц` и сценарий «JOIN без алиаса» описывают уже не алиасы таблиц,
а квалификацию столбцов. D5 осознанно сохраняет имя ради стабильной связи дельты.

- **Решение:** имя оставляем (согласен с D5, RENAMED усложняет архивацию без выигрыша), но добавить в
  `spec.md` секцию `## Notes` (или строку) о том, что заголовок исторический, реальная семантика —
  квалификация ссылок на столбцы. Это бинарно закрывает вопрос без переименования.

### R5 (Low, принять) — в design не зафиксирован CASE…END в «верхнем уровне»

D1 говорит про «вне скобок и строковых литералов», но `CASE…END` — тоже top-level конструкция.
Прототип показывает: хвостовой `end` может «срезаться» guard'ом (безвредно — `end` ключевое слово,
в findings не попадает), но это поведение не задокументировано.

- **Решение:** добавить в design одну фразу про `CASE…END` (срезание `end` безопасно) и/или регресс-тест
  `case when a then b else c end` без алиаса → findings остаются по `a`,`b`,`c`. Не обязательно, но
  закрывает вопрос поведения.

### R6 (Low, принять) — `findColLineOffsetInPart` получает уже укороченный `trimmed`

После снятия модификаторов/алиаса в `trimmed` уходит укороченная строка, а `findColLineOffsetInPart`
(`:2333`) ищет её как подстроку в `queryText`. Для `DISTINCT/TOP`-ветки это повышает шанс совпасть с
более ранним вхождением (например, `col` внутри `col1`), что влияет только на номер строки в finding,
но не на корректность.

- **Решение:** при реализации вычислять `lineOffset` от исходной (до среза) части либо учитывать это
  как известное ограничение. Малый приоритет.

### R7 (Info) — величина «>1000 в 233 файлах» в proposal не перепроверялась

В этом ревью масштаб по корпусу не пересчитывался (4550 файлов в `SERVER`). Это ожидаемо для tasks 4.3.

- **Решение:** в 4.3 снять реальные before/after и зафиксировать числа в change/памяти после реализации.
  Структурно «новых findings = 0» гарантировано: helper только удаляет хвостовой токен, поэтому
  findings могут лишь уменьшаться.

## Проверка реализуемости алгоритма (прототип D1+D2+D3)

Прототип `stripImplicitSelectAlias` (снятие `DISTINCT/ALL/TOP n [PERCENT] [WITH TIES]`, поиск хвостового
идентификатора, guard по символу перед пробелом `[A-Za-z0-9_)\]]`/кавычка, проверка top-level и строк)
прогнан на 14 кейсах — все совпали с ожиданием:

- срезаются: `u.Brief meUser`→`u.Brief`, `t.c alias`→`t.c`, `isnull(a,0) total`→`isnull(a,0)`,
  `1 flag`→`1`, `case … end x`→`case … end`, `'hello world' alias`→`'hello world'`,
  `TOP 10 col`→`col`, `TOP (@n) PERCENT WITH TIES col`→`col`, `DISTINCT col`→`col`, `DISTINCT tA.a myAlias`→`tA.a`;
- не срезаются: `a + b`, `a * b` (оператор перед «алиасом»), одиночный `col`.

Вывод: алгоритм из design реализуем как описано и закрывает все 8 сценариев дельты.

## Прочие подтверждения

- Интеграционная точка (`review_rules.go:6258-6266`) корректна: helper применяется в select-ветке
  после снятия явного `AS` и до `findAllUnqualifiedColumnRefs`, `isUpdate` исключён — WHERE/ON/UPDATE SET
  не меняются.
- `findAllUnqualifiedColumnRefs` используется только этим правилом (select/where/on) — точечная правка
  select-ветки безопасна.
- `splitTopLevelCSV` уже учитывает `CASE…END` и скобки, поэтому per-part снятие модификаторов корректно
  (в т.ч. `SELECT DISTINCT a, b`).
- Дельта `MODIFIED` воспроизводит полный текст требования и согласована с именем в основной спеке;
  `SHALL` присутствует; 8 сценариев — GIVEN/WHEN/THEN. Валидация `--strict` проходит.

## Рекомендуемые правки артефактов до реализации

1. `tasks.md` 4.2/4.3: зафиксировать использование дефолтного `min-severity` (R1).
2. `design.md` D1/D3: абзац о существующих `extractColumnAliasName`/`reParserTopN` и причине нового helper (R2).
3. Дельта-спека, тело требования: добавить `UPDATE SET` (R3).
4. Дельта-спека, `## Notes`: пометка об историческом заголовке (R4).
5. (Опционально) `design.md`/тесты: зафиксировать поведение для `CASE…END` (R5); `tasks.md` — оговорка
   про `findColLineOffsetInPart` (R6).

Пункты 1–2 существенно важнее остальных; 3–6 — уточнения без влияния на корректность фикса.
