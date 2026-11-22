# Code-review реализации change `fix-review-block-comment-masking`

Дата: 2026-11-22.
Объект ревью: рабочий дифф (`internal/review/review_parser.go`,
`internal/review/review_rules.go`, `cmd/root.go`, новый
`internal/review/review_block_comment_masking_test.go`, пересобранный
`CodeBase.exe`), его соответствие артефактам change (proposal/design/tasks/дельты,
обновлённые по итогам change-review) и отсутствие регрессов.

## Итог

Реализация **соответствует change; замечаний, блокирующих архивацию, нет**.
Замечания change-review учтены и в артефактах, и в коде: `maskSQLComments` —
байтовый length-preserving (`[]byte`, `\n`/`\r` сохраняются), кириллица не
смещает `offset`; `splitStatementsWithOffsets` маскирует вход и возвращает `stmt`
из маскированного текста; маскировка применена к 7 заявленным правилам, включая
подтверждённый по коду пробел `varUseAfterCursor`. Общий автомат вынесен в
`maskLexemes`, `sanitizeSQLForMacroScan` делегирует ему (поведение сохранено).

Проверки в ходе ревью: `go test -count=1 ./internal/review/... ./cmd/...` — ok;
`go vet ./...` — чисто; `go build ./...` — чисто; e2e на реальном файле FA —
`count: 0`; banner `0.9.5 build 1713`; `openspec validate … --strict` — valid.
Независимо (временным in-package тестом, удалён после проверки) подтверждено, что
кириллица в блоке перед триггером не сдвигает `Line` у `inSubQuery` и
`insertRowLock` через `splitStatementsWithOffsets` + ремап.

## Состав изменений (по git diff)

| Файл | Изменение |
|---|---|
| `internal/review/review_parser.go` | новый `maskLexemes(s, maskStringLiterals)` (:1692) и `maskSQLComments` (:1757); `splitStatementsWithOffsets` (:1767) маскирует вход и режет `masked` |
| `internal/review/review_rules.go` | `sanitizeSQLForMacroScan` (:1601) делегирует `maskLexemes(s, true)`; маскировка в `checkUseSelectAll` (:2450), `checkTruncTbl` (:2482), `checkInsertRowLock` (:3073), `checkVarUseAfterCursor` (:5139) |
| `internal/review/review_block_comment_masking_test.go` | новый: 3 хелпер-теста + 7 rule-регрессий + 2 регрессии `varUseAfterCursor` (315 строк) |
| `cmd/root.go:29` | `buildNumber` 1712 → 1713 |
| `CodeBase.exe` | пересобран (banner 1713) |

Посторонних изменений в диффе нет (плюс новые untracked-файлы change/теста).

## Соответствие задачам change

| Задача | Статус | Подтверждение |
|---|---|---|
| 1.1 `maskSQLComments`, байтовый инвариант, `\n`/`\r`, string-aware | Выполнено | `review_parser.go:1692-1759`; `TestMaskSQLComments_*` (длина/строки/CRLF/литерал через `\n`/`/*` и `--` в литерале/`*//*`/вложенный `/*`/незакрытый блок/Cyrillic) |
| 1.2 список вызовов сплиттера, комментарий | Выполнено | 4 вызова: `review_rules.go:4175,4375,4495,4571` (актуальные номера после сдвига); doc-комментарий у `splitStatementsWithOffsets` (:1761-1766) |
| 2.1 маскировка входа сплиттера, offset/lineNo | Выполнено | `review_parser.go:1767-1815`: `lower` и `stmt` из `masked`, `offset` = байтовый `startIdx`; длина сохранена |
| 2.2 регрессии 4 операторных правил | Выполнено | `TestBlockCommentMasking_{DeferredUpdate,MemoryUniqueIndexUpdate,InSubQuery,TooManyJoins}` — триггер в `/* */` → 0, реальный → finding |
| 3.1-3.3 построчные сканеры | Выполнено | `insertRowLock`/`useSelectAll`/`truncTbl`: маскировка + тесты, включая хвостовой `--`, `;`/`union all` в комментарии, FN-фикс `truncate /* c */ table` |
| 4.1 `varUseAfterCursor` — маскировка входа | Выполнено | `review_rules.go:5139`; регрессии: все курсорные операции в `/* */` → 0; реальные + закомментированный мусор → 1 finding |
| 5.1 e2e TmpTbl_GLReports | Выполнено | `CodeBase.exe review …TmpTbl_GLReports.sql --min-severity 1 --json` → `count: 0` (перепроверено) |
| 5.2 приёмка на корпусе 247 файлов | Отмечено, но не воспроизводимо из репозитория | baseline/результаты не сохранены в change (см. замечание 3) |
| 5.3 тесты пакета | Выполнено | `go test -count=1 ./internal/review/... ./cmd/...` — ok |
| 6.1 сборка 1713 | Выполнено | `CodeBase.exe --version` → `0.9.5 build 1713` |
| 6.2 validate strict | Выполнено | valid |

## Корректность реализации

### Маскер (`maskLexemes` / `maskSQLComments`)

- Байтовый автомат над `[]byte`, состояния normal/string/line/block; каждый байт
  комментария → `' '`, `\n` и `\r` остаются; `len(out) == len(in)`.
- `maskStringLiterals=true` (санитайзер) заменяет содержимое литералов на `?`;
  `false` (`maskSQLComments`) литералы сохраняет — правила, читающие
  `extractSetColumns`/`extractTablesFromFromClause`/`findInSubqueryPositions`,
  не страдают.
- Состояние литерала переносится через `\n`, экранирование `''` учтено; `*//*`,
  `/**/`, незакрытый `/*` до EOF обрабатываются корректно.
- `sanitizeSQLForMacroScan` после рефакторинга поведенчески эквивалентен прежнему
  коду (построчное сравнение веток автомата); его потребители
  (`apiExecOutsideFacade`, `debugProfileEnabled`) покрыты существующими тестами —
  пакет зелёный.

### Сплиттер и `Line`

- `splitStatementsWithOffsets`: `masked := maskSQLComments(text)`,
  `lower := toLowerASCII(masked)`, `stmt := strings.TrimSpace(masked[startIdx:i])`,
  `offset = startIdx`. Поскольку `len(masked)==len(text)`, `offset` валиден для
  исходного `text`, по которому потребители считают `Line`
  (`review_rules.go:4184,4383,4503,4574`).
- `Line`-координаты не меняются: маска сохраняет число `\n`; потребители читают
  `text[:offset]` из исходного текста.
- `Object` у `tooManyJoins` теперь берётся из маскированного `stmt` (первая строка),
  поэтому хвостовой `--` в первой строке в `Object` не попадает — это ожидаемое
  поведение, зафиксированное в design D3/приёмке (нормализация хвостовых
  комментариев).

### Построчные сканеры

- `insertRowLock`: прежний префиксный skip `--` заменён на `trimmed == ""`
  (`review_rules.go:3085-3088`). После маскировки любой чисто-комментарный/
  блочный фрагмент стал пробельным и пропускается так же, как раньше пропускались
  `--`-строки; пустые строки в буфере не завершают оператор
  (`isNewSQLStatement` на пустой строке ложен). Семантика реальных INSERT
  сохранена (проверено существующими `TestInsertRowLock` + новыми кейсами).
- `useSelectAll`/`truncTbl`: маскируются `fileProcessedContent`/`fileContent` до
  `strings.Split`; передний `--`-skip больше не нужен; хвостовые `--` и блоки
  скрыты. Реальные триггеры сохранены.

### `varUseAfterCursor`

- Маскируется `macroResult.Content` до `parseAllFetchIntoStatements`/
  `parseDeallocateStatements`/`parseCursorDeclarations` — закомментированные
  `fetch`/`deallocate`/`declare cursor` больше не считаются операторами (пробел
  подтверждён по коду, task 4.1).
- `Line` = `i+1` по тем же строкам, `Object` — имя переменной, поэтому
  маскировка не искажает координаты/объект. Расширение skip-спана
  `fetchVariablesLineEnd` по упоминанию переменной в комментарии становится
  неактуальным, но непротиворечивым (парсер и скан использования работают по
  одному маскированному тексту) — зафиксировано комментарием в коде.

## Регрессия (выполненные проверки)

| Проверка | Команда | Результат |
|---|---|---|
| Новые тесты | `go test -count=1 -v ./internal/review -run 'TestMaskSQLComments|TestBlockCommentMasking'` | 12 top-level тестов, все PASS (36 под-кейсов) |
| Пакет review/cmd | `go test -count=1 ./internal/review/... ./cmd/...` | ok |
| Статический контроль | `go vet ./...`, `go build ./...` | чисто |
| Сборка | `CodeBase.exe --version` | `0.9.5 build 1713` |
| E2E (task 5.1) | `CodeBase.exe review …TmpTbl_GLReports.sql --min-severity 1 --json` | `count: 0` |
| E2E до фикса (мысленно/по коду) | старый `checkInsertRowLock` на этом файле | finding `insertRowLock` line 68 (INSERT в блоке 67-79, `select` на 69 завершал буфер) — устранён |
| Кириллица/offset | временный in-package тест (удалён после прогона) | `inSubQuery` и `insertRowLock` после блока с кириллицей дают исходные `Line` (3 и 2) — байтовый инвариант держится через сплиттер |
| OpenSpec | `openspec validate fix-review-block-comment-masking --strict` | valid |

Ложных негативов по заявленным правилам не выявлено: каждый rule-тест содержит
позитивный кейс «реальный триггер → finding» с проверкой `Line`/`Object`
(где применимо).

## Замечания (не блокируют archive)

1. **Документация/lint (minor).** Из-за вставки `maskLexemes` перед
   `splitStatementsWithOffsets` две первые строки старого doc-комментария
   (`review_parser.go:1680-1681`, «`splitStatementsWithOffsets` аналогична …»)
   склеились с doc-комментарием `maskLexemes` (:1682-1691), поэтому Go-doc
   `maskLexemes` начинается с чужого предложения. Дополнительно новая
   формулировка `('' — экранирование)` в doc-комментарии приводит к тому, что
   `gofmt -l internal/review/review_parser.go` теперь показывает файл (gofmt
   переформатирует `''` → `”`); в HEAD файл был gofmt-чистым. Рекомендация:
   убрать/перенести строки 1680-1681 к `splitStatementsWithOffsets` (:1761) и
   прогнать `gofmt -w`.

2. **Тестовое усиление offset-инварианта (minor, опционально).** Коммитнутые
   тесты проверяют байтовую длину и неизменность последующих строк на уровне
   хелпера (`TestMaskSQLComments_CyrillicByteLength`) и позитивные кейсы с
   триггером на строке 1. Прямого теста «блок-комментарий (в т.ч. с кириллицей)
   перед реальным оператором → finding с исходной `Line`» для
   `deferredUpdate`/`inSubQuery`/`tooManyJoins` нет. В ходе ревью это проверено
   временным тестом и работает; для защиты от будущей rune-регрессии стоит
   добавить такой кейс в `review_block_comment_masking_test.go`.

3. **Приёмка 5.2 не воспроизводима из репозитория (process).** Задача отмечена
   выполненной, но baseline «до» и результаты «после» (247 файлов, 87 FP → 0,
   нормализованный diff остальных findings) в каталоге change не сохранены.
   Рекомендация: приложить артефакт/сводку (counts + `rule:line` diff), чтобы
   приёмку можно было перепроверить при архивации/ретроспективе.

4. **Остаточный edge (info, принято).** Замена префиксного `--`-skip на
   маскировку меняет поведение только для строк, начинающихся с `--` **внутри
   многострочного строкового литерала**: раньше такие строки пропускались
   целиком, теперь обрабатываются (содержимое литерала не маскируется). Для
   `insertRowLock` это безопасно (`isInComment` видит `--` до `insert`); для
   `useSelectAll`/`truncTbl` теоретически возможен новый FP, если внутри такого
   литерала на отдельной строке есть `select *`/`truncate table`. Конструкция
   крайне редкая; фиксируется как принятый риск, не как дефект. Bracket-quoted
   `[a--b]`/`"a--b"` — задокументированное ограничение хелпера (design D1a).

5. **CRLF в `cmd/root.go` (info).** Рабочая копия `cmd/root.go` в CRLF
   (`core.autocrlf=true`), поэтому `gofmt -l` её показывает; содержимое диффа —
   1 строка (`buildNumber`), HEAD-блоб LF и gofmt-чистый. На коммит/CI (LF) не
   влияет; упомянуто, чтобы результат `gofmt -l` не читался как регресс.

6. **Размещение хелпера (info).** design D1a говорит «рядом с
   `sanitizeSQLForMacroScan`»; фактически общее ядро `maskLexemes` —
   в `review_parser.go`, а санитайзер в `review_rules.go` делегирует ему. Это
   разумнее (маскер нужен парсеру/сплиттеру) и не влияет на поведение;
   расхождение только текстовое.

## Вердикт

Реализация соответствует спроектированному change; регрессов не выявлено
(полный прогон пакета, vet, build, e2e, независимая проверка кириллического
offset). Change **готов к archive**; замечания 1–2 стоит закрыть до коммита
(косметика/устойчивость тестов), 3 — приложить артефакт приёмки, 4–6 —
информационные.
