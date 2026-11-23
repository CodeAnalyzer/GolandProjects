# Code Review: fix-use-only-declared-cursors-expanded (реализация)

- **Дата ревью:** 2026-11-23
- **Объект:** реализация change `fix-use-only-declared-cursors-expanded` (после учёта находок change-review)
- **Метод:** дифф рабочей копии (`git diff`), чтение финального кода, независимый прогон тестов и приёмки, сверка с tasks/design/дельтой
- **Связанные документы:** `change-review-fix-use-only-declared-cursors-expanded.md` (находки F1–F5 до реализации)

## Вердикт: GO

Реализация соответствует design (D1–D4), все MUST/SHOULD-находки change-review закрыты, регресса не обнаружено. Приёмка на реальном файле подтверждена независимым прогоном: `Trunc_GL.sql` — **0 findings** (было 4 FP).

## 1. Состав изменений (git diff)

| Файл | Дифф | Оценка |
|---|---|---|
| `internal/review/review_rules.go` | 7 строк: `_ = ctx` удалён; `os.ReadFile(file.Path)` → `r.fileContent(ctx, file.Path)` (строка 5664) + поясняющий комментарий | Точно D1; остальное тело правила (regex-ы, per-procedure циклы, окно `LineStart..LineEnd`) не тронуто — один hunk |
| `cmd/root.go` | `buildNumber` `"1714"` → `"1715"` | Задача 3.1 |
| `internal/review/review_rules_test.go` | 278 строк: хелпер `newCursorRuleRunner`, миграция 9 тестов, +3 новых | Задачи 2.1–2.4 |
| `internal/review/review_helpers.go` | +1 строка `//` (godoc-отделение абзаца, `:2032`) | Behavior-neutral, из gofmt-нормализации |
| `internal/review/catalog.go`, `review_helpers_test.go`, `review_rule_coverage_test.go` | дифф пуст (проверено и `--stat`, и `--ignore-cr-at-eol`) | EOL-only, без содержательных правок — подтверждает «отступление» из отчёта реализации |

`CodeBase.exe` пересобран (ожидаемо по 3.1).

## 2. Проверка ядра фикса

- `r.fileContent(ctx, file.Path)` (review_rules.go:5664) — тот же контракт, что у `checkCursorFetchArguments`: exec-контекст обязателен, путь нормализуется, вне активного прогона — ошибка. Правило всегда вызывается из `RunSQLFileCtx`, где exec гарантирован.
- Теперь `lines`, `proc.LineStart/LineEnd` и возвращаемые `Line` — в одной expanded-системе координат; централизованный ремап `mapExpandedLineToOriginal` (runner.go:229-236) даёт корректные исходные строки. Двойной сдвиг устранён.
- `os.ReadFile` в `internal/review` остался ровно один — review_rules.go:4147 (`checkShouldBeCP866`, легитимно читает байты). Импорт `os` сохранён — формулировка задачи 1.2 скорректирована в соответствии с находкой F3. ✓

## 3. Тесты

### Хелпер `newCursorRuleRunner` (задача 2.1, закрывает F1)

Воспроизводит exec-паттерн `RunSQLFileCtx` (как у соседнего `TestCheckDatatypeFetchInto_NearestDeclarationPairing`): `normalizePath`, `replaceMacros`, `r.exec = &reviewExecContext{filePath, content, lines, macroResult}`, `parsed` из `mr.Content`, сброс `r.exec` через `t.Cleanup`. Все 9 прежних тестов `TestCheckUseOnlyDeclaredCursors_*` мигрированы, temp-файлы из них удалены. Семантика ассертов сохранена.

### Новые регрессионные тесты (задачи 2.2–2.4)

| Тест | Покрывает | Проверка корректности |
|---|---|---|
| `MultilineDefine_DeclaredCursor_NoFinding` | F4/D3: `#define` (3 строки) перед процедурой, `__DECLARE_CURSOR__(cur1)` + OPEN/FETCH/CLOSE → 0 findings | expanded-окно совпадает с границами процедуры |
| `MultilineDefine_UndeclaredCursor_OriginalLine` | F2/задача 2.3: тест **сам** применяет `mapExpandedLineToOriginal(findings[0].Line, mr.SourceMap)`, `origLine == 7` | Математика сверена: define 3 строки → сдвиг expanded = raw − 3; `open cur1` — expanded 4 → raw 7 ✓ |
| `PerProcedureScope` | D2: cur1 объявлен в ProcOne, используется в ProcTwo → ровно 1 finding, `Object=ProcTwo`, `Line=11` | Строка 11 — действительно `open cur1` в ProcTwo; фикстура использует `X_ANYMODE(...)`-терминаторы (без них парсер не закрывает процедуру — sql_parser.go:18) |

### Независимый прогон

- `go test ./internal/review/...` → **ok** (2.2s)
- `go test -run TestCheckUseOnlyDeclaredCursors_ -v` → **12/12 PASS** (9 мигрированных + 3 новых)
- `go vet ./...` → чисто
- `gofmt -l internal/review` → пусто (задача 4.1)

## 4. Приёмка на реальном файле (задачи 3.1–3.2)

- Баннер: `CodeBase 0.9.5 build 1715` ✓
- `review <FA>/fa-generalledger/GeneralLedger/SERVER/SCRIPT_UTIL/CleaningData/Trunc_GL.sql --rules useOnlyDeclaredCursors --json` → **`count: 0`** (воспроизведено в ходе ревью; было 4 FP на строках 191/193/211/217).
- Контроль ложного нуля: `meta.min_severity = 3`, фильтр инклюзивный (`finding.Severity <= minSeverity`, runner.go:240) — severity 2 (`SeverityPostgreReq`) проходит, «0» — реальный результат, а не артефакт фильтра.
- Задача 3.2 (копия файла с `open cur_Unknown` после строки 193 → finding ровно на строке 194, object `Truncate_GL_Resource_ResArrest`, копия удалена, индекс очищен) выполнена при реализации; тот же механизм координат независимо покрыт юнит-тестом `MultilineDefine_UndeclaredCursor_OriginalLine`.

Примечание: в формулировке задачи 3.1 путь указан как `FA/...` — фактически review требует путь относительно `root_path` (без префикса `FA/`). На результат не влияет (запуск выполнялся корректно), но при архивировании формулировку можно поправить.

## 5. Закрытие находок change-review

| Находка | Статус | Где видно |
|---|---|---|
| F1 (MUST): миграция 9 тестов, exec-паттерн, temp-файлы убрать | **Закрыта** | Задача 2.1 добавлена и выполнена; хелпер `newCursorRuleRunner` |
| F2 (SHOULD): тест 2.3 сам применяет ремап к raw-строке | **Закрыта** | Задача 2.3 переформулирована; тест проверяет `origLine == 7` через `mr.SourceMap` |
| F3 (MINOR): импорт `os` остаётся | **Закрыта** | Задача 1.2: «импорт `os` остаётся — он нужен `checkShouldBeCP866`»; подтверждено кодом (:4147) |
| F4 (NOTE): токены курсоров внутри раскрытых макросов участвуют в анализе | **Закрыта** | design.md Risks, пункт 4: поведение зафиксировано как желаемое; тест 2.2 покрывает |
| F5 (NOTE): повторный `strings.Split` допустим | Учтена (не-изменение) | Реализация оставила `strings.Split(string(content), "\n")` — единообразно с контрактом `fileContent` |

## 6. Соответствие артефактам change

- **tasks.md**: 10/10 задач `[x]`, формулировки соответствуют фактическому коду и прогонам (после правок по F1–F3).
- **design.md**: D1–D4 реализованы как записано; риски (зависимость от exec, раскрытие макросов, расходы) подтверждены фактическим поведением.
- **Дельта-спека** (`specs/review/postgre-requirements/spec.md`): требование обновлено (объявления/использования в expanded-тексте, строка finding'а — в исходном файле), 4 сценария один-в-один с тестами 2.2–2.4 и «OPEN без DECLARE». `openspec validate --strict` → **valid**.
- **proposal.md**: ожидаемый эффект («0 FP на Trunc_GL.sql, необъявленные курсоры ловятся») подтверждён.

## 7. Замечания (не блокирующие)

- **N1 (minor).** `gofmt -l` по всему репозиторию помечает `cmd/root.go` и 7 дореформенных файлов (`dsinfosvc`, `encoding`, `query`, `rti`, `trc`, `Tests\Temp`) — это CRLF-состояние рабочих копий при `core.autocrlf=true`, вне скоупа задачи 4.1 (она ограничена `internal/review`, который чист). Содержательных нарушений форматирования нет; при коммите git зафиксирует нормализованные EOL.
- **N2 (minor).** Путь в задаче 3.1 с префиксом `FA/` не воспроизводим дословно (см. п. 4) — косметика формулировки перед архивацией.

## 8. Итог

- Ядро фикса корректно и минимально (одна точка чтения текста, без изменений логики правила).
- Регресс отсутствует: 12/12 целевых тестов, полный пакет `internal/review` зелёный, vet/gofmt чисты, приёмка на реальном файле воспроизведена.
- Все условия GO из change-review выполнены. Change готов к архивации (с учётом косметики N1/N2 по желанию).

## Источники

- `internal/review/review_rules.go:5657-5731` (правило), `:4147` (`checkShouldBeCP866`), `:40,:46` (regex-ы макросов)
- `internal/review/runner.go:229-236` (ремап), `:240` (фильтр severity), `:473-479` (`fileContent`)
- `internal/review/review_rules_test.go:4947+` (хелпер и 12 тестов)
- `cmd/root.go:29` (buildNumber), `internal/review/review_helpers.go:2032`
- Приёмка: `FA/fa-generalledger/GeneralLedger/SERVER/SCRIPT_UTIL/CleaningData/Trunc_GL.sql`
- Артефакты change: `proposal.md`, `design.md`, `tasks.md`, `specs/review/postgre-requirements/spec.md`
