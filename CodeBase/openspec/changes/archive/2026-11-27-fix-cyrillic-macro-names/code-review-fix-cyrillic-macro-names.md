# Code-review: fix-cyrillic-macro-names

Ревью реализованного кода (build 1724). Проверены: соответствие design D1-D6,
регрессия на ASCII-контенте и корпусе, тестовое покрытие, приёмка.

## 1. Исходные данные

- Diff: `internal/review/review_parser.go` (+126/−10), `review_parser_test.go`
  (+102), новый `review_macro_guard_test.go`, `cmd/root.go` (1723→1724).
- Прогоны ревьюера: `go vet` — чисто; `go test ./internal/review/...` — ok
  (2.2 s); `CodeBase.exe --version` → build 1724; `gofmt -l` — замечание C5.
- Артефакты приёмки: `acceptance-report.md`,
  `Tests\Temp\macro_budget_measure\{report.md,diff_228_1724.txt}`.
- Стенд ревьюера: временный тест (удалён) на линейный runaway — см. C1.

## 2. Соответствие плану

| Decision | Реализация | Статус |
|---|---|---|
| D1 руна-скан имени | `review_parser.go:889-897`, `utf8.DecodeRuneInString` + `isUnicodeIdentRune` (1195) | ✓ |
| D2 руна-границы вызова | `unicodeIdentRuneBefore/At` (1202, 1212) в 1148, 1162; `isWordChar` не тронут | ✓ |
| D3 дедуп `names` | 993-995, `byName` last-wins сохранён | ✓ |
| D4 бюджет, fail-open | 1046-1050, проверка после прохода; 512 строк / 1 МБ = замер 49 / 8823 × 10.4 / × 118 | ✓ |
| D5/D6 маркеры и статистика | `macroReplaceResult` 782-785, `macroExpandStats`, 4-е значение `expandMacrosInLines`; сигнатура `replaceMacros` стабильна | ✓ |
| F7 (ревью change) `PassesExhausted` | 1053: выход из цикла проходов с `changedAny == true` | ✓ |
| Спеки F5/F6 | «MUST останавливаться», «кроме примыкания к не-ASCII» | ✓ |

Тесты 1.1-1.2, 2.3: 5 тестов идентификации (полное имя + параметры, якорь
строки вызова, `Х_Цело_` не вызов, нет макроса `_`, ASCII-имя при кириллице),
last-wins дублей, runaway через subprocess с дедлайном (вызов в **каждой**
строке тела — F1 учтён), нулевые счётчики на нормальном файле.

## 3. Регрессия

- **ASCII-контент**: D1/D2 расходятся со старым поведением только при
  примыкании не-ASCII руны к имени; D3 семантически нейтрален; D4 неактивен
  при ≤ 49 строках. Для файлов без кириллицы у границ имён — тождество.
- **Корпус** (сверено по `diff_228_1724.txt`): 208 both-PASS, findings
  removed 0 / ADDED 0 / REGRESSION 0; `GuardTrippedLines = 0` на dq228 raw и
  masked, отдельно на 36 кириллических — критерий чистоты F2 выполнен.
- **10 новых `nullComparison`**: проверено по исходнику
  `RPT_6406U_724_ExportToXml.sql:478-479` — реальный SQL
  `,'@TINброкер' = null` в списке колонок FOR XML PATH, не макро-артефакт.
  Классификация «легитимны» подтверждена.

## 4. Замечания

### C1 (Low-Medium) — путь `PassesExhausted` без положительного теста

Покрытие маркера только негативное (`TestMacroGuardNormalFileZeroCounters`).
Стенд ревьюера: тело с собственным вызовом в **одной** строке
(`#define _A_(_x_) select _A_(_x_) \` / `from t`) — линейный рост, 32 прохода,
бюджет не срабатывает → `PassesExhausted=true, PassesExhaustedLines=1,
GuardTripped=false`, 34 строки. Детерминированный fixture без subprocess.

Резолюция: добавить `TestMacroGuardPassesExhaustedLinearRunaway` в
`review_macro_guard_test.go` (и в runaway-тесте — `assert !PassesExhausted`).

### C2 (Low) — вывод дочернего процесса runaway-теста теряется

`cmd.Stdout/Stderr = nil`: при падении потомка родитель видит только
«exit status 1» без причины (`guard must trip…`).

Резолюция: `bytes.Buffer` на `cmd.Stdout/Stderr`, текст — в `t.Fatalf` родителя.

### C3 (Low, артефакты) — proposal/design не сверены с фактами приёмки

Why/Impact proposal и Context/Risks design (строки 22-25, 127-130) сохраняют
«16 виснут / 20 портятся, findings ненадёжны / у 20 findings меняются».
Приёмка установила другое: висело **20** (16 + 4 маска-эры), у **16**
невиснущих findings **идентичны** (0/0) — «тихая порча» в findings не
проявилась. tasks 5.1 также говорит про 16 файлов, покрыто 20.

Резолюция: до архивации привести численность и формулировку к фактам
(«20 → PASS, 10 легитимных находок; 16 → идентичность»).

### C4 (Low, артефакты) — D4 и Open Questions не закрыты

design D4 (96-98): «До замера рабочие значения: 256 строк»; Open Questions
(162-163): лимиты «фиксируются задачей замера»; tasks 3.1 — «256». Итоговые
512 / 1 МБ зафикрированы только в коде и отчёте замера.

Резолюция: записать в D4 итоговые константы и замер, снять Open Question.

### C5 (Low) — gofmt

`gofmt -l` → `review_parser.go`: выравнивание полей `macroReplaceResult`
(782-785). (`review_plpgsql_mask_test.go` тоже в списке — pre-existing,
файл в этом change не менялся.)

Резолюция: `gofmt -w internal/review/review_parser.go`.

## 5. Итог

Код соответствует D1-D6 и учтённым замечаниям change-review (F1-F7); регрессия
на ASCII-контенте и корпусе отсутствует и подтверждена независимыми прогонами;
приёмка воспроизводима по артефактам. Замечания C1-C5 — мелкие, не блокируют:
C1-C2 — тесты, C3-C4 — документация до архивации, C5 — форматирование.
