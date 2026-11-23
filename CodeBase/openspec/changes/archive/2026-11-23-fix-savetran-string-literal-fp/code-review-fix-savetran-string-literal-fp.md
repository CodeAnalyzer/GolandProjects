# Code review: fix-savetran-string-literal-fp

- **Change:** `openspec/changes/fix-savetran-string-literal-fp`
- **Дата ревью:** 2026-11-23
- **Бинарь:** CodeBase `0.9.5` build **1716** (пересобран)
- **Объём diff (tracked):** `cmd/root.go`, `internal/review/review_rules.go`,
  `internal/review/review_rule_coverage_test.go`, `CodeBase.exe` — иных изменений нет.

## Вердикт

**Одобрено (APPROVED).** Реализация 1:1 соответствует плану change, регрессов нет:
FP на реальном файле устранён (line 130 → 0), настоящие `SAVE TRAN` по-прежнему флагуются,
весь пакет `internal/review` зелёный. Все замечания прошлого ревью (N1–N5) учтены в артефактах.
Единственное замечание I1 — косметическое (CRLF/gofmt), не блокирует.

## 1. Соответствие запланированному change

| Task | План | Факт | Статус |
|---|---|---|---|
| 1.1 | `lower := strings.ToLower(stripQuotedSegments(line))` в `checkSaveTran` (~996) | `review_rules.go:996` именно так | ✔ |
| 1.2 | build/vet/gofmt чисты | build OK, vet OK; `gofmt -l` непусто из-за CRLF (см. I1), сам код gofmt-clean | ✔ (с оговоркой) |
| 2.1 | кейс литерал-макрос → 0 | добавлен в `TestCheckSaveTran_Coverage` | ✔ |
| 2.2 | экранированный `''` + реальный → 1, `wantLine` | добавлен, `wantLine: 2` | ✔ |
| 2.3 | `go test ./internal/review/` зелёный | зелёный | ✔ |
| 3.1 | buildNumber 1715→1716, пересборка | `cmd/root.go:29` = «1716»; exe отдаёт `build 1716` | ✔ |
| 3.2 | пример-файл → 0 (было 1 на :130) | независимо воспроизведено → `Findings: 0` | ✔ |
| 3.3 | контроль полноты на FA | подтверждено положительным контролем (см. §4) | ✔ |

## 2. Анализ правки кода

```go
// internal/review/review_rules.go:996 (checkSaveTran)
- lower := strings.ToLower(line)
+ lower := strings.ToLower(stripQuotedSegments(line))
```

- **Расположение корректно.** `hasSaveTran` (`review_helpers.go:1473`) имеет **единственного**
  потребителя — `checkSaveTran` (`review_rules.go:999`); правка в сканере покрывает правило целиком.
- **Переиспользование.** `stripQuotedSegments` (`review_rules.go:1866`) — тот же пакет/файл,
  новые импорты не нужны. Хелпер корректно обрабатывает `''` (экранированный апостроф не закрывает литерал).
- **Порядок фильтрации** (комментарии → литералы) сохранён (D3); остаточное ограничение
  (маркеры `--`/`/*` внутри литерала) задокументировано и данным change не ухудшается.
- **Побочных эффектов нет:** изменение `lower` не влияет на `line`/`lineNum`, finding остаётся
  на исходной строке (ремап не нужен).

## 3. Тесты — отсутствие регресса

- `TestCheckSaveTran_Coverage` — 6 кейсов PASS, включая 2 новых:
  - `literal in macro arg` (`PROFILE_TIME_EX('...SAVE tran...')` + `__BEGIN_TRAN__(...)`) → 0;
  - `escaped quote literal and real save tran` (`'save tran ''quoted'' text'` + `SAVE TRAN MySavePoint`) → 1, `wantLine: 2`.
- `TestHasSaveTran_Detects` / `_NoFalsePositive` — PASS.
- `TestBlockCommentHeaderScanners/saveTran_*` (dashed header, line comment, unclosed block,
  inline block, remainder after close) — все PASS → блочно-комментарное поведение не сломано.
- **Полный `go test ./internal/review/ -count=1` → ok** (регрессов по пакету нет).
- `go build ./...` — exit 0; `go vet ./internal/review/` — exit 0.

## 4. Верификация на реальных данных (независимая)

- **FP устранён:** `CodeBase.exe review ...\CardAccRelation_MassLoad.sql --rules saveTran` →
  `Findings: 0` (build 1716). Ранее — 1 на :130 внутри `PROFILE_TIME_EX('...SAVE tran...')`.
- **Нет FN (положительный контроль):** `...\SCRIPT_UTIL\CardBlank_conv.sql` (содержит и литералы
  на :866/:1397, и реальные операторы на :869/:1400) →
  `Findings: 2` на **line=869** и **line=1400**; строки-литералы **не** флагуются.
  Это доказывает, что правка подавляет только литеральные вхождения и сохраняет детект реальных.

## 5. Артефакты: учтены замечания прошлого ревью

- **N1 (порядок):** D3 дополнен residual-ограничением про `--`/`/*` внутри литерала. ✔
- **N2 (FN динамического SQL):** в Risks добавлен принятый trade-off (`sp_executesql N'save tran'`, `exec('...')`). ✔
- **N3 (Impact):** добавлены `cmd/root.go` (buildNumber) и `review_rule_coverage_test.go` + существующие тесты. ✔
- **N4 (точность ссылки):** теперь указан `onlyQuotedOccurrence` (`review_rules.go:1922`). ✔
- **N5 (место тестов):** задачи 2.1/2.2 адресуют `TestCheckSaveTran_Coverage`. ✔
- `openspec validate fix-savetran-string-literal-fp --strict` → **valid**; дельта `MODIFIED` faithful
  (main spec содержит один сценарий, дельта его воспроизводит + 2 новых — потерь нет).

## 6. Замечания

| # | Severity | Место | Суть | Рекомендация |
|---|---|---|---|---|
| I1 | Info | `review_rules.go`, `review_rule_coverage_test.go`, `cmd/root.go` (working tree) | `gofmt -l internal/review/` непусто, т.к. эти 3 файла в рабочем дереве **CRLF**, а весь пакет — LF (`core.autocrlf=true`). На LF-копиях `gofmt -l` **пусто** → дефекта форматирования кода нет, это артефакт line-endings. Коммит корректный (git нормализует CRLF→LF, diff — только целевые строки). | Не блокирует. При желании — нормализовать переводы строк в 3 файлах до LF, чтобы буквально выполнить task 1.2. |
| I2 | Info | Поведение | `SAVE TRAN` в динамическом SQL (`sp_executesql N'...'`, `exec('...')`) теперь не флагуется (новый FN). | Уже осознанно зафиксировано в `design.md` Risks; действий не требуется. |

## 7. Чек-лист приёмки

- [x] Правка `checkSaveTran` (одна строка), build/vet OK.
- [x] Регресс-кейсы 2.1/2.2 в `TestCheckSaveTran_Coverage`; полный пакет зелёный.
- [x] buildNumber «1715»→«1716», exe пересобран (`build 1716`).
- [x] Пример-файл → 0 findings (независимо воспроизведено на build 1716).
- [x] Реальные `SAVE TRAN` по-прежнему флагуются (CardBlank_conv.sql: 869, 1400).
- [x] `openspec validate --strict` → valid; замечания N1–N5 учтены.

## Заключение

Change реализован корректно, минимально и в точном соответствии с планом; регрессов не выявлено.
Можно переходить к архивации (`openspec archive fix-savetran-string-literal-fp`). Замечание I1 —
косметическое (line-endings), на функциональность и коммит не влияет.
