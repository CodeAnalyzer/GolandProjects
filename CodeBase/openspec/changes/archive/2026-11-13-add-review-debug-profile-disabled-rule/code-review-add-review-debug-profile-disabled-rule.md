# Code Review: add-review-debug-profile-disabled-rule

- **Дата**: 2026-11-13
- **Ревьюер**: opencode (L3/pro/fast)
- **Вердикт**: ⚠️ **РЕАЛИЗАЦИЯ ПРИНЯТА ПОСЛЕ R1–R2.** Механика F1/F2 из change-review закрыта корректно: правило работает по исходному тексту (`rawContent`), finding помечается `LineInOriginalSpace` и не ремапится; добавлен e2e-тест через `RunSQLFileCtx`. Тесты, сборка, `go vet` и integration-прогон зелёные. Блокируют приёмку только `gofmt` (R1) и синхронизация чисел первого прогона в артефактах (R2); R3–R5 — рекомендации.

## Что проверялось

1. **Артефакты**: `proposal.md`, `design.md`, `tasks.md` (15/15 checked), дельта `specs/review/deploy-stoppers/spec.md`; `openspec validate add-review-debug-profile-disabled-rule --strict` — valid.
2. **Код**: `internal/review/{review_rules,runner,types,catalog}.go`, новые `review_debug_profile_test.go` и `review_debug_profile_integration_test.go`, `cmd/review.go`, `cmd/root.go`, `internal/mcp/registry.go`, `README.md`.
3. **Проверки**: `go build ./...`; `go test -count=1 ./internal/review/... ./cmd/... ./internal/mcp/...`; `go test -tags integration -run TestCheckDebugProfileEnabledE2E -count=1 -v`; `go vet ./internal/review/... ./cmd/... ./internal/mcp/...`; `gofmt -l`; `git diff --check`.
4. **FA**: повторная проверка порядка директив по 170 кандидатам (`#define PROFILE`, все регистры `.sql`/`.SQL`/`.h`); сверка контрольных точек из отчёта реализации.

## Подтверждённые положительно

- **F1 закрыт**: `reviewExecContext.rawContent` заполняется из `fileContent` до `replaceMacros`; accessor `rawFileContent` (`runner.go:379-389`) возвращает исходный текст с `#define`. `checkDebugProfileEnabled` использует именно его (`review_rules.go:1694-1731`), а не expanded-`fileContent`. Контракт закреплён `TestCheckDebugProfileRawContentContract` и e2e-тестом.
- **F2 закрыт**: `Finding.LineInOriginalSpace` (`types.go:237-241`, `json:"-"`) и условный ремап (`runner.go:203-210`) сохраняют строку `#define` в original-пространстве. E2e-фикстура с двумя `#define` выше `#define PROFILE` даёт `Line=3` (проверено; без флага было бы 6).
- **Условие порядка**: реализовано по первому `#include <macros.h>` (`debugProfileMacrosIncludeRe`), `tag13_PsExtendActions.sql`-паттерн подавляется; unit- и e2e-кейсы есть. На 170 кандидатах FA файлов с `define` после любого include — ровно 3, все содержат прямой `#include <macros.h>`; косвенных include-случаев перед define не обнаружено.
- **Guard расширений**: `.sql`/`.h` через `strings.EqualFold`; боевые `fa-generalledger/*.SQL` проходят (unit-кейс `LastDateCalc_New.SQL`), `.inc` отсекается.
- **Блоки `#define` исключены из поиска точек** (строка определения + `\`-продолжения): helper-define `#define DBG1(x) PROFILE_TIME_VAR1(...)` и многострочный define не флагуются; unit-тесты на оба случая есть.
- **Список носителей** соответствует требованию спеки и проверен по `MACROS.H`; порядок альтернатив в regex исключает частичные совпадения (`PROFILE_BEGIN_EX` раньше `PROFILE_BEGIN`, `PROFILE_TIME_VAR1` раньше `PROFILE_TIME` и т.д.).
- **Регрессы лексера**: `--` в литерале, апостроф в комментарии, многострочный литерал, вложенные комментарии (зафиксированы как известное ограничение) — покрыты.
- **Транспорт/документация**: rule ID автоматически попадает в `AllRuleIDs`/`DefaultEnabledRules`; README-каталог, help CLI (`.sql or .h`) и описание MCP обновлены; `buildNumber` — 1694.
- **Проверки зелёные**: build, unit/integration tests, `go vet`, `git diff --check` (exit 0).

## Findings

### R1 [Low] — `gofmt` не применён к двум файлам

`gofmt -l` возвращает:
- `internal/review/runner.go` — выравнивание полей `rawContent`/`macroResult`/`lines` в `reviewExecContext`;
- `internal/review/review_debug_profile_test.go` — выравнивание полей литерала `reviewExecContext` в `newDebugProfileRunner`.

Это единственные замечания, ломающие принятый в проекте стиль; функционально код корректен. **Действие**: `gofmt -w internal/review/runner.go internal/review/review_debug_profile_test.go`.

### R2 [Low] — Разбивка пропусков первого прогона в proposal/tasks не сходится; design Risks устарел

В `proposal.md` Impact (строка 34) указано «147 findings / 23 пропуска», но перечисление даёт 24:
- `tag13_PsExtendActions.sql` ×3 + `tag13_PsExtCreate.sql` ×3 — «подавлены условием порядка»;
- `dcutil.h` ×3 + Reload-обёртки ×4 — «без точек»;
- «остальные 11» — фактически перечислено 10 файлов (`UCSPCBINLoad` ×2, `GenerateSwiftFromDepoInst_54x`, GL `FillBranch*` ×3 + `CalcRestByCalendar2`, factoring `SDMFactLoad`/`FactoringCommission`/`FactoringActions`).

Проверка `tag13_PsExtCreate.sql`: `#define PROFILE` — строка 1, `#include <macros.h>` — строка 2, точек профилирования нет. Значит, файл пропущен **не по условию порядка**, а по отсутствию точек; условием порядка подавлены только `tag13_PsExtendActions.sql` ×3. Корректная формула: 3 (порядок) + 20 (нет точек) = 23. В `design.md` Risks (строка 66) и `tasks.md` 4.3 всё ещё фигурирует «~146»; фактический прогон — 147. **Действие**: исправить разбивку и число в proposal/design/tasks (или явно зафиксировать +1 как расхождение методик).

### R3 [Info] — Условие порядка проверяет только прямой `#include <macros.h>`

`debugProfileMacrosIncludeRe` матчит только `#include <macros.h>` (с допустимыми пробелами). Если `macros.h` приходит косвенно (`#include <dealmain.h>` / `link_macros.h` до `#define PROFILE`), define фактически неэффективен, но правило этого не увидит. На текущем FA таких кандидатов нет (все 3 define-after-include — с прямым `macros.h`), поэтому это не дефект прогона, а ограничение эвристики. Желательно явно назвать его в design Risks рядом с false-negative «define через include»; при расширении include-резолва — покрыть.

### R4 [Info] — Поиск define не пропускает блоки `#define`, в отличие от поиска точек

`debugProfileDefineStartRe`/`inDefineBlock` применяются только при поиске точек. Цикл поиска активного `#define PROFILE` (строки 1710-1720) сканирует все строки и теоретически примет строку `#define PROFILE` внутри тела многострочного макроса за директиву. На FA таких кейсов не выявлено; для симметрии с D3 и защиты от будущих гибридных хедеров стоит либо пропускать define-блоки и в этом цикле, либо добавить тест-ограничение.

### R5 [Info] — Тест-хелпер `tmpDebugProfileFile` создаёт неиспользуемый файл

`tmpDebugProfileFile` создаёт пустой temp-файл, но возвращает `filepath.Join(filepath.Dir(tmpFile.Name()), name)` — путь, которого нет; правило читает `rawContent` из `exec`, поэтому тесты проходят. Хелпер вводит в заблуждение и оставляет лишнюю пустую запись (удаляется cleanup). Проще: `filepath.Join(t.TempDir(), name)` без `os.CreateTemp`.

## Резюме

| # | Суть | Приоритет | Действие |
|---|---|---|---|
| R1 | `gofmt` не применён к `runner.go` и `review_debug_profile_test.go` | Low | `gofmt -w` двух файлов |
| R2 | Разбивка 23 пропусков даёт 24; `tag13_PsExtCreate` не относится к условию порядка; design/tasks говорят ~146 вместо 147 | Low | Синхронизировать числа и классификацию в proposal/design/tasks |
| R3 | Условие порядка — только прямой `#include <macros.h>` | Info | Зафиксировать ограничение в Risks; FA-кейсов нет |
| R4 | Поиск define не пропускает `#define`-блоки | Info | Пропускать блоки или добавить тест-ограничение |
| R5 | `tmpDebugProfileFile` возвращает несуществующий путь | Info | Использовать `t.TempDir()` |

Механически доработка выполнена качественно: закрыты оба блокера change-review, покрытие сценариев спеки полное (23 unit-кейса + контракт raw-контента + e2e), интеграционный тест идёт через production-путь `RunSQLFileCtx`, а условие порядка и guard расширений защищены отдельными кейсами. После R1–R2 change можно архивировать; R3–R5 не блокируют.
