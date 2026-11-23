# Ревью change: fix-savetran-string-literal-fp

- **Change:** `openspec/changes/fix-savetran-string-literal-fp`
- **Дата ревью:** 2026-11-23
- **База:** CodeBase build «1715» (`cmd/root.go:29`)
- **Валидация:** `openspec validate fix-savetran-string-literal-fp --strict` → **valid**

## Вердикт

**Готов к реализации.** Правка минимальна и аддитивна, размещена в правильном месте,
переиспользует существующий хелпер, дельта-спека faithful (не теряет существующие
сценарии), задачи верифицируемы на реальных данных. Замечания ниже —
**не блокирующие** (документация/полнота), правку кода не меняют.

## 1. Проверка артефактов

| Артефакт | Статус | Замечание |
|---|---|---|
| `proposal.md` | OK | Все обязательные секции. `Modified Capabilities` = `review/deploy-stoppers` — путь совпадает с `openspec/specs/`. |
| `design.md` | OK | Все обязательные секции (`Context`/`Goals-Non-Goals`/`Decisions`/`Risks`). Решения D1–D3 обоснованы, альтернатива (quote-aware `hasSaveTran`) корректно отклонена. |
| `specs/review/deploy-stoppers/spec.md` | OK | `## MODIFIED Requirements` + `### Requirement: Использование SAVE TRAN` (совпадает с main spec, `spec.md:216`). |
| `tasks.md` | OK | Группы `## 1..3` с чек-боксами; включая bump buildNumber (3.1) и приёмку на FA (3.2–3.3). |
| `.openspec.yaml` | OK | `schema: spec-driven`; есть дельта → `skip_specs` не нужен. |

**Faithfulness MODIFIED.** Проверено по main spec: у требования «Использование SAVE TRAN»
(`openspec/specs/review/deploy-stoppers/spec.md:216-224`) **ровно один** сценарий —
`Scenario: SAVE TRAN`. Дельта воспроизводит его + добавляет 2 новых → при `archive`
**ни один существующий сценарий не теряется**. Тело требования расширено (вне литералов +
`''` не закрывает литерал) корректно.

## 2. Анализ кода под изменение

- `checkSaveTran` — `internal/review/review_rules.go:949`; целевая строка — `:996`
  `lower := strings.ToLower(line)`. Task 1.1 указывает «~996» — точно.
- `hasSaveTran` — `review_helpers.go:1473`, чистый substring + word-boundary matcher
  (`save tran` / `save transaction`).
- `stripQuotedSegments` — `review_rules.go:1866`, **в том же пакете/файле**, удаляет
  сегменты в одинарных кавычках с учётом `''`. Новых импортов не требует.
- `hasRawTransactionControl` — `review_helpers.go:1841`: литералы уже пропускает инлайн
  (причина, почему `rawTransactionControl` FP не подвержен).
- **Единственный вызов `hasSaveTran`** — в `checkSaveTran` (`review_rules.go:999`).
  Правка в `checkSaveTran` покрывает правило `saveTran` полностью; других потребителей нет.

**Проверка fix на строке FP.** `stripQuotedSegments("    PROFILE_TIME_EX('BEGIN tran and SAVE tran trnCardAccRelation_MassLoad...')")` → `    PROFILE_TIME_EX()`; `hasSaveTran` → false. FP устранён.

**Проверка сценария с экранированным апострофом (task 2.2).**
`select @s = 'save tran ''quoted'' text'` → strip → `select @s =  text` (без `save tran`);
отдельная строка `SAVE TRAN MySavePoint` не содержит кавычек → 1 finding на своей строке. Логика верна.

## 3. Соответствие реальным данным

- FP воспроизводится на `FA/fa-cards/Cards/SERVER/Card/CardAccRelation_MassLoad.sql`:
  строка **130** = `PROFILE_TIME_EX('BEGIN tran and SAVE tran trnCardAccRelation_MassLoad...')`,
  строка **132** = `__BEGIN_TRAN__(trnCardAccRelation_MassLoad)`. Совпадает с proposal.
- `SeverityDeployStopper = 1` (`types.go:70`); `RuleSaveTran` → `SeverityDeployStopper`
  (`catalog.go:187`). Дельта «severity 1» совпадает с фактом.
- `buildNumber = "1715"` (`cmd/root.go:29`) → task 3.1 (bump «1715»→«1716») корректен.
- Baseline `go test ./internal/review/ -run SaveTran` — **зелёный**.
- В unit-тестах `newRuleTestExec` кладёт в `exec.content` **нерасширенный** контент,
  а `fileContent` возвращает именно `exec.content`; `__BEGIN_TRAN__` в task 2.1 не раскроется
  и на ожидание «0 findings» не влияет.

## 4. Замечания

| # | Severity | Место | Суть | Рекомендация |
|---|---|---|---|---|
| N1 | Minor | design.md D3 | Обоснование порядка «комментарии → литералы» покрывает только апострофы внутри `--`-комментариев. Обратный случай — маркеры `--`/`/*` **внутри** литерала — по-прежнему ломает разбор (комментарий вырезается до литерала). | Residual, **pre-existing**, данным change не ухудшается. Не менять порядок (блок-коммент тесты завязаны); при желании дополнить D3 одной строкой. |
| N2 | Minor | design.md Risks | Правка глушит `SAVE TRAN` внутри **любых** литералов, включая динамический SQL: `sp_executesql N'save tran x'`, `exec('...SAVE TRAN...')` перестанут флагуеться → новый **FN**. В Risks зафиксирован только FN от незакрытого литерала. | Добавить динамический SQL в `Risks / Non-Goals` как принятый trade-off (редко; SAVE TRAN через exec — экзотика). |
| N3 | Minor | proposal.md Impact | Impact перечисляет `review_rules.go`, `review_rules_test.go`, `spec.md`, приёмку, но **не упоминает** bump `cmd/root.go` (task 3.1) и файл тестов покрытия (`review_rule_coverage_test.go`). | Дополнить Impact: `cmd/root.go` (buildNumber) и `internal/review/review_rule_coverage_test.go` (кейсы). |
| N4 | Info | design.md:8 | Ссылка «`stripQuotedSegments` используется `tableExistsInMetadata` (:1926)» — 1926 указывает на хелпер `onlyQuotedOccurrence`, которым пользуется `checkTableExistsInMetadata`. Формально неточно. | Поправить формулировку (через `onlyQuotedOccurrence`). Не блокирует. |
| N5 | Info | tasks.md 2.x | Кейсы 2.1/2.2 лучше добавить в `TestCheckSaveTran_Coverage` (`review_rule_coverage_test.go:933`) с `wantLine` для 2.2 — инфраструктура `runFileRuleCases`/`ruleCase.wantLine` это поддерживает. | Уточнить файл/функцию в задачах (опционально). |

## 5. Рекомендации (по приоритету)

1. (N2) Зафиксировать в `design.md` → `Risks / Non-Goals` принятый FN для динамического SQL в литералах.
2. (N3) Дополнить `proposal.md` → `Impact` пунктами `cmd/root.go` и тест-файлом покрытия.
3. (N1, N4) Косметика документации — при желании одной правкой.
4. Блокеров для старта реализации нет.

## 6. Чек-лист приёмки (из tasks.md — согласовано)

- [ ] `lower := strings.ToLower(stripQuotedSegments(line))` в `checkSaveTran`; `go build ./...` OK (1.1).
- [ ] `go vet ./internal/review/` + `gofmt -l internal/review/` пусто (1.2).
- [ ] Регресс-тест литерала → 0 (2.1); экранированный апостроф + реальный `SAVE TRAN` → 1 на нужной строке (2.2); `go test ./internal/review/` зелёный, включая `review_block_comment_test.go` (2.3).
- [ ] buildNumber «1715»→«1716» (`cmd/root.go:29`), пересборка (3.1).
- [ ] `review ...CardAccRelation_MassLoad.sql --rules saveTran` → 0 (было 1 на :130) (3.2).
- [ ] Контроль полноты на FA: реальные `SAVE TRAN` вне литералов флагуются, только-в-литералах — нет (3.3).
