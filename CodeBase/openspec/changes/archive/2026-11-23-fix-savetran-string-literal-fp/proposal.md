## Why

Правило `saveTran` даёт ложное срабатывание (FP) на тексте внутри строковых литералов. Прогон build 1715 на `FA/fa-cards/Cards/SERVER/Card/CardAccRelation_MassLoad.sql` выдал finding на строке 130, где `SAVE TRAN` встречается только внутри литерала `PROFILE_TIME_EX('BEGIN tran and SAVE tran trnCardAccRelation_MassLoad...')`; настоящего savepoint в коде нет — транзакция ведётся макросом `__BEGIN_TRAN__`. Причина: `checkSaveTran` чистит комментарии, но не вырезает литералы, тогда как соседнее правило `rawTransactionControl` литералы уже пропускает.

## What Changes

- В `checkSaveTran` (`internal/review/review_rules.go`) после чистки комментариев строка очищается от сегментов в одинарных кавычках существующим хелпером `stripQuotedSegments` перед вызовом `hasSaveTran` (тот же приём, что уже используется в `tableExistsInMetadata`).
- Поведение `saveTran` выравнивается с `rawTransactionControl`: вхождения `SAVE TRAN`/`SAVE TRANSACTION` внутри строковых литералов игнорируются.
- FP на файле-примере: 1 finding → 0.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/deploy-stoppers`: требование «Использование SAVE TRAN» уточняется — обнаружение только в коде вне строковых литералов; добавляются сценарии на литерал (в т.ч. с экранированным апострофом).

## Impact

- `internal/review/review_rules.go` — правка `checkSaveTran` (одна строка).
- `internal/review/review_rule_coverage_test.go` — регрессионные кейсы в `TestCheckSaveTran_Coverage`.
- `internal/review/review_rules_test.go`, `internal/review/review_block_comment_test.go` — существующие тесты должны остаться зелёными.
- `cmd/root.go` — bump buildNumber «1715»→«1716».
- `openspec/specs/review/deploy-stoppers/spec.md` — дельта требования.
- Верификация на FA: `CardAccRelation_MassLoad.sql` и другие файлы с `save tran` в литералах.
