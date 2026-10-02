# Design: fix-spec-mention-resolver-scope

## Context

См. proposal.md (Why). Текущее состояние: 7 batch-lookup'ов в `internal/store/db_lookup_sql.go` / `db_lookup_spec_mentions.go` резолвят имена глобально (`MAX(id)` / `DISTINCT ON ... id DESC`) без продуктового скоупа и без отсечения генерируемых копий; вызываются из spec-постпроцессора (`indexer_postprocess_spec.go`) и из preload'а review (`review_lookup.go`: `BatchLookupProcedureParams`, `BatchLookupProcedureProductIDs`). Контекст продукта упоминания доступен в БД (`spec_code_mentions.file_id → files.ds_product_id`), но не используется. Прецедент решения: `fix-datatype-cross-product-lookup` — трёхуровневый ORDER BY `(file_id = $n) DESC, (ds_product_id = $m) DESC, id DESC` в `FindLatestSQLColumnDefinitionType`; integration-тесты через `testutil.Open`.

Ограничение инкрементального update: pre-filter по mtime+size пропускает неизменённые файлы, поэтому новый признак файла не заполнится у существующих записей без backfill в миграции.

## Goals / Non-Goals

**Goals:**

- Детерминированный резолв одноимённых сущностей: генерируемые копии и чужой продукт не перехватывают `references_code` и продуктовые карты review.
- Заполнение признака `is_generated` как для новых, так и для уже проиндексированных файлов.
- Сохранение двухфазности резолва (спека может ссылаться на код, проиндексированный позже) и пакетности lookup'ов (без N+1).

**Non-Goals:**

- Хранение пути упоминания (`mention_path`) и резолв по точному пути — отдельная будущая работа.
- Конфигурация паттернов генерируемых копий (`generated_patterns`) — конвенции Diasoft стабильны; при появлении потребности добавляется ключ в `[indexer]`.
- Фильтрация копий в ad-hoc отчётах непокрытого кода и перестройка потребителей `references_code` — они лечатся автоматически переориентацией `target_id`.
- Удаление копий из индекса или их исключение из парсинга.

## Decisions

### D1. Признак `is_generated` на `files`, детект — хардкод-хелпер в Go, без конфига

`isGeneratedFile(relPath, ext string) bool` — сегмент пути `UPLOAD` без учёта регистра + расширение `t01`. Вычисляется один раз в walker (поле `FileInfo.IsGenerated`) и пишется в `saveFileCtx`.

Альтернативы: (а) LIKE-условия прямо в lookup-запросах — отклонено: конвенции дублируются в 7+ запросах, флаг не доступен другим потребителям (отчёты, статистика); (б) конфигурируемые паттерны `generated_patterns` — отклонено как преждевременное (решение зафиксировано в explore-диалоге), добавляемо позже без ломки схемы.

### D2. Backfill в идемпотентной миграции схемы

`ALTER TABLE files ADD COLUMN IF NOT EXISTS is_generated BOOLEAN NOT NULL DEFAULT FALSE` + `UPDATE files SET is_generated = TRUE WHERE rel_path ~* '/UPLOAD/' AND NOT is_generated OR (extension = 't01' AND NOT is_generated)` в общем списке миграций `InitSchema` (запускается и при `update`). Backfill идемпотентен и самозаживляется, покрывает пропущенные pre-filter'ом файлы.

Альтернатива: требовать полный `codebase init` — отклонено: дорог для пользователей, а `update` после деплоя иначе оставит флаг незаполненным.

### D3. Продуктовый контекст — из файла упоминания, группировка в Go

`LoadAllSpecCodeMentions` дополняется `ds_product_id` (JOIN `files` по `file_id`). Имена группируются по продукту; на каждую группу — один batch-lookup с параметром продукта. Продукт = 0 (файл без продукта) попадает в группу без продуктового предпочтения.

Альтернативы: (а) один мега-SQL с коррелированным ORDER BY по каждому упоминанию — отклонён: сложный SQL, труднее тестировать, выигрыш в числе запросов незначим (единицы продуктов); (б) keep-глобальная карта `map[name]id` — отклонена: не выражает разный резолв одного имени для разных продуктов.

### D4. Порядок предпочтения кандидатов — стилем datatype-фикса

`ORDER BY (is_generated = FALSE) DESC, (ds_product_id = $productID) DESC, id DESC` c JOIN `files` по `file_id` сущности (PK-join, дёшево). Для мультикарты (`FindAPIContractIDsByTableNames`) — фильтр `is_generated` при наличии хотя бы одного не-копии (реализуется в SQL через `COUNT(*) FILTER (WHERE NOT is_generated) > 0`).

Tie-break `id DESC`, а не `MIN(id)`/`rel_path` — консистентность с `FindLatestSQLColumnDefinitionType`; после отсечения копий и чужого продукта остаточные дубли редки, важна только детерминированность. Отклонение от формулировки баг-репорта (MIN(id)) осознанное.

### D5. Сигнатуры lookup-функций с продуктовым параметром

`FindXxxIDsByNames(ctx, names, productID int64)` — параметр, не новые функции; нулевой `productID` валиден (приоритет не-копий сохраняется). `BatchLookupProcedureProductIDs` меняет порядок на `(NOT is_generated) DESC, id DESC` (продуктовый параметр ему не нужен — он сам возвращает продукты); `BatchLookupProcedureParams` — тот же порядок. Мёртвые `FindLatestSQLProcedureIDByName` / `FindLatestSQLTableIDByName` удаляются.

Альтернатива: оставить старые сигнатуры и добавить `...Scoped` варианты — отклонено: два живых API решают одну задачу, вызывающих у старых мало (spec-постпроцессор + review preload).

### D6. Integration-тесты через `testutil.Open`

По прецеденту `db_lookup_sql_integration_test.go`: без sqlmock (отклонён в предыдущем change — D5/tasks). Кейсы: копия с большим id проигрывает; кросс-продукт проигрывает своему продукту; разные продукты одного имени резолвятся раздельно; карта продуктов без копий; фильтр мультикарты; backfill-миграция; `isGeneratedFile` (unit: UPLOAD/upload, t01, канон).

### D7. Верификация на FA — дифф до/после

Baseline берётся из существующего индекса `root_path` (fa-проекты уже проиндексированы текущим HEAD-бинарем с дефектным резолвом): до доработки сохраняются в файлы выводы `spec-by-code`, покрытия capability и выборка рёбер `references_code`; после — переиндексация fa-contracts новым бинарем и сравнение. Ожидание: рёбра `references_code` на сущности из `*/UPLOAD/*`/`.t01` уходят в ~0, канонические файлы исчезают из выборки непокрытого кода (ложные пробелы закрыты). Отдельная baseline-сборка через git worktree (прецедент `fix-datatype-cross-product-lookup`) не требуется: фикс ещё не в HEAD, текущий бинарь честно воспроизводит старое поведение.

## Risks / Trade-offs

- [Backfill UPDATE на большой таблице files] → индемпотентный, одноразовый по эффекту; при повторных запусках фильтр `NOT is_generated` ограничивает объём пустым множеством.
- [Смена сигнатур ломает вызывающих] → вызывающих мало и все в скоупе (spec-постпроцессор, review preload); компилятор укажет все точки.
- [Сущности с NULL-продуктом у спек без продукта] → группа без предпочтения; поведение не хуже текущего (глобальный latest), копии по-прежнему отсечены.
- [Дубли одного имени в одном продукте вне UPLOAD] → tie-break `id DESC` не «каноничнее» прочих; принят как осознанный trade-off (D4).
- [Копии остаются в индексе и в symbol-search] → вне скоупа; флаг даёт основу для будущей фильтрации, но сам поиск не меняется.

## Migration Plan

1. Деплой новой сборки; миграция применяется при первом `update`/`init` (backfill).
2. Рекомендуется полный `codebase update` (relations пересобираются постпроцессором автоматически) или `init` для консистентности.
3. Откат: предыдущий бинарь работает со старой схемой; лишняя колонка безвредна (данные не портятся, lookup откатывается к глобальному latest-wins).

## Open Questions

Нет — ключевые развилки (конфиг vs хардкод, tie-break, скоуп) закрыты в explore-диалоге и зафиксированы в D1/D4.
