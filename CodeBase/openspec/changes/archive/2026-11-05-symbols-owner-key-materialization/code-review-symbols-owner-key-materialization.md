# Code review: symbols-owner-key-materialization

Дата ревью: 2026-11-05. Предмет: **реализованная** доработка
`symbols-owner-key-materialization` (build 1672, изменения не закоммичены).

Ревью выполнено по фактическому коду (`git diff`), новым integration-тестам и
дельта-спекам. Код в ходе ревью не изменялся.

## Объём ревью

| Артефакт | Что проверено |
|---|---|
| `internal/store/db_schema.go` | DDL `CREATE TABLE symbols`, `ALTER ... ADD COLUMN IF NOT EXISTS`, bump v4→v5, миграция `symbols_owner_name_backfill_v1`, порядок в транзакции |
| `internal/store/db_insert_j.go` | `pq.CopyIn("symbols", ..., owner_name)`, санитизация значения |
| `internal/model/model.go` | поле `Symbol.OwnerName` |
| `internal/indexer/indexer_sql_pas.go` | `OwnerName = method.ClassName` для method-символов |
| `internal/indexer/indexer.go` | карта `formClassByID`, `OwnerName` для component-символов |
| `internal/query/query.go` | удаление `symbolOwnerKeyExpr`, `COALESCE(s.owner_name,'') AS owner_key` в `SearchSymbol`/`symbolBaseBranch` и `SearchInContent` |
| `internal/query/query_symbol_types_integration_test.go` | инлайн-эталон `legacyReportFormLookup` |
| `internal/query/query_branch_dedup_integration_test.go` | новый `TestSearchSymbolOwnerNameDedup` |
| `internal/store/db_schema_owner_name_integration_test.go` (новый) | тест колонки и backfill |
| `internal/indexer/indexer_owner_name_integration_test.go` (новый) | тест индексатора (method/component, регистр, Update) |
| `proposal.md`, `design.md`, `tasks.md`, 3 дельты | соответствие коду |

Дополнительно проверено grep'ом: иных мест создания method/component-символов
нет (только `indexer_sql_pas.go:934` и `indexer.go:844`), иных `INSERT/UPDATE
symbols` в рантайме нет (только `CopyIn` вставка и backfill-миграция), остатков
`symbolOwnerKeyExpr` в Go-коде нет (только комментарий-якорь теста).

## Итог

**Блокирующих дефектов (High/Critical) не найдено.** Реализация соответствует
design: материализация выполняется в двух существующих точках создания
символов, значение санитизируется в store по общему паттерну, три потребителя
`owner_key` закрыты, backfill — type-scoped set-based UPDATE, схема версионирована
и миграция стоит в правильном месте транзакции. Существующая семантика для
резолвнутых владельцев сохранена.

Ниже — замечания уровня Low/Info (качество тестов, защитность миграции,
документация). Они не блокируют архивацию.

## Находки

### 1. [Low] Тест идемпотентности схемы проверяет fast-path, а не проверку маркера в `applyMigrationTx`

`internal/store/db_schema_owner_name_integration_test.go:101-141`.

После первой миграции тест вручную ставит `owner_name='MANUAL'` (строка 134) и
повторно вызывает `InitSchema` (строка 137). Однако маркер текущей версии v5 к
этому моменту уже записан, поэтому `InitSchemaCtx` выходит по fast-path
(`db_schema.go:22-24`) и **вообще не доходит** до `applyMigrationTx`. Тест
доказывает no-op повторной инициализации, но не доказывает, что при наличии
маркера `symbols_owner_name_backfill_v1` UPDATE не выполняется (задача 1.3
формулировала именно «маркер есть → UPDATE не выполнился»).

Рекомендация: после мутации вызвать `testutil.ResetSchemaVersion(t, db)` (удаляет
только маркер v5, оставляя `symbols_owner_name_backfill_v1`) и затем
`InitSchema` — тогда проход дойдёт до `applyMigrationTx`, увидит backfill-маркер
и не тронет `MANUAL`. Дополнительно стоит добавить сценарий реального апгрейда
v4-БД: `ALTER TABLE symbols DROP COLUMN owner_name` + сброс маркеров →
`InitSchema` → колонка пересоздана и заполнена (сейчас проверяется только
`CREATE TABLE` на свежей БД, ветка `ALTER ... ADD COLUMN IF NOT EXISTS` автотестом
не покрыта).

### 2. [Low] Backfill UPDATE не ограничен `owner_name = ''` (перезапись уже заполненных значений)

`internal/store/db_schema.go:1276-1281`.

На штатном переходе v4→v5 все строки имеют `owner_name=''`, поэтому разницы нет.
Но UPDATE не содержит предиката `AND s.owner_name = ''`, поэтому при повторном
применении миграции (потеря/восстановление маркера, частичный restore, ручной
сброс `schema_migrations`) он перезапишет уже материализованные значения. Для
method это особенно чувствительно: backfill пишет регистр **декларации**
(`pas_classes.class_name`), а индексатор — регистр **ссылки** (`method.ClassName`,
D4.2), т.е. повторный backfill может затереть корректное reference-case значение
на declaration-case. Дополнительно это лишний WAL на уже заполненных строках.

Рекомендация: добавить `AND s.owner_name = ''` в оба UPDATE. Семантика штатного
прогона не меняется, защита от повторного применения усиливается.

### 3. [Low] `symbolOwner` в тесте индексатора читает одну произвольную строку

`internal/indexer/indexer_owner_name_integration_test.go:44-51`.

`SELECT owner_name, entity_id ... WHERE symbol_name=$1 AND symbol_type=$2` через
`QueryRow` не проверяет кардинальность. Парсер создаёт method-строки и для
объявления в классе, и для реализации, поэтому для `Execute`/`Prepare` в
`symbols` может быть более одной строки. Сейчас у них одинаковый владелец, и тест
проходит, но при регрессии одного из дублей проверка может «случайно» увидеть
правильную строку.

Рекомендация: для method/component ассертить `COUNT(*)` и все значения
`owner_name` (или использовать `array_agg`), как это уже сделано для `Run`
(строки 78-85).

### 4. [Info] Опечатка в комментарии тест-эталона

`internal/query/query_symbol_types_integration_test.go:326`: «для report_form-строк
ветка владельца всегда `”`» — вместо `''` стоит типографская кавычка. Косметика,
на код не влияет.

### 5. [Info] Design D2 не фиксирует, что backfill пишет регистр декларации

Backfill берёт `pc.class_name` (регистр декларации), а индексатор — `method.ClassName`
(регистр ссылки, D4.2). На данных с неконсистентным написанием легаси-строка
после миграции и переиндексированная строка той же ветки получат разные
`owner_key` и временно не схлопнутся. Это следствие уже принятого D4.2, но в D2
явно не проговорено. Рекомендация: одна фраза в D2/D4 о том, что backfill
воспроизводит регистр декларации (и это ожидаемое переходное расхождение).

### 6. [Info] Требование про pending-резолв вакуумно; сценарий «нерезолвнутые классы» синтетический

`specs/indexing/pas-dfm-parsing/spec.md:16-17` требует, чтобы pending-резолв не
менял уже записанного владельца. По обнаруженному при реализации факту
(design D4.1) метод с нерезолвнутым классом вообще не создаёт символа, поэтому
pending-резолв никогда не имеет символа, на который мог бы повлиять — требование
выполняется тривиально и тестом не проверяется. Аналогично сценарий
`query/symbol-search`: «нерезолвнутые классы с заполненным `owner_name`» достижим
только прямой вставкой в `symbols`, индексатор таких строк не производит.
Это не дефект, но формулировки стоит либо пометить как Notes, либо явно
квалифицировать как «поведение слоя запросов для строк с заполненным значением».

### 7. [Info] Операционное окно миграции

Backfill выполняется в транзакции `InitSchemaCtx` до записи маркера v5. Пока
транзакция не закоммичена, read-only команды видят старую версию схемы и падают
с требованием `update`, а конкурентные `init`/`update` ждут advisory-lock.
Это соответствует readonly-schema-policy и отражено в Risks, но в
`BUG-symbols-owner-name-materialization-verification-20261105.md` зафиксированы
~11 минут на первый `update`, причём отдельного замера вклада именно backfill
нет. Рекомендация: при следующем прогоне на FA-scale замерить длительность
`ALTER` + двух UPDATE отдельно от индексации, чтобы иметь точную оценку окна.

### 8. [Info] Гарантия «переиндексация пересоздаёт владельца» зависит от пре-существующего pre-filter

Сценарий `specs/indexing/pas-dfm-parsing/spec.md:51-56` и задача 4.1(в) опираются
на то, что `update` реально переиндексирует изменённый файл. FA-верификация
обнаружила пре-существующую аномалию pre-filter (touch+mtime не приводит к
переиндексации; `mtime` на диске новее, чем в БД), вынесенную за scope.
Автоматический Update-тест (`indexer_owner_name_integration_test.go:114-131`)
подтверждает пересоздание владельца только для случая, когда файл действительно
переиндексирован. Это не регрессия данной доработки, но реальная гарантия
«после переиндексации» остаётся условной до починки fingerprint'а `fswalk`.

## Соответствие дельта-спекам

| Требование | Реализация | Статус |
|---|---|---|
| `symbols.owner_name TEXT NOT NULL DEFAULT ''` в CREATE + ALTER IF NOT EXISTS | `db_schema.go:122,129` | Соответствует |
| bump схемы (readonly-schema-policy) | `db_schema.go:13` → `codebase_schema_v5` | Соответствует |
| Однократная миграция `symbols_owner_name_backfill_v1` | `db_schema.go:1275-1284` (`applyMigrationTx`, порядок после DDL, до маркера версии) | Соответствует |
| Backfill type-scoped, set-based, без per-row подзапросов | два `UPDATE ... FROM`, LEFT/INNER join по D2 | Соответствует |
| Резолвнутые строки = прежнему значению, нерезолвнутые/dangling = `''` | `COALESCE` + INNER/LEFT join; тест строки 116-128 | Соответствует |
| Повторные прогоны не выполняют backfill | `applyMigrationTx` проверяет маркер (`db_schema.go:1306-1312`) | Соответствует (тест см. находку 1) |
| Индексатор: method → имя класса как в коде | `indexer_sql_pas.go:942` | Соответствует |
| Индексатор: component → класс родительской формы того же файла | `indexer.go:777-791,849-852` | Соответствует |
| Прочие типы — `''` | `OwnerName` не заполняется, DEFAULT | Соответствует |
| Санитизация в store, сырое значение из индексатора | `db_insert_j.go:153` рядом с остальными полями | Соответствует |
| `SearchSymbol`/`SearchInContent` читают ключ из колонки | `query.go:413,535` | Соответствует |
| Планы без SubPlan владельца | по bug-report: 46.7 мс / 10 709 buffer, SubPlan нет | Подтверждено отчётом |
| Тест-эталон сохраняет старое выражение | инлайн-копия в `query_symbol_types_integration_test.go:328-334` | Соответствует |

## Выполненные проверки

- `go build ./...` — успешно.
- `gofmt -l` по всем изменённым/новым Go-файлам — пусто.
- `openspec validate symbols-owner-key-materialization --strict` — valid.
- grep: `symbolOwnerKeyExpr` в Go-коде отсутствует (остались только комментарии и
  исторические документы); иных источников method/component-символов и иных
  `INSERT/UPDATE symbols` нет.
- Совместимость отката бинаря: маркеры в `schema_migrations` накапливаются, старый
  бинарь находит v4, колонку не читает — соответствует Migration Plan.
- Integration-тесты и FA-верификация в рамках ревью не перезапускались;
  использованы зафиксированные результаты (tasks.md 4.1-4.3 и bug-report:
  store 277 с, indexer 181 с/103 с, query 223 с + querysvc 34 с; 226/226 CLI).

## Заключение

Доработка реализована полно и корректно, отклонений от design, влияющих на
функциональность, не выявлено. Замечания носят характер усиления тестов и
защитности миграции (пункты 1-3) плюс документационные/операционные заметки
(4-8). Блокеров для `/opsx-archive` нет; пункты 1-3 желательно закрыть до
архивации или зафиксировать как принятые ограничения.
