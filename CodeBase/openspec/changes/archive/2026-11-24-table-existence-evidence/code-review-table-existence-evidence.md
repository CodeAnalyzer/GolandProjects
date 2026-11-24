# Code review (реализация): `table-existence-evidence`

Дата: 2026-11-24
Сборка: `CodeBase 0.9.5 build 1719`
Объект: рабочее дерево `CodeBase` (незакоммиченные изменения) + артефакты change
`openspec/changes/table-existence-evidence/`.

## 1. Вердикт

**Реализация соответствует плану; блокирующих дефектов и регрессов не выявлено.**
Покрыты все задачи `tasks.md` (1.1–6.2) и решения `design.md` D1–D6, включая
дельта-спеки `review/deploy-stoppers` и `review/review-scope`. Замечания
предыдущего change-review (R1–R10) закрыты.

Независимо подтверждено: `go build`/`go vet`/`gofmt` чисто; unit- и **полные**
integration-наборы `internal/review` и `internal/store` зелёные;
`openspec validate --all --strict` — 41/0; живой прогон целевого кейса
`ParticipantSB_Delete.sql` — **4 → 0** finding; контрольный `ContractMassLoad.sql`
по-прежнему даёт `tableExistsInMetadata`; корпусный замер 246 → 215 finding
арифметически сходится; `EXPLAIN` показывает использование нового индекса.

Неблокирующих замечаний — 7 (F1–F7); **ни одно не требует правки кода**. F1 и F3 —
осознанные ограничения/производительность (рекомендуется 1–2 строки в
`design.md` ## Risks), F2 — среда (`-race` недоступен), F4–F6 — nits «действий не
требуется», F7 — follow-up по парсеру. Изменение готово к архивации.

## 2. Независимая проверка

| Проверка | Результат |
|---|---|
| `go build ./...` | OK |
| `go vet ./internal/review ./internal/store` | OK |
| `gofmt -l internal/review internal/store` | пусто |
| `go test ./internal/review ./internal/store ./cmd` | ok (0.43s / 0.71s / 0.25s) |
| `go test -tags integration ./internal/review -count=1` | ok 34.66s (весь пакет) |
| `go test -tags integration ./internal/store -count=1` | ok 213.32s (весь пакет) |
| targeted: `TestBatchLookupTableExistenceEvidence`, `TestInitSchema_IndexDefinitionsLowerV8` | PASS (8.93s / 1.56s) |
| targeted: `TestCheckTableExistsInMetadataEvidenceScope`, `TestIndexExistsInDBGateScopedEndToEnd` | PASS (10.06s / 1.44s) |
| `openspec validate table-existence-evidence --strict` | valid |
| `openspec validate --all --strict` | 41 passed, 0 failed |
| `CodeBase.exe --version` | `0.9.5 build 1719` |
| live: `review ParticipantSB_Delete.sql --rules tableExistsInMetadata,indexExistsInDB` | 0 findings |
| live: `review ContractMassLoad.sql` (develop) | 5 × `tableExistsInMetadata` (вкл. `pAPI_Account_MassClose` 2567,2568), 0 × `indexExistsInDB` |
| `tee_baseline_summary.txt` → `tee_after_summary.txt` | 246 / 49 имён / 40 файлов → 215 / 47 / 37 |
| арифметика ушедшего | 31 = 22 (`tReplTables`) + 9 (`tOperTemplateDoc`); 3 файла = 1 + 2; 2 имени |
| `tee_baseline_case.json` → `tee_after_case.json` | 4 (2 index + 2 table) → 0 |
| `tee_explain.txt` | Bitmap Index Scan по `idx_sql_index_definitions_table_name_lower`; Index Scan по `idx_sql_column_definitions_lower_table_col` |
| `tee_62_ancient_names.txt` | 29 имён, свидетельства покрывают 8 (совпадает с design) |

Замечание по верификации: `-race` не запускался (на машине нет gcc). Новых общих
записей изменение не добавляет: `tableEvidenceCache` пишется только в prewarm до
старта параллельных правил; гейт и `cachedLookupTableEvidence` только читают
кэши, ленивые пути не пишут в общий map. Рекомендация — гонять `-race` в CI (F2).

## 3. Соответствие change

| Задача | Реализация |
|---|---|
| 1.1 baseline | Свежий baseline 1718 на том же снимке (246/49/40); артефакты `tee_baseline_*`; 100/239 — справка |
| 2.1 индекс + v8 + тест миграции | `db_schema.go:13` (v8), `:1089` (lower-индекс); `schema_integration_test.go:121` (`requiredIndexes`), `:663-698` (v7→v8); `EXPLAIN` — индекс используется |
| 2.2 lookup свидетельств | `db_lookup_sql.go:849` — `UNION ALL`, оба `definition_kind IN (...)`, `tableSourceAreaFilter` (`:726`); store-integration PASS |
| 3.1 кэш/prewarm/`dmlTableNames` | `review_lookup.go:467` (`dmlTableNames`), `:523` (`prewarmTableEvidence`), `:549` (`cachedLookupTableEvidence`); сброс `runner.go:144` |
| 3.2 правило + сообщение | `review_rules.go:2041` (свидетельство после создателя), `:2055` (новый текст); unit-сценарии в `table_exists_metadata_test.go` |
| 3.3 scope свидетельств | `review_scope_integration_test.go:132-145` (фикстура), `:517-577` (prewarm/ленивый, develop vs SP, EXT union, чужой банк); PASS |
| 4.1 гейт | `review_lookup.go:579` — приоритет cache(false) → `db==nil` → lazy; `review_rules.go:1440`, `:1496` — до `lookupIndexExists`; `runner.go:215-227` — prewarm при `tableExistsInMetadata || indexExistsInDB`; `index_exists_gate_test.go`; block/coverage-тесты не менялись |
| 4.2 end-to-end гейт | `review_scope_integration_test.go:840-933` — (а)–(е), включая API-таблицу в области и создателя вне области; PASS |
| 5.1 README/catalog | `README.md:1056,1073` обновлены; `catalog.go` не менялся — см. F6 (действий не требуется) |
| 5.2 сборка/версия | `cmd/root.go:29` → 1719; build/vet/gofmt/тесты; `CodeBase.exe` пересобран |
| 5.3 ручной кейс/контроль | live-прогоны: 4→0 и контроль 5 findings — подтверждено |
| 5.4 validate | 41/0 |
| 6.1 замер после | 215/47/37 на том же снимке (0 переиндексации); ушедшие имена объяснены |
| 6.2 fa-payments | 29 имён, 8 покрыто; 1 216 findings на 5 непокрытых (`tKey` 623/147); зафиксировано в design |

## 4. Ревью решений D1 / D2 / D4

**D1 (два lookup'а + lower-индекс + bump v8) — принято.** Запросы в
`BatchLookupTableExistenceEvidence` повторяют семантику создателей: `JOIN files` +
`tableSourceAreaFilter` только при непустых `pairs`, глобальный вариант без
фильтра; `definition_kind` перечислен явно в обеих ветвях. Схема меняется только
индексом; данные не мигрируют, реиндексация не нужна. Fast-path
`InitSchemaCtx` защищён bump'ом; тест миграции и `requiredIndexes` на месте.
Планировщик использует оба индекса (`EXPLAIN`).

**D2 (свидетельства из снимка индекса) — принято.** Проверка стоит после
`cachedLookupTableCreator == false` и до `isViewTable`, что сохраняет атрибуцию
view. Поведение same-file зафиксировано корректно: при актуальном индексе
свидетельство файла подавляет finding, при устаревшем — нет
(`TestCheckTableExistsInMetadata_SameFileEvidence`). Остаточный риск
самоподтверждения опечатки задокументирован в Risks.

**D4 (гейт индексов по создающему источнику) — принято.** Порядок разрешения
реализован ровно как решено в R2: запись в непустом `tableCreatorCache`
(включая `false`) → иначе `db == nil → true` (unit-seam) → иначе ленивый
`cachedLookupTableCreator`. Гейт стоит до `lookupIndexExists` в обеих ветках;
ошибка БД даёт `false` (fail-open для индексов) при fail-closed table-level
правиле. API/DSArchitect-таблицы проходят гейт в области — доказано
интеграционным сценарием (г). p-таблицы не затронуты регрессом: их `TmpTbl`
создатели проиндексированы (проверено ранее на `pCons_ObjectID`), сценарий (б)
подтверждает finding при создателе.

## 5. Регресс-анализ

- **Монотонность:** оба механизма только подавляют findings (свидетельство — после
  создателя; гейт — до проверки индексов). Новых findings изменение породить не
  может. Единственное изменение вывода — расширенный текст
  `tableExistsInMetadata` (по спеке, покрыт тестом).
- **p-таблицы / API:** создатель в индексе → гейт открыт; подтверждено
  end-to-end (б),(г) и полным integration-набором.
- **Unit-seam `db == nil`:** существующие тесты с пустым кэшем остаются
  инертными (шаг 2), тесты с `false` в кэше подавляют (шаг 1) — полный unit-прогон
  зелёный, block-comment/coverage-тесты не правились.
- **Store-рефакторинг** (`normalizeTableLookupNames`, вынос
  `tableSourceAreaFilter`) семантически эквивалентен прежнему коду; полный
  integration-набор `internal/store` зелёный.
- **Схема:** bump v8 + `IF NOT EXISTS`; fast-path не пропустит DDL; миграция
  проверена на временной БД и на реальном индексе FA.
- **Реальные данные:** кейс 4→0 (артефакты + живой прогон), контроль не ослаб
  (`ContractMassLoad` 5 findings), корпус 246→215 без новых имён, ушедшие имена
  объяснены свидетельствами.

## 6. Замечания (неблокирующие)

| ID | Уровень | Область | Суть | Решение |
|---|---|---|---|---|
| F1 | низкий | `indexCheckVerifiable` (`review_lookup.go:579`) | Гейт учитывает только создателя из индекса и не смотрит same-file `CREATE`/`SELECT INTO` из `parsed` (в отличие от `created`-карты `checkTableExistsInMetadata`, `review_rules.go:2032`). В состоянии «индекс устарел» файл, сам создающий таблицу и использующий на ней index-хинт, получит подавление проверки индекса (потенциальный FN). | **Не править.** Сценарий редкий (TmpTbl-создатели — отдельные файлы; same-file create + hint нетипичен), контракт review — «`codebase update` перед review». Принять как остаточный риск; рекомендуемое (не обязательное) действие — 1 строка в `design.md` ## Risks. Правка потребовала бы прокинуть `parsed` в `checkIndexExistsInDB` и расширить stale-семантику — цена выше пользы. |
| F2 | низкий | верификация | `-race` не запускался (нет gcc). | **Принять.** Новых общих записей нет: `tableEvidenceCache` пишется только в prewarm до старта правил; `indexCheckVerifiable`/`cachedLookupTableEvidence`/`cachedLookupTableCreator` только читают, ленивые пути не пишут в общий map. Рекомендация: `-race` в CI. |
| F3 | низкий | гейт + `M_DELETE_PTABLE*` | Для макро-таблиц, которых нет в DML-наборе prewarm'а, гейт делает ленивый creator-запрос на каждую уникальную пару `table|index`; при нескольких индексах одной таблицы запрос повторяется. Корпусный замер (3 783 файла) гонял только `tableExistsInMetadata`, стоимость гейта в `indexExistsInDB` не измерена. | **Принять (не блокер).** Гейт стоит до загрузки кандидатов и на типичных файлах экономит больше, чем тратит. Рекомендуемое действие — при желании замерить macro-heavy подмножество или расширить prewarm макро-именами (follow-up); иначе зафиксировать как известное ограничение. |
| F4 | nit | `BatchLookupTableExistenceEvidence` | Нет явного nil-db guard; короткое замыкание только по пустым именам. | **Действий не требуется:** консистентно с `BatchLookupTableCreators`; все вызывающие проверяют `db`. |
| F5 | nit | `db_lookup_sql.go:726`, вызовы | Локальные `const areaFilter = tableSourceAreaFilter` избыточны. | **Действий не требуется** (читаемость). |
| F6 | nit / решение | `catalog.go:152-157,418-423` | `Description` правил не обновлены. | **Действий не требуется:** задача 5.1 условная («если нужно»); подробный пользовательский список — README (обновлён), однострочные описания каталога остаются верными. |
| F7 | наблюдение | парсер / замер | Уход `tReplTables` маскирует дефект парсера: `create table dbo.tReplTables` не индексируется как создатель (`createTableRe` берёт `dbo` именем). Свидетельство (индекс в том же файле) легитимно подавляет FP, но дефект парсера остаётся для `dbo.`-таблиц без свидетельств. | **Не входит в scope.** Зафиксировано в `design.md` ## Measurements как кандидат-последователь; рекомендую отдельный change (парсер `dbo.`-префикса в `CREATE TABLE`). |

## 7. Закрытие замечаний предыдущего change-review (R1–R10)

| ID | Закрытие | Вердикт |
|---|---|---|
| R1 | D2 переформулирован (снимок индекса, не «same-file исключение»); добавлены сценарии fresh/stale; тест фиксирует фактическое поведение | закрыто |
| R2 | Явный приоритет cache(false) → `db==nil` → lazy; `TestIndexCheckVerifiable`; D4 обновлён | закрыто |
| R3 | Фильтры `definition_kind` и в design-сниппете, и в коде | закрыто |
| R4 | Свежий baseline 1718 на том же снимке; 100/239 — только справка | закрыто |
| R5 | `TestInitSchema_IndexDefinitionsLowerV8` + `requiredIndexes` | закрыто |
| R6 | End-to-end (а)–(е) через `RunSQLFileCtx` | закрыто |
| R7 | Fail-open гейта и цена для старых таблиц — в Risks | закрыто |
| R8 | API-таблица в области (сценарий г) доказывает прохождение гейта | закрыто |
| R9 | Дельта `review/review-scope` обновлена | закрыто |
| R10 | Сценарий ветки теперь явно требует создателя в целевой ветке | закрыто |

## 8. Заключение

Реализация полная, соответствует change и не даёт регресса; целевой кейс и
контроль подтверждены на реальных данных, корпусный замер сходится
арифметически, схема и планировщик проверены. Блокеров нет; F1–F7 правок кода
не требуют. Рекомендуемые (необязательные) шаги: 1–2 строки в `design.md`
## Risks по F1/F3 и `-race` в CI. После архивации — сохранить summary по
доработке в project memory (по процессу AGENTS).

## 9. Ответ на замечания (2026-11-24)

Все семь замечаний приняты; правок кода нет — как и рекомендовано ревью.

| ID | Решение | Обоснование | Где |
|---|---|---|---|
| F1 | Принято, без правки кода | Согласен с оценкой: дефект проявляется только при устаревшем/отсутствующем в индексе файле. Экспозиция замерена по FA (`Tests/Temp/tee_f1_exposure.txt`): 1 521 файл из 35 083 с хинтами создаёт таблицу в том же файле; в 1 389 из них индекс тоже создан в файле (до изменения — FP, теперь ничего), 147 — без индекса в файле (потенциальный FN), 126 — с регулярной таблицей. Частичная правка (пропустить same-file создателя через гейт) лишь вернула бы FP старого поведения; полное закрытие требует same-file учёта и индекса — расширение stale-семантики, отдельный change | `design.md` ## Risks |
| F2 | Принято | `-race` локально недоступен (нет gcc); по коду подтверждено — `tableEvidenceCache` пишется только в prewarm до старта правил, гейт и ленивые lookup только читают | — |
| F3 | Принято | Пункт верен: для макро-таблиц вне DML-набора — точечный запрос на пару `table|index`. Зафиксировано как известное ограничение; мемоизация по таблице в точке вызова — follow-up. Отдельный замер стоимости не проводился | `design.md` ## Risks |
| F4 | Принято | Симметрично `BatchLookupTableCreators`; вызывающие проверяют `db` | — |
| F5 | Принято | Локальный алиас сохраняет тело `BatchLookupTableCreators` без правок SQL-строк (минимальный дифф существующей функции) | — |
| F6 | Принято | Однострочные описания каталога остаются верными; подробное описание — README | — |
| F7 | Принято | Вне scope; масштаб уточнён: 121 вхождение `create table dbo.<имя>` в 86 файлах FA, 8 имён | `design.md` ## Measurements |
