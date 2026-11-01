## Context

См. `proposal.md` — Why. Технические предпосылки:

- `relations` (internal/store/db_schema.go:562) не имеет FK и хранит только пары `source_type/source_id` и `target_type/target_id`; целостность обеспечивается кодом.
- `UpdateCtx` (internal/indexer/runner.go) выполняет: discovery → `GetLatestFilesByRootPath` → walk + `saveFileCtx` (всегда `INSERT INTO files ... RETURNING id`) + парсинг + per-file relations → `DeleteFilesByPathsExcept` (удаляет старые `files`-строки изменённых путей) → `DeleteFilesByPaths` (удалённые файлы) → `runPostProcessingParallel` → `cleanupStaleBranches`.
- Все сущности каскадно привязаны к `files`; при удалении старой `files`-строки старые сущности исчезают, а рёбра на них остаются.
- Постобработка (`postProcessSQLProcedureCallRelations` и др.) обрабатывает только `pending`-источники текущего прогона; рёбра немодифицированных файлов не перерезолвятся.
- `FindLatestSQLProcedureIDsByNames` / `FindLatestSQLTableIDsByNames` / API-lookup выбирают `DISTINCT ON (key) ... ORDER BY not_generated DESC, branch_match DESC, product_match DESC, default_branch DESC, id DESC`.
- Замер на FA-БД: 17,6 млн рёбер, 773 275 source-side и 115 224 target-side мёртвых рёбер; полная пересборка ~17 ч.

## Goals / Non-Goals

**Goals:**

- После успешного `Init`/`Update` в `relations` нет рёбер с отсутствующим источником или целью.
- Убрать накопление фантомов (`unknown`, пустой файл) и потерю вызывателей при переиндексации.
- Сохранить текущую модель идентичности (новый `files.id` на каждую версию файла) и не менять схему БД.
- Стоимость этапа пропорциональна изменённым файлам и рёбрам, указывающим на их старые сущности.

**Non-Goals:**

- Переход на стабильные id сущностей (upsert сущностей при сохранении `files.id`).
- FK/триггеры на `relations`, история версий графа.
- Восстановление потерянных вызывателей, накопленных до внедрения (старые строки целей уже удалены).
- Разрешение переименования/переноса файла по имени сущности между путями.
- Изменение резолверов, парсеров, MCP/CLI-контрактов и глобальных пересборок (`spec_*`, `subscribes_to_event`, `references_code`).

## Decisions

### D1. Отдельный этап согласования в `Update`, до удаления старых поколений

Новый этап выполняется после завершения worker'ов и до `DeleteFilesByPathsExcept`/`DeleteFilesByPaths`.

Порядок:

```
workers: INSERT new files + entities + per-file relations
   ↓
RECONCILE:
  1. собрать oldFileIDs (изменённые + удалённые пути, id ≠ keep) и newFileIDs
  2. purge рёбер по источникам из oldFileIDs
  3. target remap old→new для sql_procedure / sql_table / api_contract
  4. удалить рёбра, чей old target_id не попал в remap-карту (неустранимая цель)
   ↓
DeleteFilesByPathsExcept / DeleteFilesByPaths  (каскад старых сущностей)
   ↓
runPostProcessingParallel (свежие call-рёбра pending-источников)
```

Почему так:

- старые и новые сущности сосуществуют, поэтому remap возможен;
- новые сущности и per-file рёбра уже созданы, поэтому purge не удалит новое (он фильтруется по old id);
- постобработка видит только новое поколение и резолвит цели на актуальные id.

Альтернативы: стабильные id сущностей (крупный рефакторинг всех batch-insert) и полная пересборка (17 ч) отклонены; чистка «по факту» после прогона не восстанавливает потерянные цели.

### D2. Purge — по старым file-id источников, а не remap source-id

Purge покрывает все 17 типов `model.RelationEntityTypes`: 14 прямых `file_id` (9 не-spec типов + 5 `spec_*`, включая `spec_change`) и 3 через родителя (`report_field`/`report_param` → `report_forms.file_id`, `vb_function` → `report_forms.file_id`).

- прямые `file_id`: `sql_procedure`, `pas_method`, `js_function`, `query_fragment`, `sql_table`, `dfm_form`, `smf_instrument`, `report_form`, `api_contract`, `spec_capability`, `spec_change`, `spec_requirement`, `spec_scenario`, `spec_usecase`;
- через родителя: `report_field`, `report_param`, `vb_function`.

`sql_table`, `report_field`, `report_param` в текущей БД рёбер как источники не имеют, но purge покрывает их для устойчивости инварианта.

Source remap сознательно не делается: новый файл уже сгенерировал рёбра из нового содержимого, а перенос старых source-id воскресил бы удалённые из файла вызовы. Purge выполняется до каскадного удаления старых `files`-строк, пока старые id ещё известны.

### D3. Target remap — только три межфайловых типа, identity по имени

| target_type | identity-ключ | Почему межфайловый |
|---|---|---|
| `sql_procedure` | `lower(proc_name)` | вызовы из других SQL/JS/фрагментов |
| `sql_table` | `lower(table_name)` | `references_table`/`selects_from`/`inserts_into`/`updates`/`deletes_from` из других файлов |
| `api_contract` | `lower(contract_name)` + `lower(contract_kind)` | `implements_contract`/`executes_contract`/`publishes_event` из SQL-файлов |

Для старого id замена — новая сущность с тем же `files.path` и тем же ключом с максимальным id. Это воспроизводит семантику резолверов (`id DESC`) и корректно обрабатывает дубли имён внутри файла: все старые ссылки, как и раньше, указывают на «последнюю» строку.

Оговорка: эквивалентность «max id» и резолвера верна только внутри одного `path`: там `not_generated`, `branch_match`, `product_match` и `default_branch` одинаковы, поэтому остаётся только `id DESC`. Применять глобальный max id без path-скоупа нельзя.

Не remap-ятся:

- `query_fragment`, `report_field`, `report_param` — цель всегда в том же файле, что источник: источник пересоздан → связи уже новые;
- `spec_capability` и прочие `spec_*` — семейства пересобираются глобально каждый прогон.

`line_number` не меняется — это строка вызова в файле-вызывателе.

### D4. Удаление остатков

После remap удаляются рёбра, чей `target_id` принадлежит старому поколению (`oldFileIDs`) и не попал в remap-карту: сущность удалена из нового поколения файла, файл удалён или переименован (путь изменился). Критерий обязан опираться на old-id и карту, а не на `NOT EXISTS` по живой БД: старые строки на этом шаге ещё существуют, каскад впереди, поэтому проверка `NOT EXISTS` ничего не удалит. Это осознанная потеря связи при переименовании пути; fuzzy-сопоставление по имени между путями не делается из-за риска ложных склеек между продуктами/ветками (см. Risks).

### D5. Транзакции и пакетность

Этап выполняется пакетно по типам сущностей и целей (temp mapping table + `UPDATE relations ... FROM map`/`DELETE ... WHERE ...`), чтобы:

- не держать одну гигантскую транзакцию на миллионы рёбер;
- ограничить WAL и блокировки;
- логировать счётчики по каждому типу.

При частичном сбое:

- до удаления старых `files`-строк база может содержать оба поколения (дубли путей) — резолверы берут max id, мёртвых рёбер нет;
- после удаления старых строк и до постобработки живые источники могут временно не иметь новых call-рёбер — повторный прогон это исправляет;
- инвариант «нет мёртвых рёбер» проверяется после завершения всего прогона.

### D6. Наблюдаемость

Счётчики этапа: `purged` (source-side), `remapped` (target-side), `deleted_unresolved` — с разбивкой по `source_type`/`target_type`. Пишутся в общий лог прогона рядом с существующими фазами. Нулевой прогон сообщает нули.

### D7. Изоляция scope

Remap-соединение идёт по `files.path`, а путь уникален в пределах продукта/ветки/клиентского каталога. Дополнительных условий scope не требуется, но тест должен подтверждать, что одинаковые имена в разных ветках не склеиваются.

## Risks / Trade-offs

- **Инвариант «все per-file семейства рёбер пересоздаются при парсинге файла»** → audit существующих writers + тест «после изменения файла набор рёбер источника совпадает с full rebuild»; при обнаружении семейства без writer'а — добавить его в purge/regeneration, не ослаблять инвариант.
- **Переименование/перенос файла теряет входящие рёбра** → задокументировано в спеке; при необходимости отдельная задача с явным сопоставлением по (product, branch, name), но не в этом change.
- **Массовый remap при изменении файла с общими таблицами** (до сотен тысяч рёбер) → батчи, WAL-бюджет в отчёте, `VACUUM (ANALYZE)` силами autovacuum; в тестах — замер объёма.
- **Дубли identity-ключей в файле** → правило max id совпадает с резолвером; тест на отсутствие искусственных дублей рёбер (уникальность полного кортежа).
- **Сбой посреди этапа** → промежуточное состояние без мёртвых рёбер (см. D5); повторный прогон идемпотентен.
- **Падение между reconcile и удалением старых `files`-строк** → старые поколения останутся в БД навсегда: следующий прогон увидит неизменённый hash и не включит путь в изменённые, а старые сущности будут висеть без рёбер. Окно существует и сейчас, но reconcile делает его заметнее. Митигация: safety-net — в начале `Update` (до walk) находить пути с несколькими поколениями `files` (count > 1), прогонять для не-последних поколений тот же purge/remap и удалять их; тест имитирует падение между этапами.
- **Ошибка парсинга файла** → старые рёбра источника удаляются, новых нет; это согласуется с текущей семантикой «файл не проиндексирован» и не оставляет фантомов.
- **Полная пересборка** по-прежнему валидна и не конфликтует с этапом (TRUNCATE очищает всё).

## Migration Plan

- Схема БД не меняется, `CurrentSchemaVersion` не поднимается, CLI не меняется.
- Деплой: заменить бинарник; первый `update` выполнит согласование по обычным правилам.
- Исторические мёртвые рёбра на рабочих БД уже удалены вручную (888 499 строк); потерянные до внедрения вызыватели не восстанавливаются этим change — только будущие прогоны.
- Rollback: откат бинарника; данные не мигрировались, обратной операции не требуется.
- Проверка после деплоя: Q6/Q7-эквиваленты (нет рёбер с мёртвым источником/целью), `codebase_query_callers` без `unknown`, сравнение графа incremental vs full rebuild на тестовой БД.

## Приложение: аудит writers семейств рёбер (задачи 1.1–1.3)

### 1.1 Семейство рёбер → writer → триггер

| Семейство | Writer | Триггер |
|---|---|---|
| `calls_procedure` (source `sql_procedure`) | `buildSQLProcedureCallRelationsWithTargetIDs` ← pending | per-file: pending собирается при парсинге файла-источника, резолвится в `postProcessSQLProcedureCallRelations` |
| `calls_procedure` (source `js_function`, `query_fragment`) | `postProcessAllFragmentRelations` / JS-call builder ← pending | per-file: pending текущего прогона |
| `dispatches_to` | `indexer_postprocess_fragments.go:405` ← pending API-refs | per-file |
| `dispatches_to_subscriber` | `indexer_postprocess_fragments.go:280` ← pending `.t01`-подписчиков | per-file |
| `references_table`, `selects_from`, `inserts_into`, `updates`, `deletes_from` (source `sql_procedure`) | `buildSQLProcedureTableRelations` | per-file (при парсинге файла-источника) |
| `references_table` (source `query_fragment`) | `postProcessAllFragmentRelations` ← pending | per-file |
| `implements_contract`, `executes_contract`, `publishes_event` | `indexer_postprocess_fragments.go:388–405` ← pending API-refs | per-file |
| `builds_query`, `executes_query` (родитель→фрагмент) | `buildQueryFragmentRelations` (parent-рёбра), `buildVBFunctionQueryRelations` | per-file (same-file, цель в том же файле) |
| `has_field`, `has_param`, `uses_param` | `buildReportStructureRelations`, `buildReportParamUsageRelations` | per-file (same-file) |
| `subscribes_to_event` | `postProcessCallbackEventRelations` (`api_store.go` INSERT; `DeleteSubscribesToEventRelations` перед горутинами) | **глобальная пересборка каждый прогон** |
| `references_code` | `postProcessSpecCodeMentions` (DELETE + INSERT, `db_lookup_spec_mentions.go:74`) | **глобальная пересборка** |
| `depends_on_capability`, `usecase_involves`, `change_modifies` | `postProcessSpecDependencies` (DELETE + INSERT, `db_lookup_spec_deps.go:36`) | **глобальная пересборка** |

Выводы: все per-file семейства пересоздаются при повторном парсинге изменённого файла — непокрытых per-file семейств нет. `spec_change` — реальный источник рёбер (`change_modifies`, `indexer_postprocess_spec_deps.go:538,561`), входит в purge; `depends_on_capability`, `usecase_involves`, `change_modifies`, `references_code`, `subscribes_to_event` — глобально пересобираемые, в per-file инвариант не входят (их старые строки дополнительно удаляются purge, что идемпотентно пересборке).

### 1.2 Маппинг типов на таблицы (сверено с `db_schema.go`)

Прямой `file_id` (14): `sql_procedure→sql_procedures(proc_name)`, `sql_table→sql_tables(table_name)`, `pas_method→pas_methods` (file_id добавлен ALTER, db_schema.go:352–407), `js_function→js_functions`, `query_fragment→query_fragments`, `dfm_form→dfm_forms`, `smf_instrument→smf_instruments`, `report_form→report_forms`, `api_contract→api_contracts(contract_name, contract_kind)`, `spec_capability→spec_capabilities`, `spec_requirement→spec_requirements`, `spec_scenario→spec_scenarios`, `spec_usecase→spec_usecases`, `spec_change→spec_changes`. Через родителя `report_forms.file_id` (3): `report_field→report_fields(report_form_id)`, `report_param→report_params(report_form_id)`, `vb_function→vb_functions(report_form_id)`. Итого 17 = `model.RelationEntityTypes`. Identity-ключи remap: `sql_procedure — lower(proc_name)`, `sql_table — lower(table_name)`, `api_contract — lower(contract_name)+lower(contract_kind)`.

### 1.3 Перечень изменяемых файлов (без DDL)

- `internal/store/db_reconcile_relations.go` (новый) — поколения, purge, remap, удаление неустранимых целей.
- `internal/store/db_reconcile_relations_test.go` (новый) — unit-проверка маппинга.
- `internal/store/db_reconcile_relations_integration_test.go` (новый) — тесты на временной БД.
- `internal/indexer/indexer_relations_reconcile.go` (новый) — этап reconcile + safety-net.
- `internal/indexer/reconcile_relations_integration_test.go` (новый) — сценарии 4.1–4.13.
- `internal/indexer/runner.go` — вызов safety-net в начале `UpdateCtx`, этап reconcile после workers до удаления старых поколений.
- `README.md` — этап в описании пайплайна.
- `cmd/root.go` — bump buildNumber.

`db_schema.go` не меняется, `CurrentSchemaVersion` не поднимается.
