# Анализ change `relations-incremental-consistency`

Дата: 2026-10-31. Анализ подготовленного change `openspec/changes/relations-incremental-consistency` (proposal.md, design.md, tasks.md, дельта-спека `indexing/relations-consistency`) со сверкой с кодом. Код не менялся.

**Вердикт: план корректен, соответствует коду и готов к реализации.** `openspec validate --strict` проходит. Все ключевые решения D1–D7 подтверждаются исходниками. Ниже — сверка и замечания.

## Что подтверждено кодом

| Утверждение плана | Код |
|---|---|
| `relations` без FK, только пары type/id | `db_schema.go:562`; индексы `idx_relations_source/target_type_id` (`db_schema.go:1121-1122`) — для remap/purge годятся, DDL не нужен ✓ |
| Каждое изменение файла = новый `files.id`, каскад убивает старые сущности | `saveFileCtx` → `INSERT ... RETURNING id` (`indexer.go:1771`), `REFERENCES files ON DELETE CASCADE` на всех сущностях ✓ |
| Точка вставки reconcile существует, все данные в кадре | `UpdateCtx`: `modifiedPaths`/`newFileIDs` собираются при walk, `workersWG.Wait()` (runner.go:297) → `DeleteFilesByPathsExcept` (runner.go:304) → `DeleteFilesByPaths` (runner.go:319) → `runPostProcessingParallel` (runner.go:327) ✓ |
| Постобработка видит только pending-источники | `postProcessSQLProcedureCallRelations` (sql_calls.go:16), `postProcessAllFragmentRelations` — снапшоты pending из памяти; runner.go:371 прямо фиксирует «incremental не восстанавливает relations» ✓ |
| Remap-типы целей ровно 3 | Все писатели рёбер (indexer_relations.go, indexer_postprocess_fragments.go, callbacks, spec_deps): межфайловые цели — только `sql_procedure`/`sql_table`/`api_contract`; `query_fragment`/`report_field`/`report_param` — всегда same-file ✓ |
| Identity-ключи и «max id» = семантика резолверов | `proc_name` (`db_schema.go:126`), `table_name` (:140), `contract_name+kind` (:423-424); резолверы `DISTINCT ON ... ORDER BY ..., id DESC` (`db_lookup_sql.go:57,154`, `api_store.go:295`) ✓ |
| 3 через родителя | `report_fields`/`report_params`/`vb_functions` → `report_forms.file_id` (`db_schema.go:259-296`) ✓ |
| Исключения из remap корректны | `subscribes_to_event` — delete-all + rebuild каждый прогон (runner.go:402); `references_code`, `depends_on_capability`, `usecase_involves`, `change_modifies` — глобальная пересборка в `postProcessSpecCodeMentions`/`postProcessSpecDependencies` ✓ |
| Сбой парсинга | `saveFileCtx` ok + ошибка parse → continue (runner.go:258): старые рёбра purged, новых нет — сценарий спеки воспроизводим ✓ |
| Full rebuild не конфликтует | `--modified=false` → `fullRebuildCtx`/TRUNCATE (runner.go:166,373) ✓ |

## Замечания (не блокирующие, желательно устранить до/в начале реализации)

1. **Шаг 4 «удаление остатков» — неточная семантика в design.** На момент reconcile старые цели *ещё существуют* (каскад впереди). Критерий должен быть «`target_id` ∈ oldFileIDs И не найден в remap-карте», а не `NOT EXISTS` по живой БД — иначе шаг ничего не удалит. В D1/D4 это не сформулировано — легко ошибиться при реализации. Рекомендую добавить одну фразу в D4.
2. **Расхождение в подсчёте типов источников.** Design D2: «17 типов»; tasks 2.2: «13 прямых + 3 через родителя» (16). По факту писатели рёбер дают: 9 не-spec прямых + 4 `spec_*` (capability/requirement/scenario/usecase — ровно `source_type` из `spec_code_mentions`) = 13 прямых, + 3 через родителя. `spec_config/usecase_step/change/change_delta` рёбра не пишут. Задача 1.1 это закроет, но числа в design/tasks стоит выровнять.
3. **Crash-окно reconcile → DeleteFilesByPathsExcept.** Если процесс упадёт между ними, старые `files`-строки останутся навсегда: следующий прогон увидит неизменённый hash и не включит путь в `modifiedPaths`. Не новая проблема (окно есть и сейчас), но после reconcile старые сущности будут висеть без рёбер. Стоит занести в Risks (опциональный safety-net: чистка фактических дублей путей, а не только modifiedPaths).
4. **Порядок purge → remap** (D1) критичен: remap не должен трогать рёбра purged-источников. Уже задан — зафиксировать тестом (входит в 4.12).
5. **D3/D7 — «max id» совпадает с резолвером внутри пути** лишь потому, что `not_generated`/`branch_match` константны в пределах одного пути. Верно, но стоит одной оговорки в design, чтобы правило не применили «в лоб» к глобальному join.
6. Инвариант в спеке не включает `usecase_involves`/`change_modifies` — корректно (spec-семейства, глобальная пересборка), но при аудите 1.1 это зафиксировать явно.

## Итог

Артефакты согласованы между собой (proposal ↔ design ↔ tasks ↔ дельта-спека), сценарии behavior-oriented и проверяемы на коде, тестовый план (32 задачи) покрывает все критические ветки, включая property-тест «incremental == full rebuild». После мелких правок из пунктов 1–2 можно начинать реализацию с задач группы 1 (аудит).
