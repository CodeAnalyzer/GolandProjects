# Reconcile incremental: результаты прогона на тестовой БД

- **Change:** `relations-incremental-consistency`, задача 5.5
- **Дата:** 2026-11-01 (повторный прогон после code review F1: конфиг
  прогона дополнен `include_patterns = ["*.sql", "*.xml"]` — дефолтные
  маски не содержат `*.xml`, из-за чего в первом прогоне контрактная
  ветка не индексировалась; см. `Modifications/Code-Review-relations-incremental-consistency.md`)
- **Бинарник:** CodeBase 0.9.4 build **1661** (включает фикс review F2 —
  экранирование LIKE-паттерна rootPath в safety-net)
- **Стенд:** локальный PostgreSQL (localhost:5435), отдельная БД `codebase_test`
  (пересоздаётся скриптом, рабочая БД `codebase` не затрагивается)
- **Скрипт прогона:** `Tests/run-reconcile-check.ps1` (фикстура
  `Tests/reconcile-fixture` + 30 файлов `gen/GenNN.sql`, SQL-хелпер
  `Tests/reconcile-check`)
- **Логи прогона:** `Tests/reconcile-run/` (`init.log`, `update*.log`,
  `check-*.txt`, `lsn.txt`)

## Фикстура

38 файлов (37 SQL + 1 XML): `A.sql` (CallerA → exec CalleeB + select/update
tAccount), `B.sql` (CalleeB), `Helper.sql`, `C.sql` (create table tAccount),
`F.sql` (FixedCaller → exec CalleeB, не модифицируется), `D.sql` (DeleteMe →
exec CalleeB, удаляется), `E.sql` (API_CREATE_PROC(FCD_X_Get)),
XML-контракт `FCD_X_Get.xml` (service), 30 × `gen/GenNN.sql` (select
AccountID from tAccount — живые источники fragment→`references_table`).

Модель графа: `sql_procedure→sql_table` семейства — same-file occurrence;
межфайловые ссылки на таблицы идут от `query_fragment` (глобальный резолв
выбирает occurrence-строку с max id — на момент init это строка в
`gen/Gen30.sql`); вызовы процедур и контракты — межфайловые по имени
(`implements_contract` от `E.sql` → контракт из XML).

## Прогоны

### 1. `init` (baseline)

| Метрика | Значение |
|---|---|
| files scanned / indexed | 38 / 38 (SQL 37, XML 1), errors 0 |
| relations_total | 105 |
| семейства | calls_procedure 6, executes_query 35, **implements_contract 1**, references_table 32, selects_from 31 |
| инвариант (мёртвые концы, 17 типов × 2 стороны) | **0** |
| дубли полных кортежей | **0** |
| фантомные вызыватели CalleeB (unknown/пустой файл) | **0** (live=3: CallerA, DeleteMe, FixedCaller) |
| пути с несколькими поколениями files | 0 |
| WAL LSN | `A8/3F3A4350` |

### 2. `update` №1 — мутации A/B/C/XML + удаление D.sql

Изменены: тело `CalleeB`, `CallerA` (+exec Helper), `tAccount` (+BranchID),
описание `FCD_X_Get.xml`; удалён `D.sql`.

```
Relations reconcile: old_file_generations=5 purged=10 remapped=3 deleted_unresolved=0 elapsed_ms=53
  purge by source_type: query_fragment=4, sql_procedure=6
  remap by target_type: api_contract=1, sql_procedure=2
```

| Проверка | Результат |
|---|---|
| инвариант | **0** мёртвых концов |
| дубли кортежей | 0 |
| callers CalleeB | CallerA + FixedCaller (без unknown/пустых, phantom=0); DeleteMe исчез |
| Files updated / deleted | 4 / 1 |
| длительность прогона (scan_run) | 313 мс; reconcile 53 мс (17 %) |
| WAL | `A8/3F3F8CA8`, дельта ≈ 338 КБ |

`remapped sql_procedure=2`: живой вызыватель FixedCaller (процедура + её
фрагмент) переведён на новое поколение CalleeB — без reconcile эти рёбра
стали бы фантомами, а вызыватель «пропал» бы из callers.

`remapped api_contract=1`: `implements_contract` от живой процедуры `E.sql`
(API_CREATE_PROC) переведён на новое поколение контракта `FCD_X_Get` —
ветка XML-контрактов подтверждена в CLI-прогоне (в первом прогоне
отсутствовала, review F1).

### 3. `update` №2 — массовый remap таблиц (модификация `gen/Gen30.sql`)

Изменён файл, владеющий текущей occurrence-целью `tAccount`.

```
Relations reconcile: old_file_generations=1 purged=3 remapped=29 deleted_unresolved=0 elapsed_ms=57
  purge by source_type: query_fragment=1, sql_procedure=2
  remap by target_type: sql_table=29
```

29 межфайловых `references_table` рёбер живых источников перенесены на новое
поколение occurrence-строки (family references_table сохранил 32 — потерь
нет). Инвариант 0, дубли 0, reconcile 57 мс, WAL дельта ≈ 30 КБ.

### 4. `update` №3 — удаление владельца цели (`gen/Gen30.sql`)

```
Relations reconcile: old_file_generations=1 purged=3 remapped=0 deleted_unresolved=29 elapsed_ms=90
  purge by source_type: query_fragment=1, sql_procedure=2
  deleted_unresolved by target_type: sql_table=29
```

Входящие `references_table` рёбра не сопоставимы (файл-владелец удалён) —
удалены как `deleted_unresolved=29`; family references_table 32 → 2
(осознанная потеря входящих при удалении файла, design D4). Инвариант **0**,
дубли 0, reconcile 90 мс, WAL дельта ≈ 4,6 КБ.

### 5. `update` №4 — без изменений (no-op)

```
Relations reconcile: old_file_generations=0 purged=0 remapped=0 deleted_unresolved=0 elapsed_ms=0
```

Счётчики нулевые, этап не выполняет запросов к БД (пустые множества
изменённых/удалённых путей), набор рёбер не изменился (72), инвариант 0.

## Проверка на рабочей БД FA (2026-11-01, та же дата — после code review)

Первый прогон на боевой базе после ручной чистки (build 1661,
`rootPath=C:\NT\FA#\7.2GIT`, parallel=12): `git pull` дал 1133 индексируемых
файла (679 updated + 455 added + 56 deleted; 610 106 pre-filtered), errors 0.

Счётчики этапа (из лога `codebase update`):

```
Relations reconcile: old_file_generations=735 purged=95957 remapped=11214 deleted_unresolved=11 elapsed_ms=79055
  purge by source_type: dfm_form=315, pas_method=70, query_fragment=31699, report_form=82,
                        spec_change=2, spec_requirement=1129, spec_scenario=1014, sql_procedure=61646
  remap by target_type: api_contract=19, sql_procedure=1178, sql_table=10017
  deleted_unresolved by target_type: report_form=3, sql_table=8
```

Консистентность: 735 поколений = 679 изменённых + 56 удалённых путей —
точно. Без reconcile ≈ 107 тыс. рёбер за один прогон стали бы мёртвыми
(purge 95 957 + unresolved 11 + неремапленные цели) — тот механизм, что
накопил 773 тыс. мёртвых рёбер до ручной чистки.

| Проверка (SQL, DBeaver) | до update | после update |
|---|---|---|
| relations_total / files_total | 16 721 083 / 623 482 | 16 726 496 (+5 413) / 623 881 (+399 = +455 − 56) |
| инвариант: мёртвые концы, 17 типов × 2 стороны | 0 | **0** |
| фантомные вызыватели (ghost targets) | 0 | 0 |
| дубли полных кортежей | 0 | 0 |
| пути с >1 поколением files | 0 | 0 |

Стоимость: reconcile 79 с из 910 с прогона (8,7 % ≤ порога 25 %); остальное
— walk 197 с и пост-обработка 694 с (перезаезд LSA после правки 363
MD-спеков). WAL-дельта между снапшотами ≈ 2,3 ГиБ — общее движение LSN
сервера за окно (чекпойнты, LSA), не метрика этапа.

Выявленный косметический дефект прогона: блок счётчиков склеивался со
строкой спиннера (`errors=0Relations reconcile:`). Исправлен: этап получил
собственную стадию прогресс-репортера `relations reconcile` (по образцу
spec-lsa/desc-lsa) + ведущий перевод строки в блоке счётчиков
(runner.go, indexer_relations_reconcile.go), build 1661 пересобран.

## Выводы

- Инвариант «нет рёбер с отсутствующим источником/целью» выполняется после
  каждого прогона (включая удаление файла-хаба и no-op).
- Фантомные вызыватели (`unknown`/пустой файл) не появляются; неизменённые
  вызыватели переживают переиндексацию цели (remap), изменённые — получают
  рёбра из нового содержимого без дублей.
- Все три фазы этапа наблюдаемы в логе с разбивкой по типам: purge (10/3/3),
  remap (2 proc + 1 contract + 29 table), deleted_unresolved (29) —
  remap подтверждён для всех трёх межфайловых target-типов.
- Стоимость этапа на фикстуре: 53–90 мс на прогон (≤ 17 % от total 313 мс),
  no-op — 0 мс без запросов; WAL-дельта прогона 4,6–338 КБ.
- Ограничения: фикстура 38 файлов / 105 рёбер ≠ FA-масштаб (17 млн рёбер);
  экстраполяция на боевую БД — по счётчикам (на FA мутации одного прогона
  затрагивают сотни файлов и десятки тысяч рёбер, стоимость пропорциональна
  изменённым файлам, индексы `idx_relations_*` покрывают доступ). Crash-окно
  «reconcile → удаление поколений» покрыто юнит-интеграционным тестом
  safety-net (`TestReconcile_SafetyNetCleansCrashWindow`), а не этим прогоном.
