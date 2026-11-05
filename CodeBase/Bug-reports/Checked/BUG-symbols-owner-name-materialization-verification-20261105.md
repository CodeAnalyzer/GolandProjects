# BUG-отчёт: верификация change symbols-owner-key-materialization (2026-11-05)

**Change:** `symbols-owner-key-materialization` (материализация `symbols.owner_name`, схема v5, миграция `symbols_owner_name_backfill_v1`).
**Бинарь:** CodeBase 0.9.4 build 1672. **БД:** codebase@localhost:5435 (FA-индекс), 5 818 425 символов.
**Источник:** BUG-query-search-performance-bottlenecks-20261103.md, B4.

## Миграция (задача 4.1а)

Первый `codebase update` после апгрейда применил v5 за один прогон ~11 мин суммарно
(включая обход FA-дерева pre-filter'ом; сама миграция — ALTER + два type-scoped UPDATE
в транзакции `InitSchemaCtx`). Численная точность backfill:

| Метрика | Значение |
|---|---|
| method-символов всего | 966 721 |
| method с заполненным owner_name | 930 893 |
| method незарезолвнутых (class_id=0/dangling) → `''` | 35 828 |
| component-символов, все с владельцем | 232 057 |
| прочие типы с непустым owner_name | 0 |
| dangling class_id (класс удалён) | 0 |

930 893 + 35 828 = 966 721 — расхождение 0. `health` после миграции: ok.
Незарезолвнутая популяция (35 828) — это standalone-функции без класса
(пример: `GetAdmCmd` в fa-administrator/AdmCmd.pas, unit-level `function`), для них
`''` корректен и до, и после; случаев «класс существовал и удалён» (dangling) в БД нет.

## Эквивалентность до/после (задача 4.1б)

Методика: попарное сравнение полного поискового SQL со старым runtime-выражением
владельца (CASE с коррелированными подзапросами) и с материализованной колонкой
`COALESCE(s.owner_name,'')` — оба выражения на одних и тех же данных, подряд.
16 кейсов: точные имена (DoAction/Execute/Prepare/FormCreate/DataModuleCreate/SetParams
× method; dlName/btnOK × component; FormCreate × form; без типа), like-режимы,
content-поиск (SearchInContent). **Расхождений: 0 из 16.**

Замечание о методике: первый вариант сравнения (снимок «до» → миграция → снимок
«после») дал 3 ложных DIFF в like-хвостах (LIMIT 1000) — данные между снимками
дрейфовали (детали ниже), при попарном прогоне расхождений нет. Дрейф НЕ связан
с изменением: old-выражение на текущих данных ≡ new-колонке на тех же данных.

## Переиндексация (задача 4.1в)

- FA-уровень: явный touch+mtime файла AdmCmd.pas НЕ вызвал переиндексацию в
  `codebase update` (files.modified_at в БД остался 2026-03-22 при диске 2026-11-05;
  за сегодняшние прогоны update переиндексировано 0 файлов). Пре-существующее
  поведение pre-filter mtime+size, **вне scope change** — отдельное направление
  для исследования (возможно, fingerprint-сравнение не учитывает touch).
- Тестовый уровень: integration-тест `TestIndexerFillsSymbolsOwnerName` расширен
  Update-фазой (переписанный .pas → `Update` → владельцы пересозданы консистентно,
  новый класс TFormD.Renamed получает владельца, reconcile не требуется). Зелёный.

## План запроса и тайминги (задача 4.2)

`EXPLAIN (ANALYZE, BUFFERS)`, кейс B4 `query symbol --name DoAction --type method`
(4 278 строк в base):

- **SearchSymbol: SubPlan-ов нет.** Bitmap Index Scan `idx_symbols_symbol_name_type_lower`
  → Memoize-join'ы к files/ds_products/ds_products_branch. 46.7 мс actual,
  10 709 buffer hits (было: 28 625 buffer hits, per-row SubPlan).
- **SearchInContent: SubPlan-ов нет.** 862 мс — остаток это сам ILIKE-скан по
  name/signature (trgm/seq), вне scope B4.
- Warm SQL best-of-5: **40.0 мс** (базлайн B4: 259–408 мс warm).
- CLI `query symbol --name DoAction --type method --limit 1000`: 281–438 мс warm
  (805 мс первый прогрев; доминируют печать 1000 строк и старт процесса; SQL-ядро
  см. выше). Базлайн B4 CLI: 13.2 с cold / 259–408 мс warm — cold-пики SubPlan
  исключены из плана полностью.

## CLI-регресс (задача 4.3)

`Tests/run-test-cases-2026-11-05.ps1` → `test-run-2026-11-05/summary.ndjson`,
дифф с базлайном 2026-11-04 по (id, exit, count): **226/226 совпали, расхождений 0.**

## Наблюдаемые аномалии (вне scope)

1. Pre-filter не переиндексировал touch'нутый файл (см. 4.1в) — требует отдельного
   расследования поведения `fswalk` fingerprint.
2. Дрейф данных между двумя снятыми снимками 12:05→12:50 (5 818 425 total при этом
   неизменен; отдельные like-хвосты сместились) при today_files=0 — похоже на внешнего
   писателя или эффект поколений scan_runs; на результаты верификации не влияет
   (попарная методика).
3. Открытый пункт (code review, п.7): ~11 мин первого `update` не разложены на
   составляющие — при следующем прогоне на FA-scale замерить длительность окна
   `ALTER` + двух backfill UPDATE отдельно от обхода/индексации, чтобы иметь точную
   оценку advisory-lock-окна миграции.

## Вывод

Change верифицирован: миграция точна, эквивалентность 16/16, SubPlan-ы устранены,
warm 40 мс (×6–10 к базлайну), CLI-регресс чист. Готов к архивации.
