## Context

`insertTRCEvents` (`store.go`) выполняет COPY IN батчами по 50K событий.
`ParentID` вычисляется при парсинге (`IncrementalParentTracker`) как 0-based
индекс родительского события в потоке. Поскольку `trc_events.id` генерируется
БД (BIGSERIAL), оригинальный подход хранил в `parent_id` 1-based offset и
выполнял post-insert маппинг offset → реальный id. Для файла с 1.3M событий
этот маппинг (SELECT 1.3M rows → Go → COPY IN 1.3M rows → UPDATE 1.3M rows)
занял 52 минуты — неприемлемо.

## Goals / Non-Goals

**Goals:**
- `parent_id` содержит реальный `id` строки родителя сразу после COPY IN — без post-insert маппинга
- Корректная обработка файлов с >1M событий в стриминг-режиме
- `insertTRCEvents` вызывается из `SaveSession` и `ParseFileToDB` без дополнительного маппинга

**Non-Goals:**
- Изменение алгоритма `ComputeParentIDs` / `IncrementalParentTracker` (ParentID/Depth вычисляются при парсинге)
- Изменение recursive CTE в `LoadEventsForTree`
- Оптимизация COPY IN

## Decisions

### Decision 1: Вынести маппинг из `insertTRCEvents` в `MapParentIDs(ctx, db, sessionID)`

**Статус: SUPERSEDED Decision 3.** Оригинальный подход вынес маппинг в
отдельную функцию `MapParentIDs`, но Go round-trip на 1.3M строк оказался
неприемлемо медленным (52 минуты). Decision 3 заменяет этот подход.

### Decision 2: `MapParentIDs` читает `id, parent_id` из БД

**Статус: SUPERSEDED Decision 3.** Go-маппинг `ids[offset-1]` + COPY IN temp +
UPDATE JOIN работал корректно, но O(N) network round-trip + UPDATE 1.3M строк
с 6 индексами = 52 минуты.

### Decision 3: Явный `id` при COPY IN — маппинг не нужен

Перед вставкой определяется `baseID = COALESCE(MAX(id), 0) + 1 FROM trc_events`.
Каждое событие вставляется с явным `id = baseID + EventIndex` (0-based,
сквозной через все батчи). `parent_id = baseID + ParentID` если `ParentID >= 0`,
иначе NULL. После вставки sequence синхронизируется:
`SELECT setval('trc_events_id_seq', (SELECT MAX(id) FROM trc_events))`.

**Rationale:** `EventIndex` уже вычисляется `IncrementalParentTracker` как
0-based порядковый номер в потоке. `ParentID` — 0-based индекс родителя.
Если `id = baseID + EventIndex`, то `parent_id = baseID + ParentID` —
это реальный id строки родителя, маппинг не нужен. COPY IN с явным id
поддерживается PostgreSQL (BIGSERIAL колонка принимает явные значения).

**Alternatives:**
- Go round-trip `MapParentIDs` (Decision 2) — 52 минуты на 1.3M, отвергнут
- SQL `ROW_NUMBER()` + self-join — отвергнут ранее из-за таймаута, но может
  быть пересмотрен если явный id окажется неприемлемым
- `parent_offset` колонка + маппинг в query time — меняет schema и query logic

## Risks / Trade-offs

- [Race condition при concurrent insert] Если две сессии парсятся одновременно,
  `MAX(id)` может дать одинаковый baseID. → Mitigation: параллельные парсинги
  маловероятны в текущей архитектуре; при необходимости — `LOCK TABLE` или
  резервирование диапазона через `nextval`.
- [Sequence desync] Если вставка падает после получения baseID, но до setval,
  sequence отстаёт от MAX(id). → Mitigation: `setval` после каждой сессии;
  при следующем парсинге `baseID = MAX(id) + 1` корректен независимо от sequence.
- [Гарантия монотонности] id монотонны в рамках сессии, но не обязательно между
  сессиями при параллельных вставках. → Acceptable: id уникальны, монотонность
  между сессиями не требуется.
