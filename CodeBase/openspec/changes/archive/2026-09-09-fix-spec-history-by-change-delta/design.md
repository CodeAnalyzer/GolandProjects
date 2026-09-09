## Context

`executeSpecHistoryByChange` (`internal/specsvc/specsvc.go:714-744`) возвращает только список capability без delta-секций, тогда как `executeSpecHistory` (режим по capability, строки 682-712) джойнит `spec_change_delta` и заполняет `Changes`. Спека `query/spec-queries` требует delta в обоих режимах. См. proposal.md — Why.

Ключевое наблюдение из кода индексатора (`internal/indexer/indexer_postprocess_spec_deps.go:430,453`): ребро `change_modifies` уже несёт источник связи в `relations.confidence`:
- `"delta"` — связь извлечена из delta-файла change (есть delta-секции)
- `"proposal"` — связь извлечена из proposal references (change со `skip_specs: true`, delta-файла нет)

То есть признак `skip_specs` уже материализован в БД — не нужно новое поле в схеме, достаточно читать `r.confidence`. Таблица `spec_change_delta` (schema:701-711) хранит `section`, `capability_slug`, `requirement_name`, `body_text`, `line_start`, `line_end` — всё необходимое для delta-секций уже есть.

## Goals / Non-Goals

**Goals:**
- `executeSpecHistoryByChange` возвращает delta-секции (ADDED/MODIFIED/REMOVED) по каждой затронутой capability — плоский список `Changes`.
- `skip_specs`-changes (нет delta, связь из proposal) возвращают пометку `delta_source = "proposal"`, `skip_specs = true`.
- `Changes` — пустой массив `[]`, а не `null`, когда delta нет.
- Симметрия с режимом по capability: тот же join `spec_change_delta`, те же поля delta.

**Non-Goals:**
- Не меняем схему БД — `spec_change_delta` и `relations.confidence` уже хранят нужные данные.
- Не меняем режим по capability (`executeSpecHistory`) — он уже корректен.
- Не меняем форму `Capabilities` (summary-список) — оставляем как есть для обратной совместимости.
- Не добавляем пагинацию delta-секций — объём delta на change невелик.

## Decisions

### D1. Плоский список `Changes` (Вариант A) вместо вложенного

`Changes: []SpecHistoryEntry` с `capability_name` внутри каждой entry, `Capabilities` сохраняется как summary. Обоснование: симметрия с режимом по capability (там тоже плоский `Changes`), минимальное изменение структуры ответа, проще агрегация. Альтернатива (вложенные deltas внутри capability) меняет структуру `SpecHistoryCapability` и ломает обратную совместимость сильнее.

### D2. Новые поля `SkipSpecs bool` и `DeltaSource string` в `SpecHistoryEntry`

Заполняются из `r.confidence` (уже хранится как `"delta"` | `"proposal"`):
- `DeltaSource = r.confidence` (или `"delta"` по умолчанию, если confidence пуст)
- `SkipSpecs = (DeltaSource == "proposal")`

Альтернатива — переиспользовать существующее `Source` (в режиме по capability оно заполняется из `r.confidence`). Отвергнута по решению explore-фазы: `Source` в режиме по capability несёт confidence ребра как есть, а здесь нужна явная семантическая пометка skip_specs + источник delta. Отдельные поля чище и не смешивают семантику. При этом `Source` в режиме по change не заполняется (оставляем `omitempty`), чтобы избежать дублирования с `DeltaSource`.

### D3. SQL: `LEFT JOIN spec_change_delta` + `r.confidence` в SELECT

```sql
SELECT DISTINCT sc.change_name, sc.status, c.id, c.capability_name, c.title,
       COALESCE(dp.product_name, ''),
       COALESCE(d.section, ''), COALESCE(d.requirement_name, ''), COALESCE(d.body_text, ''),
       COALESCE(r.confidence, '')
FROM relations r
JOIN spec_changes sc ON r.source_type = 'spec_change' AND sc.id = r.source_id
JOIN spec_capabilities c ON r.target_type = 'spec_capability' AND c.id = r.target_id
LEFT JOIN ds_products dp ON dp.id = c.ds_product_id
LEFT JOIN spec_change_delta d ON d.change_id = sc.id AND LOWER(d.capability_slug) = LOWER(c.capability_name)
WHERE r.relation_type = 'change_modifies'
  AND LOWER(sc.change_name) = LOWER($1)
  AND ($2 = '' OR dp.product_name = $2)
ORDER BY c.capability_name, c.id, COALESCE(d.section, ''), COALESCE(d.line_start, 0)
```

`LEFT JOIN` — потому что skip_specs-changes не имеют записей в `spec_change_delta`, но связь в `relations` есть (confidence = "proposal"). Для них `d.*` будет NULL — формируется entry с пустыми section/requirement_name и `delta_source = "proposal"`.

### D4. Инициализация `Changes: make([]SpecHistoryEntry, 0)`

В `executeSpecHistoryByChange` инициализировать `Changes` пустым слайсом, чтобы JSON давал `[]`, а не `null`. Поле `Changes` в `SpecHistoryResult` (строка 132) оставляем без `omitempty` — пустой массив информативнее отсутствия поля.

### D5. `Capabilities` заполняется отдельно

Чтобы избежать дублирования capability в плоском `Changes` (одна capability с N delta-секциями даёт N строк), `Capabilities` собирается отдельным проходом по уникальным capability из того же запроса, либо отдельным запросом без delta-join (как сейчас). Решение: отдельный запрос без delta-join для `Capabilities` (текущий SQL), плюс новый запрос с delta-join для `Changes`. Два простых запроса чище, чем дедуп в Go.

## Risks / Trade-offs

- **[Два SQL-запроса вместо одного]** → Допустимо: оба простые, по одному change объём данных мал. Альтернатива (дедуп в Go) усложняет код.
- **[Новые поля в `SpecHistoryEntry` — расширение ответа]** → Обратно совместимое: старые клиенты игнорируют новые поля. `skip_specs`/`delta_source` с `omitempty` не появляются в режиме по capability.
- **[`DISTINCT` + LEFT JOIN может давать дубли]** → Если у capability несколько delta-секций, `DISTINCT` не схлопнет их (тела разные). Это корректно — каждая delta-секция отдельная entry. Дубли возможны только при идентичных секциях, что маловероятно.
- **[confidence может быть пустым для старых данных]** → `COALESCE(r.confidence, '')` + default `"delta"` в Go для непустых delta-секций; `SkipSpecs` true только при явном `"proposal"`.

## Migration Plan

Изменение обратно совместимое, миграция БД не требуется. Деплой: пересобрать бинарник, перезапустить MCP-сервер. Откат — вернуть прежний `executeSpecHistoryByChange`. Данные в БД не меняются.

## Open Questions

(нет)
