# Дизайн: счётчик использования сессий

## Context

Reuse по `(file_hash, parser_version)` (архив `2026-10-20-parse-session-reuse-by-file-hash`)
сделал сессии общими: несколько анализов (CLI/MCP-агенты) получают один и тот же
`session_id`. Хеш проставляется последним шагом (`SetSessionHash`) после успешного
сохранения; конфликт параллельных разборов разрешается в пользу первой записавшей
хеш сессии (`store.IsUniqueViolation` → re-Find → `DeleteSession` проигравшего).
`rti delete`/`trc delete` (`ExecuteDelete` в `rtisvc`/`trcsvc` runtime.go) сегодня
всегда удаляют окончательно; `prune` удаляет батчами по возрасту/количеству.
Удаление никак не согласовано с reuse — держатель, закончивший анализ, уничтожает
данные остальных. Мотивация — в proposal.md.

Единая точка входа parse/delete для CLI и MCP — сервисный слой `internal/rtisvc`,
`internal/trcsvc`; транспорт меняется автоматически.

## Goals / Non-Goals

**Goals:**

- Счётчик `use_count` с инвариантом «каждый успешный parse → +1, каждый delete →
  −1 или смерть при ≤ 1», без идентичности держателей.
- Атомарность инкремента/декремента на уровне одного SQL-стatement — без
  read-modify-write гонок.
- Честный ответ delete (удалено/декрементировано) и видимость счётчика в
  parse/list — CLI и MCP одинаково.
- Обходной путь для принудительного удаления: `--hard` / `hard = true`.

**Non-Goals:**

- Идентификация держателей (таблица leases, client_id) — держатели анонимны.
- Точная сборка мусора утекших ссылок — утечки ограничиваются `prune`.
- Изменение формата `SummaryResult` и прочих аналитических ответов.
- Тачание `resolveSession`/`--file`-режима (сессии не создаёт и не удаляет).

## Decisions

### D1. Refcount-инвариант без идентичности держателей

`use_count` = число «получений» сессии парсингами минус число «отпусканий»
delete-ами. Плюсы: одна колонка, нулевая миграционная стоимость, legacy-строки
(счётчик 0, хеш NULL) ведут себя ровно как сегодня — не реюзятся и удаляются
окончательно. Альтернатива — таблица holders с уникальными держателями — отвергнута:
у CLI/MCP-вызовов нет устойчивой идентичности, а полезный эффект (защита общих
данных) достигается и анонимным счётчиком.

### D2. Инкремент встроен в существующие точки записи хеша

- `SetSessionHash` дополняется: `UPDATE ... SET file_hash = $2, parser_version =
  $3, use_count = use_count + 1 WHERE id = $1` — первый парсинг даёт 0 → 1 в том
  же statement, что и хеш; ретрай после ошибки записи хеша не двойит счётчик
  (первый ретрай не меняет строку из-за unique violation на хеше — счётчик
  инкрементируется только при успешной записи).
- Reuse-ветка и победитель конфликта используют новый атомарный acquire:
  `UPDATE ... SET use_count = use_count + 1 WHERE file_hash = $1 AND
  parser_version = $2 RETURNING id, use_count` — найти-и-инкрементнуть одним
  statement (0 строк — сессии нет). `FindSessionByHash` заменяется этим acquire
  (потребителей вне parse-пути нет); для победителя конфликта тот же acquire
  даёт «+1 за второго держателя».
- Альтернатива «отдельный `IncrementSessionUse` после FindSessionByHash» отвергнута:
  два statement = окно гонки между find и increment (hard-delete между ними
  оставит инкремент без строки).

### D3. Декремент-или-удаление — один атомарный statement с фолбэком

Новый `ReleaseSession(ctx, db, sessionID) (remaining int, released bool, err)`:

```sql
UPDATE rti_sessions SET use_count = use_count - 1
WHERE id = $1 AND use_count > 1
RETURNING use_count
```

Строка обновилась — декрементировано (возвращён остаток); 0 строк — либо
`use_count <= 1` (окончательное удаление через существующий `DeleteSession`), либо
сессии нет (ошибка «not found», как сегодня). Рейс между «прочитали use_count» и
«удалили» исключён: UPDATE атомарен, а `DeleteSession` идемпотентен к отсутствию
строки. `DeleteSession` остаётся чистым hard-delete — его используют `--hard`,
prune-пути сервисного слоя и удаление проигравшего в конфликте.

### D4. `--hard` / `hard = true` — осознанная отмена решения «без force-флага»

Прошлый change зафиксировал «принудительный перепарсинг = delete + parse, флаг
`--force` не вводится». С refcount эта связка ломается (delete при `use_count > 1`
оставляет сессию переиспользуемой), поэтому вводится флаг именно у delete, а не у
parse: семантика «удалить окончательно, несмотря на других держателей». `Hard bool`
добавляется в `DeleteParams` обоих сервисов; CLI — `rti delete --hard`,
`trc delete --hard`; MCP — опциональный boolean-параметр `hard` у
`codebase_rti_delete`/`codebase_trc_delete` с обновлённым описанием.

### D5. `DeleteResult` разделяет исходы

```go
type DeleteResult struct {
    Deleted        bool    `json:"deleted"`                    // хотя бы одна удалена окончательно
    Decremented    bool    `json:"decremented"`                // хотя бы одна декрементирована
    SessionID      int64   `json:"session_id"`                 // одиночный случай
    FilePath       string  `json:"file_path,omitempty"`
    DeletedCount   int     `json:"deleted_count"`              // окончательно удалено
    SessionIDs     []int64 `json:"session_ids,omitempty"`      // окончательно удалённые
    DecrementedIDs []int64 `json:"decremented_ids,omitempty"`  // выжившие (декрементированные)
    UseCount       int     `json:"use_count,omitempty"`        // остаток после декремента (одиночный случай)
}
```

Совместимость: одиночный delete при `use_count = 1` даёт прежний ответ
(`deleted = true`, `deleted_count = 1`, `session_ids = [N]`); при `use_count > 1`
`deleted` становится `false` + `decremented = true` — осознанное изменение
внешнего поведения, зафиксированное в дельтах. Удаление по имени обходит
совпадения независимо и агрегирует оба списка.

### D6. `use_count` в `ParseResult` — аддитивно, всегда присутствует

`UseCount int` без `omitempty`: fresh-парсинг = 1, reuse = значение из
`RETURNING` acquire, победитель конфликта = значение после его инкремента,
fallback без БД = 0 (как `session_id = 0`). CLI-вывод `parse` дополняется строкой
счётчика; формат JSON-конверта не меняется.

### D7. Prune игнорирует счётчик

`PruneSessions`/`PruneSessionsOlderThan` не читают `use_count` — janitorial
семантика: возраст/порядок важнее держателей, иначе общие сессии станут
неудаляемыми. Это же граница утечек анонимных ссылок (держатель умер без delete).
Изменений в SQL prune не требуется — только фиксация поведения в спеке.

### D8. Схема — одна идемпотентная миграция

`ALTER TABLE rti_sessions ADD COLUMN IF NOT EXISTS use_count INTEGER NOT NULL
DEFAULT 0` (и для `trc_sessions`) в `db_schema.go` рядом с существующими
миграциями reuse. Схема живой БД применяется через `codebase health`
(`rti`/`trc` команды `InitSchema` не вызывают) — как у прошлой миграции.

## Risks / Trade-offs

- [Утечка анонимных ссылок] держатель умер, не сделав delete → сессия живёт «зря»
  → Mitigation: `prune` по возрасту/количеству чистит независимо от счётчика;
  `list` показывает `use_count`, подозрительные «вечные» сессии видны.
- [Перевес при двойном delete] один клиент может отпустить дважды и убить сессию
  при живом втором держателе → Mitigation: принято (анонимность держателей);
  вероятность мала, данные восстановимы повторным `parse` (reuse больше не
  сработает — будет полный разбор).
- [Breaking-ответ delete при `use_count > 1`] клиенты, проверяющие `deleted ==
  true`, увидят `false` → Mitigation: поля разделяются явно, описания MCP-инструментов
  обновляются; сценарий задокументирован в дельтах.
- [Гонка acquire и hard-delete] между `RETURNING` acquire и `parseResultFromSession`
  сессию могут удалить (`--hard`/prune) → Mitigation: та же толерантность, что и
  сегодня в reuse-ветке (ошибка «session not found»); повторный `parse` создаст
  новую сессию.
- [Счётчик при rollback SetSessionHash] unique violation откатывает statement
  целиком — инкремент не «прилипает» к неудачной записи → риска нет, считаем
  корректным поведением.

## Migration Plan

1. Миграция схемы идемпотентна (`ADD COLUMN IF NOT EXISTS DEFAULT 0`), применяется
   при следующем `codebase health`/`init` — даунтайма и блокировок нет.
2. Откат: колонку можно оставить (код старой версии её не читает); при необходимости
   `ALTER TABLE ... DROP COLUMN use_count`.
3. Развёртывание одного бинарника сразу меняет parse/delete/list — смешанных
   версий долго живущих MCP-серверов избегаем рестартом (как у прошлого change).
