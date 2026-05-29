# Bug Report: `query api-publishers --event` теряет часть подписчиков `CallbackEvent`

## Summary

Команда:

```powershell
.\Tools\CodeBase\CodeBase.exe query api-publishers --event OnAfterRuleD_MassCrtDoc --json
```

возвращает не всех подписчиков события `OnAfterRuleD_MassCrtDoc`.

Фактически в выдаче присутствует только один `subscribes_to_event` (например, `RKO_AfterRuleD_MassCrtDoc`), тогда как в репозитории есть и другие callback-подписчики, например:

- `fa-contracts/API_Credit/DSArchitectData/CallbackEvent/CON_AfterRuleD_MassCrtDoc.xml`

---

## Environment

- Repo root: `C:\NT\FA#\7.2GIT`
- Tool: `Tools\CodeBase\CodeBase.exe`
- Query command: `query api-publishers --event <EVENT_NAME> --json`

---

## Reproduction Steps

1. Выполнить:
   ```powershell
   .\Tools\CodeBase\CodeBase.exe query api-publishers --event OnAfterRuleD_MassCrtDoc --json
   ```
2. Убедиться, что в `items` присутствует только часть `subscribes_to_event`.
3. Проверить наличие дополнительных callback-файлов, например:
   - `fa-contracts/API_Credit/DSArchitectData/CallbackEvent/CON_AfterRuleD_MassCrtDoc.xml`
4. Убедиться, что в XML корректно заполнены:
   - `<UsedObjectName>OnAfterRuleD_MassCrtDoc</UsedObjectName>`
   - `<UsedModuleSysName>API_GAAP</UsedModuleSysName>`

---

## Expected Result

`query api-publishers --event OnAfterRuleD_MassCrtDoc --json` должен возвращать всех подписчиков (`subscribes_to_event`) из `DSArchitectData/CallbackEvent/*.xml`, указывающих на это событие.

---

## Actual Result

Возвращается только часть подписчиков. Остальные корректные callback-объекты не попадают в `relations` и, соответственно, в результат `api-publishers`.

---

## Root Cause Analysis

Проблема в этапе индексации relations, а не в самой SQL-выборке `query api-publishers`.

### Где формируется выдача `api-publishers`

- `Tools/CodeBase/Source/internal/query/api_query.go`
  Функция `SearchAPIPublishers(...)` использует `relations` с `relation_type = 'subscribes_to_event'`.

### Где создается `subscribes_to_event`

- `Tools/CodeBase/Source/internal/indexer/indexer_relations.go`
  Функция `buildCallbackEventRelations(...)`.

### Ошибка логики

`buildCallbackEventRelations(...)` вызывается сразу при обработке конкретного XML-файла:

- `Tools/CodeBase/Source/internal/indexer/indexer.go`

Внутри выполняется поиск целевого event-контракта через БД:

- `FindLatestAPIContractIDByNameKindAndOwnerModule(usedObjectName, "event", usedModuleSysName)`

Если на момент обработки callback-файла соответствующий event еще не проиндексирован (из-за порядка/параллельности обхода), получается `ErrNoRows`, и relation пропускается без ретрая/достройки.

Индексация выполняется параллельно:

- `Tools/CodeBase/Source/internal/indexer/runner.go`

Поэтому поведение недетерминированно: часть подписчиков теряется в зависимости от порядка обработки файлов.

---

## Impact

- Неполный граф API-связей по событиям.
- Ложные выводы при анализе подписчиков.
- Нестабильные результаты между прогонами индексации.

---

## Suggested Fix

1. Перенести построение `subscribes_to_event` в post-process после завершения полного прохода индексации (когда все `api_contracts` уже загружены).
2. Либо добавить второй обязательный проход достройки callback-relations в конце `Init/Update`.
3. Для `update --only-modified` пересчитывать callback-relations глобально (иначе старые пропуски сохраняются).

---

## Notes

Проблема воспроизводится на событии `OnAfterRuleD_MassCrtDoc`, но носит общий характер для любых `CallbackEvent -> Event` связей.
