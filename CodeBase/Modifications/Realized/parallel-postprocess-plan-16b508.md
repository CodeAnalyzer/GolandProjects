# Параллелизация post-process шагов Init

План: проверить независимость трёх post-process шагов и безопасно вынести независимые этапы в параллельное выполнение с контролем конфликтов в таблице `relations`.

## Текущий вывод

- `postProcessPASPending` работает с PAS/DFM-связями: обновляет `pas_classes`, `pas_methods`, `pas_fields`, читает `dfm_forms`, `dfm_components`, `pas_classes`.
- `postProcessSQLProcedureCallRelations` читает pending SQL calls, читает `sql_procedures`, вставляет `relations` типа `calls_procedure`.
- `postProcessCallbackEventRelations` удаляет и заново вставляет `relations` типа `subscribes_to_event` между `api_contract`.
- Прямой логической зависимости между этими тремя шагами не найдено.

## Риски параллельного запуска

- `postProcessSQLProcedureCallRelations` и `postProcessCallbackEventRelations` оба пишут в `relations` через `BatchInsertRelations` / `COPY`. Их данные имеют разные `relation_type`, поэтому логически не пересекаются.
- `postProcessCallbackEventRelations` делает `DELETE FROM relations WHERE ... subscribes_to_event`; этот delete не должен удалить SQL `calls_procedure`, но может конкурировать с одновременными вставками в `relations` на уровне PostgreSQL locks/IO.
- `postProcessPASPending` не пишет в `relations` и выглядит наиболее безопасным кандидатом для параллельного запуска с остальными.
- Общий `statsCollector` потокобезопасен через mutex.
- Общий `pendingMu` защищает snapshot pending-структур, но при параллельном запуске коротко сериализует только чтение/обнуление pending arrays.

## Рекомендуемый вариант

1. Сначала параллелить только независимые top-level шаги через `sync.WaitGroup`:
   - `postProcessPASPending`
   - `postProcessSQLProcedureCallRelations`
   - `postProcessCallbackEventRelations`
2. После реализации обязательно проверить на полном `init` и сравнить:
   - итоговые counts relations
   - `query callers` для SQL bug case
   - callback subscribers
   - PAS/DFM links
3. Если PostgreSQL покажет lock/contention на `relations`, оставить `postProcessCallbackEventRelations` последовательным, а параллелить только `PAS` и `SQL`.
4. Следующая более эффективная оптимизация — распараллелить внутри `postProcessSQLProcedureCallRelations` обработку pending SQL calls по чанкам, потому что именно этот шаг является основной причиной регресса.

## Итоговый ответ

Запустить эти три функции параллельно в режиме `Init` в целом можно: явной бизнес-зависимости между ними нет. Но это не обязательно даст максимум ускорения, потому что два шага конкурируют за таблицу `relations`; наиболее полезная оптимизация — отдельная параллелизация SQL calls post-process по чанкам.
