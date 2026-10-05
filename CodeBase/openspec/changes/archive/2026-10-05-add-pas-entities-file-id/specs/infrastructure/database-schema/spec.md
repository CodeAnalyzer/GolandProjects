## ADDED Requirements

### Requirement: Прямая привязка PAS-сущностей к файлу

Таблицы `pas_classes`, `pas_methods`, `pas_fields` SHALL содержать колонку `file_id BIGINT NOT NULL` с внешним ключом `REFERENCES files(id) ON DELETE CASCADE` — по единой модели остальных таблиц сущностей (sql_procedures, dfm_forms и т.д.). На колонке `file_id` каждой из трёх таблиц SHALL существовать btree-индекс (`idx_pas_classes_file_id`, `idx_pas_methods_file_id`, `idx_pas_fields_file_id`): без него каждое каскадное удаление файла выполняет seq scan по таблице. Batch insert (COPY IN) этих таблиц SHALL заполнять `file_id` из контекста индексируемого файла тем же значением, что и `pas_units`.

Миграция схемы на существующей БД MUST быть идемпотентной и НЕ требовать переиндексации: добавление колонки (`ADD COLUMN IF NOT EXISTS`), удаление строк с разрушенной цепочкой привязки (классы/методы с несуществующим `unit_id`, поля с несуществующим или NULL `class_id`), backfill `file_id` из цепочки `pas_units` (классы и методы — через `unit_id`; поля — через `class_id → unit_id`), затем `SET NOT NULL` и FK-констрейнт. Повторный запуск миграции на уже мигрированной БД MUST быть no-op.

#### Scenario: Свежая инициализация схемы

- **GIVEN** пустая БД PostgreSQL
- **WHEN** выполняется `InitSchema`
- **THEN** таблицы `pas_classes`, `pas_methods`, `pas_fields` созданы с колонкой `file_id NOT NULL`, FK на `files(id)` с `ON DELETE CASCADE` и индексами `idx_pas_*_file_id`

#### Scenario: Миграция существующей БД без переиндексации

- **GIVEN** проиндексированная БД предыдущей версии схемы (902k методов, 58k классов, 295k полей без `file_id`)
- **WHEN** выполняется `InitSchema` (в т.ч. при `codebase update`)
- **THEN** колонка `file_id` добавлена и заполнена backfill-ом из `pas_units` для всех строк
- **AND** существующие данные не переиндексируются и не повреждаются
- **AND** повторный запуск `InitSchema` не меняет результат

#### Scenario: Сироты удаляются при миграции

- **GIVEN** БД содержит строки `pas_methods` с несуществующим `unit_id` (накопленные инкрементальными update до миграции)
- **WHEN** выполняется миграция схемы
- **THEN** такие строки удалены до backfill
- **AND** после миграции запрос «методы без валидного юнита» возвращает 0 строк

#### Scenario: Каскадное удаление .pas-файла

- **GIVEN** проиндексированный `.pas`-файл с юнитом, классами, методами и полями
- **WHEN** файл исчезает из дерева и выполняется `codebase update` (удаление строки `files`)
- **THEN** связанные `pas_units`, `pas_classes`, `pas_methods`, `pas_fields` удалены каскадно
- **AND** строк-сирот в этих таблицах не остаётся

#### Scenario: Batch insert заполняет file_id

- **GIVEN** распарсенный `.pas`-файл с классами, методами и полями в per-file транзакции
- **WHEN** выполняется batch insert PAS-сущностей
- **THEN** каждая новая строка трёх таблиц содержит `file_id` вставляемого файла
- **AND** повторная индексация того же файла не оставляет строк со старым `file_id`
