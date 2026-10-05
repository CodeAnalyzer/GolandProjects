## Context

Текущая модель PAS-сущностей нормализована через цепочку `pas_methods.unit_id → pas_units → files` (`pas_classes.unit_id`, `pas_fields.class_id` — аналогично; FK только у `pas_units`). Из этого следуют два дефекта (см. proposal.md — Why):

1. Generic-резолвер `lookupEntityIDsByNames` (`db_lookup_spec_mentions.go:107`) строит `JOIN files f ON f.id = e.file_id` — для `pas_methods` колонки нет, запрос падает `pq: column e.file_id does not exist`; все `references_code → pas_method` рёбра молча теряются с доработки `2026-09-07-add-openspec-artifacts-indexing`.
2. `DELETE FROM files` каскадит только на `pas_units`; классы/методы/поля остаются сиротами. Утечка подтверждена экспериментально (BEGIN/DELETE/ROLLBACK на живой БД). Масштаб таблиц: 902k методов, 58k классов, 295k полей, 9.5k юнитов.

Механизм миграций: идемпотентные statements в общем списке `InitSchemaCtx` (advisory-lock, одна транзакция) + версионированные маркеры `schema_migrations` (`applyMigrationTx`, прецедент `spec_search_vectors_weighted_v1`); прецедент денормализации с backfill — `spec_usecase_steps.file_id` (db_schema.go:701-718). Маркер совместимости `CurrentSchemaVersion = "codebase_schema_v1"` проверяется `CheckSchemaVersion` (read-only, для MCP-readiness).

Вставка PAS-сущностей идёт в per-file транзакции (`processFileInTx`): units → SELECT-back id → classes/methods/fields через COPY IN с явными списками колонок; `fileID` доступен в точке вставки. Отложенный резолв (`class_id` через UPDATE в пост-обработке) затрагивает только `class_id`, не `unit_id`/`file_id`.

## Goals / Non-Goals

**Goals:**

- `pas_classes`, `pas_methods`, `pas_fields`: `file_id BIGINT NOT NULL` + FK `ON DELETE CASCADE` + индекс — единая модель остальных таблиц сущностей.
- Идемпотентная миграция существующих БД без переиндексации (backfill ~1.25M строк).
- Восстановление резолва method-упоминаний спек без правок кода резолвера.

**Non-Goals:**

- Cleanup висячих `relations` после каскадных удалений — отсутствует для всех типов сущностей и сегодня; PAS приводится к общему режиму, не улучшается точечно.
- Переход остальных запросов (`internal/query`) с JOIN через `pas_units` на прямой `file_id` — JOIN остаётся валидным (нужен и для `unit_name`); оптимизация по мере надобности.
- Баг счётчика `files_indexed=0` в `scan_runs` (update-путь) — отдельная доработка.
- Денормализация `dfm_form_id`/`dfm_component_id`-цепочек — не требуется ни одним запросом.

## Decisions

### 1. Денормализация `file_id` вместо FK-цепочки

Альтернатива: FK `pas_methods.unit_id → pas_units(id) ON DELETE CASCADE` (+ `class_id → pas_classes`, + поля на классы). Чинит целостность, но НЕ резолвер — generic-запрос всё равно требует `e.file_id`, пришлось бы вводить спец-ветку с другим JOIN-путем. Денормализация закрывает обе проблемы одной колонкой; цена ~7 МБ на 902k строк и один backfill. Значение стабильно (unit не меняет файл), аномалий обновления нет: строки пересоздаются при переиндексации файла.

### 2. `NOT NULL` + FK `ON DELETE CASCADE` + индекс

Стандарт проекта: все таблицы сущностей так устроены. Индекс на `file_id` обязателен: без него каждый deleted parent row ищет детей seq scan-ом (902k строк × число удалённых файлов в батче). Отдельно именованные констрейнты `pas_classes_file_id_fkey` / `pas_methods_file_id_fkey` / `pas_fields_file_id_fkey` — по образцу `pas_units_file_id_fkey`, для условной проверки `pg_constraint` в миграции.

### 3. Миграция: идемпотентные statements + named-маркер + bump схемы до `codebase_schema_v2`

Порядок statements (в транзакции `InitSchemaCtx`, после advisory-lock):

```
1. ALTER TABLE <t> ADD COLUMN IF NOT EXISTS file_id BIGINT          -- ×3
2. DELETE сирот:
   - pas_classes  WHERE unit_id NOT IN pas_units
   - pas_methods  WHERE unit_id NOT IN pas_units
   - pas_fields   WHERE class_id IS NULL OR class_id NOT IN pas_classes
3. Backfill:
   - pas_classes/pas_methods: file_id = pu.file_id FROM pas_units (unit_id)
   - pas_fields: file_id = pu.file_id FROM pas_classes c JOIN pas_units pu (class_id)
     (WHERE file_id IS NULL — повторная безопасность)
4. ALTER TABLE <t> ALTER COLUMN file_id SET NOT NULL                -- ×3
5. DO $$ ... IF NOT EXISTS pg_constraint ... ADD CONSTRAINT ... FK CASCADE -- ×3
6. CREATE INDEX IF NOT EXISTS idx_pas_<t>_file_id                   -- ×3 (в общем списке индексов)
```

Statements идут в общем списке `InitSchemaCtx` (идемпотентны каждый, как у `spec_usecase_steps.file_id`), а маркер `pas_entities_file_id_v1` записывается отдельным `applyMigrationTx` с теми же statements — для трассировки применения. Дополнительно `CurrentSchemaVersion` → `codebase_schema_v2`: новый бинарник на не-мигрированной БД получает внятный `ErrSchemaUpdateRequired` («выполнить init/update»), а не невнятную ошибку COPY IN на отсутствующую колонку. Альтернатива «не поднимать версию, положиться на авто-миграцию при следующем прогоне» отклонена: она молчит ровно до первого падения.

Удаление сирот на шаге 2 оправдано: строки с разрушенной цепочкой недостижимы ни одним запросом (все читающие запросы идут INNER JOIN через unit/class) и являются мусором по определению. Поля с навсегда-неразрешённым `class_id IS NULL` удаляются по той же причине (резолв выполняется в том же прогоне индексации; к моменту миграции неразрешённое = неразрешимое). На актуальной БД сейчас 0 таких строк — ветка чисто защитная.

### 4. Вставка: `FileID` в модели + колонка в CopyIn

`model.PASClass/PASMethod/PASField` получают поле `FileID int64`; PAS-обработка индексатора заполняет его из `fileID` per-file транзакции (то же значение, что у `pas_units`); CopyIn-списки в `db_insert_pas.go` дополняются `"file_id"`. Отложенный резолв `class_id` (UPDATE в пост-обработке) не затрагивается.

### 5. Резолвер спек-упоминаний не меняется

`FindPASMethodIDsByNames` и `lookupEntityIDsByNames` работают без правок: `e.file_id` становится валидным, приоритеты (не-генерируемый → продукт → свежесть) применяются к методам автоматически. Поведенческие требования `openspec-parsing` (method → pas_methods) и `database-schema` (lookup с приоритетом канона) становятся выполняемыми by construction — дельт для них не нужно.

## Risks / Trade-offs

- [Backfill 1.25M строк блокирует таблицы на время UPDATE] → разовая операция в транзакции `InitSchemaCtx` на локальной БД, секунды-десятки секунд; все statements идемпотентны, повтор безопасен.
- [`SET NOT NULL` падает, если после очистки остались NULL] → шаг 2 удаляет все строки с невозможным backfill; страховка — порядок statements внутри одной транзакции (падение = полный откат).
- [Висячие `relations` на удалённые каскадом методы] → консистентно с поведением всех остальных типов сущностей; LEFT JOIN-запросы дают NULL-строки так же, как сегодня для процедур/форм. Глобальный cleanup — отдельная будущая доработка.
- [Старый бинарник на мигрированной БД] → read-only пути (query/MCP) совместимы: v1-маркер остаётся в schema_migrations, CheckSchemaVersion старого бинарника проходит. Но индексация `.pas` старым бинарником упадёт: CopyIn без `file_id` нарушает NOT NULL — update/init допустимы только новым бинарником (проверено на прогоне FA: старый риск-прогноз «полная совместимость» был неверен).
- [Избыточность `file_id` против `unit_id`] → принятая цена денормализации; рассинхрон невозможен (обе колонки заполняются одним значением в одной транзакции, строки не мигрируют между файлами).

## Migration Plan

1. Деплой нового бинарника; первый же `codebase init`/`update` прогоняет `InitSchemaCtx`: миграция + маркеры `pas_entities_file_id_v1`, `codebase_schema_v2`.
2. Откат: вернуть старый бинарник — схема обратно совместима (лишняя колонка/FK/индекс не мешают); при необходимости полного отката — `ALTER TABLE ... DROP CONSTRAINT`, `DROP COLUMN file_id` вручную.
3. Верификация на FA: `codebase update`, затем SQL-проверки (сироты = 0; `spec_search`/`spec_coverage` показывают резолвнутые method-упоминания; лог без `column e.file_id does not exist`).

## Open Questions

- Стоит ли в будущем распространить cleanup висячих `relations` на все типы сущностей (один `DELETE ... NOT EXISTS` в пост-обработке) — вне скоупа, фиксируется как кандидат в backlog.
