# Design: dsinfo-parser

## Context

См. `proposal.md` (Why). Исходное большое изменение —
`Modifications/dsinfo-product-branches-6f2a1c.md`; данная доработка — его
автономная первая часть: парсер и хранилище профилей без потребителей.

Структура реального файла (проверено на `Modifications/DiasoftSystemInfo.csv`,
70 123 строки, WIN-1251, разделитель `;`, хвостовые `;`, пустые поля):

| Строка | Секция | Содержимое | Судьба |
|---|---|---|---|
| 0 | Информация | `Parameter;Info` — алиас SQL, БД, пути | `client_alias` |
| 22 | Параметры системы | настройки, лицензии | пропуск |
| 386 | Продукты Digital Q | 62 строки `NAME[\PACKET_REF]` | **dsinfo_products** |
| 450 | Модули | заголовок 18 колонок + ~200 BPL/EXE | **dsinfo_modules** |
| 653 | Процедуры | статистика + 62 649 строк объектов БД | **dsinfo_procedures** |
| 63330 | Конфигурация | сервер, база, версия SQL Server | `db_server/db_name` |
| 63335 | Продукты | внутренние модули БД с описаниями | пропуск |
| 63792 | Модули → Установленные пакеты | история установки (3 колонки) | пропуск |
| … | параметры, dssql, FileSign | — | пропуск |

Существующие образцы: RTI/TRC-сессии (`internal/rtisvc`, `internal/store/db_schema.go`
`rti_sessions`: `file_hash + parser_version` partial unique, reuse), CLI
(`cmd/rti.go`), MCP-registry (`internal/mcp`). Декодер WIN-1251 — `internal/encoding`.

## Goals / Non-Goals

**Goals:**

- Разбор CSV потоком в один проход, сниффинг по содержимому (имя файла не важно).
- Полный снимок: манифест + модули + процедуры, без потери полей.
- Профили идемпотентны по `SHA-256 + parser_version`; активный профиль в БД.
- CLI `codebase dsinfo ...` и MCP `dsinfo_*` по общим контрактам проекта.

**Non-Goals:**

- Проброс `--dsinfo` в `query`/`rti`/`trc` и MCP-инструменты анализа (отдельное
  изменение после веточной схемы).
- Сопоставление профиля с `ds_products`/`ds_products_branch`, статусы
  `ok/branch_not_indexed` — веток ещё нет.
- `prune` для профилей, разбор «Установленных пакетов» и параметров модулей.
- `use_count` и «мягкое» удаление: у профиля нет дорогого парсинга и
  разделяемых потребителей — удаление всегда окончательное.

## Decisions

### D1. Слои по образцу RTI

`internal/dsinfo` — чистый парсер (файл → `dsinfo.Profile`, без БД);
`internal/dsinfosvc` — `ExecuteParse/ExecuteList/ExecuteShow/ExecuteDelete/ExecuteUse`;
`internal/store/ds_info_store.go` — CRUD + batch insert; `cmd/dsinfo.go` — CLI;
`internal/mcp` — 5 инструментов. Альтернатива «всё в одном пакете» отвергнута:
в проекте устоялось разделение parser/svc/store/cmd, оно даёт переиспользование
CLI↔MCP и юнит-тестирование парсера без PostgreSQL.

### D2. Схема (5 таблиц, все `CREATE TABLE IF NOT EXISTS`)

```sql
CREATE TABLE IF NOT EXISTS dsinfo (
    id               BIGSERIAL PRIMARY KEY,
    file_path        TEXT NOT NULL,
    session_name     TEXT NOT NULL DEFAULT '',
    file_size        BIGINT NOT NULL DEFAULT 0,
    file_hash        TEXT,
    parser_version   INTEGER,
    db_server        TEXT NOT NULL DEFAULT '',
    db_name          TEXT NOT NULL DEFAULT '',
    client_alias     TEXT NOT NULL DEFAULT '',
    products_count   INTEGER NOT NULL DEFAULT 0,
    modules_count    INTEGER NOT NULL DEFAULT 0,
    procedures_count INTEGER NOT NULL DEFAULT 0,
    parsed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dsinfo_file_hash
    ON dsinfo(file_hash, parser_version) WHERE file_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dsinfo_parsed_at ON dsinfo(parsed_at DESC);

CREATE TABLE IF NOT EXISTS dsinfo_products (
    id           BIGSERIAL PRIMARY KEY,
    dsinfo_id    BIGINT NOT NULL REFERENCES dsinfo(id) ON DELETE CASCADE,
    line_no      INTEGER NOT NULL DEFAULT 0,
    raw_name     TEXT NOT NULL,
    packet_ref   TEXT NOT NULL DEFAULT '',
    product_name TEXT NOT NULL,
    branch_key   TEXT NOT NULL,
    hotfix_name  TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_dsinfo_products_profile ON dsinfo_products(dsinfo_id);

CREATE TABLE IF NOT EXISTS dsinfo_modules (
    id               BIGSERIAL PRIMARY KEY,
    dsinfo_id        BIGINT NOT NULL REFERENCES dsinfo(id) ON DELETE CASCADE,
    file_name        TEXT NOT NULL,
    file_description TEXT NOT NULL DEFAULT '',
    module_name      TEXT NOT NULL DEFAULT '',
    product_version  INTEGER NOT NULL DEFAULT 0,
    product_release  INTEGER NOT NULL DEFAULT 0,
    product_issue    INTEGER NOT NULL DEFAULT 0,
    product_build    INTEGER NOT NULL DEFAULT 0,
    packet_name      TEXT NOT NULL DEFAULT '',
    hotfix_name      TEXT NOT NULL DEFAULT '',
    comments         TEXT NOT NULL DEFAULT '',
    file_hash        TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_dsinfo_modules_profile ON dsinfo_modules(dsinfo_id);
CREATE INDEX IF NOT EXISTS idx_dsinfo_modules_module  ON dsinfo_modules(dsinfo_id, module_name);

CREATE TABLE IF NOT EXISTS dsinfo_procedures (
    id           BIGSERIAL PRIMARY KEY,
    dsinfo_id    BIGINT NOT NULL REFERENCES dsinfo(id) ON DELETE CASCADE,
    elem_schema  TEXT NOT NULL DEFAULT '',
    object_name  TEXT NOT NULL,
    install_date TEXT NOT NULL DEFAULT '',
    module_name  TEXT NOT NULL DEFAULT '',
    packet_name  TEXT NOT NULL DEFAULT '',
    source_file  TEXT NOT NULL DEFAULT '',
    state        INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL DEFAULT '',
    comment      TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_dsinfo_procedures_profile ON dsinfo_procedures(dsinfo_id);
CREATE INDEX IF NOT EXISTS idx_dsinfo_procedures_object  ON dsinfo_procedures(dsinfo_id, object_name);
CREATE INDEX IF NOT EXISTS idx_dsinfo_procedures_packet  ON dsinfo_procedures(dsinfo_id, packet_name);
CREATE INDEX IF NOT EXISTS idx_dsinfo_procedures_module  ON dsinfo_procedures(dsinfo_id, module_name);

CREATE TABLE IF NOT EXISTS dsinfo_state (
    id              INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    active_dsinfo_id BIGINT REFERENCES dsinfo(id) ON DELETE SET NULL
);
```

Решения внутри:

- **Имена таблиц** — `dsinfo`/`dsinfo_products` как в большом доке (решение
  пользователя), плюс симметричные `dsinfo_modules`/`dsinfo_procedures`.
- **Снимок без FK на ветки** — принцип большого дока «профиль — снимок, не FK»:
  статусы сопоставления вычислятся позже при использовании.
- **`dsinfo_state` singleton** (`CHECK(id=1)` + `ON DELETE SET NULL`) — БД сама
  поддерживает инвариант «активный указывает на существующий профиль или NULL»:
  удаление активного профиля сбрасывает ссылку автоматически, код лишь
  фиксирует факт для ответа. Альтернатива «колонка is_active в dsinfo» отвергнута:
  допускает два активных без частичного уникального индекса.
- **Даты как TEXT** — в файле встречаются `dd/mm/yyyy hh:mm:ss`,
  `dd.mm.yyyy` и нулевая `01/01/1900`; интерпретация не нужна для снимка, а
  неоднозначность d/m/y опасна. Хеш даты не участвует в логике.
- **18 колонок «Модули» → 12 колонок таблицы**: хранятся значимые поля
  (FileName, FileDescription, ModuleName, 4 версии, PacketName, HotfixName,
  Comments, Hash); Addition*/PacketVersion* во всех просмотренных строках
  пусты — при обнаружении заполненных полей в новых файлах схема расширяется
  аддитивно.

### D3. Сниффинг и конечный автомат секций

Парсер читает файл через `bufio.Scanner` (буфер 1 МБ), декодируя WIN-1251
посредством `internal/encoding` (charmap). Логика — один проход, state machine
по секциям:

- **Заголовок секции** — строка без `;`, не пустая, короче ~60 символов.
- **`manifest`** («Продукты Digital Q»): строки `A[\B];` → `raw_name`, `packet_ref`.
- **`modules`**: ожидается заголовок колонок, начинающийся с `FileName;`;
  последующие строки с ≥10 колонками — модули; подсекция заканчивается заголовком
  «Процедуры».
- **`procedures`**: строки с ≥8 колонками — объекты БД (блоки «Статистика»
  имеют ≤2 колонок и отсеиваются тем же правилом); 3-я колонка не валидируется
  как дата — хранится как есть.
- **Вторая «Модули»** («Установленные пакеты») не имеет заголовка `FileName;...`
  — не входит в состояние `modules`, поэтому история пакетов не попадает в
  `dsinfo_modules` (главный сюрприз реального файла).
- **`info`/`config`**: `client_alias` из «Алиас SQL-сервера», `db_server`/`db_name`
  из «Имя сервера»/«Имя базы».

**Признак валидности**: к концу прохода найдены маркеры «Продукты Digital Q» И
«Модули» с заголовком `FileName;` — иначе ошибка «файл не похож на
DiasoftSystemInfo» и ничего не сохраняется. Всё читается в один проход: файл
14 МБ разбирается за десятки миллисекунд, отдельного pre-scan не требуется.

### D4. Нормализация имён (правило «до первого числа»)

`product_name` — токены `raw_name` до первого токена из одних цифр, lowercase
(ведущие нули числовых токенов сохраняются как в файле: `11-01-7`, `9-01-553`).
`branch_key` = `product_name` + первые три числовых токена. `hotfix_name` =
`packet_ref` целиком, если он матчит `-HF<цифры>`, иначе пусто. Правило
проверено на всех 62 строках реального манифеста, включая PKG-, SP6- и
составные `7-02-2-7-02-2` случаи.

### D5. Идемпотентность без use_count

`parse`: SHA-256 файла → `SELECT id FROM dsinfo WHERE file_hash AND parser_version`
→ hit: reuse (обновляется только `parsed_at`? — нет, не трогаем, ответ
сообщает `reused: true`); miss: `INSERT` профиля + батчи дочерних строк
(мульти-VALUES по ~500 строк) в одной транзакции. Гонка двух параллельных parse
одного файла закрывается unique-индексом: проигравший INSERT получает конфликт →
перечитывает существующий id. `parser_version` — константа `dsinfo.ParserVersion`,
инкрементируется при изменении логики разбора. Отказ от `Acquire/Release`
(как у RTI) осознанный: нет дорогого парсинга и разделяемых потребителей.

### D6. CLI и MCP

- `codebase dsinfo parse <file> [--name]` — печатает id, счётчики, таблицу
  продуктов; `--json` по общему контракту.
- `list [--limit]` — профили по убыванию `parsed_at`, активный помечен `*`.
- `show <id> [--module|--packet|--search] [--limit]` — метаданные + манифест +
  модули; процедуры только с фильтром (без фильтра — счётчик). `--search` —
  подстрока в `object_name` (ILIKE).
- `delete <id>` — окончательное удаление; если удаляли активный — сообщение о
  сбросе.
- `use [<id>]` — установить/показать активный профиль.
- MCP: `dsinfo_parse/list/show/delete/use` с теми же параметрами; регистрация во
  всех профилях сервера (инструменты самодостаточны и не зависят от домена
  анализа). Большие выводы `show` ограничиваются `limit` (по умолчанию 100,
  максимум 1000) и при превышении используют стандартную пагинацию MCP.
- Терминология: «профиль dsinfo» (сущность — строка `dsinfo`, активный профиль в
  `dsinfo_state`) не имеет отношения к флагу `--profile` MCP-сервера (подмножество
  инструментов `query`/`rti`/`trc`/`review`, cmd/mcp.go). Сущность живёт в
  `internal/store` (`FindProfileByHash` и др.) и `internal/dsinfosvc`; CLI
  использует позиционный `dsinfo use <id>`, флаг `--profile` в командах dsinfo не
  появляется. При обновлении README оговорить различие явно.

### D7. Сопутствующие правки

`internal/store/db_reset.go` — добавить `dsinfo*` в TRUNCATE-список (каскадно от
`dsinfo`); `README.md` — раздел dsinfo; `internal/mcp` — registry.
Юнит-тесты парсера — на фрагментах реального CSV (манифест с HF/PKG/SP6,
модуль с пакетом в HotfixName, процедуры с state 16/32, вторая «Модули»);
интеграционные — по образцу `store_integration` (reuse по hash, каскад,
активный профиль, фильтры show).

## Risks / Trade-offs

- [Формат CSV различается между версиями системы] → маркеры секций + заголовки
  колонок как единственные точки привязки; неизвестные секции пропускаются;
  отсутствие обязательных маркеров — явная ошибка, а не пустой профиль.
- [63 тыс. строк процедур на профиль, профили накапливаются] → объём сопоставим
  с одной RTI-сессией; управление жизненным циклом — `dsinfo delete`; при
  необходимости prune добавится отдельно.
- [Даты хранятся как TEXT] → снимок честен, но сортировка/сравнение по дате
  установки потребует парсинга при выводе; для заявленных сценариев не нужно.
- [Пустые PacketVersion/Addition* колонки не хранятся] → при появлении
  заполненных полей в других файлах схема расширяется аддитивно; повторный parse
  после bump `parser_version` создаст новый профиль без конфликта.
- [Активный профиль пока никем не читается] → это заявлено в спеке; контракт
  зафиксирован, чтобы будущая интеграция не меняла CLI.

## Migration Plan

Схема применяется `InitSchema` идемпотентно при первом запуске любой команды;
откат не требуется (таблицы новые, поведение остального кода не меняется).
Чистка — `dsinfo delete` по профилям; полная переустановка — существующий
`reset`.
