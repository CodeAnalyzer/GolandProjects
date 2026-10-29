# Мультиветочная индексация продуктов FA и профиль развёртывания `dsinfo`

Предлагается научить CodeBase хранить в одной БД несколько релизных линий
каждого продукта FA (`develop` + несколько `-SP`-веток) и выбирать нужную линию
на время анализа инцидента по конфигурации банка из `DiasoftSystemInfo.csv`.
Конфигурация сохраняется командой `dsinfo parse` и передаётся в команды анализа
опциональным `--dsinfo <id>`. Без `--dsinfo` поведение остаётся прежним: все
lookup'ы идут в `develop` (настраивается в `codebase.toml`).

Статус: реализовано в рамках 4 changes:
- openspec\changes\archive\2026-10-24-dsinfo-parser
- openspec\changes\archive\2026-10-24-branch-aware-indexing
- openspec\changes\archive\2026-10-26-branch-aware-lookup
- openspec\changes\archive\2026-10-29-dsinfo-integration

---

## Контекст проблемы

### Рабочий сценарий

Задача приходит из офисной системы: описание проблемы, RTI-логи и/или TRC-трейсы,
плюс текущая конфигурация системы в виде `DiasoftSystemInfo.csv` (WIN-1251,
пример — `Modifications/DiasoftSystemInfo.csv`, ~14 МБ).

Конфигурация содержит две важные секции:

```
Продукты Digital Q
FA-Administrator-11-01-7;
FA-Contracts-11-01-7\FA-Contracts-11-01-7-HF008;
FA-GAAP-11-01-4;
FA-Payments-11-01-21\FA-Payments-11-01-21-HF004;
FA-Contracts-Ext-9-01-553-PKG-IFRS9_TOYOTA\...-75-26012604;
...

Модули
D5NTLOAN.BPL;;;11;1;7;723;;;;;;;;;FA-Contracts-11-01-7-HF008-26090207;02.09.2026;C70AC970;
D5NTGAAP.BPL;;GAAP;11;1;4;3026;FA-GAAP-11-01-4-26073102;;;;;;;;;31.07.2026;BAFB7572;
...
```

То есть **у разных продуктов одновременно разные релизные линии**: Contracts
11.01.7 + HF008, GAAP 11.01.4, Payments 11.01.21 + HF004, Cards 9.01.2 и т. д.
Сейчас анализ всегда смотрит на текущую ветку индекса (практически всегда
`develop`), поэтому процедуры и API из трейса сопоставляются с кодом не той
версии. Для RTI/TRC это не косметика: обогащение отдаёт `source_file` и номера
строк, то есть **указывает не туда, но выглядит правдоподобно**.

### Проверено на локальных репозиториях

Имена `PacketName`/`HotfixName` из секции «Модули» — это реальные git-теги, а
релизные линии — ветки:

| Продукт | Релизная ветка | Тег сборки из CSV |
|---|---|---|
| FA-Contracts | `FA-Contracts-11-01-7-SP` | `FA-Contracts-11-01-7-26081201`, `FA-Contracts-11-01-7-HF008-26090207` |
| FA-GAAP | `FA-GAAP-11-01-4-SP` | `FA-GAAP-11-01-4-26073102`, `FA-GAAP-11-01-4-HF001-26091502` |

Принято решение (см. «Открытые вопросы»): гранулярность — **релизная линия**
(`-SP`), хотфиксы отдельно не индексируются. Это компромисс: head линии может
быть новее сборки банка (HF008 от 2026-09-02), поэтому возможны лишние процедуры
и сдвиг номеров строк. Апгрейд до точных тегов остаётся возможным позже, так как
теги из CSV существуют.

### Почему «одна ветка на сессию» не работает

Инцидент одновременно затрагивает несколько продуктов на разных линиях. Единый
глобальный переключатель ветки не может быть правильным по определению. Нужен
**профиль**: отображение `продукт → релизная линия`. Это и есть `dsinfo`.

### Факты текущей реализации

| Звено | Файл:строка | Что делает |
|---|---|---|
| Определение продукта | `internal/indexer/indexer.go:1788-1817` | первый путь-сегмент с `fa-`, `tokens[:3]` |
| Присвоение продукта файлу | `internal/indexer/indexer.go:1753-1762` | `extractCanonicalDSProductName` → `GetOrCreateDSProductIDByName` |
| Резолв имён в графе | `internal/store/db_lookup_sql.go:36-47` | `DISTINCT ON`, приоритет `not_generated → product_match → id` |
| Пост-обработка вызовов | `internal/indexer/indexer_postprocess_sql_calls.go:19` | `FindLatestSQLProcedureIDsByNames(..., 0)` — без продукта |
| Пост-обработка фрагментов | `indexer_postprocess_fragments.go:49,55,149,221,313` | то же, `productID=0` |
| Детали процедуры | `internal/query/query_sql.go:480-489` | `QueryRow` по имени **без `ORDER BY`** |
| Обогащение RTI/TRC | `internal/rti/enrich.go:50`, `internal/trc/enrich.go:35` | `GetProcedureResult(name)` — глобально, без ветки |
| Модуль RTI-вызова | `internal/rti/parser.go:238`, `internal/rti/symbols.go:455` | `ModuleID → ModuleName` (частичная таблица) |
| BPL клиента в RTI | `internal/rti/model.go:129` | `{file, version, title, comment}` из лога |
| Обход файлов | `internal/fswalk/fswalk.go:189-190` | каталоги, начинающиеся с `.`, пропускаются |
| Инкрементальное удаление | `internal/store/db_files.go:14-24` | `GetLatestFilesByRootPath` по префиксу пути |
| Полная пересборка | `internal/store/db_reset.go:50` | `TRUNCATE` всех таблиц |

### Корневая причина

Модель индекса предполагает «одно каноническое тело на имя в продукте»:
дубликаты есть, но они семантически идентичны (UPLOAD-копии, общие DDL-утилиты).
Ветка добавляет вторую ось, где **обе копии легитимны**, и стратегия «кто
последний — тот и выиграл» перестаёт быть безобидной недетерминированностью.
Дополнительно продукт определяется эвристикой по имени каталога, которая ломается
на предлагаемой раскладке:

```
extractCanonicalDSProductFromPath("fa-contracts-11-01-7/...")
    tokens = [fa, contracts, 11, 01, 7] → tokens[:3] → "fa-contracts-11"   ❌
extractCanonicalDSProductFromPath("fa-contracts-develop/...")
    → "fa-contracts-develop"                                              ❌ псевдопродукт
```

---

## Цели / не-цели

**Цели:**

1. Хранить в одной БД `develop` + 2–3 последние релизные линии по каждому
   продукту.
2. Определять продукт и ветку из git-состояния каталога, а не из имени каталога.
3. Разбирать `DiasoftSystemInfo.csv` в именованный профиль (`dsinfo_id`).
4. Резолвить процедуры/API/таблицы в ветке продукта из профиля; при отсутствии
   профиля — в `develop` (как сейчас).
5. Не допускать «молчаливого» использования `develop` вместо запрошенной линии.
6. Резолвить граф вызовов внутри ветки, а не глобально.

**Не-цели:**

1. Точное соответствие хотфиксу (`HF*`) — только релизная линия.
2. Хранение всех исторических веток и тегов.
3. Cross-branch diff как отдельная команда (остаётся возможным, но не входит в
   объём).
4. Автоматическое создание checkout'ов/клонов (см. открытый вопрос).
5. Изменение форматов RTI/TRC-логов.

---

## Термины

| Термин | Значение |
|---|---|
| **Продукт** | репозиторий FA; каноническое имя = basename `origin` URL (`fa-contracts`) |
| **Ветка продукта (source)** | конкретный checkout продукта в каталоге: `(product, branch_name, path)` |
| **Релизная линия** | git-ветка `FA-<Product>-<major>-<minor>-<patch>-SP` |
| **`dsinfo`** | сохранённый профиль развёртывания из CSV: набор `product → branch` |
| **default branch** | ветка, используемая при lookup'е без профиля (`develop`) |
| **branch_used** | фактически выбранная ветка; всегда попадает в вывод |

---

## Обзор решения

```
FA/
├─ fa-contracts-develop/      git: develop                  ┐
├─ fa-contracts-11-01-7/      git: FA-Contracts-11-01-7-SP  ├─ init/update
├─ fa-contracts-11-01-6/      git: FA-Contracts-11-01-6-SP  │  → ds_products
├─ fa-gaap-develop/           git: develop                  │  → ds_products_branch
└─ ...                                                      ┘  → files.ds_product_branch_id

DiasoftSystemInfo.csv ──dsinfo parse──► dsinfo + dsinfo_products (снимок)
                                              │
                 query / rti / trc --dsinfo N ┘
                           │
                           ▼
         product + branch из профиля → сущности этой ветки
         (fallback на default branch, всегда с пометкой в выводе)
```

Ключевые принципы:

- **Директория — не источник истины.** Продукт — из git remote, ветка — из git
  HEAD; имя каталога нужно только человеку.
- **Профиль — снимок, не FK.** В `dsinfo_products` хранятся имена продукта и
  ветки; резолв в актуальные `ds_products_branch` — при использовании.
- **Контекст опционален.** Нет `--dsinfo` → `default_branch` из конфига.
- **Никаких молчаливых подмен.** Если запрошенная линия не проиндексирована,
  это видно в `dsinfo parse`, в предупреждении и в поле `branch_used`.

---

## Решение 1. Продукт и ветка — из git

### Определение

```
product = basename(git remote get-url origin)      // fa-contracts
branch  = git rev-parse --abbrev-ref HEAD          // FA-Contracts-11-01-7-SP
commit  = git rev-parse HEAD
```

Проверено на локальном дереве: basename remote'а совпадает с каноническим именем
продукта у всех репозиториев, включая extension-пакеты:

| Каталог | Branch | Remote basename |
|---|---|---|
| `fa-contracts` | develop | `fa-contracts` |
| `fa-contracts-ext-novikom-persform` | `prj/NOVIKOM-PERSFORM` | `fa-contracts-ext` |
| `fa-gaap` | develop | `fa-gaap` |

Побочный эффект: снимается старая проблема схлопывания `fa-contracts-ext-*`
(сейчас все PKG-клоны становятся одним `fa-contracts-ext`, но только благодаря
обрезке `tokens[:3]`; при других именах обрезка даёт ложные продукты).

### Раскладка каталогов

Плоская, как в исходной постановке:

```
D:\GITHUB\GolandProjects\FA\fa-contracts-develop
D:\GITHUB\GolandProjects\FA\fa-contracts-11-01-7
D:\GITHUB\GolandProjects\FA\fa-contracts-11-01-6
```

Иерархическая раскладка (`11-01-7\fa-contracts`) не запрещена, но не даёт
преимуществ: при определении из git имя каталога не участвует. Плоская проще для
`git worktree` и для скриптов.

### Ограничения и оговорки

- **Detached HEAD** (checkout тега) даёт пустой `branch --show-current`; для
  `ds_products_branch` использовать `git describe --tags --always` или короткий
  SHA. Для релизных линий не требуется, но поведение должно быть
  детерминированным.
- **Worktree.** В worktree `.git` — файл, поэтому чтение `.git/HEAD` напрямую
  недостаточно; нужен `git -C <dir> ...`. Git CLI становится зависимостью
  индексатора (один вызов на source, не на файл).
- **Один каталог — одна ветка.** Переключение ветки в существующем каталоге
  заменяет содержимое индекса этой ветки (файлы с теми же путями удаляются и
  вставляются заново с новым `branch_id`). Для одновременного хранения веток
  нужны отдельные каталоги/`git worktree`.
- **Не-git каталоги** (`DCP`, `dcu`, `exe`, `logs`, `Tools`) продукта не имеют.
  Для них сохраняется прежний путь через `extractCanonicalDSProductName` либо
  `ds_product_id = NULL`.

---

## Решение 2. Схема БД

### Новые таблицы

```sql
CREATE TABLE IF NOT EXISTS ds_products_branch (
    id               BIGSERIAL PRIMARY KEY,
    ds_product_id    BIGINT NOT NULL REFERENCES ds_products(id) ON DELETE CASCADE,
    branch_name      TEXT NOT NULL,          -- как в git: FA-Contracts-11-01-7-SP
    branch_key       TEXT NOT NULL,          -- нормализовано: fa-contracts-11-01-7
    path             TEXT NOT NULL,
    commit_sha       TEXT,
    is_default       BOOLEAN NOT NULL DEFAULT FALSE,
    indexed_at       TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ds_products_branch_product_key
    ON ds_products_branch(ds_product_id, branch_key);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ds_products_branch_path
    ON ds_products_branch(path);
CREATE INDEX IF NOT EXISTS idx_ds_products_branch_product
    ON ds_products_branch(ds_product_id);
```

- `branch_name` — сырое имя git-ветки (для отображения и git-операций).
- `branch_key` — нормализованное (lowercase, `-SP` снят, `.`/`_` → `-`) для
  сопоставления с манифестом; см. «Нормализация».
- `is_default` — ветка, используемая как fallback продукта. Глобальный дефолт —
  `indexer.default_branch` в `codebase.toml`; флаг позволяет переопределить для
  продукта без `develop`.

### Новые таблицы профиля

```sql
CREATE TABLE IF NOT EXISTS dsinfo (
    id           BIGSERIAL PRIMARY KEY,
    source_file  TEXT NOT NULL,
    file_hash    TEXT NOT NULL,             -- SHA-256 содержимого CSV
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dsinfo_products (
    id             BIGSERIAL PRIMARY KEY,
    dsinfo_id      BIGINT NOT NULL REFERENCES dsinfo(id) ON DELETE CASCADE,
    raw_name       TEXT NOT NULL,           -- как в CSV: FA-Contracts-11-01-7
    product_name   TEXT NOT NULL,           -- fa-contracts
    branch_key     TEXT NOT NULL,           -- fa-contracts-11-01-7
    hotfix_name    TEXT,                    -- для аудита, не влияет на выбор
    status         TEXT NOT NULL,           -- ok | product_not_indexed | branch_not_indexed
    ds_products_branch_id BIGINT REFERENCES ds_products_branch(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_dsinfo_products_dsinfo ON dsinfo_products(dsinfo_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dsinfo_file_hash ON dsinfo(file_hash);
```

`idx_dsinfo_file_hash` делает `dsinfo parse` идемпотентным: повторный разбор того
же файла возвращает существующий `dsinfo_id` (по образцу reuse сессий RTI/TRC).

### Связь файлов с веткой

```sql
ALTER TABLE files ADD COLUMN IF NOT EXISTS ds_product_branch_id BIGINT
    REFERENCES ds_products_branch(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_files_ds_product_branch_id
    ON files(ds_product_branch_id);
```

`files.ds_product_id` остаётся (денормализация для существующих запросов и
`spec_configs`). Новый код использует `ds_product_branch_id`.

### Миграция существующего индекса

`update` пропускает неизменённые файлы по mtime+size, поэтому старые строки
сохранят старый `ds_product_id` и `ds_product_branch_id = NULL`. Нужен backfill:

- при регистрации source в `init`/`update` выполнить
  `UPDATE files SET ds_product_id = $1, ds_product_branch_id = $2
   WHERE path = $3 OR path LIKE $3 || '/%'`;
- по образцу backfill'а `is_generated` (`internal/store/db_schema.go:59-64`).

Без этого часть файлов останется в псевдопродуктах вида `fa-contracts-ext`.
Продукт из пути восстановить нельзя (имя каталога теперь содержит ветку), поэтому
backfill выполняется при сканировании source, а не SQL-миграцией.

---

## Решение 3. Индексация веток

### Что держим

- `develop` для всех продуктов.
- 2–3 последние релизные линии по каждому продукту.
- Дополнительно (рекомендуется): линии, которые уже встречались в разобранных
  `dsinfo` — это точнее, чем «последние по semver», потому что в эксплуатации
  одновременно живут 9.x, 10.x и 11.x.

Полный набор для 25 продуктов × 3 линии — это десятки гигабайт рабочих деревьев.
Меры:

- `git worktree` вместо полных клонов (объекты общие);
- `sparse-checkout` только нужных расширений (`*.sql`, `*.pas`, `*.h`, `*.inc`,
  `*.js`, `*.smf`, `*.dfm`, `*.tpr`, `*.rpt`, `*.xml`, `*.md`, `*.yaml`);
- каталоги веток — вне скрытых путей (`.refs` будет пропущен `fswalk`,
  `internal/fswalk/fswalk.go:189-190`).

### Discovery sources в `init`/`update`

Новый шаг перед обходом файлов:

1. Пройти непосредственных детей `root_path`.
2. Для каждого каталога с `.git` (директория или файл worktree) выполнить
   `git remote get-url origin`, `git rev-parse --abbrev-ref HEAD`, `git rev-parse HEAD`.
3. Upsert `ds_products` (по basename remote) и `ds_products_branch`
   (по `product + branch_key`).
4. Построить карту «префикс пути → source»; при сохранении файла брать самый
   длинный подходящий префикс.
5. Backfill `files` по префиксу source (см. выше).

Каталог, который не является git-репозиторием, обрабатывается по старой логике.

### Инкрементальность

- `GetLatestFilesByRootPath` уже матчит по префиксу (`db_files.go:22`), поэтому
  удаление исчезнувших файлов работает в пределах `root_path`.
- Удаление каталога ветки → каскадное удаление файлов и сущностей при следующем
  `update`.
- Если ветка в каталоге переключена, `commit_sha`/`branch_key` обновляются;
  файлы с изменившимся содержимым переиндексируются. Старая ветка при этом
  теряется — это ожидаемо, для параллельного хранения нужны разные каталоги.

### Нормализация имён веток

```
normalize("FA-Contracts-11-01-7-SP")     → "fa-contracts-11-01-7"
normalize("sup/11-01-7")                 → "sup/11-01-7"  (не матчится автоматически)
normalize("FA-Contracts-11-01-7-HF008")  → "fa-contracts-11-01-7"  (HF снят)
```

Правило: lowercase; снять суффикс `-SP`; снять `-HF<NNN>` (для сравнения с
манифестом); `.`/`_` → `-`. Рекомендуется чекаутить именно `-SP`-ветку, чтобы
`branch_key` совпадал с манифестом без эвристик.

---

## Решение 4. `dsinfo parse`

### Формат CSV

- Кодировка WIN-1251; файл ~14 МБ — читать потоком (не `ReadAll`).
- Разделитель `;`, хвостовой `;`, пустые поля.
- Секция «Продукты Digital Q» — строки `NAME` или `NAME\NAME-HFxxx` до строки
  `Модули`.
- Секция «Модули» — строки BPL/EXE с колонками `ProductVersion`,
  `ProductRelease`, `ProductIssue`, `PacketName`, `HotfixName`, `Comments`.

### Разбор продукта и версии

Правило: **продукт = токены до первого чисто числового**; версия = первые три
числовых токена.

| Строка манифеста | product | branch_key |
|---|---|---|
| `FA-Contracts-11-01-7` | `fa-contracts` | `fa-contracts-11-01-7` |
| `FA-Contracts-11-01-7\FA-Contracts-11-01-7-HF008` | `fa-contracts` | `fa-contracts-11-01-7` |
| `FA-GAAP-11-01-4` | `fa-gaap` | `fa-gaap-11-01-4` |
| `FA-Contracts-Ext-9-01-553-PKG-IFRS9_TOYOTA\...` | `fa-contracts-ext` | `fa-contracts-ext-9-01-553` |
| `FA-Contracts-Ext-7-02-2-7-02-2` | `fa-contracts-ext` | `fa-contracts-ext-7-02-2` |
| `FA-Administrator-Ext-8-01-6-PKG-toyota-repltask-term` | `fa-administrator-ext` | `fa-administrator-ext-8-01-6` |

`HF*` сохраняется в `hotfix_name` для аудита, но на выбор ветки не влияет
(граниularity — релизная линия).

### Секция «Модули»

Разбирать как дополнительный источник (рекомендуется):

- `BPL/EXE → PacketName/HotfixName` даёт точную привязку модуля к продукту;
- используется для RTI: `ModuleID → ModuleName` (частичная таблица
  `internal/rti/symbols.go:455`) + `ModuleName → продукт` из CSV уточняют
  продукт вызова, когда имя процедуры встречается в нескольких продуктах.

Если секция не разбирается в первой версии — не блокирует основной сценарий.

### Сопоставление с индексом

Для каждой строки манифеста:

1. Найти `ds_products` по `product_name`.
2. Найти `ds_products_branch` по `(ds_products_id, branch_key)`.
3. Выставить `status`: `ok`, `product_not_indexed`, `branch_not_indexed`.

`dsinfo parse` **не запускает индексацию** и не создаёт checkout'ы; он только
сохраняет профиль и сообщает, чего не хватает.

### Команды

```
codebase dsinfo parse <path\DiasoftSystemInfo.csv>   → печатает dsinfo_id + таблицу
codebase dsinfo delete <dsinfo_id>                   → удаляет профиль (каскадно строки)
codebase dsinfo list                                 → список профилей
codebase dsinfo show <dsinfo_id>                     → таблица сопоставления и статусы
```

Пример вывода `parse`:

```
dsinfo_id: 7
product              manifest_ref             matched_branch              status
fa-contracts         FA-Contracts-11-01-7     FA-Contracts-11-01-7-SP     ok
fa-gaap              FA-GAAP-11-01-4          FA-GAAP-11-01-4-SP          ok
fa-agreement         FA-Agreement-9-01-1      —                           branch_not_indexed
fa-adpsuv            FA-ADPSUV                —                           product_not_indexed

matched 14/63 products; 4 branches missing (use `codebase update` after checkout)
```

Опечатка/путь: `dsinfo delete` принимает `dsinfo_id`; `config_id` из обсуждения —
то же самое, но в CLI лучше единое имя `dsinfo_id`.

---

## Решение 5. Резолв в запросах

### Флаг

`--dsinfo <id>` — **опциональный**. Отсутствует → `indexer.default_branch`
(по умолчанию `develop`), то есть текущее поведение.

Область регистрации: `PersistentFlags` на группах `query`, `rti`, `trc` — чтобы
не дублировать объявление в листовых командах. «Persistent» здесь означает
наследование флага, а не обязательность.

Не принимают флаг: `init`, `update`, `dsinfo ...`, `health`, `stats`.

Дополнительно (рекомендуется): переменная окружения `CODEBASE_DSINFO_ID` и/или
`dsinfo use <id>` для агентов, чтобы не передавать ID в каждый вызов.

### Цепочка разрешения

Для каждого lookup'а:

```
1. (product, branch) из профиля dsinfo           → точная линия
2. is_default ветка продукта (indexer.default_branch = develop)
3. любая проиндексированная ветка продукта (свежая)
4. глобальный latest (legacy-поведение)
```

Для lookup'ов без известного продукта (например, `GetProcedureResult(name)` из
RTI/TRC) профиль задаёт **приоритет**, а не жёсткий фильтр: сначала кандидаты из
продуктов профиля, затем остальные. Жёсткий фильтр — отдельный
`--strict-dsinfo` (см. открытые вопросы).

### SQL-приоритет

Аналог существующего `is_generated`, добавляется `branch_match`:

```sql
ORDER BY
  not_generated DESC,
  branch_match DESC,       -- f.ds_product_branch_id = <branch из профиля>
  product_match DESC,      -- f.ds_product_id = <product>
  default_branch DESC,     -- ds_products_branch.is_default
  id DESC                  -- детерминизм
```

Для `GetProcedureDetails`/`GetProcedureResult` (`query_sql.go:480-489`,
`513-523`) добавляется тот же `ORDER BY ... LIMIT 1` — сейчас порядок не
определён.

### Структура scope вместо роста параметров

Сейчас `FindLatest*` принимают `productID int64`. Предлагается заменить на:

```go
type LookupScope struct {
    ProductID int64 // 0 = любой
    BranchID  int64 // 0 = default
    DsinfoID  int64 // 0 = без профиля
}
```

Это затрагивает ~10 функций (`FindLatestSQLProcedureIDsByNames`,
`FindLatestSQLTableIDsByNames`, `FindLatestSQLColumnDefinitionType`,
`FindLatestPASClassIDsByNames`, `FindLatestDFMFormIDsByClassNames`,
`FindLatestAPIContractIDsByNamesAndKinds`, `FindLatestEventContractIDsByNames`
и т. п.) и все места вызова. Альтернатива — добавить `branchID` рядом с
`productID`; выигрыш меньше, риск перепутать аргументы выше.

### Вывод

В результаты добавляются:

- `product` и `branch` (фактически использованная);
- признак `branch_fallback` и `branch_requested`, если они различаются;
- предупреждение в envelope (`warnings`), если запрошенная ветка не найдена.

Это изменение machine-readable контракта (`format_version`) — обратно
совместимое добавление полей.

---

## Решение 6. Граф связей

Без этого пункта индекс трёх веток даст неверный граф. Пост-обработка сейчас
резолвит имена глобально (`productID=0`):

```
indexer_postprocess_sql_calls.go:19
indexer_postprocess_fragments.go:49,55,149,221,313
```

Правки:

1. Передавать в резолв `branch_id` файла-источника (и `product_id`), а не `0`.
2. В `ORDER BY` всех `FindLatest*` добавить `branch_match DESC` перед
   `product_match`.
3. Для фрагментов/PAS/DFM/API — та же логика: сначала та же ветка, затем
   ветка по умолчанию, затем глобально.
4. Рёбра, у которых целевая сущность не найдена в ветке, остаются
   неразрешёнными (`pending`/NULL) — это лучше, чем ребро в чужую ветку.

Пересчёт при смене профиля не требуется: рёбра привязаны к веткам на этапе
индексации, а профиль влияет только на выбор при запросе.

---

## Решение 7. RTI/TRC

### RTI

- `RTICall.ModuleID` → `ModuleNameByID` (`parser.go:238`) уже есть.
- CSV «Модули» даёт `ModuleName/BPL → product + PacketName/HotfixName`; это
  позволяет уточнить продукт вызова и, при желании, показать точную сборку.
- `RTIClientEvent.BPL[]` (лист загруженных BPL с версиями) — дополнительный
  источник для клиентских событий.
- Обогащение (`internal/rti/enrich.go:50`) получает `dsinfoID` и резолвит
  процедуру по цепочке; в `ProcedureEnrichment` добавляется `Branch`.

### TRC

- Модуля в трейсе нет; продукт определяется по индексу (кто объявляет
  процедуру).
- Профиль задаёт приоритет: сначала продукты/ветки из `dsinfo`.
- Дубли общих DDL-утилит побайтово идентичны, поэтому выбор ветки для них
  не критичен; для продуктовых процедур имя обычно уникально.

### Вывод

В `rti details`/`errors`/`tree` и `trc`-обогащении рядом с `source_file`
показывать `branch` и `product`. `source_file` уже указывает на конкретный
checkout — это готовый «прыжок в код» нужной ветки.

---

## Решение 8. LSA

Корпус утроится, `lsa_min_df` подобран под текущий размер. Предлагается:

- обучать LSA только на `is_default`-ветках (или на `develop`);
- либо дедуплицировать документы по `body_hash` перед обучением.

Иначе desc-search начнёт выдавать пары develop/release одного и того же
документа, а пороги придётся перетюнивать.

---

## Принятый компромисс по хотфиксам

Гранулярность — релизная линия `-SP`. Следствия:

- head линии может быть новее сборки банка (HF008 от 2026-09-02);
- возможны процедуры, которых в банке нет, и сдвиг номеров строк;
- `hotfix_name` из CSV сохраняется, чтобы это было видно.

Митигация (не входит в объём): показывать `commit_sha`/дату head'а рядом с
датой пакета; при необходимости позже чекаутить точный тег
(`PacketName`/`HotfixName`) — они существуют в репозиториях.

---

## Рассмотренные альтернативы

| Вариант | Почему не выбран |
|---|---|
| Одна ветка на сессию | Невозможно: продукты одновременно на разных линиях |
| Ветка в имени продукта (`fa-contracts-11-01-7` как продукт) | Смешивает две оси; `product_match` перестаёт работать как продукт; diff требует парсинга имён; обрезка `tokens[:3]` даёт `fa-contracts-11` |
| Иерархия `11-01-7\fa-contracts` | Ломает экстрактор; не даёт преимуществ при определении из git |
| Отдельная БД на ветку | Не работает для инцидента: нужны разные ветки разных продуктов одновременно |
| Точные теги вместо линий | Дороже по checkout'ам; отложено |

---

## Затронутые файлы

| Файл | Действие |
|---|---|
| `internal/store/db_schema.go` | `ds_products_branch`, `dsinfo`, `dsinfo_products`, `files.ds_product_branch_id`, индексы |
| `internal/store/db_products.go` | upsert продукта/ветки, `FindBranchByProductKey` |
| `internal/store/ds_info_store.go` | **новый**: CRUD профиля, `FindDsinfoByHash` |
| `internal/store/db_lookup_sql.go` | `LookupScope`, `branch_match` в `ORDER BY` |
| `internal/store/db_lookup_pas.go`, `db_lookup_api.go`, `db_lookup_dfm.go` | то же для остальных lookup'ов |
| `internal/indexer/runner.go` | discovery sources, backfill, передача scope в пост-обработку |
| `internal/indexer/indexer.go` | `saveFileCtx` — `ds_product_branch_id` вместо эвристики; `resolveDSProductID` |
| `internal/indexer/indexer_postprocess_sql_calls.go` | scope источника вместо `0` |
| `internal/indexer/indexer_postprocess_fragments.go` | то же |
| `internal/query/query_sql.go` | `ORDER BY`/`LIMIT 1` в `GetProcedureDetails`/`GetProcedureResult`, поля `product`/`branch` |
| `internal/query/*.go` | приём `dsinfo_id`, проброс scope |
| `internal/rti/enrich.go`, `internal/trc/enrich.go` | резолв с профилем, `Branch` в результате |
| `internal/dsinfo/` | **новый** пакет: парсер CSV (WIN-1251, потоковый), нормализация |
| `cmd/dsinfo.go` | **новый**: `parse`, `delete`, `list`, `show` |
| `cmd/query.go`, `cmd/rti.go`, `cmd/trc.go` | опциональный `--dsinfo` (PersistentFlags групп) |
| `cmd/root.go` | при необходимости — общий флаг/окружение |
| `internal/mcp/*` | параметр `dsinfo_id` у lookup/enrichment-инструментов |
| `internal/config/config.go` | `indexer.default_branch` |
| `codebase.toml` | `default_branch = "develop"` + комментарий |
| `README.md` | разделы про dsinfo, ветки, fallback |
| `internal/dsinfo/*_test.go`, `internal/store/*_integration_test.go`, `internal/query/*_integration_test.go` | тесты |

---

## План (10 шагов)

### Шаг 1. Схема и миграция

- Добавить таблицы/колонки из «Решение 2» в `db_schema.go` (идемпотентно:
  `CREATE TABLE IF NOT EXISTS`, `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`).
- `files.ds_product_branch_id` — nullable; старые строки не ломаются.
- Backfill выполняется в `init`/`update` по префиксу source (шаг 3).

### Шаг 2. Git-детекция источника

- Новый helper (например, `internal/indexer/source_git.go`):
  `DetectSource(dir) (product, branch, commit, err)` — вызовы
  `git remote get-url origin`, `git rev-parse --abbrev-ref HEAD`,
  `git rev-parse HEAD`.
- Юнит-тесты: обычный репозиторий, worktree (`.git` — файл), detached HEAD,
  отсутствие remote.
- Кэш по каталогу на время скана.

### Шаг 3. Discovery в `init`/`update`

- Перед обходом: пройти детей `root_path`, найти `.git`, upsert
  `ds_products`/`ds_products_branch`.
- Построить префиксную карту и передать её в `saveFileCtx`.
- Backfill `files` по префиксу source.
- Каталог без `.git` — legacy-путь.

### Шаг 4. `LookupScope` и приоритет ветки

- Ввести `LookupScope{ProductID, BranchID, DsinfoID}`.
- Заменить `productID int64` в `FindLatest*` на scope (или добавить `branchID`,
  если рефакторинг слишком широк).
- Добавить `branch_match`/`default_branch` в `ORDER BY`.
- Детерминированный `ORDER BY ... LIMIT 1` в `GetProcedureDetails`/
  `GetProcedureResult`.
- Интеграционные тесты: две ветки, одно имя, разные тела — выбирается
  запрошенная; без ветки — default.

### Шаг 5. Граф

- Пробросить scope файла-источника в пост-обработку вызовов/фрагментов/PAS/
  DFM/API.
- Тест: процедура в ветке A вызывает процедуру с тем же именем, что есть в A и
  B → ребро указывает на A.

### Шаг 6. Парсер `dsinfo`

- Новый пакет `internal/dsinfo`: потоковое чтение WIN-1251, секции, правило
  «продукт до первого числа», нормализация `branch_key`, опционально «Модули».
- Юнит-тесты на фрагментах реального CSV (включая PKG-строки и HF).
- Идемпотентность по SHA-256 файла.

### Шаг 7. Команды `dsinfo`

- `cmd/dsinfo.go`: `parse`, `delete`, `list`, `show`.
- `parse` печатает `dsinfo_id` и таблицу статусов; JSON-режим — по общему
  контракту (`success`, `format_version`, `items`, ...).
- `delete` — каскад по FK.

### Шаг 8. Проброс `--dsinfo` в анализ

- `PersistentFlags` на `query`/`rti`/`trc` (опциональный).
- Без флага — `indexer.default_branch`.
- `rti`/`trc` enrichment получает scope; в результат добавляется `Branch`.
- MCP: `dsinfo_id` в параметрах lookup/enrichment-инструментов.
- Опционально: `CODEBASE_DSINFO_ID` / `dsinfo use`.

### Шаг 9. LSA

- Обучать на default-ветках (или дедуп по `body_hash`).
- Проверить, что `lsa_min_df` не требует перетюна.

### Шаг 10. Документация и smoke

- `README.md`: раскладка каталогов, `dsinfo parse`, `--dsinfo`, fallback.
- `codebase.toml`: `default_branch`.
- Smoke на реальном `DiasoftSystemInfo.csv` и одном RTI/TRC.

---

## Порядок выполнения

1. Шаги 1–3 (схема, git-детекция, discovery) — база.
2. Шаги 4–5 (резолв и граф) — корректность.
3. Шаг 6–7 (dsinfo) — профиль.
4. Шаг 8 (проброс в CLI/MCP) — сценарий.
5. Шаги 9–10 (LSA, документация, smoke).

---

## Тесты и верификация

**Unit:**

- `DetectSource`: обычный репозиторий, worktree, detached HEAD, нет remote.
- Нормализация `branch_key`: `-SP`, `HF`, регистр, `.`/`_`.
- Парсер CSV: продукт/версия для `FA-Contracts-11-01-7`,
  `FA-Contracts-Ext-9-01-553-PKG-...`, строк с HF.
- Lookup SQL: `branch_match` приоритетнее `product_match`; детерминизм.

**Integration (PostgreSQL):**

- Два источника одной процедуры (develop и `-SP`) с разными телами:
  - с профилем → тело `-SP`;
  - без профиля → `develop`;
  - запрошенная ветка отсутствует → `develop` + `branch_fallback=true`.
- Граф: ребро из `-SP` не указывает на develop-копию.
- `dsinfo parse` дважды на один файл → один `dsinfo_id`.
- Backfill: старые строки получают `ds_product_branch_id` после `update`.

**Smoke:**

1. Разложить `fa-contracts-develop` и `fa-contracts-11-01-7` (checkout
   `FA-Contracts-11-01-7-SP`).
2. `codebase init`/`update` → проверить `ds_products_branch`.
3. `codebase dsinfo parse Modifications/DiasoftSystemInfo.csv` → таблица, статусы.
4. `codebase rti details --session N --dsinfo <id>` → `source_file` указывает в
   `fa-contracts-11-01-7`, `branch` = `FA-Contracts-11-01-7-SP`.
5. Тот же вызов без `--dsinfo` → develop (как раньше).
6. Контроль: `query procedure --name <proc> --dsinfo <id>` и без флага дают
   разные тела только там, где ветки реально различаются.

---

## Совместимость

- Без `--dsinfo` поведение сохраняется: `default_branch` = `develop`.
- Схема расширяется аддитивно; старые строки `files` продолжают работать.
- `ds_product_id` остаётся; новые поля nullable.
- Формат JSON расширяется полями `product`/`branch`/`branch_fallback`
  (обратно совместимо; при необходимости — `format_version` bump).
- `spec_configs` (уникальность `(ds_product_id, root_dir)`) не меняется: у
  каждого checkout свой `root_dir`.
- Существующие продукты, созданные эвристикой (`fa-contracts-ext`), переживают
  backfill без переименования: basename remote для них тот же.

---

## Риски

| Риск | Митигация |
|---|---|
| Молчаливый fallback на develop искажает анализ | `branch_used` в каждом ответе + предупреждение; опциональный `--strict-dsinfo` |
| Граф смешивает ветки | обязательный `branch_match` в пост-обработке; тест на изоляцию |
| Объём: 25 продуктов × 3 ветки | worktree + sparse-checkout; держать линии по факту появления в dsinfo |
| Git CLI как зависимость | один вызов на source; понятная ошибка при отсутствии git |
| Detached HEAD/worktree | определять через `git -C`, `describe --tags --always` как fallback |
| Хотфиксы не видны | принятый компромисс; `hotfix_name` в профиле; путь к точным тегам |
| LSA-корпус утраивается | обучать на default-ветках / дедуп |
| Backfill пропускает неизменённые файлы | backfill по префиксу source при `update`, а не SQL-миграцией |
| Совпадение имён процедур между продуктами | приоритет продуктов профиля; `branch_used` в выводе |

---

## Открытые вопросы

Решено в обсуждении:

- [x] Гранулярность: релизная линия `-SP`, хотфиксы отдельно не индексируются.
- [x] Предзагрузка: `develop` + 2–3 последние `-SP` по каждому продукту.
- [x] Парсинг CSV — CodeBase (`dsinfo parse` / `dsinfo delete`).
- [x] Источник истины для ref — git-состояние каталога.
- [x] `--dsinfo` — опциональный; без него `default_branch` из `codebase.toml`.
- [x] Раскладка каталогов — плоская (`fa-contracts-develop`,
      `fa-contracts-11-01-7`).

Требуют подтверждения:

1. **Строгость `--dsinfo`**: приоритет с пометкой fallback (предлагается) или
   жёсткая фильтрация? Оба режима?
2. **Создание checkout'ов**: вручную или `codebase sources sync` с
   `git worktree`? От этого зависит, останется ли git CLI только для чтения.
3. **«2–3 последних»**: по semver в пределах major-линии или по факту появления
   в разобранных `dsinfo`?
4. **`dsinfo use` / env**: нужен ли активный профиль, или только явный
   `--dsinfo` на каждом вызове?
5. **Секция «Модули»**: парсить сразу для RTI-маппинга или отложить?

---

## Связанные артефакты

- Исходное исследование: `Modifications/indexing-file-by-branch.txt`
- Пример конфигурации: `Modifications/DiasoftSystemInfo.csv` (WIN-1251)
- Пример RTI/TRC инцидента: `Modifications/DIAPR-391.trc`, `Modifications/DIAPR-391.xml`
- Аналогия приоритизации: `is_generated` — `internal/store/db_schema.go:59-64`,
  `db_lookup_sql.go:36-47`
- Аналогия идемпотентного парсинга: `Modifications/Realized/parse-session-reuse-by-file-hash-d7a1c3.md`
- Спеки: `openspec/specs/indexing/*`, `openspec/specs/query/*`,
  `openspec/specs/rti-analysis/*`, `openspec/specs/trc-analysis/*`
