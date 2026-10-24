# Design: branch-aware-indexing

## Context

Исходный дизайн — `Modifications/dsinfo-product-branches-6f2a1c.md` (решения
1–3, шаги 1–3 плана). Предшественник `dsinfo-parser` (архив 2026-10-24)
построил парсер CSV и нормализацию `product_name`/`branch_key`/`hotfix_name`
в `internal/dsinfo/normalize.go` — проверена на всех 62 строках реального
манифеста. В рабочем дереве FA уже развёрнуты `fa-contracts-11016`/`fa-contracts-11017`
(worktree на SP-ветках); в индексе БД их файлов нет, но любой `codebase
update` до этого изменения создаст псевдопродукты и смешает тела веток —
изменение срочное.

Ключевые точки текущего кода:

| Звено | Файл | Что там |
|---|---|---|
| Эвристика продукта | `internal/indexer/indexer.go:1788-1817` | `tokens[:3]` от `fa-`-сегмента пути |
| Backfill-паттерн | `internal/store/db_schema.go:59-64` | `is_generated` — UPDATE по условию при скане |
| Удаление по префиксу | `internal/store/db_files.go:14-24` | `GetLatestFilesByRootPath` матчит префикс `root_path` |
| Обход файлов | `internal/fswalk/fswalk.go:189-190` | dot-каталоги пропускаются; `.git`-файл worktree не матчится |
| Сброс | `internal/store/db_reset.go:50` | TRUNCATE-список |

## Goals / Non-Goals

**Goals:**

- Хранить в одной БД develop + произвольное число релизных линий каждого
  продукта из отдельных checkout'ов/worktree'ов.
- Продукт и ветка — из git (remote basename, HEAD), имя каталога — только
  для человека.
- Backfill существующих строк `files` без переиндексации содержимого.
- Единая функция `branch_key` для git-веток и dsinfo-манифеста.
- LSA-корпуса (spec и desc) не растут от параллельных веток.

**Non-Goals:**

- Приоритет выбора ветки в lookup'ах и графах — срез `branch-aware-lookup`.
- Профиль развёртывания `--dsinfo` и матчинг манифеста на ветки — срез
  `dsinfo-integration`.
- Теги/хотфиксы как отдельная гранулярность; cross-branch diff;
  автоматическое создание checkout'ов (`sources sync`).
- Иерархическая раскладка (`11-01-7/fa-contracts`) — discovery идёт только
  по непосредственным детям `root_path`.

## Decisions

### D1. Детекция через git CLI, один раз на каталог

`DetectSource(dir) (product, branch, commit, err)` в новом
`internal/indexer/source_git.go`: `git -C dir remote get-url origin`,
`git rev-parse --abbrev-ref HEAD`, `git rev-parse HEAD`. Обращение через
`git -C` обязательно: в worktree `.git` — файл, чтение `.git/HEAD` напрямую
недостаточно. Detached HEAD: `--abbrev-ref HEAD` вернёт `HEAD` — тогда
`git describe --tags --always` (детерминированно). Кэш результатов по
каталогу на время скана. Git отсутствует/недоступен → источник пропускается
с warning, каталог идёт по legacy-эвристике (решение: не валить update —
деревья без git легитимны). Побочный эффект: basename remote решает старую
проблему схлопывания `fa-contracts-ext-*` без обрезки `tokens[:3]`.

### D2. Схема и миграция

`ds_products_branch` — как в исходном доке (Решение 2): `id`, `ds_product_id`
FK CASCADE, `branch_name`, `branch_key`, `path`, `commit_sha`, `is_default`,
`indexed_at`, `created_at`, `updated_at`; unique `(ds_product_id, branch_key)`,
unique `path`, index `ds_product_id`. `files.ds_product_branch_id BIGINT
NULL REFERENCES ds_products_branch(id) ON DELETE SET NULL` + индекс. Всё —
идемпотентные `CREATE TABLE/INDEX IF NOT EXISTS`, `ALTER TABLE ... ADD
COLUMN IF NOT EXISTS` в `db_schema.go`; `ds_products_branch` добавляется в
TRUNCATE-список `db_reset.go`. Миграция данных — не SQL, а backfill при
скане (см. D4): `update` пропускает неизменённые файлы по mtime+size, а
продукт из нового имени каталога восстановить нельзя.

### D3. Нормализация — общий пакет `internal/branchkey`

Правила переносятся из `internal/dsinfo/normalize.go` в новый пакет без
зависимостей `internal/branchkey` (экспорт `NormalizeBranchKey(name string)
string` и вспомогательных функций разбора манифест-строки). `internal/dsinfo`
переводится на него; корпус существующих юнит-тестов dsinfo (все 62 строки
манифеста, PKG/SP6/HF-случаи) защищает рефакторинг — поведение парсера не
меняется, `dsinfo.ParserVersion` не инкрементируется. Git-ветка
`FA-Contracts-11-01-7-SP` → снять `-SP`; манифест `...\...-HF008` → снять
`-HF<цифры>`; результат идентичен: `fa-contracts-11-01-7`. Альтернатива
«индексер импортирует internal/dsinfo» отвергнута: домен CSV-парсера не
должен быть зависимостью индексатора.

### D4. Discovery и backfill в `init`/`update`

Новый шаг в `runner.go` до обхода файлов: непосредственные дети `root_path`;
для каждого с `.git` — `DetectSource`, upsert `ds_products` по basename
remote (`GetOrCreateDSProductIDByName`), upsert `ds_products_branch` по
`(product, branch_key)` с обновлением `branch_name`/`commit_sha`/`path`/
`indexed_at`; пометка `is_default` по `default_branch`. Префиксная карта
`[]struct{prefix, productID, branchID}` передаётся в `saveFileCtx` — файл
берёт самый длинный подходящий префикс. Backfill (по образцу `is_generated`):
`UPDATE files SET ds_product_id=$1, ds_product_branch_id=$2 WHERE path =
$3 OR path LIKE $3 || '/%'` — выполняется при обнаружении источника, чем
чинит и старые псевдопродукты при повторном скане. Инкрементальность:
`GetLatestFilesByRootPath` уже матчит по префиксу — исчезновение каталога
ветки удаляет его файлы каскадно при следующем `update`; строка
`ds_products_branch` без файлов удаляется тем же сканом.

### D5. `is_default` и конфигурация

`indexer.default_branch` (default `develop`) в `internal/config` и
`codebase.toml` с комментарием. `is_default` пересчитывается при каждом
discovery (снятие флага со старой default-ветки при смене параметра). У
продукта без `develop` default-ветки нет — допустимо (не ошибка).

### D6. LSA-корпуса

Выборка корпуса (spec: `specfts`; desc: пост-обработка описаний) получает
фильтр «предпочтительна default-ветка»: в SQL выбора документов — приоритет
`is_default DESC, id DESC` по группе «документ», побайтовые дубли
схлопываются по хешу. Реализация — единый helper выбора «актуальной копии
документа» в `internal/store`, переиспользуемый обоими корпусами. Fingerprint
корпуса вычисляется от уже схлопнутого набора — вторая ветка с теми же
телами не переобучает модель (существующие машины переобучения по
fingerprint остаются без изменений). Пороги `lsa_min_df`/`lsa_max_df` не
перетюниваются (корпус не растёт).

### D7. Один каталог — одна ветка

Переключение ветки в каталоге: discovery обновляет `branch_key`/`commit_sha`
строки (или переносит на другую строку при том же `path` — unique по `path`
заставляет UPDATE существующей строки), файлы с изменившимся содержимым
переиндексируются, старая линия теряет файлы. Параллельное хранение линий —
только разными каталогами/worktree'ами; это документируется.

## Risks / Trade-offs

- [Git CLI — новая зависимость индексатора] → один набор вызовов на каталог
  с кэшем; недоступность git — warning + legacy-путь, не авария.
- [Backfill по LIKE-префиксу на 140+ тыс. строк] → выполняется один раз на
  источник при обнаружении, индекс `idx_files_ds_product_branch_id` +
  существующие индексы по path; по образцу backfill `is_generated`.
- [Detached HEAD/странные имена веток] → `describe --tags --always`/SHA;
  ключи, не совпавшие с манифестом, просто не матчятся (снимок в dsinfo
  сохранён, автосопоставления не требуется).
- [Появление второй ветки меняет объём корпуса/графа до среза B] → до
  `branch-aware-lookup` запросы при нескольких ветках одного продукта не
  детерминированы; поэтому срезы выполняются подряд, а update с веточными
  каталогами до среза B не выполняется (см. tasks).
- [`is_default` при отсутствии develop] → default-ветки нет; поведение
  lookup без default — фиксируется в срезе B (fallback на свежую ветку).

## Migration Plan

Схема применяется `InitSchema` идемпотентно при первом запуске после
обновления бинарника. Backfill и discovery — при следующем `init`/`update`.
Откат не требуется: без веточных каталогов поведение идентично текущему,
колонка nullable. Порядок развёртывания: срез A → `update` (discovery
работает, псевдопродуктов нет) → срез B → срез C.
