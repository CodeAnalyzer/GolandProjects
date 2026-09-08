## Why

CodeBase индексирует исходники Diasoft 5NT, но OpenSpec-спецификации финпродуктов — второй по ценности источник знаний о коде — остаются вне индекса. В дереве FA десять продуктов с ~2200 capability (~3100 markdown-файлов, ~700 файлов changes): бизнес-требования, сценарии, use-case-слои и архив изменений, которые уже сегодня ссылаются на процедуры, таблицы, API-контракты и формы по имени. Без индексации агент не может ни найти требование по бизнес-термину, ни ответить «какая спека описывает эту процедуру», ни показать историю изменения capability по changes.

## What Changes

- Обход ФС индексирует `.md` и `.yaml`; в дереве детектируются openspec-корни (`openspec/` с `config.yaml`, schema: spec-driven, case-insensitive) и продукт-владелец по сегменту `fa-*`.
- Авто-детект кодировки markdown-файлов (UTF-8 с fallback CP1251) вместо жёсткой карты расширение→кодировка.
- Новая семья таблиц `spec_*`: конфиг продукта-профиль, capability, requirement, scenario, usecase, usecase_step, change, change_delta + staging `spec_code_mentions`. Сущности дублируются в `symbols` (dual-write), связи — только через существующую полиморфную `relations` (4 новых relation_type), без собственной таблицы рёбер.
- Двухфазный резолв `references_code`: упоминания имён/путей в спеках → staging → пост-обработка в relations; нерезолвнутые остаются как измеримые пробелы покрытия.
- Зависимости capability (`depends_on_capability`) извлекаются из пяти фактических маркеров cross-refs с confidence explicit|notes|inline.
- Полнотекстовый поиск по спекам в два слоя: лексический (tsvector 'russian' + pg_trgm, все уровни) и семантический (TF-IDF + LSA на gonum, capability-уровень, k=128; модель в файле рядом с БД, пересчёт по порогу изменений).
- Шесть новых MCP-инструментов (`spec_search`, `spec_by_code`, `spec_deps`, `spec_usecase`, `spec_coverage`, `spec_history`) и зеркальные CLI-подкоманды `codebase query spec …`.

## Capabilities

### New Capabilities
- `indexing/openspec-parsing`: детект openspec-корня и продукта; парсинг `spec.md` (capability/requirement/scenario), usecase-слоя (три формата: scenarios/, usecases/, business-processes/), changes (активные+архив, delta-формы ADDED/MODIFIED/REMOVED, skip_specs→извлечение из proposal), config.yaml→профиль продукта; извлечение упоминаний кода; построение depends_on/child_of/usecase_involves/change_modifies; dual-write в symbols.
- `query/spec-search`: двухслойный поиск по спекам — точный (tsvector+trgm по capability/requirement/scenario/usecase) и семантический (TF-IDF+LSA), раздельные секции результата; управление LSA-моделью (обучение при пост-обработке, пересчёт по порогу).
- `query/spec-queries`: запросы-навигация — по коду (обратные references_code), граф зависимостей capability, usecase-слой с involved capabilities, покрытие кода спеками, история изменения capability по changes.

### Modified Capabilities
- `indexing/file-walking`: supported extensions пополняются `.md`/`.yaml`; для markdown — авто-детект кодировки вместо фиксированной карты; include/exclude-паттерны применяются к новому типу файлов наравне с существующими.
- `infrastructure/database-schema`: 8 таблиц `spec_*` + staging `spec_code_mentions`, tsvector-колонки с GIN, btree-индексы; batch-insert и lookup-методы для новых таблиц; 4 новых relation_type в существующей `relations`.
- `infrastructure/encoding-cli`: авто-детект кодировки для `.md` (UTF-8 без BOM → CP1251 fallback), интеграция с encoding-инфраструктурой CodeBase.
- `mcp-server/mcp-transport-tools`: registry пополняется группой spec-инструментов наравне с query/RTI/TRC/review; пагинация применяется к новым инструментам без изменений механизма.

## Impact

- `internal/fswalk` — supported extensions, encoding/language для `.md`/`.yaml`.
- `internal/encoding` — детектор кодировки для markdown.
- `internal/parser/openspecmd` (новый пакет) — markdown-парсеры spec.md / usecase / changes / config.yaml, извлечение упоминаний.
- `internal/indexer` — диспетчер `processFileInTx` (новые language-кейсы MD/YAML), глобальная пост-обработка спек (резолв mentions, depends_on, иерархия, LSA-пересчёт), shared-pending по образцу существующих.
- `internal/store` — DDL, batch-insert, lookup, FTS-индексы.
- `internal/mcp` — registry: 6 инструментов; `internal/query` — реализации запросов; `cmd` — CLI-подкоманды.
- Новая зависимость: `gonum.org/v1/gonum` (SVD/матрицы для LSA).
- Объём первого прогона по FA: ~2200 capability, ~30–60K requirements, ~40K документов — сопоставимо с текущими `sql_procedures`/`relations`, отдельный шардинг не требуется.
- Регрессия отсутствует: новые таблицы пусты до включения, существующие пути кода не меняют поведения для прочих типов файлов.
