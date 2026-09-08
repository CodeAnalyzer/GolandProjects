## Context

CodeBase индексирует исходники Diasoft 5NT (SQL/PAS/JS/H/DFM/SMF/TPR/RPT/XML/T01) в файл-центричную модель: `files` → типизированные таблицы сущностей → unified `symbols` + полиморфная `relations`, с двухфазной пост-обработкой связей (`runPostProcessingParallel`). В дереве FA параллельно существует ~3100 openspec-артефактов (10 продуктов, ~2200 capability, ~700 файлов changes, три формата usecase-слоя), которые ссылаются на код по именам и путям, но индексом не покрываются. Мотивация — proposal.md, Why.

Ограничения: legacy-кодировки (CP866/CP1251) для кода против UTF-8 спек; русская лексика требований; вложенные таксономии capability (`FORMS/0409120/usage`); существующий валидатор спек распознаёт только английские SHALL/MUST (на распознавание структуры парсером не влияет).

## Goals / Non-Goals

**Goals:**
- Спеки как полноценный источник сущностей общего индекса: те же файл-центричные таблицы, тот же `symbols`, та же `relations`.
- Двусторонний мост код ↔ требования: `references_code` с обратным запросом.
- Двухслойный поиск с раздельными секциями: точный (tsvector/trgm) и семантический (TF-IDF+LSA, capability-уровень).
- Инкрементальность спек по общим механизмам (pre-filter, CASCADE, DeleteFilesByPathsExcept).

**Non-Goals:**
- Валидация спек финпродуктов (SHLL/MUST-контроль) — этим занимается openspec CLI самих продуктов.
- Embeddings внешних моделей (fastText и т.п.) — baseline TF-IDF+LSA; прирост решается расширением (синонимы, псевдо-relevance feedback).
- LSA на requirement-уровне (Q9: только capability-уровень; tsvector покрывает требования).
- Межпродуктовое сравнение capability (spec_compare) — отложено (Q8).
- Индексация справочных материалов вне openspec-корней (`reference/` и т.п.) — только аннотации источников в текстах спек.

## Decisions

### D1. Схема: типизированные таблицы спек, связи только через существующую relations

8 таблиц сущностей (`spec_configs`, `spec_capabilities`, `spec_requirements`, `spec_scenarios`, `spec_usecases`, `spec_usecase_steps`, `spec_changes`, `spec_change_delta`), staging `spec_code_mentions`, 2 таблицы полнотекстового слоя (`spec_vocab`, `spec_embeddings`). Все с `file_id` FK CASCADE по общей модели; `spec_configs`/`spec_capabilities` с `ds_product_id`. FK-выразимая иерархия (parent_id, capability_id, requirement_id) в relations НЕ дублируется — туда входят только `depends_on_capability`, `usecase_involves`, `references_code`, `change_modifies`. *Альтернатива — собственная таблица рёбер спек: отвергнута (принцип «не изобретать новый граф», раздувание графа, потеря единого `codebase_query_relations`).*

### D2. Slug capability = полный путь относительно `specs/`

`FORMS/0409120/usage`, а не `usage`. *Альтернатива — имя директории: отвергнута, коллизии во вложенных таксономиях (fa-reports: 1784 capability, `settings` встречается под сотнями форм; правило уже продиктовано config.yaml fa-reports).* Регистр сегментов сохраняется как есть (UPPERCASE-группы fa-reports); lookup — по LOWER-индексу, точное совпадение по исходной форме. Контейнерные директории без `spec.md` — capability-контейнер с `parent_id`, `file_id` от ближайшего вышестоящего артефакта.

### D3. Один парсер на всё openspec-семейство, классификация файлов позиционная

`internal/parser/openspecmd` обрабатывает всё семейство по позиции файла в openspec-корне (`specs/**/spec.md`, `scenarios|usecases|business-processes`, `changes/**`, `config.yaml`) — по прецеденту `h-report-parsing` (H+TPR+RPT одной capability). Структурные якоря — только `### Requirement:` / `#### Scenario:` и секции `##`; H1 и `## Requirements` опциональны (fa-factoring живёт без обоих). Ключевые слова сценариев — bold и plain варианты; AND присоединяется к последнему распознанному блоку G/W/T.

### D4. Упоминания кода — персистентный staging + глобальный резолв

Фаза 1 (per-file): извлечение в `spec_code_mentions` (источник capability|requirement|scenario|usecase, строка, эвристический kind). Фаза 2 (пост-процессор, параллельно с существующими): резолв имён по сущностям индекса → `references_code` в relations. Нерезолвнутые остаются в staging как «пробелы покрытия» — доступ запросу gaps. *Альтернатива — in-memory pending-структуры по образцу `pendingFragmentRefs`: отвергнута, пробелы должны быть видимыми и запрашиваемыми.* Порядок индексации файлов не важен: резолв глобальный, после полного прогона.

### D5. depends_on_capability: 5 маркеров, 3 уровня confidence

markdown-ссылки (`../specs/X/spec.md`, `../X/spec.md`, `cci:9://…` — резолв по хвосту пути) → explicit; текстовые маркеры Notes («Связан с доменами:», «Связи с другими spec:») → notes; inline-backtick («см. спецификацию ``slug``», «(см. ``slug``)», «Смежные capability домена») → inline. Sibling-резолв inline-slug по общему префиксу пути (fa-reports аспекты одной формы). Один способ — одно ребро; разные маркеры одной пары — несколько рёбер с разными confidence.

### D6. Поиск: гибрид tsvector+trgm, LSA на capability-уровне, раздельные секции

Лексический слой: `to_tsvector('russian', …)` на 4 таблицах (title/name — A-вес, тела — B) + GIN; техимена — через pg_trgm similarity по текстам спек. Семантический слой: токенизация 3 категорий (русские — стемминг; `API_*`/`FCD_*`/`t*` — единый токен; `758П` — как есть), словарь с minDF/maxDF-фильтрацией, TF-IDF (log TF, smooth IDF, L2), SVD через gonum, k=128, `embed_level='spec'` (N≈2200 документов — dense-разложение приемлемо после фильтрации словаря; правило k < min(N, V)/2). Результат — две секции (exact / semantic) без слияния ранжирования. *Альтернативы: RRF-слияние — отвергнуто (объяснимость секций важнее); LSA на requirement-уровне — отвергнут на старте (40K×30K dense ≈ 9.6 ГБ; Q9).* Пересчёт модели — по порогу изменённых capability, матрица Vᵀ — в `spec_lsa_model.bin` рядом с БД.

### D7. Спеки в walker и кодировках

`.md`/`.yaml` входят в supported extensions и дефолтные include-паттерны. Для `.md` — авто-детект кодировки (UTF-8 приоритет, CP1251 fallback) через существующий `encoding`-слой; openspec-корень детектируется по директории `openspec|OpenSpec` + `config.yaml` со `schema: spec-driven` (case-insensitive). Продукт — существующий `extractCanonicalDSProductFromPath`. Markdown вне openspec-корней: сохраняется только файловая запись (владелец может исключить паттерном).

### D8. Сервисный слой и экспозиция инструментов

`internal/specsvc` по образцу `querysvc`/`systemsvc` — общий execution-слой для CLI (`codebase query spec search|by-code|deps|usecase|coverage|history`) и MCP (`codebase_query_spec_*`), с ctx-пропагацией и sentinel-ошибками `errs`. Пагинация MCP — существующий механизм без изменений.

## Risks / Trade-offs

- [Объём первого прогона: 1784 capability fa-reports, ~40К документов] → COPY IN батчи (batch_insert_size), пагинация ответов, метрики спек в `codebase stats`; спеки — мелочь рядом с sql_procedures.
- [Рост корпуса → dense SVD неприемлем] → правило k < min(N,V)/2, фильтрация словаря (minDF=3/maxDF=0.3); при выходе за пределы — переход на sparse randomized SVD (james-bowman/sparse) без смены интерфейса LSA.
- [Качество простого суффиксного стеммера] → технический текст хорошо отделяется от русской лексики (3 категории); при низком recall — словарь синонимов и/или псевдо-relevance feedback поверх (не меняет схему).
- [Нерезолвнутые mentions как шум] → это фича (gaps), но извлечение фильтруется: стоп-паттерны для обиходных слов («Система», «Confluence», pageId-числа без контекста); kind unknown размечается явно.
- [Извлечение capability из proposal при skip_specs — эвристика] → связь помечается источником (proposal-derived), confidence inline; неточная связь не создаёт ложных delta.
- [Case-sensitivity slug при UPPERCASE-группах] → хранение как есть + LOWER-индексы; документы фиксируют, что точное имя = исходная форма.
- [Русские имена файлов БП и директорий] → UTF-8 пути штатно; ILIKE/LOWER в SQL-lookups.
- [Формат спек продуктов эволюционирует] → парсер не опирается на H1/заголовки секций Requirements; профили продуктов пересчитываются при каждом прогоне.

## Migration Plan

1. Deploy: DDL идемпотентен (`CREATE TABLE/INDEX IF NOT EXISTS`) — первый запуск `codebase init`/`update` на существующей БД создаёт семейство `spec_*` без изменения существующих таблиц. `relations` не меняется (новые значения enum-подобных колонок TEXT).
2. `go get gonum.org/v1/gonum` — единственная новая зависимость.
3. Rollback: `DROP TABLE` семейства `spec_*` + `DELETE FROM relations WHERE relation_type IN (…)`, удаление `spec_lsa_model.bin`. Существующие сущности и связи кода не затрагиваются.

## Open Questions

- Значение порога пересчёта LSA по умолчанию (кандидат: 50 изменённых capability) и минимальный размер корпуса для первого обучения (кандидат: ≥ 100 capability).
- Расположение `spec_lsa_model.bin` (рядом с executable / в каталоге данных / рядом с конфигом) — решить при реализации.
- Формат CLI-подкоманды: `query spec search` (суб-уровень) vs `query spec-search` (плоско) — согласовать с существующим стилем `cmd/query.go` при реализации.
