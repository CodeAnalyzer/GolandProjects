## 1. Схема БД и модели сущностей спек

- [x] 1.1 Добавить DDL семейства `spec_*` в `internal/store/db_schema.go` (8 таблиц сущностей + staging `spec_code_mentions` + `spec_vocab` + `spec_embeddings`, FK CASCADE, parent_id SET NULL) и проверить: `codebase init` на пустой БД создаёт все таблицы, повторный запуск — идемпотентен
- [x] 1.2 Добавить индексы: GIN на `search_vector` 4 таблиц, pg_trgm на тексты спек, btree по списку из `infrastructure/database-schema` delta — и проверить их наличие через `\di` после InitSchema
- [x] 1.3 Определить структуры моделей в `internal/model` (SpecConfig, SpecCapability, SpecRequirement, SpecScenario, SpecUsecase, SpecUsecaseStep, SpecChange, SpecChangeDelta, SpecCodeMention, SpecVocabTerm, SpecEmbedding) и метрики `ScanStats` (`SpecCapabilities`, `SpecRequirements`, `SpecScenarios`, `SpecUsecases`, `SpecChanges`, файловые `MDFiles`/`YAMLFiles`) — проверить компиляцию пакета и unit-тест на mergeScanStats новых полей (параллельные воркеры суммируются)

## 2. Walker и кодировки

- [x] 2.1 Расширить `internal/fswalk` (`GetSupportedExtensions`, `getEncodingAndLanguage`): `.md` → language MD с авто-детектом, `.yaml` → YAML/UTF8; добавить MD/YAML в дефолтные include-паттерны — проверить unit-тестом на фикстурах (openspec-спека, посторонний README, config.yaml)
- [x] 2.2 Реализовать авто-детект кодировки markdown в `internal/encoding` (UTF-8 приоритет: BOM/валидность → UTF-8; иначе CP1251 fallback) и проверить unit-тестами: UTF-8 без BOM, UTF-8 с BOM, CP1251 legacy-файл, чистый ASCII
- [x] 2.3 Проверить инкрементальность на `.md`/`.yaml`: `codebase update` после правки одного spec.md переиндексирует только его (pre-filter по mtime+size работает для новых расширений)

## 3. Парсер openspecmd

- [x] 3.1 Создать пакет `internal/parser/openspecmd`: детект openspec-корня (директория `openspec|OpenSpec` + `config.yaml` со `schema: spec-driven`, case-insensitive) и позиционную классификацию файла внутри корня (specs / scenarios / usecases / business-processes / changes / config) — unit-тесты на дереве-фикстуре со всеми ветками классификации
- [x] 3.2 Парсер `config.yaml` → SpecConfig (schema_name, product_name, context_text) — unit-тест на реальном config.yaml финпродукта (fa-cards) и на конфиге без секции context
- [x] 3.3 Парсер `spec.md` → SpecCapability (slug = полный путь от specs/, title из H1 опционален, purpose/notes/related_code, границы строк) + контейнерные capability для промежуточных директорий — unit-тесты: вложенная таксономия (`FORMS/0409120/usage`), спека без H1 и без `## Requirements` (фикстура fa-factoring)
- [x] 3.4 Парсинг требований и сценариев: `### Requirement:` / `#### Scenario:`, bold и plain GIVEN/WHEN/THEN, AND к последнему блоку, given/when/then раздельными полями, req_order/scn_order, line_start/end — unit-тесты: жирные и простые WHEN/THEN, AND после THEN, русская ДОЛЖНА-формулировка, спека с таблицами Markdown внутри тела требования (raw text сохраняется)
- [x] 3.5 Парсер usecase-слоя (3 формата): scenarios/*.md (актёры, пред/постусловия, main/alternative шаги), usecases/REQ-NNN-SC-NNN (имя из файла), business-processes/ (pageId из текста/INDEX, шаги) — unit-тесты на трёх фикстурах по одному формату, включая файл с русским именем
- [x] 3.6 Парсер changes: активные и archive/ (имя, статус, артефакты), delta-спеки `## ADDED/MODIFIED/REMOVED Requirements` → SpecChangeDelta (section, requirement_name, body_text), `skip_specs: true` → извлечение capability из proposal.md по упоминаниям путей `openspec/specs/<…>` — unit-тесты: архивный change с delta, change со skip_specs, change без specs-директории
- [x] 3.7 Извлечение упоминаний кода: Related code (подсекции `### …`), inline-тексты требований/сценариев, тексты delta; эвристики kind (API_*/FCD_*/t[A-Z]*/p[A-Z]*, пути *.smf/*.dfm/*.pas/*.sql, fallback sql_procedure/unknown), стоп-паттерны обиходных слов — unit-тесты: подсекция API-процедур, inline в WHEN, имя без цели с kind unknown

## 4. Интеграция в индексатор

- [x] 4.1 Добавить language-кейсы MD/YAML в диспетчер `processFileInTx` (`internal/indexer`) с вызовом парсера openspecmd; не-openspec markdown — только saveFile; проверить на смешанном дереве: спеки дают сущности, посторонние README — нет
- [x] 4.2 Batch insert методов в `internal/store` для всех spec-таблиц (COPY IN, `to_tsvector('russian', …)` при вставке search_vector) — integration-тест на 10К синтетических требований через тестовую БД
- [x] 4.3 Dual-write в `symbols` (spec_capability/requirement/scenario/usecase/change) при вставке сущностей — проверить запросом `codebase query symbol --type spec_capability` после индексации фикстуры
- [x] 4.4 Спек-постпроцессор в `runPostProcessingParallel` (по образцу существующих): резолв `spec_code_mentions` → relations `references_code` по именам сущностей (lookup через symbols/таблицы), удаление старых references_code при переиндексации файла; no-op при отсутствии спек — integration-тесты: резолв в обе стороны порядка индексации, нерезолвнутый mention остаётся в staging
- [x] 4.5 Построение `depends_on_capability` (5 маркеров, confidence explicit/notes/inline, sibling-резолв по префиксу пути, cci:-хвост) и `change_modifies` (из delta-файлов и proposal-извлечений) в том же постпроцессоре — integration-тесты на фикстурах: markdown-ссылка, «Связан с доменами», inline «(см. `calc-flow`)», cci:-ссылка, skip_specs-change
- [x] 4.6 Профиль продукта `spec_configs` детекцией (usecase_layout, id_style, has_changes, has_adr, normative_lang, cross_ref_style) при завершении индексации продукта — integration-тест: два продукта с разными layout дают разные профили

## 5. Полнотекстовый слой

- [x] 5.1 Токенизатор с 3 категориями терминов (русские — lowercase+стемминг простым суффиксным стеммером; техимена — единый токен по регэкспам; гибриды `758П`/`275-ФЗ` — как есть) + стоп-словарь (система, shall, then, …) — unit-тесты на корпусе примеров из design (арест/ареста → один стем; CON_STP_MassAccrual → один токен)
- [x] 5.2 Словарь (term, doc_freq; фильтры minDF/maxDF) и TF-IDF (log TF, smooth IDF, L2-нормализация) — unit-тест на известной мини-матрице с ручным расчётом весов
- [x] 5.3 SVD/LSA через gonum (k=128, U·Σ для документов, Vᵀ для проекции запросов), сохранение/загрузка модели `spec_lsa_model.bin`, детерминизм — unit-тест: повторное обучение того же корпуса даёт идентичные векторы; проекция запроса «блокировка счёта» находит документ про «арест счёта» в синтетическом корпусе
- [x] 5.4 Триггер пересчёта в пост-обработке: порог изменённых capability из конфигурации (секция `[spec]`, дефолт по Open Question из design), минимальный размер корпуса для первого обучения; вставка `spec_vocab`/`spec_embeddings` (embed_level='spec') — integration-тест: init обучает модель; update с изменением ниже порога не пересчитывает
- [x] 5.5 Добавить gonum в `go.mod`, проверить `go mod tidy` и сборку без регрессий

## 6. Сервисный слой и CLI

- [x] 6.1 Создать `internal/specsvc`: ExecuteSpecSearch (двухслойный: tsquery+trgm exact / LSA semantic, раздельные секции, фильтры product/level/status), ExecuteSpecByCode, ExecuteSpecDeps (направление, глубина, защита от циклов, parent-иерархия), ExecuteSpecUsecase (шаги, involved, честный пустой результат по профилю), ExecuteSpecCoverage (сохранённые метрики | вычисление, mode=gaps), ExecuteSpecHistory (change→capability, delta-секции) — unit/integration-тесты по сценариям из `query/spec-queries` delta
- [x] 6.2 Sentinel-ошибки `errs` для спек-запросов (ErrSpecNotFound и пр. по образцу существующих) и ctx-пропагация во все методы specsvc — unit-тест на errors.Is и отмену контекста
- [x] 6.3 CLI-подкоманды `codebase query spec search|by-code|deps|usecase|coverage|history` с флагами и `--json`/`--ndjson` envelope (формат имени суб-уровня решить по Open Question design) — ручная проверка `--help` + smoke-тест на проиндексированном FA
- [x] 6.4 Метрики спек в `codebase stats` (агрегаты COUNT по spec-таблицам в `GetStats` + FILTER md/yaml в файловых счётчиках) — проверка `codebase stats --json` показывает новые счётчики
- [x] 6.5 Блок «Spec Entities» в сводке `codebase init`/`update` (capabilities, requirements, scenarios, usecases, changes — по образцу существующих блоков в `cmd/init.go`) и строки `MD files`/`YAML files` в файловых счётчиках — ручная проверка вывода на FA-фикстуре

## 7. MCP-инструменты

- [x] 7.1 Зарегистрировать 6 инструментов `codebase_query_spec_*` в registry `internal/mcp` поверх specsvc с контрактом «чистые доменные данные», таймаутом `query_timeout_sec` и существующей пагинацией — проверка tools/list через MCP-клиента и ручные вызовы на FA-индексе
- [x] 7.2 Проверить большие ответы: spec_search по fa-reports (1757+ capability) возвращает первый чанк с continuation_id, дочитывается `codebase_read_more`

## 8. Приёмочная верификация на FA

- [x] 8.1 Доиндексация спек живого индекса `codebase update` по D:\GITHUB\GolandProjects\FA (init на существующем завершённом индексе отказывает — cmd/init.go; update подхватывает новые .md/.yaml как новые файлы): перед прогоном обновить `codebase.toml` (в явный `include_patterns` добавить `*.md`, `*.yaml` — явный список перекрывает дефолты); все 10 продуктов детектированы, ~2200 capability, спеки fa-reports с полными путями, usecase-слои трёх форматов, changes с архивами — сверка счётчиков stats с оценками из proposal
- [x] 8.2 Сквозные проверки шести запросов из «Шесть базовых задач ИИ-агента» на живом индексе: поиск по бизнес-термину («отключение SMS-уведомлений»), спецификации по процедуре (API_DepoAccount_MassInsert), зависимости capability, usecase с involved, покрытие, история изменения — каждый запрос возвращает непустой осмысленный результат
- [x] 8.3 Регрессия: индексация проекта без openspec-корней проходит без ошибок и без spec-сущностей; существующие query/review/rti/trc работают без изменений
- [x] 8.4 `openspec validate --strict` собственного changeset'а и обновление затронутых main-спек CodeBase через sync после реализации
