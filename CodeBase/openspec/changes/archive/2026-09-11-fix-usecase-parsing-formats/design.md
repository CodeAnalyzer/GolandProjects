## Context

Парсер `ParseUsecaseFile` (usecase.go) написан под формат `scenarios/` (fa-cards): H2-секции по имени (`## Пользователи`, `## Основной поток`), нумерованные шаги (`1. текст`), pageId через `Confluence pageId NNN`. Два других формата — `usecases/` (fa-financialasset, fa-generalledger) и `specs/usecases/` (fa-custody) — используют H3/H4-секции, inline-метаданные `**Ключ**: Значение`, шаги `**Шаг N**. текст` и табличные шаги. ClassifyPath не распознаёт `specs/usecases/` (сегмент `usecases` внутри `specs/`), поэтому ~40 файлов fa-custody вообще не индексируются.

Существующий код:
- `openspecmd.go:ClassifyPath` — проверяет `isUsecaseDir(rest[0])`, но `rest[0]` для `specs/usecases/...` равно `"specs"`, не `"usecases"`
- `usecase.go:ParseUsecaseFile` — switch по H2-заголовкам (`setSection`), таблицы только для `Kind == "business-process"`, шаги только `reNumberedStep` / `reBulletStep`
- `specsvc.go:ExecuteSpecUsecase` — один SQL-запрос по `usecase_name`, без параметра `product`
- `registry.go` — схема инструмента с единственным параметром `name`

## Goals / Non-Goals

**Goals:**
- ClassifyPath распознаёт `specs/usecases/**/*.md` как `KindUsecase`
- ParseUsecaseFile извлекает inline-метаданные, H3/H4-секции, `**Шаг N**.` и табличные шаги для всех форматов
- pageId извлекается из URL `pageId=NNN` и из `Confluence pageId NNN`
- Секция «Ветвления» парсится в alternative-шаги
- Секция «Спеки-компоненты (запчасти)» извлекается в `spec_code_mentions` для `usecase_involves`
- `ExecuteSpecUsecase` поддерживает параметр `product` (режим списка) и поиск по pageId

**Non-Goals:**
- Изменение формата исходных usecase-файлов в продуктах FA — парсер адаптируется к существующим форматам
- Полный BPMN-парсинг — BPMN-схемы остаются ссылками
- Извлечение изображений из media/ — только текстовый контент
- Полнотекстовый поиск по usecase (уже работает через `spec_search` с `level=usecase`)
- Изменение схемы БД `spec_usecases` — существующих колонок достаточно

## Decisions

### 1. ClassifyPath: проверка `specs/usecases/` через rest[1]

**Решение:** Добавить проверку `len(rest) >= 2 && strings.EqualFold(rest[0], "specs") && isUsecaseDir(rest[1])` перед default-веткой. SourceDir устанавливается в `rest[1]` (т.е. `"usecases"`), Kind — `KindUsecase`.

**Альтернатива:** Регистрировать `specs/usecases/` как capability-контейнер с под-capabilities по разделам. Отклонено: fa-custody AGENTS.md явно описывает `specs/usecases/` как usecase-слой, не как capability.

### 2. Inline-метаданные: regex `^\*\*(.+?)\*\*:\s*(.+)$`

**Решение:** Добавить regex для строк вида `**Ключ**: Значение` в начале цикла парсинга, до проверок H2/таблиц. Маппинг ключей (case-insensitive, частичное совпадение):
- `описание` → description
- `пользователи и системы`, `роли и системы`, `актёры`, `акторы` → actors
- `бизнес-ценность`, `ценность` → business_value
- `точка старта` → architecture (ближайшее по семантике поле)
- `источник` → source-строка (не сохраняется в отдельное поле, но из неё извлекается pageId)

**Альтернатива:** Добавить отдельные колонки в БД для каждого поля. Отклонено: существующих полей (description, actors, business_value, architecture) достаточно.

### 3. H3/H4-секции: расширение setSection для H3 и H4

**Решение:** Добавить regex `reUsecaseH3` (`^###\s+`) и `reUsecaseH4` (`^####\s+`), вызывать `setSection` для всех уровней. Существующий switch по имени секции уже покрывает `предусловия`, `постусловия`, `основной поток` и т.д. — нужно только добавить распознавание заголовков H3/H4 как якорей секций. Для H3 `### Сценарий ...` — устанавливать flow=flowMain (сценарий = основной поток).

### 4. Шаги `**Шаг N**. текст`: новый regex

**Решение:** Добавить `reBoldStep = regexp.MustCompile(`^\*\*Шаг\s+(\d+)\*\*[.)]?\s*(.+)$`)`. Если matches и `flow != flowNone` — добавлять шаг с `StepOrder = N` (номер из regex, не инкрементальный счётчик).

### 5. Табличные шаги для Kind="usecase"

**Решение:** Расширить условие табличного парсинга с `uc.Kind == "business-process"` на `uc.Kind == "business-process" || uc.Kind == "usecase"`. Для usecase-формата таблицы имеют 4 колонки (`| Шаг | Пользователь/система | Действие | Ожидаемый результат |`) — объединять в текст шага: `"<Действие> — <Ожидаемый результат>"` или просто `"<Действие>"` если результат пуст. Колонка «Шаг» используется как step_order (если число).

### 6. PageId из URL

**Решение:** Добавить `reURLPageID = regexp.MustCompile(`[?&]pageId=(\d+)`)`. Применять после `reConfluenceID` (существующий regex для `Confluence pageId NNN`). Если первый не нашёл — пробовать второй.

### 7. Секция «Ветвления»: alternative-шаги

**Решение:** Добавить `"ветвления"` в switch `setSection` → `flow = flowAlt`. Буллеты в этой секции (`- **WHEN** ...`) парсятся существующим `reBulletStep` в alternative-шаги.

### 8. Секция «Спеки-компоненты (запчасти)»: spec_code_mentions

**Решение:** Добавить `"спеки-компоненты"`, `"спеки-компоненты (запчасти)"`, `"запчасти"` в switch `setSection` → `setTarget(nil)`, но установить флаг `inSpecComponents = true`. Буллеты в этой секции (`- `specs/<cap>/spec.md` — шаг N. Требования: «...»`) парсятся через существующий `ExtractSpecReferences` — они уже попадут в mentions с `mention_kind = "spec_ref"`. Постпроцессор `indexer_postprocess_spec_deps.go` уже строит `usecase_involves` из `spec_ref` mentions.

### 9. ExecuteSpecUsecase: параметр product

**Решение:** Добавить опциональный параметр `product string` в `ExecuteSpecUsecase`. Логика:
- `name != "" && product == ""` — текущее поведение (поиск по имени/pageId)
- `name == "" && product != ""` — список usecase'ов продукта: `SELECT usecase_name, title, source_dir, usecase_kind FROM spec_usecases JOIN spec_configs ... WHERE product_name = $1`
- `name != "" && product != ""` — поиск по имени с фильтром по продукту

Поиск по pageId: если `name` — число, сначала пробовать `WHERE page_id = $1::bigint`, затем fallback на `usecase_name`.

## Risks / Trade-offs

- [Inline-метаданные `**Ключ**: Значение` могут встречаться в тексте шагов] → Mitigation: regex применяется только вне активных секций шагов (flow=flowNone) и только когда строка начинается с `**`
- [H3 `### Сценарий ...` может сбросить активную секцию] → Mitigation: H3 устанавливает flow=flowMain, не сбрасывает sectionTarget (предусловия/постусловия остаются привязанными)
- [Табличные шаги для usecase могут сломать парсинг таблиц в scenarios-формате] → Mitigation: таблицы в scenarios-формате (Kind="scenario") не попадают в табличный парсинг — остаётся только для business-process и usecase
- [ClassifyPath для specs/usecases/ может конфликтовать с реальными capability-директориями с именем "usecases"] → Mitigation: проверка `isUsecaseDir(rest[1])` сработает только если имя директории точно `usecases` (case-insensitive), что совпадает с конвенцией fa-custody
- [Поле `architecture` используется для «Точка старта» — семантически неточно] → Mitigation: acceptable trade-off, поле architecture в БД достаточно широкое; при необходимости можно переименовать в будущем
