## Why

Парсер usecase-слоя `ParseUsecaseFile` поддерживает только формат `scenarios/` (fa-cards): H2-секции по имени, нумерованные шаги `1. текст`. Два других фактических формата — `usecases/` (fa-financialasset, fa-generalledger: H3/H4-секции, `**Шаг N**.`, inline-метаданные) и `specs/usecases/` (fa-custody: таблицы шагов, `**Ключ**: Значение`, ветвления WHEN/ELSE, трассируемость шаг→capability) — не парсятся. Результат: ~114 файлов из 3 продуктов индексируются как пустые оболочки (только Name + Title) или вообще не индексируются (fa-custody `specs/usecases/` — ClassifyPath не распознаёт каталог внутри `specs/`). Запрос `codebase_query_spec_usecase` также не поддерживает фильтр по продукту и поиск по pageId/REQ-ID, предусмотренные спекой.

## What Changes

- **ClassifyPath**: распознавание `specs/usecases/` как usecase-слоя (сегмент `usecases` внутри `specs/`, не только на верхнем уровне openspec-корня)
- **ParseUsecaseFile**: извлечение inline-метаданных `**Ключ**: Значение` (Описание, Пользователи и системы, Бизнес-ценность, Точка старта, Источник) для всех форматов
- **ParseUsecaseFile**: распознавание H3/H4-секций (`#### Предусловия`, `#### Шаги`, `#### Постусловия`) в формате `usecases/`
- **ParseUsecaseFile**: распознавание шагов `**Шаг N**. текст` (формат `usecases/`)
- **ParseUsecaseFile**: табличный парсинг шагов для `Kind="usecase"` (не только `business-process`), с поддержкой 4-колоночных таблиц `| Шаг | Пользователь/система | Действие | Ожидаемый результат |`
- **ParseUsecaseFile**: извлечение `pageId` из URL (`pageId=NNN`) и из строки `Confluence pageId NNN` (уже работает, но только для `business-process`)
- **ParseUsecaseFile**: извлечение секции `## Ветвления` (WHEN/ELSE) в поля шагов с `flow_kind="alternative"` или в отдельное поле
- **ParseUsecaseFile**: извлечение секции `## Спеки-компоненты (запчасти)` — mapping шаг→capability→требование для построения `usecase_involves` relations
- **ExecuteSpecUsecase**: добавление опционального параметра `product` — при передаче только продукта (без name) возвращается список usecase'ов продукта с `source_dir` и `usecase_kind`; при передаче name — детали одного usecase (текущее поведение)
- **ExecuteSpecUsecase**: поиск по `page_id` и по `usecase_name` (уже работает), плюс по patterns вида `REQ-NNN-SC-NNN` (извлечение из usecase_name)
- **MCP registry**: добавление опционального параметра `product` в схему инструмента `codebase_query_spec_usecase`

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `indexing/openspec-parsing`: Requirement «Парсинг usecase-слоя» — расширение поддержки трёх форматов: H3/H4-секции, inline-метаданные, табличные шаги для usecases, `**Шаг N**.` формат, ветвления, спеки-компоненты, pageId из URL; ClassifyPath для `specs/usecases/`
- `query/spec-queries`: Requirement «Usecase-слой (spec_usecase)» — добавление параметра `product` (режим списка), поиск по pageId

## Impact

- `internal/parser/openspecmd/openspecmd.go` — ClassifyPath: распознавание `specs/usecases/`
- `internal/parser/openspecmd/usecase.go` — ParseUsecaseFile: inline-метаданные, H3/H4-секции, `**Шаг N**.`, таблицы для usecase-kind, ветвления, спеки-компоненты, pageId из URL
- `internal/specsvc/specsvc.go` — ExecuteSpecUsecase: параметр product, режим списка
- `internal/mcp/registry.go` — схема инструмента codebase_query_spec_usecase: параметр product
- `cmd/query_spec.go` — CLI флаг --product для query spec usecase
- `internal/indexer/indexer_spec.go` — parseUsecaseMD: обработка specs/usecases классификации, извлечение usecase_involves из «Спеки-компоненты»
- `internal/indexer/indexer_postprocess_spec_deps.go` — постобработка: построение usecase_involves из mentions «Спеки-компоненты»
- Повторная индексация продуктов fa-financialasset, fa-generalledger, fa-custody для заполнения ранее пустых записей
