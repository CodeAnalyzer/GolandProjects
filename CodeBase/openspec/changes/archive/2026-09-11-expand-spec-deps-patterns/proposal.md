## Why

Индексатор `depends_on_capability` relations извлекает зависимости из текстов spec-файлов, но распознаёт только 6 жёстко заданных маркеров. Реальные spec-файлы в продуктах FA (fa-generalledger, fa-warranty, fa-contracts, fa-cards) используют более широкий набор форматов упоминания capabilities. Из-за этого `codebase_query_spec_deps` возвращает `null` для целых продуктов — например, fa-generalledger `operations` имеет 8 явно декларированных связей в Notes, но ни одна не извлекается. Аналогично fa-warranty (6 spec) и часть fa-contracts теряют зависимости из-за несовпадения формата.

## What Changes

- Расширение `extractCapabilityDeps` в `internal/indexer/indexer_postprocess_spec_deps.go` для распознавания 6 дополнительных паттернов упоминания capabilities, обнаруженных в реальных spec-файлах:
  - "Связи с доменами: slug (desc)" — plain text slugs в скобках (fa-cards)
  - "Связи с другими доменами: desc (slug, slug)" — slugs в круглых скобках (fa-cards)
  - "Связи с другими spec: slug (desc)" — plain text slugs (fa-generalledger)
  - "Связан с `slug`" без слова "доменами" — backtick-wrapped (fa-contracts, fa-warranty)
  - "Связь с доменом «Name» (slug)" — slug в круглых скобках (fa-warranty)
  - "Связь с spec `slug`" и "См. spec `slug`" — backtick-wrapped (fa-contracts, fa-generalledger)
- Расширение `reSeeAlso` для распознавания "см. `slug`" без обязательных скобок (fa-reports)
- Все новые relations получают `confidence: "notes"` (как существующие notes-паттерны)
- Существующие паттерны (markdown-ссылки, "Связан с доменами `slug`", cci:-хвост, ExtractSpecReferences) остаются без изменений

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `indexing/relations-postprocessing`: добавление новых паттернов извлечения `depends_on_capability` из текстов spec-файлов

## Impact

- **Код**: `internal/indexer/indexer_postprocess_spec_deps.go` — функция `extractCapabilityDeps`, новые regex-переменные
- **Тесты**: `internal/indexer/indexer_postprocess_spec_deps_test.go` — новые тест-кейсы для каждого паттерна
- **Индексация**: после переиндексации продуктов FA количество `depends_on_capability` relations значительно возрастёт; `codebase_query_spec_deps` начнёт возвращать реальные зависимости для fa-generalledger, fa-warranty и других
- **Совместимость**: обратно совместимо — новые паттерны только добавляют relations, не удаляя и не изменяя существующие; `confidence` новых relations = `"notes"`, как у текущих notes-паттернов
