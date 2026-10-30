# Design: default-branch-mark-ordering

## Context

Discovery (`discoverGitSources`, `internal/indexer/source_registry.go`)
обходит детей `root_path` в порядке `os.ReadDir` (побайтовая сортировка имён)
и для каждого git-каталога делает `UpsertDSProductBranch`. Исходная
реализация маркировала default сразу после upsertа **первого** каталога
продукта (`markedProducts[productID]`), рассчитывая на то, что develop-чекаут
обработается первым.

Это предположение нарушается побайтовой сортировкой: у `fa-contracts-ext`
на рабочем ПК порядок `fa-contracts-ext-ZAPK` (Z = 0x5A) →
`fa-contracts-ext-check-save-pti` (c = 0x63) → `fa-contracts-ext-develop`
(d = 0x64), что подтверждается id строк 6, 7, 8. `UpsertDSProductBranch` на
`ON CONFLICT (path) DO UPDATE` выставляет `is_default = FALSE`
(`internal/store/db_products.go`) — сброс нужен при смене ветки в каталоге,
но он же затирает метку, когда строка develop-upsertится после маркировки.
На первом скане метка дополнительно ставится до создания строки develop.
В обоих случаях повторного вызова `SetDefaultBranch` в прогоне нет.

## Goals / Non-Goals

**Goals**

- `is_default` корректно выставляется независимо от имён/порядка каталогов
  продукта; дефект исправляется на существующих БД очередным `update`.

**Non-Goals**

- Не меняем имя default-ветки (политика `indexer.default_branch` = `develop`)
  и семантику «нет default-ветки — не ошибка».
- Не трогаем сброс `is_default = FALSE` в `ON CONFLICT` — он нужен для смены
  ветки в каталоге (прежняя default-линия обязана терять флаг).
- Не решаем различие «версионные vs клиентские линии» (union-семантика
  запросов, профиль dsinfo, missing-ветки) — отдельный дизайн.

## Decisions

- **D1. Маркировка после цикла discovery.** В цикле накапливаем
  `map[int64]struct{}` productID, `SetDefaultBranch` вызываем после цикла по
  одному разу на продукт. Все upsert-ы (и их сбросы) уже выполнены — метку
  некому затереть; порядок обхода перестаёт иметь значение.
  Альтернативы отвергнуты: убрать `is_default = FALSE` из `ON CONFLICT` —
  ломает смену ветки в каталоге; перевызывать `SetDefaultBranch` после
  каждого upsertа — лишние UPDATE на каждый каталог.
- **D2. Тест воспроизводит дефект буквально.** Integration-тест
  (`-tags integration`) создаёт два git-каталога одного продукта: клиентский
  (`prj/CHECK_SAVE_PTI`, имя каталога сортируется раньше) и develop
  (`fa-contracts-ext-develop`), запускает discovery **дважды** (первый скан
  и установившийся прогон) и в обоих требует `develop is_default = true`.
  Без фикса первый прогон падает на «метка до создания строки», второй — на
  «сброс после метки».

## Risks / Trade-offs

- Если git-детекция develop-каталога падает (git CLI недоступен, нет remote),
  поведение прежнее: строки нет, `SetDefaultBranch` — no-op. Не регресс.
- Маркировка смещается на время после всех upsertов — на поведение выборок
  не влияет (discovery завершается до обхода файлов).
- Конкурентности нет: discovery однопоточен, гонок на этапе не возникает.
