## 1. Store: lookup-функции

- [x] 1.1 Реализовать `LoadSpecConfigProfile(ctx, product)` в `internal/store/db_lookup_spec.go` — SELECT профиля из `spec_configs` JOIN `ds_products` по `product_name`. Возвращает `SpecConfigProfileRow` или `sql.ErrNoRows`. Проверить: unit-тест на существующий и несуществующий продукт.
- [x] 1.2 Реализовать `LoadSpecConfigStats(ctx, configID)` — один запрос с `UNION ALL` по `spec_capabilities`, `spec_requirements`, `spec_scenarios`, `spec_usecases`, `spec_changes` с фильтром по `spec_config_id`. Возвращает `SpecConfigStatsRow`. Проверить: счётчики совпадают с `COUNT(*)` по каждой таблице.
- [x] 1.3 Реализовать `LoadSpecConfigHierarchy(ctx, configID)` — SELECT `id, parent_id, capability_name, title` из `spec_capabilities` WHERE `spec_config_id = $1` ORDER BY `capability_name`. Возвращает все узлы дерева; сборка дерева в Go. Проверить: количество узлов совпадает с `COUNT(*)` по config.

## 2. Specsvc: ExecuteSpecConfig

- [x] 2.1 Добавить структуры `SpecConfigResult`, `SpecConfigProfile`, `SpecConfigStats`, `SpecConfigNode` в `internal/specsvc/specsvc.go`. Проверить: структуры компилируются, JSON-теги соответствуют спеке.
- [x] 2.2 Реализовать `ExecuteSpecConfig(ctx, db, product, includeHierarchy, depth)` — lookup профиля (ErrSpecNotFound если нет), статистики, опциональной иерархии. Сборка дерева в Go из плоской выборки с ограничением `depth`. `is_container` = title пустой AND purpose IS NULL AND notes IS NULL. `children_count` считается из плоской выборки. Проверить: unit-тест на плоский продукт (fa-cards) и глубокий (fa-reports depth=2).

## 3. MCP registry

- [x] 3.1 Зарегистрировать `codebase_query_spec_config` в `internal/mcp/registry.go` — Definition с InputSchema (product required, include_hierarchy bool, depth int), Handler вызывает `specsvc.ExecuteSpecConfig`. Добавить в `allowedTools` map. Проверить: инструмент виден в `codebase_ping` / tools list.
- [x] 3.2 Исправить описание `codebase_query_spec_usecase` в `registry.go` — убрать «Returns an honest empty result», заменить на «Returns ErrSpecNotFound if the usecase is not found». Проверить: описание обновлено, поведение кода не изменилось.

## 4. CLI

- [x] 4.1 Добавить `querySpecConfigCmd` в `cmd/query_spec.go` — флаги `--product` (required), `--include-hierarchy` (bool, default true), `--depth` (int, default 2). Вызывает `specsvc.ExecuteSpecConfig`. Проверить: `codebase query spec config --product fa-cards` возвращает JSON с профилем и иерархией.
- [x] 4.2 Зарегистрировать подкоманду: `querySpecCmd.AddCommand(querySpecConfigCmd)`. Проверить: `codebase query spec config --help` показывает флаги.

## 5. Тесты

- [x] 5.1 Интеграционный тест `ExecuteSpecConfig` в `internal/specsvc/specsvc_test.go` — продукт с профилем, плоской иерархией, статистикой. Проверить: все поля профиля заполнены, stats корректны, hierarchy = плоский список.
- [x] 5.2 Интеграционный тест `ExecuteSpecConfig` — продукт с глубоким деревом (depth=2 vs depth=3). Проверить: depth=2 обрезает внуков, children_count на листьях корректен.
- [x] 5.3 Интеграционный тест `ExecuteSpecConfig` — несуществующий продукт. Проверить: возвращается `ErrSpecNotFound`.
- [x] 5.4 Интеграционный тест `ExecuteSpecConfig` — `include_hierarchy=false`. Проверить: поле `hierarchy` отсутствует в ответе.
- [x] 5.5 Обновить существующие тесты `ExecuteSpecUsecase` — ожидать `ErrSpecNotFound` вместо пустого результата при отсутствии usecase. Проверить: тесты проходят.
- [x] 5.6 Запустить `go test ./internal/specsvc/... ./internal/store/... ./internal/mcp/...` и проверить, что все тесты проходят.
