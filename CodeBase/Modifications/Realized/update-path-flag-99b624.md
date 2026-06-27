# Флаг --path для команды codebase update

Добавить флаг `--path`/`-p` в команду `codebase update`, который перекрывает `root_path` из `codebase.toml` и позволяет обновить индекс только по указанному каталогу/продукту.

## Изменения

### 1. `cmd/update.go` — добавить флаг и логику

- Добавить переменную `updatePath string`
- В `init()`: `updateCmd.Flags().StringVarP(&updatePath, "path", "p", "", "override root_path from config; scan only this directory")`
- В `RunE`:
  - Если `updatePath` непустой — проверить `os.Stat`, использовать его как `rootPath` вместо `cfg.RootPath`
  - Вывод `rootPath=...` и вызов `idx.Update(...)` использовать `rootPath` (а не `cfg.RootPath`)
  - Если `updatePath` пустой — поведение без изменений (берётся `cfg.RootPath`)

### 2. Тесты

- Unit-тест в `cmd/update_test.go` (если есть) или минимальная проверка: флаг регистрируется, значение по умолчанию пустое

## Совместимость с --modified=false

При `--modified=false --path=D:/.../FA/SomeProduct` — все файлы в `SomeProduct` инвалидируются и переиндексируются. Существующая логика `Update` уже это поддерживает: `onlyModified=false` пропускает проверку хэша и переиндексирует все найденные файлы.

## Не меняется

- `internal/indexer/runner.go` — без изменений, `Update(rootPath, ...)` уже работает с подкаталогами
- `internal/store/db_files.go` — `GetLatestFilesByRootPath` уже фильтрует по префиксу пути
