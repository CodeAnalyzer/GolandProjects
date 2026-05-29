# Bug Report: DFM Forms Not Found via Symbol Search by Class Name

## Версия CodeBase
- **Версия**: 0.6.7 build 636
- **Дата обнаружения**: 05.05.2026

## Описание проблемы
DFM формы не находятся через `query symbol --name <CLASS_NAME> --type form` при поиске по имени класса формы. Поиск работает только по имени объекта из DFM файла.

## Пример воспроизведения
```powershell
& ".\Tools\CodeBase\CodeBase.exe" query symbol --name TAim_T --type form
```

**Ожидаемый результат:** Найдена форма с классом `TAim_T`

**Фактический результат:** Пустой результат `[]`

## Файлы с примером
- `C:\NT\FA#\7.2GIT\fa-contracts\Consumer\CLIENT\Consumer\AIMT.DFM` (строка 1):
  ```dfm
  object Aim_T: TAim_T
  ```
  - FormName = "Aim_T" (имя объекта)
  - FormClass = "TAim_T" (имя класса)

- `C:\NT\FA#\7.2GIT\fa-contracts\Consumer\CLIENT\Consumer\AimT.pas` (строка 29):
  ```pascal
  TAim_T = class(TDsTableForm, ILookupForm)
  ```
  - ClassName = "TAim_T"

## Корневая причина

### 1. Индексация DFM форм (`Source/internal/indexer/indexer.go` строки 228-235)
```go
symbolsBatch = append(symbolsBatch, &model.Symbol{
    FileID:     fileID,
    SymbolName: form.FormName,  // Используется имя объекта из DFM
    SymbolType: "form",
    EntityType: "dfm",
    LineNumber: form.LineStart,
    Signature:  form.FormClass,  // Имя класса сохраняется только в Signature
})
```

### 2. Поиск символов (`Source/internal/query/query.go` строки 259-261)
```go
func (q *Query) SearchSymbol(name string, symbolType string, like bool, limit int) ([]SymbolResult, error) {
    lookupValue := buildLookupValue(name, like)
    lookupCondition := buildNameLookupCondition([]string{"s.symbol_name"}, like, 1)  // Поиск только по symbol_name
```

### 3. Специализированный поиск форм (`Source/internal/query/query.go` строки 425-427)
```go
func (q *Query) SearchDFMForm(name string, like bool, limit int) ([]DFMFormResult, error) {
    lookupValue := buildLookupValue(name, like)
    lookupCondition := buildNameLookupCondition([]string{"df.form_name", "df.form_class", "df.caption"}, like, 1)
```

## Вывод
Это **НЕ ошибка теста**, а особенность архитектуры CodeBase:
- `query symbol` ищет по `symbol_name` (для DFM форм это имя объекта из DFM)
- Имя класса формы сохраняется в `signature`, но не индексируется для поиска
- Для поиска по имени класса нужно использовать специализированную команду `query form`, которая ищет по `form_class`

## Обходной путь
Использовать специализированную команду для поиска форм:
```powershell
& ".\Tools\CodeBase\CodeBase.exe" query form --name TAim_T
```
Эта команда ищет по `form_name`, `form_class` и `caption`.

## Предлагаемое решение
Добавить поиск по `signature` для типа `form` в функции `SearchSymbol`, чтобы поддерживать поиск DFM форм по имени класса:

```go
func (q *Query) SearchSymbol(name string, symbolType string, like bool, limit int) ([]SymbolResult, error) {
    lookupValue := buildLookupValue(name, like)
    fields := []string{"s.symbol_name"}
    if symbolType == "form" {
        fields = append(fields, "s.signature")  // Добавить поиск по signature для форм
    }
    lookupCondition := buildNameLookupCondition(fields, like, 1)
```

## Влияние
- **Серьезность**: Low
- **Влияние**: Пользователи могут быть сбиты с толку, если ищут форму по имени класса через `query symbol`
- **Обходной путь**: Использовать `query form` вместо `query symbol --type form`

## Связанные файлы
- `Source/internal/indexer/indexer.go` - индексация DFM форм
- `Source/internal/query/query.go` - поиск символов и форм
- `Source/internal/parser/dfm/dfm_parser.go` - парсер DFM файлов
