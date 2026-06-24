# Bug: diffTypesComparison — ложное срабатывание на макросе `_CONVERT_DATE_TO_INT_`

**Правило:** `diffTypesComparison`
**Severity:** 2 (Conversion Error)
**Статус:** Ложное срабатывание (false positive)
**Дата обнаружения:** 2026-06-24
**Файл:** `fa-contracts/API_Credit/Server/Callback/API_CON_Acc_GetNumberByMask.sql`

## Описание проблемы

Правило `diffTypesComparison` выдаёт ложные срабатывания на строках 217, где сравнивается поле `cc.DateInt` (тип `DSINT_KEY` = int) с результатом макроса `_CONVERT_DATE_TO_INT_(@DateStart)` и `_CONVERT_DATE_TO_INT_(@DateEnd)`. CodeBase определяет тип выражения макроса как `DSDATETIME` (по типу аргумента `_date_`), но фактически макрос разворачивается в `convert(int, convert(varchar, _date_, 112))`, что возвращает `int`. Сравнение `int` с `int` корректно.

## Воспроизведение

```powershell
codebase review C:\NT\FA#\7.2GIT\fa-contracts\API_Credit\Server\Callback\API_CON_Acc_GetNumberByMask.sql --rules diffTypesComparison
```

Результат:
```
- [2] diffTypesComparison line=217 object=cc.DateInt > _CONVERT_DATE_TO_INT_(@DateStart)
  Сравнение разных типов: cc.DateInt (DSINT_KEY) > _CONVERT_DATE_TO_INT_(@DateStart) (DSDATETIME)
- [2] diffTypesComparison line=217 object=cc.DateInt <= _CONVERT_DATE_TO_INT_(@DateEnd)
  Сравнение разных типов: cc.DateInt (DSINT_KEY) <= _CONVERT_DATE_TO_INT_(@DateEnd) (DSDATETIME)
```

Исходный код (строки 214-220):

```sql
    select @Days = count(1)
      from tConsCalendarContentSync   cc M_NOLOCK_INDEX(XAK0tConsCalendarContentSync)
     where cc.CalendarID = @CalendarID
       and cc.DateInt             >   _CONVERT_DATE_TO_INT_(@DateStart)
       and cc.DateInt             <=  _CONVERT_DATE_TO_INT_(@DateEnd)
    M_ISOLAT
```

## Определение макроса

Макрос определён в файле `fa-contracts/API_Credit/Server/Callback/API_Const_CallBack.h`, строка 21:

```c
#define _CONVERT_DATE_TO_INT_(_date_)  convert(int, convert(varchar, _date_, 112))
```

Макрос конвертирует дату (`DSDATETIME`) в целое число формата `YYYYMMDD` (стиль 112 = `yyyymmdd`). Результат разворачивания — `convert(int, ...)`, то есть тип результата — **int**, а не `DSDATETIME`.

Также макрос присутствует в:
- `fa-contracts/API_Credit/Server/UPLOAD/API_Const_CallBack.h` (строка 21)
- `fa-contracts/Consumer/SERVER/UPLOAD/API_Const_CallBack.h` (строка 21)

## Причина

### Механизм ложного срабатывания

CodeBase индексирует макросы `#define` и сохраняет их сигнатуру. Для макроса `_CONVERT_DATE_TO_INT_` сигнатура в индексе:

```
convert(int, convert(varchar, _date_, 112))
```

При анализе правила `diffTypesComparison` CodeBase определяет тип выражения `_CONVERT_DATE_TO_INT_(@DateStart)` по типу аргумента `@DateStart`, который имеет тип `DSDATETIME`. CodeBase **не раскрывает макрос** и не анализирует тип результата выражения `convert(int, convert(varchar, @DateStart, 112))`, которое равно `int`.

### Корневая причина

CodeBase не выполняет препроцессорное раскрытие макросов `#define` при определении типа выражения. Вместо этого тип выводится из типа аргумента макроса, что некорректно для макросов, изменяющих тип (например, `convert(int, ...)`).

## Ожидаемое поведение

1. Тип выражения `_CONVERT_DATE_TO_INT_(@DateStart)` должен определяться как `int` (результат `convert(int, ...)`)
2. Сравнение `cc.DateInt` (int) с `_CONVERT_DATE_TO_INT_(@DateStart)` (int) не должно вызывать замечание `diffTypesComparison`
3. Замечания для данного файла не должны генерироваться

## Предложение по исправлению

### Вариант 1: Раскрытие макросов при определении типа (предпочтительный)

При анализе типа выражения, содержащего макрос `#define`, выполнять подстановку тела макроса и определять тип результата. Для `_CONVERT_DATE_TO_INT_(_date_)` подстановка даёт `convert(int, convert(varchar, @DateStart, 112))`, тип результата — `int`.

### Вариант 2: Анализ типа по телу макроса

Для макросов, тело которых содержит `convert(тип, ...)`, определять тип результата по первому аргументу `convert`. Для `_CONVERT_DATE_TO_INT_` тело содержит `convert(int, ...)`, следовательно тип — `int`.

### Вариант 3: Whitelist макросов, изменяющих тип

Добавить конфигурационный список макросов с явно указанным типом результата. Для `_CONVERT_DATE_TO_INT_` указать тип результата `int` (или `DSINT_KEY`). Это простое решение, не требующее препроцессора.

```toml
[review.diffTypesComparison.macroReturnTypes]
"_CONVERT_DATE_TO_INT_" = "int"
```

### Вариант 4: Использование таблицы `symbols` в review-движке (предпочтительный)

Индексатор CodeBase уже строит таблицу `symbols`, содержащую макросы `#define` с их сигнатурами. Для макроса `_CONVERT_DATE_TO_INT_` в таблице `symbols` хранится:

- `name` = `_CONVERT_DATE_TO_INT_`
- `type` = `define`
- `signature` = `convert(int, convert(varchar, _date_, 112))`
- `file` = путь к `.h`-файлу

Review-движок при анализе выражений в правиле `diffTypesComparison` (и других правил) не обращается к таблице `symbols` для разрешения макросов. Если бы review-движок делал lookup по имени макроса в `symbols`, он бы:

1. Нашёл запись с сигнатурой `convert(int, convert(varchar, _date_, 112))`
2. Определил тип результата как `int` (по первому аргументу `convert`)
3. Не сгенерировал false positive

Реализация:

```go
// При определении типа выражения в review-движке:
// 1. Проверить, является ли идентификатор макросом #define
// 2. Если да — lookup в таблице symbols по name + type="define"
// 3. Извлечь сигнатуру и определить тип результата по телу макроса
//    (например, для convert(int, ...) тип = int)

func (r *Review) resolveMacroType(expr string) (string, bool) {
    // Извлечь имя макроса из выражения (например, _CONVERT_DATE_TO_INT_(...))
    macroName := extractMacroName(expr)
    if macroName == "" {
        return "", false
    }
    // Lookup в таблице symbols
    macro, found := r.index.LookupSymbol(macroName, "define")
    if !found {
        return "", false
    }
    // Определить тип результата по сигнатуре
    // Для "convert(int, ...)" вернуть "int"
    return inferTypeFromSignature(macro.Signature), true
}
```

Преимущества:
- Не требует препроцессорного раскрытия макросов в SQL-файле
- Использует уже готовый индекс `symbols`, построенный индексатором
- Работает для любых макросов, изменяющих тип, без ручного whitelist
- Сигнатуры макросов уже содержат достаточно информации для вывода типа

Недостатки:
- Требует доступа review-движка к индексу (в настоящее время review может работать без индекса)
- Вывод типа из сигнатуры требует парсинга тела макроса (но это проще, чем полный препроцессор)

## Затронутые файлы

- `internal/review/review_rules.go` — функция `checkDiffTypesComparison` (определение типа выражения)
- `internal/indexer/` — индексация макросов `#define` (добавление информации о типе результата)

## Влияние

Без исправления правило `diffTypesComparison` генерирует ложные срабатывания (severity 2) на любых SQL-файлах, где используется макрос `_CONVERT_DATE_TO_INT_` в сравнениях с полями типа int. Макрос используется минимум в 3 файлах:

- `fa-contracts/API_Credit/Server/Callback/API_CON_Acc_GetNumberByMask.sql`
- `fa-contracts/API_Credit/Server/UPLOAD/API_Const_CallBack.h`
- `fa-contracts/Consumer/SERVER/UPLOAD/API_Const_CallBack.h`

Аналогичная проблема может возникать с другими макросами, изменяющими тип данных (например, `_CONVERT_INT_TO_DATE_` и подобными).
