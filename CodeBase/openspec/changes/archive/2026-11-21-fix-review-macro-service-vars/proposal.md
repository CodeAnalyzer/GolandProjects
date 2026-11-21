## Why

Правило `datatype` (и другие правила, использующие карту типов переменных) слепо к сервисным переменным Diasoft-макросов: `@RetVal`/`@trancount` декларируются не в SQL-файле, а раскрытием макроса `__BEGIN_PROCEDURE__` из ядрового `macros.h` (`fa-commonfiles\CommonFiles\SERVER\Include\MACROS.H:1346`), который review-препроцессор не резолвит (`replaceMacros` видит только `#define` самого файла). В результате тип цели неизвестен и проверка тихо пропускается: на `Cons_BankPartnerInstPrepare.sql` (build 1711) `select @RetVal = max(NTFID)` из `pCons_EPPC_CrtByEDPCIDNTF` (NTFID — DSIDENTIFIER, precision 15 → int, precision 10) не даёт finding, хотя это реальная потеря точности. Слепота распространяется на все macro-based процедуры FA (то есть почти на все), при этом самодостаточные patch-файлы с инлайновым `declare @RetVal int` проверяются корректно.

## What Changes

- В карту типов переменных (`collectVariableTypes`, `internal/review/review_helpers.go`) добавляются встроенные типы сервисных переменных нераскрытых Diasoft-макросов:
  - нераскрытый `__BEGIN_PROCEDURE__(...)` в контенте → `retval = int`, `trancount = int`;
  - нераскрытый `DCL_PROC_BODY(...)` (PG-стиль) → `tran_count = int`, `err_code = int`, `msg = varchar(255)`.
- Встроенные типы заполняют только пробелы: явные `declare`-блоки и параметры сигнатуры анализируемого файла имеют приоритет (паттерн fill-gaps, как в `enrichVariableTypesFromAPI`).
- Эффект распространяется на все правила, потребляющие карту типов: `datatype` (fetch into / select assign / exec params), `dateIntoString`, `emptyStringDate`, `isNullSameTypes`, `diffTypesComparison`, `floatToStringConvert`.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/fine-code`: новое требование о встроенных типах сервисных переменных нераскрытых макросов `__BEGIN_PROCEDURE__`/`DCL_PROC_BODY` при разрешении типов переменных правилом `datatype`.

## Impact

- Код: `internal/review/review_helpers.go` (`collectVariableTypes`), тесты `internal/review`.
- Правила-потребители карты типов (перечислены выше) начинают видеть `@RetVal`/`@trancount`/PG-набор — ожидается рост полноты покрытия `datatype`; контроль FP на остальных правилах входит в приёмку.
- Индексация, схема БД и CLI-контракты не затрагиваются; `buildNumber` инкрементируется (1711 → 1712) с пересборкой `CodeBase.exe`.
- Вне scope: include-резолвинг `#include <macros.h>` из индекса и раскрытие ядровых макросов (отдельное изменение); индексация `#define` из `.h`-файлов.
