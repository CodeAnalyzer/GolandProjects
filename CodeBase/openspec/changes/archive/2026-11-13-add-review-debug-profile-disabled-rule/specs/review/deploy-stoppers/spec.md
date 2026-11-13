## ADDED Requirements

### Requirement: Включённое отладочное профилирование

Система SHALL обнаруживать в проверяемом файле активную препроцессорную директиву `#define PROFILE` (директиву, не находящуюся в комментарии — строчном `--` или блочном `/* */` — и не входящую в строковый литерал), совместно с которой в файле присутствует хотя бы одна точка профилирования, и формировать finding `debugProfileEnabled` severity 1. Точка профилирования — это прямой вызов макроса `PROFILE_BEGIN`, `PROFILE_BEGIN_EX`, `PROFILE_TIME`, `PROFILE_TIME_EX`, `PROFILE_TIME_VAR1`, `PROFILE_TIME_VAR2`, `PROFILE_TIME_VAR3`, `PROFILE_END` или `PROFILE_END_EX`, либо каркасный макрос-носитель, тело которого в общем заголовке `MACROS.H` разворачивается в профилировочные вызовы: `RETURN_PROFILE`, `RETURN_PROFILE_EX`, `DCL_PROC_BODY`, `DCL_PROC_END`, `COMMIT_TRAN`, `__END_TRAN__`, `__BEGIN_PROCEDURE__`, `__END_PROCEDURE__`, `__CHECK_ERROR__`, `M_TRACE_DCL_PROC_END_START`. Если файл содержит активный `#define PROFILE`, но точек профилирования в нём нет, finding MUST NOT формироваться: без точек макросам нечего разворачивать, профилирования не будет. Если файл содержит директиву `#include <macros.h>` (без учёта регистра), а первый активный `#define PROFILE` расположен в файле после неё, finding MUST NOT формироваться: препроцессор обрабатывает заголовок линейно, и к моменту обработки `MACROS.H` макрос `PROFILE` ещё не определён — профилировочные макросы уже развернуты в пустые строки, поздний define ничего не включает. Директива `#define PROFILE_AVAILABLE` и директива `#undef PROFILE` данным правилом не рассматриваются (первая отличается именем макроса, вторая безопасна). Finding MUST формироваться не более одного раза на файл — на строке первого активного `#define PROFILE` в координатах исходного файла. Правило применяется к файлам с расширением `.sql` или `.h` (сравнение без учёта регистра); на файлах с другими расширениями finding MUST NOT формироваться.

#### Scenario: Активный define с явным профилировочным вызовом

- **GIVEN** SQL-файл с директивой `#define PROFILE` в первой строке и вызовами `PROFILE_BEGIN_EX('Cons_MassOverdraft - start')` / `PROFILE_END_EX(...)` в теле
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `debugProfileEnabled`, severity 1, на строке первого `#define PROFILE`

#### Scenario: Активный define с каркасным макросом-носителем без явных вызовов

- **GIVEN** SQL-файл с директивой `#define PROFILE`, `#include <macros.h>` и обвязкой процедуры через `DCL_PROC_BEGIN(Имя)` + `DCL_PROC_BODY`/`DCL_PROC_END`, без явных `PROFILE_*` вызовов в исходнике
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `debugProfileEnabled`, severity 1 — `DCL_PROC_BODY`/`DCL_PROC_END` разворачиваются в `PROFILE_BEGIN_EX`/`PROFILE_END_EX`

#### Scenario: Закомментированный define

- **GIVEN** SQL-файл с закомментированной директивой `-- #define PROFILE` и вызовами `PROFILE_BEGIN_EX(...)`/`PROFILE_END_EX(...)`
- **WHEN** выполняется review
- **THEN** finding `debugProfileEnabled` не формируется — макросы разворачиваются в пустую строку

#### Scenario: Define отсутствует

- **GIVEN** SQL-файл с `#include <macros.h>` и вызовами `PROFILE_BEGIN_EX('Cons_AimCnt_Delete - start')`/`PROFILE_END_EX(...)`, без директивы `#define PROFILE`
- **WHEN** выполняется review
- **THEN** finding `debugProfileEnabled` не формируется

#### Scenario: Активный define без точек профилирования

- **GIVEN** SQL-файл с активной директивой `#define PROFILE` и `#include <macros.h>`, но без прямых `PROFILE_*` вызовов и без каркасных макросов-носителей
- **WHEN** выполняется review
- **THEN** finding `debugProfileEnabled` не формируется — профилирования в этом файле не будет

#### Scenario: Define в блочном комментарии

- **GIVEN** SQL-файл, где `#define PROFILE` находится внутри блочного комментария `/* ... */`, и с вызовом `PROFILE_TIME(...)`
- **WHEN** выполняется review
- **THEN** finding `debugProfileEnabled` не формируется

#### Scenario: Define в строковом литерале

- **GIVEN** SQL-файл со строковым литералом `select '#define PROFILE'` и с активной директивой `#define PROFILE` выше по тексту
- **AND** файл содержит вызов `PROFILE_END_EX(...)`
- **WHEN** выполняется review
- **THEN** сформирован ровно один finding — на строке активной директивы, вхождение внутри литерала не учитывается

#### Scenario: Определение PROFILE_AVAILABLE не является нарушением

- **GIVEN** SQL-файл с директивой `#define PROFILE_AVAILABLE` и вызовами `PROFILE_BEGIN_EX(...)`
- **WHEN** выполняется review
- **THEN** finding `debugProfileEnabled` не формируется — правило проверяет только `#define PROFILE`

#### Scenario: Несколько активных define в одном файле

- **GIVEN** SQL-файл с тремя активными директивами `#define PROFILE` на строках 1, 40 и 120 и с точками профилирования
- **WHEN** выполняется review
- **THEN** сформирован ровно один finding `debugProfileEnabled` — на строке 1 (первое вхождение)

#### Scenario: Include-заголовок с активным define

- **GIVEN** `.h`-файл с активной директивой `#define PROFILE`, вызовами `PROFILE_BEGIN_EX(...)`/`PROFILE_END_EX(...)` в теле включаемых макросов
- **WHEN** выполняется review этого файла
- **THEN** сформирован finding с rule `debugProfileEnabled`, severity 1

#### Scenario: Директива undef PROFILE безопасна

- **GIVEN** SQL-файл с директивой `#undef PROFILE` и вызовами `PROFILE_BEGIN(...)`
- **WHEN** выполняется review
- **THEN** finding `debugProfileEnabled` не формируется

#### Scenario: Define после включения macros.h

- **GIVEN** SQL-файл с `#include <macros.h>` в строке 2, активным `#define PROFILE` в строке 5 и вызовом `PROFILE_TIME_EX(...)` в строке 105
- **WHEN** выполняется review
- **THEN** finding `debugProfileEnabled` не формируется — заголовок раскрыл профилировочные макросы в пустые строки до срабатывания define

#### Scenario: Расширение SQL в верхнем регистре

- **GIVEN** файл `LastDateCalc_New.SQL` (расширение в верхнем регистре) с активным `#define PROFILE` и точками профилирования
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `debugProfileEnabled`, severity 1

#### Scenario: Файл с другим расширением

- **GIVEN** файл `script.inc` с активным `#define PROFILE` и точкой профилирования
- **WHEN** выполняется review
- **THEN** finding `debugProfileEnabled` не формируется — правило применяется только к `.sql`/`.h`
