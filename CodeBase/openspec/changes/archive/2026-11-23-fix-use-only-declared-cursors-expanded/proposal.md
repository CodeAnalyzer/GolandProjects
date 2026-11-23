## Why

Правило `useOnlyDeclaredCursors` даёт ложные срабатывания на файлах, где перед процедурами есть многострочные `#define`: правило сканирует сырой текст файла (`os.ReadFile`), но границы процедур берёт из `parsed.Procedures`, то есть из expanded-текста после раскрытия/вырезания макросов. Сдвиг координат отправляет проверку в тело предыдущей процедуры, из-за чего объявленный курсор выглядит необъявленным, а номер строки в finding указывает не туда.

Реальный пример: `FA/fa-generalledger/.../Trunc_GL.sql`, build 1714 — 4 FP на `Truncate_GL_Resource_ResArrest` (`cur_Oper`), потому что 56-строчный `#define __BALANCE_CLEAR__` (строки 8–63) вырезан из expanded, и окно проверки сдвинулось на 56 строк назад.

## What Changes

- Правило `useOnlyDeclaredCursors` MUST читать контент через expanded-контент review-сессии (`r.fileContent`), а не через `os.ReadFile(file.Path)`, чтобы содержимое и номера строк были в той же системе координат, что и `parsed.Procedures`.
- Объявления курсоров (`DECLARE ... CURSOR FOR`, `__DECLARE_CURSOR__(name)`) и их использования (`OPEN`/`FETCH`/`CLOSE`/`DEALLOCATE`) собираются и проверяются на одном expanded-тексте в границах процедуры.
- Номер строки finding'а MUST указывать на строку фактического использования в исходном файле (после централизованного ремапа expanded → original), а не на сдвинутую строку.
- Добавляются регрессионные сценарии: файл с многострочным `#define` перед процедурами даёт 0 findings при корректно объявленном курсоре; реальное использование необъявленного курсора по-прежнему ловится.

## Capabilities

### New Capabilities
(нет)

### Modified Capabilities
- `review/postgre-requirements`: требование «Использование курсоров без объявления» уточняется: сбор объявлений и использований выполняется в expanded-пространстве, номер строки finding'а соответствует исходному файлу.

## Impact

- `internal/review/review_rules.go` — `checkUseOnlyDeclaredCursors`: источник контента для разбиения на строки.
- `internal/review/review_rules_test.go` — регрессионные тесты на сдвиг координат из-за `#define`.
- Поведение CLI `review` и MCP `codebase_review_sql` (устраняется FP и неверные line), без изменения формата вывода и severity.
- Аудит: другие правила, читающие `os.ReadFile` напрямую (`checkShouldBeCP866` — оправдано, анализирует байты кодировки и не опирается на номера строк), на смешение raw/expanded координат.
