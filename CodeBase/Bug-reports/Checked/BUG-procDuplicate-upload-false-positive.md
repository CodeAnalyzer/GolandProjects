# BUG: procDuplicate — ложное срабатывание на UPLOAD/.t01 файлах

## Описание

Правило `procDuplicate` ошибочно срабатывает, когда процедура определена в одном исходном `.sql`-файле продукта, но при этом в индексе присутствует её скомпилированный вариант из каталога `UPLOAD/` другого продукта (`.sql` и/или `.t01`).

## Воспроизведение

Файл: `fa-contracts-ext-novikom-persform/API_Credit_Ext/Server/LoanExt/API_LoanExt_FindListReqByParam.sql`

```
codebase review "...API_LoanExt_FindListReqByParam.sql" --min-severity 1
```

Результат:
```
- [1] procDuplicate line=48 object=API_LoanExt_FindListReqByParam
  Процедура создаётся в нескольких файлах
```

Проверка через `codebase query symbol`:
```json
[
  { "file": "fa-contracts-ext-develop/API_Credit_Ext/Server/UPLOAD/API_LoanExt_FindListReqByParam.sql" },
  { "file": "fa-contracts-ext-develop/API_Credit_Ext/Server/UPLOAD/API_LoanExt_FindListReqByParam.t01" },
  { "file": "fa-contracts-ext-novikom-persform/API_Credit_Ext/Server/LoanExt/API_LoanExt_FindListReqByParam.sql" }
]
```

Второй и третий вхождения — это скомпилированные артефакты (`UPLOAD/`, `.t01`), а не дубли в исходниках другого продукта.

## Корневая причина

Функция `lookupProcedureCreateFiles` (файл `internal/review/review_lookup.go`, строка ~102) выполняет запрос:

```go
SELECT DISTINCT f.id
FROM sql_procedures p
JOIN files f ON f.id = p.file_id
WHERE LOWER(p.proc_name) = LOWER($1)
ORDER BY f.id
```

Запрос не фильтрует:
1. Файлы из каталогов `UPLOAD/` (скомпилированные файлы для загрузки в БД)
2. Файлы с расширением `.t01` (препроцессированные варианты с раскрытыми макросами)

В результате для одной процедуры возвращается более одного `file_id`, и правило считает это дублированием.

## Ожидаемое поведение

Правило `procDuplicate` должно срабатывать только если процедура определена в двух или более **исходных** `.sql`-файлах (не в `UPLOAD/` и не в `.t01`).

## Предлагаемое исправление

В запросе `lookupProcedureCreateFiles` добавить фильтрацию по пути файла и расширению:

```sql
SELECT DISTINCT f.id
FROM sql_procedures p
JOIN files f ON f.id = p.file_id
WHERE LOWER(p.proc_name) = LOWER($1)
  AND f.path NOT LIKE '%/UPLOAD/%'
  AND f.path NOT LIKE '%\UPLOAD\%'
  AND LOWER(f.path) NOT LIKE '%.t01'
ORDER BY f.id
```

Либо добавить в таблицу `files` признак типа файла (`is_source`, `is_upload`, `is_preprocessed`) и фильтровать по нему.

## Severity

Severity 1 (deploy stopper) — ложное срабатывание блокирует деплой без реальной причины.
