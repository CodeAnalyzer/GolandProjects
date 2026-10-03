## 1. Верификация перед реализацией

- [x] 1.1 Проверить SQL-запросом к живому индексу отсутствие легитимных таблиц с префиксом `M_`/`#M_` (выполняется вручную; если найдены — использовать fallback: явный список хинтов вместо префиксного фильтра, см. design.md D3); зафиксировать результат в design.md. Запросы (regex `~*`, НЕ `ilike` — в LIKE `_` это wildcard одного символа и даёт ложные совпадения):
  ```sql
  -- 1. Имена с префиксом M_/#M_ по всем контекстам
  SELECT table_name, context, count(*) AS rows
  FROM sql_tables
  WHERE table_name ~* '^#?m_'
  GROUP BY table_name, context
  ORDER BY rows DESC;

  -- 2. Уточнение: есть ли среди них DDL (legit-таблицы)
  SELECT table_name, count(*) AS rows
  FROM sql_tables
  WHERE context = 'create'
    AND table_name ~* '^#?m_'
  GROUP BY table_name
  ORDER BY table_name;
  ```
  Статус 2026-10-02 — выполнено: запрос 2 (context='create') вернул 0 строк → легитимных `M_`-таблиц с DDL нет, префиксный фильтр безопасен, fallback не нужен. Запрос 1 вернул только хинты-макросы в стейтмент-контекстах: `M_ISOLAT`/`#M_ISOLAT` в select (1586/890), `M_FORCEORDER`/`#M_FORCEORDER` в update (891/1285) и insert (549/562), `M_KEEPPLAN` в insert/update (495/442) — загрязнение охватывает select/insert/update, фильтр в `isIgnoredTableName` покрывает все ветки извлечения. Имена вида `##M_TABNAME##`, `##mTable##` — макро-плейсхолдеры временных таблиц (начинаются с `##`, фильтром `^#?M_` не задеваются), вне скоупа.

## 2. Фикс парсера (`internal/parser/sql/sql_parser.go`) и индексера (`internal/indexer/runner.go`)

- [x] 2.1 Отключить включение list-режима после `UPDATE`-строки: `inFromTableList = !p.updateRe.MatchString(line)` в ветке `tableNameRe` (:1422); верификация — `go test ./internal/parser/sql/`
- [x] 2.2 Дополнить стоп-лист ветки продолжения (:1443–1455) префиксами `set ` и `values `; верификация — юнит-тест из 3.3
- [x] 2.3 Добавить в `isIgnoredTableName` фильтр идентификаторов `^#?M_[A-Za-z0-9_]+$` (хинт-макросы); верификация — юнит-тест из 3.4
- [x] 2.4 Реализовать аккумуляцию `SET`-части многострочного `UPDATE`-стейтмента (от строки с `set` до `from`/`where`/конца) и разбор assignments существующим механизмом (:1396–1420) с записью колонок в `result.Columns` с привязкой к таблице `UPDATE`; верификация — юнит-тест из 3.5
- [x] 2.5 Добавить в `isIgnoredTableName` фильтр имён, начинающихся с `##` (макро-плейсхолдеры шаблонов DDL); перед включением проверить распределение `##`-имён запросом к индексу (если есть легитимные lowercase `##temp`-таблицы — сузить паттерн до обёрнутых `##x##`/ALL-CAPS, см. design.md D4); верификация — юнит-тест из 3.8. Распределение проверено 2026-10-02: все `##`-имена (~210 строк, 42 уникальных) — плейсхолдеры, легитимных `##temp` нет → широкий фильтр безопасен

- [x] 2.6 Фикс семантики флага `--modified` (решение от 2026-10-02, design D6): в `UpdateCtx` ставить `walker.SetPreFilter(fingerprints)` только при `onlyModified` (`internal/indexer/runner.go:161`), чтобы `--modified=false` давал полный перепрох; верификация — `go test ./internal/indexer/` и на FA: `codebase update --modified=false` → `files_indexed ≈ files_scanned` (scan 27: 134 738 из 145 341, errors=0, ~4ч39м)
- [x] 2.7 Остаточные векторы из верификации scan 27 (первый полный перепрох): `isIgnoredTableName` добавлен в ветки `insertTableRe` и `deleteTableRe` (плейсхолдеры `##` в insert/delete); `hintMacroRe` сделан case-sensitive `^#?M_[A-Z0-9_]+$` — lowercase `m_table` в delete — легитимная мем-таблица FA, не хинт; стоп-префикс `where` без пробела (случай `where-- ...` оставлял флаг списка живым → `MBBufferID` попадал в `sql_tables`); верификация — `go test ./internal/parser/sql/` (тесты WhereCommentContinuation, InsertDeleteIgnorePlaceholders)

## 3. Юнит-тесты по сценариям дельты (`internal/parser/sql`)

- [x] 3.1 Тест «многострочный UPDATE SET»: воспроизведение `Cons_GetDocToProcess.sql:813-821` — в таблицах только `pCreditDocument` и `pAPI_FO_Template`, LHS-идентификаторы (`TemplateSysName`, `OperationType`) отсутствуют
- [x] 3.2 Тест «UPDATE и SET в одной строке»: `update t set a = 1,` + строка `b = 2` — таблица только `t`, идентификаторы `a`/`b` не таблицы
- [x] 3.3 Тест «хинт-макрос после JOIN»: `UPDATE t ... INNER JOIN x ...` + строка `M_FORCEORDER` (и вариант `#M_FORCEORDER`) — хинт не в таблицах
- [x] 3.4 Тест «isIgnoredTableName»: `M_FORCEORDER`, `#M_FORCEORDER`, `M_KEEPPLAN` игнорируются; легитимные имена (`tContract`, `pCreditDocument`) не подавлены
- [x] 3.5 Тест «колонки многострочного SET в sql_columns»: колонки `a`/`b` из `UPDATE t SET a = 1,` + `b = 2 FROM ...` сохранены с привязкой к `t`
- [x] 3.6 Регресс-тест «список FROM через запятую»: `FROM t1,` / `t2,` / `t3` — все три таблицы извлечены
- [x] 3.7 Регресс-тест «INSERT INTO с VALUES на следующей строке»: таблица только `t`, `VALUES` и элементы списка не таблицы
- [x] 3.8 Тест «макро-плейсхолдер в шаблоне DDL»: `create table ##M_TABNAME## (ID int NULL)` — `##M_TABNAME##` не в `sql_tables`; `##OUT_COMMIS_TABLE`, `##_TABLENAME_##` также подавлены
- [x] 3.9 Тест «локальная временная таблица не задета»: `SELECT Col1 INTO #Temp FROM tContract` — `#Temp` сохранена с `is_temporary = true`
- [x] 3.10 Прогон полного пакета: `go test ./...` и `go vet ./...` — без ошибок

## 4. Верификация на FA

- [x] 4.1 Собрать бинарь и выполнить полную пересборку индекса FA фиксированным парсером. Фактически: `codebase init` на чистой БД (старая переименована; build 1549; 22:04:31–23:40:05 2026-10-02; errors=0; Tables 1 127 999, Columns 2 787 931 — +228к колонок многострочного SET, ранее терявшихся). Промежуточный прогон `update --modified=false` (scan 27: scanned=145 341, indexed=134 738, errors=0, 4ч39м) показал непрактичность пути — per-file каскадные DELETE в update-ветке; повторный прогон прерван на ~47 тыс. файлов. Доработка скорости — `Modifications/update-full-rebuild-truncate-c41d7f.md` (см. design.md D6)
- [x] 4.2 Выполнить воронки-запросы из баг-репорта. Итог (DBeaver-прогон пользователя, `Tests/SQL-пак для DBeaver.txt`, 2026-10-02, init-индекс): кандидаты без DDL 67 860 → 28 286 строк / 2 673 имён (верхняя оценка, в осн. легитимные p-таблицы); only-update 31 416/8 011 → **200/102**; целевые имена-мусор (`TemplateSysName`, `TenderDate`, `TermCalendarID`, `OperationType`, `M_FORCEORDER`, `M_KEEPPLAN`) → **0**; `##`-плейсхолдеры → **22** (delete-контекст); хинты-остатки ≈ 8 имён / ~38 строк в экзотических путях (`dfm_embedded` — вкл. склейку `pAPI_User_FindLst#M_NOLOCK_INDEX`, underscore-токены `__M_JOIN_PARTS__`, mixed-case `M_ISOlAT`). Примечание: запрос Воронки 3 был без якорей — поймал легитимные `pAPI_SM_*`/`pRpt_RTM_*` с подстрокой `M_`; фильтр парсера якорный и case-sensitive, поэтому их не задел
- [x] 4.3 Проверить кейс-репорт: `garbage_names = 0` (`TemplateSysName`/`TenderDate`/`TermCalendarID`/`OperationType` отсутствуют в `sql_tables`); колонки `SET` присутствуют в `sql_columns` для `Cons_GetDocToProcess.sql` — `OperationType`@814, `TemplateSysName`@815 (по-строчная привязка работает)
- [x] 4.4 Спот-чек отсутствия регрессий: `pCreditDocument` (127), `pAPI_FO_Template` (509), `tContract` (10 569), `tEntattrValue` (3) — на месте; выдача покрытия спеками — см. примечание: ложные «таблицы»-колонки из выдачи исчезли (целевые имена-мусор = 0)

## 5. Завершение

- [x] 5.1 Обновить статус в `Bug-reports/BUG-sql-tables-update-set-columns-as-tables-20260930.md` → Fixed (со ссылкой на change; добавлен раздел «Решение» с фиксами, верификацией и ссылками на `Tests/SQL-пак для DBeaver.txt` и `Modifications/update-full-rebuild-truncate-c41d7f.md`)
