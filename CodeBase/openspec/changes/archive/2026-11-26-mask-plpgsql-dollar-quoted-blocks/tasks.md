## 1. Сканер доллар-кавычек

- [x] 1.1 Создать `internal/review/review_plpgsql_mask.go` с функцией
  `maskDollarQuotedBlocks(content string) string`: однопроходный автомат
  состояний (normal / single-quote с удвоением `''` / line-comment `--` /
  block-comment `/* */` / dollar-quote с тегом), грамматика тега
  `$[A-Za-z_0-9]*$`, закрытие — парный тег, маска — пробелы с сохранением
  переводов строк `\n` и `\r` (маска идёт по raw до CRLF-нормализации в
  `replaceMacros` — как `maskBlockCommentsKeepLines`). Guard: серия из трёх
  и более подряд `$` не открывает и не закрывает область (пропуск серии
  целиком, симметрично для открывающего и закрывающего тега). Проверка:
  юнит-тесты на синтетических кейсах (тело `$$…$$`, тег `$body$…$body$`,
  `$$$$$$$$` внутри литерала, `$$` в комментарии, файл с CRLF-концами строк,
  строка-маркер `$$$NPPBEGIN$$$` / `$$$FAKE_…` без маскирования,
  незакрытый тег → fail-open без изменений).
- [x] 1.2 Добавить обратное расширение маски до `create`-заголовка
  (`(?i)^\s*create\s+(or\s+replace\s+)?(function|procedure)\b`, барьер `;`)
  и убедиться, что `do $$…$$;` маскируется без расширения. Проверка:
  юнит-тесты (сигнатура с `= null` попадает в маску; create-заголовок
  отсутствует/за `;` → маска только тела).

## 2. Интеграция в пайплайн review

- [x] 2.1 Встроить вызов в `internal/review/runner.go` (`runSQLFileCtx`):
  `maskDollarQuotedBlocks(raw)` до `replaceMacros`; `rawContent` остаётся
  немаскированным. Проверка: `go test ./internal/review` — существующие
  тесты проходят без правок (line-preserving, SourceMap не затронут).

## 3. Тесты по сценариям спеки

- [x] 3.1 Юнит-тесты review-пайплайна, покрывающие сценарии дельта-спеки:
  чистый PG-файл → 0 findings; смешанный файл → findings MSSQL-ветки
  сохранены (rule/object/line), PG-ветки отсутствуют; `var_param1
  DSIDENTIFIER = null` в PG-сигнатуре → nullComparison молчит; код после
  литерала с `$$$$$$$$` всё ещё анализируется; `create procedure` без
  доллар-кавычек анализируется как раньше. Проверка: `go test
  ./internal/review`.

## 4. Приёмка на корпусе FA

- [x] 4.1 ADM_STPPGEXECUTE.sql (fa-administrator, Administrator Server
  Core): findings 27 → 0. Проверка: `CodeBase.exe review --files <файл>
  --json` до/после, диф в `Tests\Temp`.
- [x] 4.2 RPT_Exec.sql (fa-reports, Reports Server Scripts): findings
  38 → 4, все 4 — из MSSQL-ветки (tableHintExists+useEqColumn @107,
  datatype @109/@139), line совпадают с прежним прогоном. Проверка: тот же
  CLI-прогон с дифом.
- [x] 4.3 Батч 45 смешанных файлов (dollar-quoting + DCL_PROC_BEGIN; выборка
   воспроизводима, без worktree-дублей fa-contracts-11016/11017):
   `$dq = rg -l -i -P 'language\s+plpgsql|\bdo\s+\$[A-Za-z_0-9]*\$|\bas\s+\$[A-Za-z_0-9]*\$' -g '*.sql' -g '!fa-contracts-11016/**' -g '!fa-contracts-11017/**' D:\GITHUB\GolandProjects\FA`,
   `$dcl = rg -l 'DCL_PROC_BEGIN' -g '*.sql' -g '!fa-contracts-11016/**' -g '!fa-contracts-11017/**' D:\GITHUB\GolandProjects\FA`,
   пересечение `$dq | Where-Object { $dcl -ccontains $_ }`; список в
   `Tests\Temp` через `cmd /c "… > файл"` (PowerShell `>` пишет UTF-16 и
   ломает `--files`): в findings не осталось строк из plpgsql-областей.
   Проверка: per-file прогон (позиционный аргумент, не `--files` — это
   флаг списка путей), 50 с таймаут: 25/45 отревьюены (20 файлов висят в
   обеих сборках — до-масочный дефект макрорасширения на кириллических
   `#define`, кандидат fix-cyrillic-macro-names); findings 1068 → 36
   (−1032, добавлений 0); RPT_F350: вис в baseline → PASS 48.8 с.
- [x] 4.4 Эквивалентность на MSSQL-корпусе: прогон батча файлов без
   dollar-quoting (например, fa-generalledger Server Scripts) до/после —
   0 diff по findings. Проверка: равномерная выборка 623/9966 .sql
   fa-generalledger (0 кириллических define, 0 dollar-quoting), обе
   сборки 623/623 PASS, диф по (rule|line|object): removed=0, added=0
   (дубли basename MB_BalResource_Update/MB_GLInstTypeAttr_select
   сверены отдельно).
- [x] 4.5 Батч всех 228 dollar-quoting-файлов (выборка воспроизводима:
   `rg -l -i -P 'language\s+plpgsql|\bdo\s+\$[A-Za-z_0-9]*\$|\bas\s+\$[A-Za-z_0-9]*\$' -g '*.sql' -g '!fa-contracts-11016/**' -g '!fa-contracts-11017/**' D:\GITHUB\GolandProjects\FA`;
   naive `rg -l '\$\$'` не годится — пропускает `$tag$`-файлы и ловит `$$` в
   литералах): спот-чек оставшихся findings на ложность/истинность. Проверка:
   per-file прогон обеих сборок: 207 файлов PASS в обеих (findings 7108 →
   454, удалено 7356, добавлено 0), 20 висят в обеих (до-масочный дефект),
   1 (RPT_F350) вис в baseline → PASS с маской; спот-чек: оставшиеся
   findings — в T-SQL-ветках (RPT_Exec 4 реальных @107/@109/@139,
   DistrObjProc exec/procParamDef на T-SQL, `$$`-в-комментарии файлы не
   затронуты).
- [x] 4.6 Целевой прогон ClientMacros.sql (fa-administrator, SystemMonitoring
   SERVER Log — файл с `$$$`-маркерами в обычном коде, в набор 4.5 не входит):
   findings до/после — 0 diff (маркеры макросов не маскируются).
   Проверка: CLI-прогон до/после: base=1 (tableExistsInMetadata) =
   masked=1, removed=0, added=0.

## 5. Финализация

- [x] 5.1 `go vet ./...` и `go test ./...` чисто (34 пакета ok); bump
    `buildNumber` в `cmd/root.go` (29): `"1721"` → `"1722"`, пересборка
    `CodeBase.exe`, контрольный повтор 4.1–4.2 на новом бинарнике
    (ADM 0; RPT_Exec 4: tableHintExists+useEqColumn @107, datatype
    @109/@139).

## 6. Отработка code-review (N1, N2; N3 — осознанное ограничение)

- [x] 6.1 Устранить замечания ревью кода N1/N2 — оба класса «молчаливая
    потеря находок» из-за over-mask: N1 — подсчёт глубины вложенных
    блочных комментариев (`/* /* */ */`, PG допускает вложенность; раньше
    преждевременный выход из комментария мог открыть ложную область и
    замаскировать реальный код); N2 — грамматика тега
    `$([A-Za-z_][A-Za-z_0-9]*)?$` (первый символ — буква/подчёркивание,
    как unquoted-идентификатор PG; `$1$`-псевдотеги не открывают область).
    N3 (барьер `;` без учёта литералов) оставлен ограничением — класс
    видимого FP, в корпусе 0 вхождений (см. design Risks). Проверка:
    юнит- и пайплайн-тесты (вложенный комментарий с `$$`, пара `$1$` с
    нарушением внутри), дифференцирующая сила доказана прогоном на старом
    сканере (4 кейса падают); `go test ./internal/review` ok, vet чист;
    design D3 и дельта-спека дополнены (клауза + 2 сценария),
    `openspec validate --strict` чист.
- [x] 6.2 Bump `buildNumber` 1722 → 1723, пересборка `CodeBase.exe`
    (0.9.5 build 1723, health ok), контрольный повтор приёмки: 4.1 ADM=0,
    4.2 RPT_Exec=4 (tableHintExists+useEqColumn @107, datatype @109/@139),
    4.6 ClientMacros=1; повторный батч 208 reviewable dq-файлов (228 минус
    20 висячих в обеих сборках — до-масочный дефект макрорасширения) на
    сборке 1723: 208/208 PASS, диф по (rule|line|object) относительно
    прогона со старой маской — removed=0, added=0, regressions=0
    (`Tests\Temp\mask_dq\run_masked_1723.tsv`, `diff_228_1723.txt`);
    `go vet ./...` чист, `go test ./...` — 34 пакета ok.
