# BUG: `query callers` не показывает вызовы SQL-процедур из `SMF/JS` через `ExecProc`

## Кратко
Команда `query callers --procedure <PROC_NAME>` не возвращает вызовы из `SMF/JS`, если процедура вызывается через `ExecProc("<PROC_NAME>", ...)`.

## Ожидаемое поведение
`query callers --procedure UndoConsSale_PurchPortfolio --json` должен включать вызовы из:
- `fa-contracts/Consumer/Scripts/Consumer/ConsPurchase_Portfolio.smf`
- `fa-contracts/Consumer/Scripts/Consumer/ConsSale_Portfolio.smf`
- `fa-contracts/Consumer/Scripts/Consumer/TS_Cess_PurchasePortf.smf`
- `fa-contracts/Consumer/Scripts/Consumer/TS_Cess_SellPortfolio.smf`

## Фактическое поведение
`query callers` показывает только вызовы из SQL (`ConsSale_PurchPortfolio.sql/.t01`) и не показывает SMF/JS-вызовы через `ExecProc`.

## Шаги воспроизведения
1. Выполнить:
   ```powershell
   & ".\\Tools\\CodeBase\\CodeBase.exe" query callers --procedure UndoConsSale_PurchPortfolio --json
   ```
2. Проверить, что в результате нет файлов `*.smf`.
3. Выполнить текстовый поиск по репозиторию по строке `UndoConsSale_PurchPortfolio` и увидеть вызовы `ExecProc(...)` в SMF.

## Технический анализ (корень проблемы)
1. Парсер JS корректно извлекает `ExecProc` в `ProcedureCalls`:
   - `Tools/CodeBase/Source/internal/parser/js/js_parser.go`
   - `execProcRe`, заполнение `result.ProcedureCalls`
2. Индексатор не сохраняет `ProcedureCalls` и не строит relation `calls_procedure` для JS/SMF:
   - `Tools/CodeBase/Source/internal/indexer/indexer.go`
   - в `parseJSFile` обрабатываются `Functions` и `QueryCalls`, но не `ProcedureCalls`
   - в `parseSMFFile` (JS-часть SMF) также не обрабатываются `ProcedureCalls`
3. `query callers` строится по таблице `relations`, поэтому SMF/JS-вызовы теряются:
   - `Tools/CodeBase/Source/internal/query/query_sql.go`
   - `FindCallers(...)` читает только `relations` с `relation_type in ('calls_procedure', 'dispatches_to_subscriber')`

## Влияние
- Неполный граф вызовов.
- Ложное ощущение, что процедура вызывается только из SQL.
- Снижение качества impact-анализа и root-cause анализа по продукту.

## Предложение по исправлению
1. На этапе индексации JS/SMF:
   - резолвить `ProcedureCalls[].ProcName` в `sql_procedures.id`;
   - создавать `relations`:
     - `source_type = 'js_function'` (или `smf_instrument`/`query_fragment` по принятой модели),
     - `target_type = 'sql_procedure'`,
     - `relation_type = 'calls_procedure'`,
     - `line_number = ProcedureCalls.LineNumber`.
2. Убедиться, что `FindCallers` уже включит эти связи (запрос в целом готов, так как допускает `source_type = 'js_function'`).
3. Добавить regression-тест:
   - фикстура с `ExecProc("Test_Proc")` в JS/SMF,
   - проверка, что `query callers --procedure Test_Proc` возвращает источник из этого файла.

## Приоритет
Средний (может быть повышен до высокого для команд, активно использующих `query callers` для анализа связей).
