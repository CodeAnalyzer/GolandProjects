Code review: dsinfo-integration

Заключение

Статус: принято с доработками. Архивировать change пока нельзя.

Архитектурно срез C ложится на design: контекст профиля вынесен в dsinfosvc → querysvc → query.ProfileContext, границы CLI/MCP соблюдены, RTI/TRC enrichment получает product/branch_used. Сборка и статический анализ чистые, openspec validate dsinfo-integration --strict — valid. Но есть два дефекта уровня контракта (warning-семантика и продукты с несколькими линиями в манифесте) и несколько существенных замечаний.

Что проверено
go build ./... — без ошибок.
go vet ./... и go vet -tags integration ./... — без ошибок.
go test -count=1 ./cmd ./internal/mcp ./internal/branchkey ./internal/dsinfo ./internal/query ./internal/rtisvc ./internal/trcsvc ./internal/querysvc — зелёные.
openspec validate dsinfo-integration --strict — valid.
Интеграционные тесты в рамках ревью не запускались: они пишут в БД. Проверена только их компиляция с -tags integration.
В рабочем дереве незакоммиченные срезы B и C одновременно; C всё ещё в openspec/changes/, не архивирован.

P1 — блокирующие замечания
1. Предупреждения считаются по профилю, а не по фактическому ответу
dsinfosvc.ResolveAnalysisContext генерирует warnings только для строк со статусом branch_not_indexed (internal/dsinfosvc/context.go:118-158). Эти warnings затем добавляются в каждый ответ query/rti/trc.
Это даёт два дефекта относительно требования «Семантика fallback с явным предупреждением»:
а) Пропускается реальный mismatch. Если линия продукта проиндексирована (статус ok), но конкретной сущности в этой линии нет, а копия нашлась в другой ветке, ответ вернёт branch_requested != branch_used, но warning не будет — профиль считает линию найденной. Пример: MyProc есть только в develop, а профиль указывает на fa-contracts-11-01-7, которая проиндексирована, но не содержит MyProc.
б) Мошка предупреждений. На реальном манифесте (62 строки, проиндексированы далеко не все) почти каждый запрос будет нести десятки предупреждений о продуктах, которых запрос вообще не касался. Design D4 говорит об агрегации «одна строка на (продукт, запрошенная линия, использованная линия)», то есть о warnings по фактическим результатам, а не по всему профилю.
Рекомендация: собирать warnings после выполнения запроса из элементов результата (пара branch_requested/branch_used), агрегируя по продукту; ResolveAnalysisContext.Warnings оставить только для dsinfo parse/show либо удалить из анализа. Нужны тесты: «линия есть, сущности в ней нет», «сущность только в develop», «ответ без результатов — warnings нет».

2. Продукт с несколькими линиями в манифесте теряет линии
В реальном Modifications/DiasoftSystemInfo.csv у fa-contracts-ext одновременно присутствуют линии fa-contracts-ext-7-02-2 и fa-contracts-ext-9-01-553 (8 строк манифеста), у fa-payments-ext — 3 строки.

ResolveAnalysisContext хранит Branches map[productID]branchID и Requested map[productID]string (internal/dsinfosvc/context.go:118-139), а querysvc.ResolveQueryProfile перекладывает это в map[productName]string (internal/querysvc/runtime.go:44-55). При нескольких строках одного продукта последняя строка перезаписывает предыдущую.
Последствия:
- если проиндексирована только 7-02-2, а последняя строка 9-01-553 не проиндексирована, fallback перезапишет корректную OK-линию на default-ветку (например, develop), и запрос покажет не тот код;
- branch_requested будет относиться к последней строке, а не к выбранной линии;
- при двух проиндексированных линиях одна из установленных линий игнорируется без диагностики.
Нужно либо зафиксировать инвариант «одна линия на продукт» и явно диагностировать его нарушение, либо выбрать детерминированную политику среди OK-линий (OK приоритетнее fallback, затем версия/порядок) и не перекрывать OK-линию fallback'ом. Нужен интеграционный тест на fa-contracts-ext с двумя разными branch_key.

P2 — существенные замечания

3. Несуществующий --dsinfo местами молча игнорируется
Требование цепочки говорит: несуществующий id из любого источника — ошибка (dsinfo-resolution). Фактически:
query callers/relations/inspect идут через executeQueryPlain (cmd/query_execution.go:26-35, cmd/query_commands.go:41,63,151), поэтому --dsinfo 999 не проверяется;
RTI/TRC резолвят профиль только внутри guard'ов «есть что enrich'ить» (internal/rtisvc/runtime.go:268,328,420,469, internal/trcsvc/runtime.go:507,528), поэтому на пустой сессии --dsinfo 999 тоже не даёт ошибки.
Или валидировать explicit/env id в начале команды независимо от поверхности (дешево), или явно ослабить требование и documentировать это в spec/design.

4. Fallback по числовым токенам ломается на продуктах с цифрами в имени
branchkey.NumericTokens извлекает цифры из любых токенов (internal/branchkey/branchkey.go:88-106). Для реального продукта fa-ftfns440reqprocadapter запрошенная последовательность получается [440, 11, 01, 11], а кандидат sup/11-01-11 даёт [11, 01, 11] → длины не совпадают, fallback не срабатывает (NumericTailMatches, строки 115-128). Точное совпадение со стандартной веткой спасает, но нестандартные sup/... линии для этих продуктов не матчатся.
Рекомендация: токенизировать только чисто числовые сегменты (игнорировать цифры внутри буквенных токенов) либо сравнивать версию после префикса продукта. Добавить тест на fa-ftfns440reqprocadapter + sup/11-01-11.

5. Сводка matched N/M products считает строки манифеста, а не продукты
ResolveProfile увеличивает matched/missing на каждый item, а ProductsTotal = len(items) (internal/dsinfosvc/runtime.go:130-146). На реальном CSV 62 строки, но fa-contracts-ext занимает 8 строк, fa-payments-ext — 3. Формулировка «products» вводит в заблуждение. Либо считать distinct product_name, либо переименовать в «package lines» и синхронизировать spec.

6. Пробелы в тестах относительно tasks.md
Не покрыты:
продукт с несколькими разными линиями (реальный fa-contracts-ext);
реальный warning-mismatch «линия OK, сущности в ней нет»;
--dsinfo/CODEBASE_DSINFO_ID на boundary-командах и на пустых RTI/TRC-сессиях;
профильный выбор для API (api_impl/publishers/consumers) и лексического desc-search;
продукт вне профиля на уровне entity-выборки;
fallback по нестандартной линии sup/11-01-7 на уровне entity-запроса (сейчас проверен только матчинг профиля);
продуктовые имена с цифрами в fallback.

7. Расхождение design и кода
Design D2 описывает ProfileContext.Requested как map[int64]string (design.md:105-108), в коде — map[string]string (internal/query/query_profile.go:11-15). Design D4 описывает агрегацию warnings по фактической паре requested/used; реализована профильная агрегация. Артефакты надо привести в соответствие через openspec-update-change — иначе после архивации в спеках останется неверная модель.

P3 — замечания
- loadAPIContractParams/loadAPIContractTables вызывают q.branchRequested(item.Product), но item.Product в этих функциях не заполняется (internal/query/api_query.go:556,589). Сейчас безвредно, потому что вызывающий код перезаписывает поля, но это мёртвая логика.
- MCP-обработчики rti_tree/errors/slow/details читают dsinfo_id как dsinfoID, _ := optionalInt(...) (internal/mcp/registry.go:850,872,887,906), тогда как остальные инструменты проверяют ошибку. Невалидный тип параметра молча превращается в 0.
- CLI RTI в человекочитаемом режиме не печатает result.Warnings (и product/branch_used), хотя TRC печатает (cmd/trc.go:482,538; cmd/rti.go:189,271,331). Fallback-warning виден только в JSON.
queryWarnings — package-level mutable state (cmd/query_execution.go:20); для CLI безопасно, для параллельных тестов/встраивания — риск.
- executeQuery использует context.Background() вместо контекста команды (cmd/query_execution.go:43), отмена Ctrl+C не доходит до открытия БД/резолва профиля.
- При branch_not_indexed, если у продукта вообще нет линий, warning формируется с пустым defaultKey и утверждает «показан код default-ветки» (context.go:148-158). Нужен отдельный текст/пропуск.
- Формулировка «линии X, Y продукта P не проиндексирована» при нескольких линиях грамматически неверна (не критично).
- SearchDescriptions схлопывает exact-хиты по (kind, name) независимо от продукта (desc_query.go:104-120); при одинаковом имени в двух продуктах профиль не гарантирует, что останется копия продукта профиля. Это пограничный случай, но стоит учесть вместе с профильной приоритизацией.

Итоговая рекомендация

Сначала исправить P1: response-level warnings и multi-line products.
Затем P2: валидация id на всех путях, токенизация fallback, сводка, тесты.
Привести design/tasks/spec в соответствие с фактической семантикой; повторно прогнать openspec validate --strict.
Сейчас код готов к принятию как рабочая реализация happy-path, но не как завершённый и архивируемый change.