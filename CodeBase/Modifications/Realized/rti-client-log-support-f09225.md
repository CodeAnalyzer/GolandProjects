# Поддержка клиентских RTI-логов в `codebase rti`

Добавить в `codebase rti` парсинг и анализ клиентских трейс-логов Diasoft 5NT (Debug.d5ntsys/SQL/SQL_TranCount/Error.*) наряду с уже поддержанными серверными (Trace.Server.*), включая файлы со смешанным содержимым, с полной моделью клиентских событий, связыванием client SQL exec → server Enter и персистентностью в БД.

## 1. Изученная структура клиентского лога

Формат заголовка строки-события (TSV):
```
DD.MM.YYYY HH:MM:SS.mmm\tLEVEL\tCategory\tClassName\tMethodName\tPID\tSeq\t\tNum1\tNum2
```
Категории и тела событий, встреченные в `Logs/AFEDOROV_diasoft_kdoms15_IFRS09_demo.rti`:

- **`Debug.d5ntsys` / `TDBLFileHandler.WriteAllBPL2Log`** — блок листинга загруженных BPL: строка `Текущий PID: N`, `Список загруженных BPL`, таблица `Файл / Версия / Название / Комментарий` (кириллица в CP866, колонки фиксированной ширины).
- **`Debug.d5ntsys` / `DsADORecordset.Open`** — пары `Enter`/`Exit` **без имени процедуры и без `@@TranCount`** (просто слово `Enter` / `Exit` на следующей строке). Конфликтует по имени с текущими `reEnter`/`reExit`, но не матчится ими (нет обязательных групп) — потребуется отдельный regex.
- **`SQL` / `DSConnectorADO.DoConnect`** — блок информации о соединении: `--- New Connection : SPID = N`, `--- Server`, `--- Database`, `--- User`, `--- Application Name`.
- **`SQL` / `<ObjectName>` (`DsQuery`, `Green_guy`, `.`, произвольные имена бизнес-объектов)** — заголовок с `SPID =`, `SERVER =`, `DATABASE =`, затем `PREPARED`, затем произвольный текст SQL (может быть многострочный `exec ProcName @p1 = v1, ...` или raw `select/create table/...`), заканчивается пустыми строками.
- **`SQL` / `Green_guy` (после исполнения)** — `Duration = 'N.NNN'`, `STARTED`.
- **`SQL_TranCount` / `DsQuery.WriteStartedToRTI`, `DSConnectorADO.ExecSQLDirect`** — `trancount = N`.
- **`Debug.d5ntsys` / `WriteCurrentProcessMemoryUsage`** — дамп памяти (кириллица): `Пам-ть (Delphi manager): N`, `Пам-ть (WINAPI manager): N`, `Дескрипторы: N; Объекты User: N; Объекты GDI: N`.
- **`SEVERE` / `Error.d5ntServ` / `TCodeProtection.ReportViolation`** — текст ошибки (следующая строка).
- Прочие `INFO`/`FINE` строки с произвольным `ClassName.MethodName` без специфичного тела — общий случай "простое сообщение уровня".

Ключевой факт: **PID** в заголовке (7-е поле) выполняет для клиентского лога роль, аналогичную SPID у сервера (разделение потоков/сессий клиента), но это НЕ SPID сервера — реальный SPID сервера появляется внутри `SQL`/`SQL_TranCount` блоков (`SPID = N`).

## 2. Уточнённые решения (по ответам пользователя)

1. **Глубина**: полная структурированная модель клиентских событий — каждое событие → `RTIClientEvent` с category/class/method/level/timestamp + типизированные доп.-данные для каждого вида тела (BPL-версии, connection info, SQL text/params, trancount, memory usage, error text). SQL Started/Prepared блоки образуют вложенность (дерево) по PID.
2. **Связывание с сервером**: пытаться сопоставлять `exec ProcName ...` из клиентского SQL-блока с `Enter ProcName @@TranCount ...` в серверной части того же файла — по имени процедуры + близости SPID/времени. Результат — необязательная ссылка `ServerCallID` на `RTICall`.
3. **Персистентность**: полная — новые таблицы в БД, поддержка в `SaveSession`/`LoadCalls`, работоспособность `--session` для клиентских событий как для серверных.

## 2.1 Правило кодирования: компиляция regex

Все статические `regexp.MustCompile(...)`, не зависящие от параметров функции/переменных времени выполнения (без `regexp.QuoteMeta(paramVar)` и подобной интерполяции), выносятся в package-level `var (...)` блок в начале файла — компилируются один раз при инициализации пакета, а не при каждом вызове функции. Это касается всех новых файлов из этого плана: `internal/rti/parser.go` (расширения regex, п.4), `internal/rti/link.go`, `internal/rti/enrich_client.go`. Regex, зависящие от параметров (напр. динамическая подстановка имени процедуры/таблицы через `regexp.QuoteMeta`), остаются локальными — их нельзя вынести без потери корректности.

## 3. Новые/изменяемые структуры данных (`internal/rti/model.go`)

```go
type RTIClientEvent struct {
    ID          int64
    Timestamp   time.Time
    Level       string   // INFO/FINE/SEVERE/...
    Category    string   // Debug.d5ntsys, SQL, SQL_TranCount, Error.d5ntServ, ...
    ClassName   string
    MethodName  string
    PID         int
    SeqNo       int
    Line        int
    Kind        string   // "bpl_list" | "recordset_open" | "connection" | "sql_block" | "trancount" | "memory_usage" | "error" | "generic"
    // typed payloads (заполняется по Kind, остальные — zero value)
    BPL         []RTIBPLModule       `json:",omitempty"`
    Connection  *RTIConnectionInfo   `json:",omitempty"`
    SQL         *RTISQLBlock         `json:",omitempty"`
    TranCount   *int                 `json:",omitempty"`
    Memory      *RTIMemoryUsage      `json:",omitempty"`
    ErrorText   string               `json:",omitempty"`
    RawBody     string               `json:",omitempty"` // fallback для generic/неизвестных тел
    ElapsedMs   int                  `json:",omitempty"`
    ParentID    *int64               `json:",omitempty"` // вложенность SQL-блоков по PID
    Children    []int64              `json:",omitempty"`
    ServerCallID *int64              `json:",omitempty"` // связь client exec -> server RTICall.ID
}

type RTIBPLModule struct { File, Version, Title, Comment string }
type RTIConnectionInfo struct { SPID int; Server, Database, User, AppName string }
type RTISQLBlock struct {
    SPID int; Server, Database string
    Text string           // полный текст SQL/exec
    ExecProcedure string   // извлечённое имя процедуры, если Text начинается с "exec"
    ExecParams []RTIParam  // разобранные @param = value из exec
    DurationSec float64
    State string // STARTED/PREPARED/…
}
type RTIMemoryUsage struct { DelphiKB, WinAPIKB, Descriptors, ObjectsUser, ObjectsGDI int }
```

Расширение `RTIParseResult`:
```go
type RTIParseResult struct {
    Calls         []*RTICall         // существующее (сервер)
    ClientEvents  []*RTIClientEvent  // новое (клиент)
    Summary       RTISummary
    UnparsedLines int
}
```

Расширение `RTISummary`: `ClientEventsCount int`, `ClientErrorsCount int`, `ClientSlowSQLCount int`, опционально `TopSlowClientSQL []RTIClientEvent`.

## 4. Парсер (`internal/rti/parser.go`)

- Единый проход `parseContent` научится диспетчеризовать строки в один из двух контуров:
  - Существующие regex (`reTrace`, `reEnter`, `reExit`, `reBLogHeader`, ...) — как сейчас, привязаны к серверному стеку по SPID.
  - Новые regex для клиентских заголовков и тел (см. п.5), привязаны к клиентскому стеку по PID.
- Общий построчный `scanner` остаётся один; порядок проверок regex важно выстроить так, чтобы не было ложных пересечений (например, серверный `reTrace` требует `Trace.Server.` в категории — конфликтов с `SQL`/`Debug.d5ntsys` нет, т.к. категории не пересекаются).
- Новый набор regex:
  - `reClientHeader` — общий заголовок клиентской строки: `^(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}:\d{2}\.\d{3})\t(\w+)\t([\w.]+)\t([^\t]*)\t([^\t]*)\t(\d+)\t(\d+)\t\t(-?\d+)\t(-?\d+)`. Захватывает timestamp, level, category, class, method, PID, seq, num1, num2.
  - `reBPLHeaderLine` — `^Файл\s+Версии\s+Название\s+Комментарий` (граница таблицы, после строки `===...`) — определять по фиксированным колонкам через `regexp` на пробелы, парсить последующие строки до следующей `===` или пустой строки.
  - `reClientEnterBare` / `reClientExitBare` — `^Enter$` / `^Exit$` (для `DsADORecordset.Open` и подобных методов без параметров).
  - `reNewConnection` — `^---\s+New Connection\s*:\s*SPID\s*=\s*(\d+)`; далее `--- Server`, `--- Database`, `--- User`, `--- Application Name` по отдельным regex.
  - `reSQLHeaderInfo` — `^SPID\s*=\s*(\d+)$`, `^SERVER\s*=\s*'(.*)'$`, `^DATABASE\s*=\s*'(.*)'$`, `^(STARTED|PREPARED)$`.
  - `reDuration` — `^Duration\s*=\s*'([\d.]+)'`.
  - `reTranCountClient` — переиспользовать существующий смысл `trancount = N` (уже есть паттерн в BusinessLog? Нет — добавить `reTranCountLine = ^trancount\s*=\s*(\d+)`).
  - `reMemUsageLine1/2` — парсинг кириллических строк по маркерам `(Delphi manager)`, `(WINAPI manager)`, `Дескрипторы`.
  - `reExecCall` — `^exec\s+(\w+)\s*(.*)`, с последующим разбором `@name = value` через запятую (учитывать многострочность — параметры продолжаются на следующих строках с ведущими пробелами до пустой строки или `PREPARED`/след. заголовка).
- Состояние парсера дополняется:
  - `clientStacks map[int][]*clientStackEntry` — стек SQL-блоков по PID (для вложенности Started/Prepared).
  - `pendingClientHeader` — распарсенный заголовок для последующей маршрутизации тела к нужному конструктору события.
  - `bplCapture` — аналогично `captureTable`, режим построчного чтения BPL-таблицы.
- Тело каждого события собирается по тому же принципу, что уже используется для `BusinessLog`/`Table` — заголовок переводит парсер в специфичный "режим ожидания тела", тело поглощается до признака конца (пустая строка / следующий заголовок с timestamp).

## 5. Связывание client exec ↔ server Enter (`internal/rti/link.go`, новый файл)

- После полного прохода (когда оба списка `Calls` и `ClientEvents` заполнены):
  - Функция `LinkClientServerCalls(calls []*RTICall, events []*RTIClientEvent)`:
    - Индексирует `RTICall` по `Procedure` → список (отсортированный по `EnterTime`).
    - Для каждого `RTIClientEvent` с `Kind == "sql_block"` и непустым `ExecProcedure`:
      - Ищет ближайший по времени `RTICall` с тем же именем процедуры, `EnterTime >= event.Timestamp` (с допуском окна, например ±2 сек, конфигурируемо константой), предпочитая совпадение по SPID из `SQLBlock.SPID` (сервер) если возможно сопоставить (клиентский SQL-блок содержит реальный серверный SPID в заголовке `SPID = N` — это именно то поле, которое нужно сравнивать с `RTICall.SPID`).
      - При найденном совпадении проставляет `event.ServerCallID = &call.ID`.
  - Вызывается из `parseContent` в конце, либо из `ParseFile` после парсинга.
- Точное сопоставление не гарантировано (эвристика) — документировать это в комментариях и в выводе (`matched: true/false`).

## 6. Дерево клиентских событий (`internal/rti/tree.go`)

- Добавить `BuildClientTree(events []*RTIClientEvent, maxDepth int) *RTIClientTreeNode` — по аналогии с `BuildTree`, но корни — верхнеуровневые SQL-блоки/BPL-блоки на каждый PID.
- `FormatClientTree` — текстовый вывод, показывает связанный `ServerCallID → Procedure` если есть линк.
- Рассмотреть объединённый `FormatUnifiedTimeline(calls, events)` — сортировка обоих списков по timestamp для наглядного общего вывода (используется в новой подкоманде `timeline`, см. п.8).

## 7. Хранение в БД (`internal/store/db_schema.go`, `internal/rti/store.go`)

Новые таблицы:
```sql
CREATE TABLE rti_client_events (
    id BIGSERIAL PRIMARY KEY,
    session_id BIGINT REFERENCES rti_sessions(id) ON DELETE CASCADE,
    parent_id BIGINT,
    timestamp TIMESTAMPTZ,
    level TEXT, category TEXT, class_name TEXT, method_name TEXT,
    pid INT, seq_no INT, line_no INT,
    kind TEXT,
    elapsed_ms INT,
    payload JSONB,          -- сериализованные типовые данные (BPL/Connection/SQL/Memory/Error)
    server_call_id BIGINT REFERENCES rti_calls(id)
);
CREATE INDEX ON rti_client_events(session_id);
CREATE INDEX ON rti_client_events(session_id, kind);
```
- `SaveSession` дополняется шагом `insertRTIClientEvents` (batch/CopyIn, payload как `JSONB` через `json.Marshal`).
- `LoadCalls` дополняется загрузкой `ClientEvents` для сессии (`LoadClientEvents`), включая восстановление `parent_id`/`children` и `payload` → typed struct через `json.Unmarshal` по `Kind`.
- `RTISession`/`rti_sessions` — добавить агрегаты `client_events_count` (миграция схемы, `ALTER TABLE` в `db_schema.go` при инициализации, аналогично существующему подходу).

## 8. CLI (`cmd/rti.go`)

- Существующие команды получают клиентские данные "бесплатно" через расширенный `RTIParseResult`:
  - `summary` — добавить блок "Client events: N (errors: N, slow SQL: N)".
  - `errors` — объединить серверные (`RetVal != 0`) и клиентские (`Kind == "error"` или SQL с ошибкой, если распознаётся) находки, с пометкой `[server]`/`[client]`.
  - `slow` — учитывать клиентские SQL-блоки (`DurationSec`/`ElapsedMs`) наравне с серверными вызовами, помечая источник.
- Новые подкоманды:
  - `client-tree [<file>]` — дерево клиентских событий (`--pid` вместо `--proc` опционально).
  - `timeline [<file>]` — объединённая хронологическая лента client+server событий с пометками источника и связей (`→ server call #id`).
- Флаг `--json` работает единообразно для новых команд/полей.

## 9. MCP (`internal/mcp/registry.go`)

- Существующие `codebase_rti_summary/errors/slow/tree/details` — расширить возвращаемые структуры клиентскими полями (обратная совместимость: старые поля не удаляются).
- Новые инструменты: `codebase_rti_client_tree`, `codebase_rti_timeline` (по аналогии с существующими `rti_tree`/сигнатурой `session_id`/`file_path`).

## 10. Тесты

- `internal/rti/parser_test.go`:
  - Юнит-тесты на каждый новый regex/блок (BPL-таблица, DsADORecordset bare Enter/Exit, connection info, SQL block с exec, trancount, memory usage, SEVERE error).
  - Тест на смешанный файл (синтетический fixture с частями и клиентского, и серверного формата в одном потоке) — проверить, что оба списка (`Calls`, `ClientEvents`) заполняются корректно и не мешают друг другу.
- `internal/rti/link_test.go` (новый): тесты на `LinkClientServerCalls` — совпадение по имени+SPID+временному окну, отсутствие ложных связей при несовпадении SPID.
- `internal/rti/store_test.go` (если существует, иначе добавить): round-trip `SaveSession`/`LoadCalls` для клиентских событий (потребует тестовой БД — использовать существующий паттерн интеграционных тестов проекта, если есть; иначе — таблично проверить SQL-генерацию через sqlmock, следуя текущему стилю репозитория).
- `cmd/rti_test.go` — smoke-тесты новых подкоманд с `--json`.
- Регрессия на реальных файлах: прогнать `codebase rti summary/timeline` на обоих логах из `Logs/`, убедиться что `AFEDOROV_...rti` больше не даёт `total_calls=0, unparsed_lines≈1620`, а показывает осмысленную структуру клиентских событий; `TS-DIASOFT-EXT5_...rti` не регрессирует (44 calls, как раньше).

## 11. Обогащение UI-цепочкой (`pas_classes/pas_methods/pas_fields/pas_units`, `dfm_forms/dfm_components`, `query_fragments`)

По уточнению пользователя реализуется полная цепочка **DFM-форма → PAS-метод → SQL/exec → серверная процедура**, тем же паттерном, что уже используется для серверных вызовов в `EnrichCalls`/`EnrichProcedure` (`@/D:/GITHUB/GolandProjects/CodeBase/internal/rti/enrich.go`): **без отдельного флага**, автоматически в `tree`/`errors`/`details`/новой `timeline`, с дедупликацией запросов по ключу; `parse`/`summary` обогащение не делают (остаются быстрыми).

### 11.1 Новый модуль `internal/rti/enrich_client.go`

```go
type ClientLookup interface {
    FindPASMethodsByName(methodName string, like bool, limit int) ([]query.MethodResult, error)
    SearchQueryFragment(text string, limit int) ([]query.QueryFragmentResult, error)
}

type ClientEnrichment struct {
    ClassName    string
    MethodName   string
    Unit         string               // pas_units.unit_name
    SourceFile   string
    LineNumber   int
    DFMFormName  string               // если pas_classes.dfm_form_id указывает на форму
    DFMCaption   string
    Found        bool

    // Только для sql_block/exec-событий:
    QueryFragmentFile string          // где найден похожий текст запроса
    QueryFragmentLine int
    OriginMethod      string          // метод/функция, содержащая фрагмент (из QueryFragmentResult)
}

func EnrichClientEvent(q ClientLookup, ev *RTIClientEvent) *ClientEnrichment
func EnrichClientEvents(q ClientLookup, events []*RTIClientEvent) map[string]*ClientEnrichment // ключ: ClassName+"."+MethodName
```

- **Шаг 1 — класс/метод**: по `ev.ClassName`+`ev.MethodName` вызывается `FindPASMethodsByName(methodName, like=false, limit=…)`, результат фильтруется по совпадению `ClassName` (регистронезависимо); берётся источник (unit/file/line) первого совпадения.
- **Шаг 2 — DFM-форма**: `MethodResult` уже содержит достаточно данных для повторного `SearchDFMForm`/прямого JOIN — расширить `query.FindPASMethodsByName` (или добавить новый `query.FindPASMethodWithDFMByName`) чтобы одним запросом отдавать `dfm_form_id → dfm_forms.form_name/caption` через `pas_classes.dfm_form_id`. Если класс не привязан к форме (инфраструктурные классы вроде `DsADORecordset`, `TCodeProtection`) — поле остаётся пустым, это ожидаемо и не ошибка.
- **Шаг 3 — query_fragments для exec-блоков**: только для `Kind == "sql_block"` с непустым `SQL.ExecProcedure`:
  - Нормализация текста: убрать конкретные литералы параметров (`@Param = <значение>` → `@Param`), оставить структуру вызова `exec ProcName @p1, @p2, ...` для сравнения.
  - Вызов `SearchQueryFragment(execProcedure, limit)` (поиск по тексту, как в существующем `codebase_query_sql_fragment`), затем среди результатов выбрать тот, чей нормализованный текст ближе всего к нормализованному тексту из лога (простое сравнение по набору имён параметров/подстроке, без сложного diff-алгоритма на первой итерации).
  - При совпадении — заполняется `QueryFragmentFile/Line/OriginMethod` (из `QueryFragmentResult.ParentName`/`File`/`Line`, по образцу существующего `mcp0_codebase_query_sql_fragment`).
- Все шаги — **best-effort**: при отсутствии совпадений `Found=false`/пустые поля, никаких ошибок наружу.

### 11.2 Изменения в `internal/query` (при необходимости)

- Проверить, достаточно ли текущего `FindPASMethodsByName` (`@/D:/GITHUB/GolandProjects/CodeBase/internal/query/query_sql.go:420`) или нужно добавить вариант с JOIN на `pas_classes.dfm_form_id → dfm_forms`, чтобы не делать второй отдельный запрос на каждый метод (важно для дедупликации/производительности).
- `SearchQueryFragment` (`@/D:/GITHUB/GolandProjects/CodeBase/internal/query/query.go:546`) уже возвращает нужные поля — переиспользуется как есть.

### 11.3 Интеграция в CLI/MCP

- `cmd/rti.go`: в `runRTITree`, `runRTIErrors`, `runRTIDetails` и новой `runRTITimeline` — рядом с текущим `rti.EnrichCalls(q, result.Calls)` добавить `rti.EnrichClientEvents(q, result.ClientEvents)`, вывод дополняется строкой `UI: <DFMForm> → <Class>.<Method> (<file>:<line>)` и, если применимо, `Origin query: <file>:<line> (<method>)`.
- MCP: расширить возвращаемые JSON-структуры аналогичных инструментов полем `client_enrichment`/`ui_chain` (обратная совместимость сохраняется).

### 11.4 Тесты

- `internal/rti/enrich_client_test.go`: mock `ClientLookup` (по аналогии с существующим mock для `ProcedureLookup` в `enrich_test.go`) — тесты на успешный матч класс/метод, отсутствие матча, матч с DFM-формой и без, матч/не матч query_fragment для exec-блока, нормализацию текста exec-параметров.
- Обновить `cmd/rti_test.go` / smoke-тест: проверить, что вывод `tree`/`errors` содержит UI-цепочку на синтетических данных с моком БД (если в проекте есть паттерн интеграционных тестов с реальной/тестовой БД — следовать ему).

## 12. Порядок реализации (итерации с проверкой после каждого шага)

1. Модель данных (`model.go`) + заглушки regex — билд зелёный.
2. Парсер клиентских событий без связывания и без БД — `go test ./internal/rti/...`, ручная проверка на `AFEDOROV_...rti` (summary показывает client events).
3. Линковка client↔server (`link.go`) + тесты.
4. Дерево/форматирование клиентских событий + `timeline`.
5. Схема БД + `SaveSession`/`LoadCalls` для клиентских событий + тесты.
6. CLI-команды (`summary`/`errors`/`slow` расширение + новые `client-tree`/`timeline`) + smoke-тесты.
7. Обогащение UI-цепочкой (`enrich_client.go`) + интеграция в `tree`/`errors`/`details`/`timeline` + тесты (п.11).
8. MCP-инструменты + обновление `registry.go` тестов.
9. Финальная регрессия на обоих реальных `.rti` файлах, обновление памяти проекта.
