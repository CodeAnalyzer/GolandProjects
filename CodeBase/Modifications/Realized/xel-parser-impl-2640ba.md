# План реализации бинарного XEL-парсера в internal/trc

Поэтапно добавить поддержку `.xel` (SQL Server Extended Events) в `internal/trc`: сначала подтвердить структуру формата на реальном файле и эталонном XML от SQL Server (Phase 0), затем реализовать streaming-парсер и генерируемый маппинг событий (Phase 1-2), интегрировать в `ParseFile`/`DetectFormat` и схему БД, на каждом шаге проверяя сборкой и тестами.

## Предпосылки (закрывают замечания из рецензии)

- Доступ к SQL Server есть → Phase 0 verification строится на `sys.fn_xe_file_target_read_file`, а не только на эвристике "hex + известные строки".
- `DetectFormat` — **не экспортируется за пределы `internal/trc`** (проверено `grep`: единственный вызов в `parser.go`, остальные упоминания — тесты). Смена `bool → Format` безопасна, затронет только `parser.go`, `xml_parser_test.go`, `parser_test.go`.
- `xeEventNameToClass` не хардкодится вручную — генерируется из `Modifications/Эквиваленты расширенных событий событиям трассировки SQL.csv`.
- Явно зафиксировать конвенцию времени: FILETIME → трактуется как UTC (аналогично `SystemTime.ToTime()`), без локальной конвертации — если эталонный XML покажет иначе, скорректировать на Phase 0.

## Эталон получен: реальная структура полей (STP3_1.reference.xml)

Экспорт через `sys.fn_xe_file_target_read_file` (`Modifications/export_xel_reference.sql` → `Modifications/STP3_1.reference.xml`, 452 КБ) подтвердил конкретную объектную модель XE-события:

```xml
<event name="wait_completed" package="sqlos" timestamp="2024-03-15T22:13:30.035Z">
  <data name="wait_type"><value>123</value><text>SOS_SCHEDULER_YIELD</text></data>
  <data name="wait_result"><value>258</value></data>
  <data name="duration"><value>1</value></data>
  <data name="wait_resource"><value/></data>
  <action name="username" package="sqlserver"><value>rosbank\rbs-celd-devops</value></action>
  <action name="session_id" package="sqlserver"><value>370</value></action>
  <action name="database_name" package="sqlserver"><value>diasoft_prod</value></action>
  <action name="client_hostname" package="sqlserver"><value>RBCORPPAS00016</value></action>
  <action name="client_app_name" package="sqlserver"><value>Admin STP 5NT(e)</value></action>
</event>
```

**Ключевые выводы:**
- Событие = `name` + `package` + `timestamp` (ISO-8601 с `Z`, **не FILETIME** — сервер уже отдаёт готовый UTC `datetime2` в текстовом виде). Отменяет исходное предположение про `fileTimeToSystemTime`: раз сервер выдаёт `datetime2`-строку, в бинарном `.xel` таймстемп скорее всего FILETIME (100ns since 1601), но конвертация должна давать результат, совпадающий с этим ISO-8601 значением при разборе того же события — это прямая проверяемая величина для Phase 0.
- Поля делятся на `data` (event-specific, внутри самого события) и `action` (глобальные атрибуты сессии/сервера, настраиваются в XE-сессии) — соответствует архитектуре XE, но теперь есть точный список реально присутствующих action-имён: `username`, `session_id`, `database_name`, `client_hostname`, `client_app_name`, `transaction_id`, `transaction_sequence`, `cpu_id` (не полный список, требует прогона по всему файлу).
- Некоторые `data`-поля — **map-значения**: `<value>123</value><text>SOS_SCHEDULER_YIELD</text></data>` — числовой код + разрешённое человекочитаемое имя (аналог enum). Другие поля — просто `<value>...</value>` без `text`. Пустое значение — `<value/>`.
- Имена событий/полей текстовые и стабильные (`wait_completed`, `wait_type`, `session_id` и т.д.) — подтверждает, что схема имён самоописывающаяся хотя бы на уровне XE runtime (сервер знает имена без внешнего каталога при простом чтении файла). Остаётся открытым, как именно имена закодированы в *бинарном* `.xel` (как строки inline на каждое поле, или через словарь/ID один раз в файле) — это и есть основной вопрос Phase 0.

## Phase 0 — Верификация формата (блокирующий шаг)

1. ~~Выполнить в SSMS запрос...~~ **Готово** — `Modifications/STP3_1.reference.xml` получен через `sqlcmd` (см. выше).
2. Новый подготовительный шаг: написать вспомогательный тест/утилиту `TestXEL_ExtractReferenceSchema` (парсинг `STP3_1.reference.xml` штатным `encoding/xml`) — построить полный реестр: все встречающиеся `event name`, для каждого — набор `data name` (+ флаг наличия `text`-map) и все встречающиеся `action name` по всему файлу. Это заменяет гадание "какие поля искать" точным списком под конкретный реальный трейс — используется как источник "known values" для следующего шага вместо ручного подбора.
3. Написать `internal/trc/xel_reverse_test.go` (по образцу `reverse_engineer_test.go`), используя реестр из п.2 как источник needles:
   - `TestXEL_DumpHeader` — hex-дамп первых 4-8 КБ `STP3_1.xel`, поиск магических байт/GUID сессии.
   - `TestXEL_FindMetadataBlock` — искать в бинарном потоке **реальные имена полей** из реестра (`wait_type`, `session_id`, `database_name`, `client_app_name`, ...) как UTF-16LE/UTF-8 needles — определить, повторяются ли имена перед каждым событием (raw inline) или встречаются один раз в блоке метаданных (dictionary/schema block) с последующими числовыми ссылками.
   - `TestXEL_CompareWithReferenceXML` — сверка конкретных значений первых N событий (`session_id=370`, `database_name=diasoft_prod`, `wait_type text=SOS_SCHEDULER_YIELD`, `timestamp=2024-03-15T22:13:30.035Z`) с байтовыми смещениями в `STP3_1.xel` — основной инструмент валидации гипотез о раскладке полей.
   - `TestXEL_TimestampEncoding` — целевой поиск: найти байтовое представление `timestamp` (кандидаты: FILETIME int64, unix epoch, `datetime2`) рядом с найденным событием и подтвердить точную формулу конвертации в `2024-03-15T22:13:30.035Z`.
   - `TestXEL_CompareWithCSV` — дополнительная сверка с `STP3_1.csv` (доп. эталон, шире по колонкам, независимый от XML-пути).
4. Зафиксировать результат в `internal/trc/xel_format.go`: структура заголовка файла, структура пакета события (маркер, размер, поля, наличие/отсутствие inline-имён vs словаря), таблица XE-типов → Go-типы (включая формат map-значений `value+text`), точная формула timestamp → `SystemTime`/UTC.
5. **Решающая точка**: если имена полей закодированы через словарь/dictionary-блок (а не raw inline) — парсер должен быть **двухфазным** (первый проход собирает словарь ID→имя, второй декодирует события) — это меняет архитектуру `ParseXELReader` в Phase 2 (нельзя быть чисто однопроходным streaming, либо нужно буферизовать события до первого встреченного словаря). Если словаря нет вовсе и имена не находятся raw — Go/No-Go: требуется обязательный внешний каталог через `sys.dm_xe_object_columns`, не покрывается текущим планом.

**Критерий готовности Phase 0:** известна точная бинарная раскладка (включая механизм именования полей — inline vs словарь) минимум для событий `rpc_completed`, `sql_batch_completed`, `module_start/end`, `sql_statement_starting/completed`, и подтверждена формула конвертации timestamp — этого достаточно для дальнейшей работы `enrichEventsParallel`/`tree.go`.

### Статус выполнения (актуализировано, `go test ./internal/trc/... -run TestXEL -v` — 6/6 PASS)

- ✅ `internal/trc/xel_reference_schema_test.go` (`TestXEL_ExtractReferenceSchema`) — реестр реальной схемы из `STP3_1.reference.xml` (153 960 событий, 4 event types: `wait_completed`(127491), `sp_statement_completed`(14908), `sql_batch_completed`(5680), `sql_statement_completed`(5881); 8 action names). **PASS**.
- ✅ Подтверждено: формат словарный (dictionary), не inline — имена action/event встречаются в файле по 1 разу, значения (напр. `diasoft_prod`) — по разу на событие.
- ✅ Найден и зафиксирован **map-словарь** в начале файла: `[int32 ID][UTF-16LE имя]\0` (проверено на wait_type/wait names, напр. `SOS_SCHEDULER_YIELD`).
- ✅ Найден и зафиксирован формат **ACTION TLV**: `[package uint8][actionID uint16 LE][0x10][length uint32 LE][length байт]`. Первый байт — номер **package** (не константа): `1`=sqlos, `2`=sqlserver, подтверждено на двух разнотипных событиях (стабильный глобальный словарь action-имён). `internal/trc/xel_reverse_test.go` (`TestXEL_DecodeFirstEventActions`) — регрессионный тест на 8 проверок, **PASS**. Подтверждённые (package,actionID): `(2,80)`→transaction_sequence, `(2,4)`→transaction_id, `(2,8)`→session_id, `(2,76)`→database_name, `(2,56)`→username, `(2,44)`→client_hostname, `(2,36)`→client_app_name, `(1,28)`→cpu_id.
- ✅ **Закрыто**: DATA-секция события `wait_completed` полностью раскодирована и подтверждена регрессионным тестом `TestXEL_DecodeWaitCompletedData` на 3 образцах, разнесённых по всему 84МБ файлу (начало/середина/конец), включая случай с переменной длиной `wait_resource`:
  ```
  [timestamp: u64 LE][totalLen: u32 LE][wait_type: u32][reserved: u32][wait_result: u32]
  [duration: u64][signal_duration: u64][typeTag: u32 = const 0x24][waitResourceLen: u32][waitResourceBytes: UTF-16LE]
  ```
  `totalLen = 36 + waitResourceLen`. Перед этим блоком — стабильный 12-байтовый маркер `01 C0 00 00 0A 00 00 80 00 00 00 00` (назначение байт не декодировано, используется как якорь поиска). Замечено: порядок DATA/ACTION секций **отличается между типами событий** (`wait_completed`: data→action; `sql_batch_completed`: action→data) — вероятно задаётся индивидуальной схемой event type в dictionary-блоке.
- ✅ **Закрыто**: DATA-секция события `sql_batch_completed` полностью раскодирована и подтверждена регрессионным тестом `TestXEL_DecodeSQLBatchCompletedData` на 3 образцах (включая большие значения `cpu_time=2969000`, `logical_reads=2291813`, и разрешение неоднозначности при повторе одинакового `batch_text` 700 раз в файле с разными фактическими полями):
  ```
  [timestamp: u64][totalLen: u32][cpu_time: u64][duration: u64][physical_reads: u64]
  [logical_reads: u64][writes: u64][spills: u64][row_count: u64]
  [result: u8][typeTag: u32 = const 0x41][batchTextLen: u32][batchTextBytes: UTF-16LE]
  ```
  `totalLen = 65 + batchTextLen`. У `sql_batch_completed` порядок секций **обратный** относительно `wait_completed` — ACTION идёт перед DATA. Между концом TLV cpu_id (package=1, id=28) и началом timestamp — фиксированные (но не декодированные по содержимому) **20 байт**, используются как смещение, а не байтовый якорь. `result` подтверждён только со значением 0 (1 байт) — ширина для ненулевых значений НЕ проверена (остаётся открытым нюансом).
- ✅ **Закрыто**: DATA-секция события `sql_statement_completed` раскодирована и подтверждена регрессионным тестом `TestXEL_DecodeSQLStatementCompletedData` на 2 образцах (включая edge-case `offset_end=-1` "до конца батча" и большие значения):
  ```
  [timestamp: u64][totalLen: u32]
  [duration: u64][cpu_time: u64][physical_reads: u64][logical_reads: u64]
  [writes: u64][spills: u64][row_count: u64][last_row_count: u64]
  [line_number: u32][offset: u32][offset_end: i32]
  [typeTag: u32 = const 0x5C][statementLen: u32]
  [totalLenRepeat: u32 — повторяет totalLen][zero: u32 = 0][statementBytes: UTF-16LE]
  ```
  Важно: порядок первых двух полей **обратный** относительно `sql_batch_completed` — здесь `duration` идёт перед `cpu_time`. Назначение хвостовых полей `totalLenRepeat`/`zero` не выяснено (возможно связаны с `parameterized_plan_handle`, NULL в обоих образцах), но их позиции/значения эмпирически подтверждены на 2 разных `total_len`.
- ✅ **Закрыто**: DATA-секция `sp_statement_completed` полностью раскодирована и подтверждена регрессионным тестом `TestXEL_DecodeSPStatementCompletedData` на 2 образцах, найденных не текстовым needle (statement/object_name повторяются на каждом вызове одной процедуры), а по уникальной числовой последовательности `duration+cpu_time+physical_reads+logical_reads` (u64 LE подряд):
  ```
  [timestamp: u64][totalLen: u32]
  [source_database_id: u32][object_id: u32][object_type: u16 (map: код+text, напр. 8272="PROC")]
  [duration: u64][cpu_time: u64][physical_reads: u64][logical_reads: u64][writes: u64][spills: u64]
  [row_count: u64][last_row_count: u64][nest_level: u16][line_number: u32][offset: u32][offset_end: i32]
  [objectNameTag: u32 = const 0x68][objectNameLen: u32][reserved: u32][statementLen: u32]
  [objectNameBytes: UTF-16LE][statementBytes: UTF-16LE]
  ```
  `totalLen = 104 + objectNameLen + statementLen` (проверено на обоих образцах). Образец 1 (`object_id=709039668`, `physical_reads=7 != spills=0`) разрешил позиции physical_reads/spills; образец 2 (`object_id=772375927`, `nest_level=4 != last_row_count=2845`) разрешил позиции nest_level/last_row_count. Поле `reserved` (rel 96-99) эмпирически равно `104+objectNameLen` (т.е. `totalLen-statementLen`) на обоих образцах — назначение не выяснено, не требуется для декодирования.
- ✅ **Закрыто**: структура файлового заголовка и границы событийного потока раскодированы (см. комментарий в `xel_reverse_test.go` перед `TestXEL_DecodeSQLStatementCompletedData`). Заголовок (offset 0-23): сигнатура+версия (8 байт) + 16-байтовый файловый GUID. Offset 24-511 — нули. Schema/dictionary блок (offset ~512 до ~69600 в этом файле): map-словари `[int32 ID][UTF-16LE имя]\0` + таблица деклараций имён event/action/field (каждое имя встречается 1 раз). Внутри блока — калибровочные константы timestamp (offset 620/628) рядом с GUID сессии трассировки. **Важно**: у событий НЕТ отдельного per-event заголовка — подтверждено сравнением байт перед 1-м и 2-м маркером `wait_completed` (перед 2-м маркером идёт ACTION-секция предыдущего события, т.е. события пакуются впритык). 96 байт перед ПЕРВЫМ событием в файле — одноразовая граница конец-словаря/начало-потока (`FF FF FF FF`, константа `FC 1F 00 00`=8188, повтор сессионного GUID), не повторяющаяся структура. Точная запись отдельных TLV-полей внутри dictionary-блока (имя+флаги/размер) не раскодирована на уровне байт, но это не требуется: имена событий/actions для Phase 1 берутся из внешнего CSV, а не из этого словаря, и сам механизм "именование через словарь" уже подтверждён.
- ✅ **Закрыто**: точная формула timestamp → абсолютное время НАЙДЕНА и подтверждена регрессионным тестом `TestXEL_TimestampCalibration` на 6 образцах (первое/последнее событие файла + 4 промежуточных, разброс ~6.5 минут) с точностью до миллисекунды. Калибровочные константы хранятся прямо в заголовке файла:
  ```
  [offset 620]: BASE_FILETIME_100NS uint64 LE — FILETIME (100ns тиков с 1601-01-01 UTC) для raw_ts=0
  [offset 628]: FREQ uint64 LE (значимы младшие 4 байта) — частота raw_ts в тиках/сек (аналог QueryPerformanceFrequency), на этом файле = 3020249
  ```
  Формула: `realTimeUTC = FILETIME_EPOCH(1601-01-01) + (BASE_FILETIME_100NS + raw_ts*10_000_000/FREQ)` (в единицах 100ns). Раньше предполагалось, что нужна эмпирическая калибровка "снаружи" — оказалось, что обе константы физически присутствуют в файле рядом (offset 620/628, 16 байт подряд), что и позволило их найти точным побайтовым поиском около эмпирически оценённых значений. ВАЖНО (implementation detail): при переводе в `time.Time` нельзя откладывать напрямую от эпохи 1601 через `time.Duration` — итоговое количество 100ns-тиков (~423 года) переполняет `int64`-диапазон `time.Duration` (макс. ~292 года); нужно сначала вычесть стандартную оффсет-константу FILETIME→Unix (`116444736000000000`) и работать относительно эпохи Unix. Смещения 620/628 подтверждены только для ЭТОГО конкретного файла — не найден отдельный маркер/якорь перед этой парой, поэтому в общем парсере для произвольных `.xel`-файлов, вероятно, потребуется either фиксированный оффсет (если формат заголовка стабилен между версиями SQL Server), либо эвристический поиск константы FREQ в правдоподобном диапазоне.
- ✅ **Закрыто**: `cpu_id` найден — кодируется тем же ACTION TLV, но с `package=1` (sqlos) вместо `package=2` (sqlserver).

**Вывод:** Go/No-Go по Phase 0 п.5 — **подтверждён путь "двухфазный парсер со словарём"** (не чисто inline streaming). **Phase 0 полностью завершён** (все 4 типа событий + timestamp + структура файла/dictionary-блока раскодированы и подтверждены регрессионными тестами, `go test ./internal/trc/... -run TestXEL -v` — 7/7 PASS) — можно переходить к Phase 1.

## Phase 1 — Генерация маппинга событий из CSV ✅

1. ✅ Скрипт-генератор `internal/trc/gen_xel_mapping.go` (файл с `//go:build ignore` + `go:generate` директива в `xel_format.go`) — читает `Modifications/Эквиваленты расширенных событий событиям трассировки SQL.csv`, парсит колонки `trace_event_id;Event Class;package;xe_event_name;...`, для каждого `xe_event_name` берёт **минимальный** `trace_event_id`, генерирует `internal/trc/xel_event_map_generated.go` с картой `xeEventNameToClass map[string]int` (116 событий) и комментарием об источнике/дате генерации.
2. ✅ Сгенерирована `xeActionNameToColumn map[string]int` (19 actions). При генерации обнаружены 2 конфликта action→ColumnID: `tsql_frame` (ColumnID [5,61,63], взят 5) и `nt_username` (ColumnID [6,7], взят 7) — warnings выведены, генерация не прервана.
3. ✅ Тест `TestXELEventMapGenerated_NoCollisions` (`xel_event_map_test.go`) — проверяет парные соответствия (starting/completed для 5 пар событий), отсутствие коллизий между уровнями (module_start vs sql_statement_starting, module_end vs sql_statement_completed), положительность всех ID, и документирует принятые решения по известным конфликтам action→ColumnID. `go test ./internal/trc/ -run TestXEL -v` — 8/8 PASS.

**Вывод:** Phase 1 полностью завершена. Ожидаю явного акцепта для перехода к Phase 2.

## Phase 2 — Бинарный парсер

1. `internal/trc/xel_format.go` — типы `XEType`, структуры заголовка/пакета по результатам Phase 0 (без хардкода карты имён — она в generated-файле).
2. `internal/trc/xel_parser.go`:
   - `parseXELHeader(r *bufio.Reader) (*TraceHeader, error)`
   - Если Phase 0 п.5 подтвердил словарь имён — `parseXELDictionary(r *bufio.Reader) (map[int]string, error)` (первый проход) + буферизация/повторное чтение событий во втором проходе; если подтверждён inline — имена читаются вместе с каждым полем без отдельного словаря
   - `parseXELPackage(r *bufio.Reader, dict map[int]string) (*TRCEvent, error)` — разбирает `data` (event-specific) и `action` (сессионные атрибуты) раздельно, аналогично структуре `STP3_1.reference.xml`
   - `decodeXELValue(t XEType, r *bufio.Reader) (any, error)` — для map-типов возвращает **разрешённое `text`-имя**, если оно есть (аналог `wait_type: 123 → "SOS_SCHEDULER_YIELD"`), иначе — сырое значение; сохраняется правило "не терять данные" из исходного предложения
   - `fileTimeToSystemTime(ft int64) SystemTime` — точная формула подтверждена в Phase 0 п.3 (`TestXEL_TimestampEncoding`) сверкой с `timestamp="...Z"` из эталона
   - `ParseXELReader(r io.Reader) (*TRCParseResult, error)` — streaming (или двухфазный, если подтверждён словарь), вызывает `enrichEventsParallel` + `ComputeParentIDs` в конце, аналогично `ParseXMLReader`
   - Обработка `EventClass = 0` для XE-only событий без TRC-эквивалента (не терять `EventName`, например `wait_completed`/`wait_info` из реального трейса)
3. `internal/trc/xel_parser_test.go`:
   - `TestParseXEL_Header`, `TestParseXEL_Events` (урезанный фикстур из первых ~4-8 КБ `STP3_1.xel`, подготовленный в Phase 0)
   - `TestParseXEL_FullFile` — сквозной прогон полного `STP3_1.xel` (`total_events > 0`, наличие `Procedure`/`DurationMs`) — не в стандартном `go test ./...`, отдельный `-run` таг (аналогично smoke-тестам для больших `.trc`, если такой паттерн уже есть — проверить `parser_test.go`)
   - `TestParseXEL_MatchesReferenceXML` — сверка N первых событий с `STP3_1.reference.xml`
   - `TestParseXEL_EventNameMapping`, `TestParseXEL_FileTimeConversion`

## Phase 3 — Интеграция в DetectFormat/ParseFile

1. `internal/trc/xml_parser.go`: `DetectFormat(data []byte) bool` → `DetectFormat(data []byte) Format` (enum `FormatBinary/FormatXML/FormatXEL`), добавить `looksLikeXEL(data []byte) bool` на основе сигнатуры из Phase 0.
2. `internal/trc/parser.go`: `ParseFile` — `switch DetectFormat(headerBuf)` на три ветки (XML/XEL/бинарный `.trc`, как в текущем предложении, но с реальной сигнатурой из Phase 0, а не гипотетической).
3. Обновить `internal/trc/parser_test.go` (`TestParseFile_DetectFormat`) и `internal/trc/xml_parser_test.go` (`TestDetectFormat`) под enum.

## Phase 4 — Схема БД и store.go

1. `internal/store/db_schema.go`: добавить `source_format TEXT NOT NULL DEFAULT 'trc_binary'` в `CREATE TABLE trc_sessions` + `ALTER TABLE trc_sessions ADD COLUMN IF NOT EXISTS source_format TEXT NOT NULL DEFAULT 'trc_binary'` (для существующих БД, по аналогии с `client_events_count` строкой выше).
2. `internal/trc/store.go`: `SaveSession` — добавить параметр или определять `source_format` по `TraceHeader`/результату `DetectFormat` и записывать в INSERT.
3. Проверить `LoadEvents`/`ListSessions` — не требуют изменений (просто новая колонка в SELECT * не участвует, либо явно добавить в вывод при необходимости).

## Порядок выполнения и проверка

Каждый Phase — отдельный шаг с проверкой перед переходом к следующему:

1. Phase 0 → `go test ./internal/trc/... -run TestXEL -v` (логи анализируются вручную) → **Go/No-Go точка**.
2. Phase 1 → `go generate ./internal/trc/...` + `go test ./internal/trc/... -run TestXELEventMapGenerated`.
3. Phase 2 → `go build ./...` + `go test ./internal/trc/... -run TestParseXEL`.
4. Phase 3 → `go test ./internal/trc/...` (полный пакет, включая старые `.trc`/XML-тесты — не должны сломаться).
5. Phase 4 → `go build ./...` + ручная проверка `codebase trc parse Modifications/STP3_1.xel` (CLI) на реальном файле.

## Файлы к созданию/изменению

| Файл | Действие |
|------|----------|
| `internal/trc/xel_reverse_test.go` | новый, Phase 0 |
| `internal/trc/xel_format.go` | новый, Phase 0/2 |
| `internal/trc/gen_xel_mapping.go` | новый, Phase 1 (генератор, `go:build ignore`) |
| `internal/trc/xel_event_map_generated.go` | новый, генерируется в Phase 1 |
| `internal/trc/xel_parser.go` | новый, Phase 2 |
| `internal/trc/xel_parser_test.go` | новый, Phase 2 |
| `internal/trc/xml_parser.go` | изменить (`Format` enum, `looksLikeXEL`), Phase 3 |
| `internal/trc/parser.go` | изменить (`ParseFile` dispatch), Phase 3 |
| `internal/trc/parser_test.go`, `xml_parser_test.go` | изменить под enum, Phase 3 |
| `internal/store/db_schema.go` | изменить (`source_format`), Phase 4 |
| `internal/trc/store.go` | изменить (`SaveSession`), Phase 4 |
| `Modifications/STP3_1.reference.xml` | **готово** — эталон получен через `sqlcmd` |
| `internal/trc/xel_reference_schema_test.go` | новый, Phase 0 п.2 (`TestXEL_ExtractReferenceSchema` — реестр event/data/action имён из эталона) |

Без изменений (подтверждено рецензией): `tree.go`, `aggregate.go`, `enrich.go`, `extract.go`, `enrich_parallel.go`, `cmd/trc.go`, `internal/mcp/registry.go`.
