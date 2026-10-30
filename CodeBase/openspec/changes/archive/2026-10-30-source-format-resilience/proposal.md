# Proposal: source-format-resilience

## Why

Инструменты «новодела» FA порождают исходники в форматах, которые индексатор не
умеет читать: UTF-16 SQL-тесты (pgonline, коммит `9f1f086b`), XML с ложной
декларацией UTF-8 при CP1251-байтах (`API_GSOD_MassCreate.xml`), события
DSArchitect в InfoPath-формате с корнем `<event>` (fa-ratings). Каждый такой
файл целиком выпадает из индекса (13 ошибок на 623k файлов в прогоне
2026-10-28), а потеря данных замалчивается пре-фильтром mtime+size после первой
ошибки. Полный разбор — `Bug-reports/BUG-utf16-files-not-decoded-20261028.md`
(заключение по коду подтверждено 2026-10-30).

## What Changes

- **A. UTF-16**: детекция BOM `FF FE`/`FE FF` (и UTF-8 BOM `EF BB BF` с удалением
  при декодировании) в `internal/encoding`; эвристика без BOM по плотности NUL на
  позициях одной чётности; значения кодировки `UTF16LE`/`UTF16BE` в декодерах.
  Архитектура: walk-воркер определяет кодировку (в `FileInfo.Encoding` →
  `files.encoding`), но НЕ декодирует `Content` — сырые байты остаются в
  `FileInfo.Content`, декодирование выполняют точки входа формата: SQL/H/T01/
  PAS/JS/DFM/RPT/MD/YAML — существующим `decodeIndexedFileContent` (DecodeBytes
  получает UTF-16-ветки), XML — `DetectXMLEncoding` (получает BOM/NUL-шаг), SMF/TPR —
  их внутренний перебор кодировок расширяется UTF-16. NUL перестаёт доезжать до
  PostgreSQL по всем путям.
- **A2. Валидация XML-декларации**: при `encoding="UTF-8"` проверяется
  `utf8.Valid(data)`; при провале — fallback на эвристику WIN1251/CP866
  (покрывает GSOD-кейс: файл индексируется, русские описания попадают в индекс).
  Воркер дополнительно вызывает XML-детекцию для `.xml`, чтобы фактическая
  кодировка попадала в `files.encoding` (GSOD → WIN1251, EncodingRefined).
- **A3. Charset-ветки xml-декодера**: `xmlCharsetReader` получает pass-through для
  `utf-16`/`utf-16le`/`utf-16be`/`ucs-2` (контент уже предекодирован в UTF-8) и
  ветку `cp866` — без этого XML в UTF-16 с честной декларацией и XML с
  cp866-декларацией падают после декода.
- **B. NUL-strip safety-net**: зачистка `0x00` из текстовых значений перед
  batch-вставкой в store (страховка на случай нераспознанной экзотики).
- **C. InfoPath `<event>`-формат**: отдельная структура `eventXML` в default-ветке
  dsxml-парсера — контракты kind=event из kebab-тегов (`name`, `descr-short`,
  `description`, `input-tables`/`output-tables` → `table`/`field`, `name-rus`,
  булевы `true/false/1/0`). `module-depend` игнорируется (в `api_contracts` нет
  колонки — см. design D5), `retvals`/`contexts`/`return-resultset` — без сущностей.
- **D. Graceful-skip неизвестных корней XML**: несоответствие корневого элемента
  ожиданиям path-ветки (например, `<Table>` вне `/Table/` или корень вне
  {`Object`,`Table`,`Param`,`message`,`fasdocument`,`event`}) → файл индексируется
  без сущностей: парсер возвращает признак skip в результате, индексер ведёт
  счётчик skip вместо errors_count.
- **E. Recovery битых XML (опционально)**: автозакрытие открытых элементов до
  совпадающего тега при ошибке «element closed by» — покроет битый fmcore-файл.
  Выполняется последней группой задач; может быть исключена без ущерба для A–D.

Не входит (осознанно, см. design Non-Goals): механика автоматического ретрая
законсервированных пре-фильтром файлов — верификация через touch/DELETE строк
`files` (существующий `--modified=false` тоже покрывает принудительный перепрох).

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `infrastructure/encoding-cli`: ADDED «Детекция UTF-16 по BOM и NUL-плотности»
  (единый BOM/NUL-детект, вызываемый всеми детекторами по содержимому; значения
  кодировки UTF16LE/UTF16BE; UTF-8 BOM удаление при декодировании); MODIFIED
  «Детекция кодировки XML» (BOM/NUL-шаг, валидация декларации UTF-8 через
  utf8.Valid с fallback-эвристикой); MODIFIED «Кодировки по типам файлов»
  (оговорка UTF-16 для форматов с фиксированной кодировкой: yaml/t01/xml/md).
- `indexing/file-walking`: MODIFIED «Детекция кодировки по содержимому в
  walk-воркере» — UTF-16 BOM/NUL-детект выполняется до ASCII fast-path и до
  диспетчеризации по расширению (кодировка записывается в FileInfo.Encoding,
  Content остаётся сырым); для `.xml` воркер вызывает XML-детекцию для
  фактического значения files.encoding.
- `indexing/dsxml-parsing`: ADDED «Извлечение контрактов InfoPath event-формата»
  (module-depend игнорируется); MODIFIED «Пропуск неподдерживаемых форматов»
  (graceful-skip при несоответствии корня path-ветке, признак skip в результате
  парсера вместо ошибки файла); MODIFIED «Поддержка кодировки windows-1251»
  (charset-ветки utf-16*/ucs-2* pass-through и cp866).
- `infrastructure/database-schema`: ADDED «Sanitization NUL в текстовых значениях»
  (batch insert зачищает 0x00).

## Impact

- **Код**: `internal/encoding/encoding.go` (BOM/NUL-детект, UTF-16 enum + декодер,
  валидация декларации), `internal/fswalk/fswalk.go` (единый пре-детект в воркере),
  `internal/parser/dsxml/parser.go` (`eventXML`, graceful-skip), `internal/store/*`
  (NUL-strip в batch insert текстовых значений).
- **Данные**: `files.encoding` получает новые значения `UTF16LE`/`UTF16BE` —
  обратно совместимо (строка); ранее выпавшие файлы индексируются при принудительном
  перепрохе.
- **Зависимости**: `golang.org/x/text/encoding/unicode` — уже в go.mod, новых нет.
- **Метрики**: счётчик skip-файлов XML (graceful-skip) в сводке прогона;
  `EncodingRefined` учитывает UTF-16-детект.
- **Тесты/фикстуры**: `Bug-reports/TC01_TechAddAgree_Status.sql` (UTF-16LE),
  `OnAfterScr_MassDel.xml` (InfoPath `<event>`), `API_GSOD_MassCreate.xml`
  (ложная декларация); BE-вариант UTF-16, skip-сценарий, регресс CP866/CP1251/UTF-8;
  для группы E — фикстура `pAPI_DFA_BfrMassDelPmnt.xml` копируется из рабочего
  дерева fa-fmcore в `Bug-reports/`.
- **Upstream (вне change)**: pgonline-тесты писать UTF-8; fmcore-файл починить
  владельцам; GSOD-файл пересохранить (пользователь сообщил владельцам).
