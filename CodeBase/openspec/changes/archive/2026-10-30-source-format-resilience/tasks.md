# Tasks: source-format-resilience

## 1. UTF-16 детект и декодер (A)

- [x] 1.1 Добавить в `internal/encoding` константы `UTF16LE`/`UTF16BE` и единый BOM/NUL-детект (`FF FE`→UTF16LE, `FE FF`→UTF16BE, `EF BB BF`→UTF8; без BOM — NUL-плотность ≥30% в первых 4096 байтах при ≥90% NUL на одной чётности; NUL-порог понимается как ASCII-зависимый — чисто кириллический UTF-16 без BOM может не детектироваться, зафиксировано в design). Юнит-тесты: фикстура `Bug-reports/TC01_TechAddAgree_Status.sql` → UTF16LE, синтетика BE без BOM, CP866 с редкими NUL обеих чётностей → не UTF-16, UTF-8 BOM → UTF8
- [x] 1.2 Подключить UTF16LE/UTF16BE в `DecodeBytes`/`ReadFile`/`GetDecoder` через `golang.org/x/text/encoding/unicode` (BOM удаляется); в `DecodeBytes` для UTF8 — удаление ведущего BOM `EF BB BF` (сейчас возвращается как есть). Тесты: декод фикстуры TC01 даёт валидный SQL с русскими комментариями (`-- pgonline version:`) без NUL; UTF8+BOM-строка декодируется без U+FEFF в начале
- [x] 1.3 Встроить UTF-16 пре-детект в walk-воркер (`internal/fswalk/fswalk.go`) — только метаданные: до ASCII fast-path и до диспетчеризации по расширению (включая `.xml`/`.md`/`.yaml`/`.t01`) выставлять `FileInfo.Encoding` = UTF16LE/UTF16BE; `FileInfo.Content` остаётся сырыми байтами (декодируют точки входа формата: SQL/H/T01/PAS/JS/DFM/RPT/MD/YAML — `decodeIndexedFileContent`; XML/SMF/TPR — собственные детекторы, задачи 2/3); `EncodingRefined` учитывает; SHA-256 по сырым байтам. Тест fswalk: FileInfo.Encoding=UTF16LE для UTF-16LE `.sql`, Content не изменён, hash равен SHA-256 сырых байтов

## 2. Валидация XML-декларации и charset-ветки (A2/A3)

- [x] 2.1 В `DetectXMLEncoding`: шаг (0) — BOM-детект UTF-16/UTF-8 и NUL-эвристика (UTF-16 без BOM иначе виден как валидный UTF-8); при заявленном `utf-8` — проверка `utf8.Valid(data)`, при провале — маркерные диапазоны (fallback WIN1251 для CP1251-подобного). Тесты: `Bug-reports/API_GSOD_MassCreate.xml` → WIN1251; UTF-16LE XML без BOM → UTF16LE; регресс: честные UTF-8 и windows-1251 декларации, XML без декларации (обе ветки utf8.Valid)
- [x] 2.2 Для `.xml` в walk-воркере: вызвать `DetectXMLEncoding` по прочитанному буферу и записать фактическую кодировку в `FileInfo.Encoding`/`files.encoding` (GSOD → WIN1251, EncodingRefined). Тест: после индексации фикстуры GSOD `files.encoding` = WIN1251
- [x] 2.3 Расширить `xmlCharsetReader` (dsxml): pass-through для `utf-16`/`utf-16le`/`utf-16be`/`ucs-2` (контент предекодирован) и `cp866`/`ibm866`. Тесты: XML, предекодированный из UTF-16LE с декларацией `encoding="UTF-16"`, разбирается без `unsupported xml charset`; XML с cp866-декларацией после предекода — аналогично

## 3. SMF/TPR/MD: UTF-16 в собственных детекторах (A)

- [x] 3.1 В перебор кодировок SMF-парсера (`internal/parser/smf`) и TPR-парсера (`internal/parser/tpr`) первой попыткой добавить UTF-16 BOM/NUL-детект + декод (воркер их `Content` не декодирует — см. design D1). Тесты: UTF-16LE `.smf` и `.tpr` парсятся, кириллические атрибуты читаемы; регресс CP1251/CP866/UTF8-перебор не изменён
- [x] 3.2 В `DetectMarkdownEncoding` добавить BOM/NUL-детект UTF-16. Тест: md-файл в UTF-16LE декодируется корректно; путь повторного чтения с диска (`internal/indexer/indexer_postprocess_spec_deps.go`) — тем же детектором, покрывается автоматически

## 4. InfoPath event-формат (C)

- [x] 4.1 Структура `eventXML` (корень `event`, kebab-теги) в `internal/parser/dsxml`: name→ContractName, descr-short/description→Short/Full, implemented (true/false)→bool, architect-approval=OK→ArchApproval=1, input/output-tables→table+field (name, name-rus→RusName, type, required, description), kind/owner из пути. `module-depend`, `retvals`, `contexts`, `return-resultset` — игнор (в `api_contracts` нет колонок; Open Question в design). Тест: фикстура `Bug-reports/OnAfterScr_MassDel.xml` → контракт kind=event, таблица `pAPI_Scr_AfterDelScr` (input) с полями ScoreID/Date и русскими именами
- [x] 4.2 Смешанный `required` (true/false у table, 1/0 у field) — кастомный unmarshaler; whitespace-элементы (`<output-params>` из пробелов) — пустые. Тест: обе записи распознаются; `implemented=true` отражён в модели
- [x] 4.3 Integration-тест индексатора: `<event>`-файл в дереве `/BObject/Rating/Event/` → строки в api_contracts/api_contract_tables/api_contract_table_fields, русский текст доступен desc-search

## 5. Graceful-skip неизвестных/несогласованных корней (D)

- [x] 5.1 В `ParseResult` (dsxml) добавить признак `Skipped` + причина; skip-проверка после `xmlRootName` сверяет корень с ожиданием path-ветки (default: object/event/message/fasdocument; `/Table/`: table; `/Param/`: param; несоответствие или неизвестный корень → пустой результат + Skipped). Тесты: корень `<somethingNew>` → Skipped, не ошибка; `<Table>` вне `/Table/` → Skipped; битый XML (незакрытый тег) → ошибка, не skip
- [x] 5.2 Индексер по `ParseResult.Skipped` инкрементирует `ScanStats.XMLSkipped`; печать во всех сводках: `cmd/init.go` (рядом с Encoding refined), `cmd/update.go` (рядом с Pre-filtered/Encoding refined), JSON-представление stats (`cmd/stats.go`, при наличии полей последнего скана). Тест: прогон с skip-файлом показывает счётчик во всех трёх поверхностях
- [x] 5.3 Аудит call-sites строковой кодировки (`files.encoding`, `encoding.Encoding(...)` switch-ветки): новые значения UTF16LE/UTF16BE не ломают consumers (default-ветки безопасны). Греп-проверка + фикс при находках

## 6. NUL-strip safety-net (B)

- [x] 6.1 Санитизация текстовых значений в batch-insert слое store: предикат `IndexByte(s,0)` без копирования при отсутствии NUL, `ReplaceAll` при наличии; точки — query_fragments, описания процедур/контрактов, symbols. Юнит-тест: значения с NUL вставляются (integration/test-БД), без NUL — без аллокаций (бенчмарк)

## 7. Тесты, регресс, верификация

- [x] 7.1 Регресс кодировок: прогон всех тестов `internal/encoding` (корпус CP866/CP1251/UTF-8) + dsxml/smf/tpr/fswalk до и после — ноль расхождений на существующих фикстурах
- [x] 7.2 Бамп buildNumber до 1656 в `cmd/root.go` (сейчас 1650); сборка + `go vet` чисто; полный `go test ./...` зелёный; пересборка `go build -o CodeBase.exe .` из корня CodeBase — бинарник обновлён
- [x] 7.3 Верификация на рабочем дереве: touch (или DELETE строк `files`) затронутых файлов (TC*, GSOD, ratings-events) → update → errors_count не растёт; `query api-contract --name OnAfterScr_MassDel` возвращает контракт; desc-search по русским описаниям GSOD/событий возвращает результаты; `files.encoding` = UTF16LE у TC-файлов и WIN1251 у GSOD
- [x] 7.4 Полный прогон update на дереве: errors_count 13 → 3 (только fmcore до правки владельцами), EncodingRefined/XMLSkipped в сводке соответствуют ожиданиям; обновить `Bug-reports/BUG-utf16-files-not-decoded-20261028.md` статусом фикса

## 8. Recovery битых XML (E — опционально)

- [x] 8.1 Автозакрытие открытых элементов до совпадающего тега при ошибке `element closed by` в dsxml. Фикстура `pAPI_DFA_BfrMassDelPmnt.xml` скопирована в `Bug-reports/`; тест: дублированный `<Indexses>` при незакрытом `<Index>` → файл индексируется (таблица+поля+индекс XPK), предупреждение в лог вместо ошибки; orphan-close и превышение лимита вставок — исходная ошибка; дельта-требование «Recovery незакрытых элементов XML» добавлено в `specs/indexing/dsxml-parsing`
- [x] 8.2 Бамп buildNumber до 1657 в `cmd/root.go` (сейчас 1656); предупреждение recovery — в `errorLogger` (`indexer_errors_*.log`, формат `WARNING: File: ... - auto-closed ...`), не в консоль со спиннером; сборка + `go vet` чисто; полный `go test ./...` зелёный; пересборка `go build -o CodeBase.exe .` из корня CodeBase — бинарник обновлён
- [x] 8.3 Верификация на рабочем дереве: DELETE строк `files` для `pAPI_DFA_BfrMassDelPmnt.xml` (3 чек-аута: fa-fmcore, fa-fmcore-11-01-3, fa-fmcore-11-01-4) → `update` → errors_count **3 → 0**; 3 × `WARNING ... auto-closed 2 unclosed element(s)` в `indexer_errors_*.log` (консоль чистая); сущности вставлены: `api_business_object_tables` ×3 (BO DigitalFinancialAsset, русское имя читаемо), поля ×6 (PaymentID, DigitalFinancialAssetID), индексы XPK ×3; баг-репорт дополнен абзацем «Recovery fmcore (2026-10-30, build 1657)»
