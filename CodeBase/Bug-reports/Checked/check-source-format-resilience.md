Проверил change, делители и код, на который он опирается.

openspec validate source-format-resilience --strict — valid; openspec validate --all --strict — 34 passed, 0 failed. Формально артефакты корректны, но семантически change неполон: есть блокеры, из-за которых часть дельт нереализуема или не даст заявленного эффекта.

Блокеры
1. module-depend некуда сохранять.
Требование Извлечение контрактов InfoPath event-формата и сценарий module_depend = false предполагают поле ModuleDepend, но в model.APIContract его нет (internal/model/model.go:457-479), в api_contracts колонки нет (internal/store/db_schema.go, api_contracts), в BatchInsertAPIContracts (internal/store/api_store.go:177) его тоже нет.
Нужно либо добавить ModuleDepend в модель + DDL + bump CurrentSchemaVersion + вставку/выборку, либо явно исключить module-depend из маппинга и убрать module_depend из сценария. Сейчас требование невыполнимо.
2. Архитектура «декод UTF-16 в walk-воркере» противоречит фактическому коду.
В fswalk воркер кладёт в FileInfo.Content сырые байты (internal/fswalk/fswalk.go:281-322), а декод делает decodeIndexedFileContent по FileInfo.Encoding (internal/indexer/indexer.go:426-428). При этом:
- parseXMLFile (internal/indexer/indexer.go:1376-1379), parseSMFFile (:1263-1266) и parseTPRFile (:1002-1005) передают в парсер сырой file.Content и вообще не используют file.Encoding;
- dsxml.ParseBytes сам вызывает DetectXMLEncoding (internal/parser/dsxml/parser.go:185-192);
- SMF/TPR сами перебирают WIN1251/CP866/UTF8 (internal/parser/smf/smf_parser.go:257, internal/parser/tpr/tpr_parser.go:66).
Следствие: UTF-16 в .smf/.tpr не будет исправлен, а если по задаче 1.3 реально декодировать Content в воркере и оставить Encoding=UTF16LE, то decodeIndexedFileContent декодирует UTF-8 повторно как UTF-16 → мусор. Если ставить Encoding=UTF8, то SMF/TPR начнут читать UTF-8 как WIN1251. Нужно явно выбрать одну модель и добавить задачи для XML/SMF/TPR (либо оставить Content сырым и добавить UTF-16-детект во все точки входа, либо ввести признак «контент уже декодирован» и не декодировать повторно).
3. UTF-16 XML с декларацией encoding="UTF-16" упадёт после декода.
DetectXMLEncoding вернёт UTF16LE, DecodeBytes декодирует содержимое, но unmarshalXML затем видит в XML declaration encoding="UTF-16" и вызывает xmlCharsetReader (internal/parser/dsxml/parser.go:146-154). Для utf-16/utf-16le/utf-16be там нет ветки — будет unsupported xml charset: utf-16.
Нужна pass-through-ветка (контент уже UTF-8) или нормализация declaration + тесты. В текущих задачах этого нет, хотя сценарий «XML в UTF-16 с BOM» заявлен.

Существенные пробелы
4. Graceful-skip не имеет канала «я пропустил файл».
ParseResult (internal/parser/dsxml/parser.go:19-37) не содержит признака skip. Дизайн D6 говорит «return res, nil + счётчик XMLSkipped в ScanStats», но парсер не имеет доступа к ScanStats, а индексер не может отличить «пустой валидный <Object>» от «неизвестный корень». Нужен ParseResult.Skipped/SkippedReason либо типизированный sentinel и задача в индексер.
5. Набор «известных корней» не совпадает с path-based dispatch.
В D6 известными считаются object, table, param, message, fasdocument, event. Но dispatch в ParseContent идёт по пути: /BObject/.../Table/ → standaloneTableXML, /Param/ → standaloneParamXML, иначе default → objectXML (internal/parser/dsxml/parser.go:229-341, 358).
<Table> вне /Table/ пройдёт skip-проверку как «известный», а затем упадёт в default с expected element type <Object>. Либо skip должен срабатывать при несоответствии корня и пути, либо dispatch нужно строить по rootName.
6. UTF-16 без BOM для XML/MD не покрыт.
Задача 2.1 добавляет в DetectXMLEncoding только BOM-детект. Файл UTF-16LE без BOM:
для ASCII-содержимого utf8.Valid вернёт true из-за NUL → DetectXMLEncoding вернёт UTF8;
для кириллицы, скорее всего, вернёт WIN1251;
walker-детект не поможет, потому что dsxml читает сырой file.Content.
То же у DetectMarkdownEncoding (internal/encoding/encoding.go:244-248), а indexer_postprocess_spec_deps.go:91-93 вообще повторно читает файл с диска и снова вызывает DetectMarkdownEncoding. Нужен единый UTF-16 пре-детект в DetectXMLEncoding/DetectMarkdownEncoding или передача результата walker-а в парсер.
7. Конфликт с основной спекой «Кодировки по типам файлов».
В openspec/specs/infrastructure/encoding-cli/spec.md требование «Кодировки по типам файлов» фиксирует: YAML — UTF-8, T01 — CP866, MD/XML — собственные правила. Новая дельта indexing/file-walking говорит, что UTF-16-детект применяется ко всем, включая .xml, .md, .yaml, .t01. При этом в воспроизведённом MODIFIED-тексте остаётся фраза «.md, .xml, .yaml, .t01 обрабатываются прежними правилами», а следующий абзац её фактически перекрывает. Нужно либо MODIFIED требования «Кодировки по типам файлов», либо явная формулировка «кроме случая обнаружения UTF-16».

Мелочи
8. D2: неверное обоснование «кириллический UTF-16LE даёт ~49% NUL».
У кириллицы U+04xx старший байт 0x04, а не 0x00; NUL дают в основном ASCII-символы (пробелы, переводы строк, латиница). Чисто кириллический UTF-16LE без BOM может не пройти порог 30%. Это стоит зафиксировать как ограничение эвристики, а не как «~49%».
9. Нет фикстуры для опциональной группы E.
Задача 7.1 ссылается на pAPI_DFA_BfrMassDelPmnt.xml, но в Bug-reports/ есть только TC01..., OnAfterScr..., API_GSOD.... Нужно добавить фикстуру или указать, откуда её брать.
10. UTF-8 BOM removal не доведён до DecodeBytes.
Требование говорит, что BOM EF BB BF удаляется при декодировании. В задаче 1.2 это явно указано только для UTF-16; DecodeBytes для UTF8 сейчас возвращает string(data) как есть (internal/encoding/encoding.go:314-316). Нужен явный пункт и тест.
11. files.encoding для GSOD-кейса останется UTF8.
.xml не входит в isContentDetectedExt (internal/fswalk/fswalk.go:427-439), A2-детект выполняется внутри dsxml, и фактическая WIN1251 не попадает в files.encoding. Если метаданные важны — нужно либо детектировать XML в walker-е, либо возвращать фактическую кодировку из парсера.
12. Неверная команда в задаче 6.3.
query api-contract --name ... не существует; в CLI есть query symbol --type api_contract --name ... / query inspect. query desc-search — существует.
13. Тест «регресс cp866 XML-декларации» в задаче 2.1 сейчас не пройдёт.
xmlCharsetReader поддерживает только windows-1251 и utf-8; декларация cp866 после декода вызовет unsupported xml charset: cp866. Либо добавить поддержку, либо убрать этот пункт из регресса.
14. Счётчик XMLSkipped нужно вывести во всех местах сводки.
Задача 4.1 говорит «печать в сводке», но сводки есть в cmd/init.go, cmd/update.go, cmd/stats.go и JSON-представлении. Стоит перечислить их явно.

Что уже сделано хорошо
A/A2/B/C/D покрыты, E вынесена в опциональную группу.
MODIFIED-требования воспроизведены полностью, ADDED/MODIFIED не конфликтуют формально.
Учтён механизм консервации mtime+size: retry вынесен в Non-Goals, верификация через touch/DELETE/--modified=false.
Фикстуры и регресс CP866/CP1251/UTF-8 заявлены, миграция и откат описаны.

Итог: change можно валидировать и брать в работу только после исправления блокеров 1–3 и добавления задач по SMF/TPR/XML no-BOM, иначе заявленный «errors_count → 0» и «UTF-16 для всех расширений» не будут достигнуты.