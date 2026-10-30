# Bug Report: экзотические форматы исходников FA — UTF-16 не декодируется, XML с ложной декларацией UTF-8, DSArchitect `<event>` (InfoPath) не парсится, неизвестный корень XML роняет файл

**Дата:** 2026-10-28 (дополнено 2026-10-29 по итогам финального прогона; статус фикса — 2026-10-30)
**Файл:** `internal/encoding/encoding.go` (нет UTF-16; `DetectXMLEncoding:109-136` доверяет декларации без проверки); `internal/parser/dsxml/parser.go:358-362` (default-ветка ждёт корень `<Object>`); проявление — `internal/indexer` (batch-вставка `query_fragments`, ошибки обработки XML-файлов)
**Версия CodeBase:** 0.9.4 (build 1646; поведение идентично на всех предыдущих сборках)
**Статус:** Исправлено (build 1656, change `source-format-resilience`, 2026-10-30). Верификация на рабочем дереве: DELETE 203 строк `files` (TC/GSOD/события) → `update` → **errors 13 → 3**; `files.encoding`: TC01 → `UTF16LE` (MS-вариант корректно остался CP866), GSOD → `WIN1251`; 24 InfoPath-события → контракты `kind=event` (implemented=true, arch_approval=1); `XML skipped: 0`; Encoding refined: 2382.

**Уточнение по GSOD (2026-10-30):** Service-копии `API_GSOD_MassCreate.xml` (fa-stockmarket develop, 11-01-11, 11-01-12) — **битый исходник upstream**: файл обрезан (1016 строк, последний `<ReturnValue>` не закрыт, `</ReturnValues>`/`</Object>` отсутствуют). Декодирование WIN1251 после фикса проходит, xml-декодер корректно падает `unexpected EOF on line 1017` — это и есть оставшиеся 3 ошибки прогона. UsedService-копии fa-custody валидны и дали контракты `kind=used_service`. Требуется правка файла владельцами fa-stockmarket (восстановить закрывающие теги). Дефект исходника зафиксирован тестом `TestParseFile_GSODFixture_TruncatedUpstream`.

**Recovery fmcore (2026-10-30, build 1657):** группа E (опциональная) реализована — при ошибке вложенности «element closed by» парсер dsxml автозакрывает незакрытые элементы до совпадающего закрывающего тега (лимит 32 вставки; orphan-close и обрезанные файлы не чинятся). Верификация на рабочем дереве: DELETE 3 строк `files` (`pAPI_DFA_BfrMassDelPmnt.xml` × fa-fmcore/11-01-3/11-01-4) → `update` → **errors 3 → 0**, таблицы/поля/индексы вставлены (3+6+3), предупреждения `WARNING: File: ... - auto-closed 2 unclosed element(s)` — в `indexer_errors_*.log` (не в консоль). Фикстура `Bug-reports/pAPI_DFA_BfrMassDelPmnt.xml`, тесты `TestParseContent_FmcoreFixture_AutoClosed`, `TestInitXML_RecoveredTableIndexed`. Прогон верификации — errors=0 (GSOD Service-копии в нём не парсились повторно: законсервированы пре-фильтром после прогона build 1656; их 3 ошибки `unexpected EOF` воспроизводятся только принудительным перепрохом до правки upstream).

## Summary

Четыре дефекта одного класса — «новодел» FA-инструментами порождает форматы,
которые индексатор не умеет читать. Каждый выявлен на реальном дереве
(рабочий ПК, полный прогон всех продуктов в 2+ ветках, 623k файлов,
2026-10-28 12:16 → 10-29 05:53, errors=13):

1. **UTF-16 без декодирования.** Файлы с BOM `FF FE` не распознаются
   детектором кодировок (только CP866/CP1251/UTF-8); байты идут как есть,
   ASCII в UTF-16LE = `символ + 0x00`, NUL доезжает до PostgreSQL →
   `pq: неверная последовательность байт для кодировки "UTF8": 0x00` на
   batch-вставке `query_fragments`; файл целиком выпадает из индекса.
   Источник: коммит `9f1f086b` (etroshin, 09.10.2026, qtskt-808415) —
   pgonline-тесты `FARPRT_Solution/Test/PG/.../TC0{1,2,3}_*.sql` в UTF-16
   (MS-варианты нормальные; в develop до коммита файлов нет).

2. **XML с ложной декларацией UTF-8 (фактически CP1251).**
   `API_GSOD_MassCreate.xml` в ветках fa-stockmarket-11-01-11/-12 объявляет
   `<?xml version="1.0" encoding="UTF-8"?>`, но 9290 байтов ≥ 0x80 и ноль
   валидных UTF-8 кириллических пар — содержимое CP1251. `DetectXMLEncoding`
   доверяет декларации → байты без транскодирования уходят в xml-парсер →
   `XML syntax error on line 13: invalid UTF-8` (первая русская строка —
   `<ShortDescription>Массовое добавение…`); файл выпадает целиком. 2 ошибки.

3. **DSArchitect `<event>` (InfoPath-формат).** События fa-ratings в ветках
   11-01-2/11-01-3 сериализованы InfoPath-шаблоном (`<?mso-infoPathSolution
   …?>`, корень `<event>`, kebab-теги `name`/`descr-short`/`input-tables`),
   а не классическим `<Object><ObjectName>…` (весь остальной FA —
   `<Object>`). Default-ветка парсера жёстко ожидает `<Object>` →
   `expected element type <Object> but have <event>`; файл выпадает целиком.
   4 файла × 2 ветки: `OnAfterAgency_MassDelete/MassSave`,
   `OnAfterScr_MassSave/MassDel` (`API_RTG/DSArchitectData/BObject/Rating/Event/`).

4. **Неизвестный корень XML = ошибка файла.** Любой будущий дрейф корня
   (как `<event>` или битый `pAPI_DFA_BfrMassDelPmnt.xml` в fmcore, где
   продублирован блок `<Indexses>` при незакрытом `<Index>`) роняет весь
   файл в errors_count вместо изолированной потери сущностей.

Общее следствие для всех четырёх: ошибка «законсервирована» — строка `files`
обновляется на этапе walk+save до парсинга, инкрементальный update
пре-фильтрует по mtime+size, повторной попытки нет до изменения файла.

Итоговый errors_count прогона = 13: 3 (fmcore, битый XML) + 8 (`<event>`
InfoPath) + 2 (ложная декларация UTF-8). UTF-16 0x00-ошибки в этом прогоне
НЕ повторились (TC-файлы уже были пропущены ранее пре-фильтром; кейс
остаётся по логу 2026-10-26 и воспроизводится фикстурой).

Критичность: **LOW–MEDIUM** (сейчас ~17 файлов на 623k; риск роста при
продолжении использования соответствующих генераторов).

---

## Environment

- CodeBase 0.9.4 (build 1646), PostgreSQL (5435), БД `codebase`
- Рабочее дерево FA `C:\NT\FA#\7.2GIT` (623k файлов после гет-веток),
  локальная копия `D:\GITHUB\GolandProjects\FA` (16 источников)
- Фикстуры рядом с отчётом: `Bug-reports/TC01_TechAddAgree_Status.sql`
  (UTF-16LE), `Bug-reports/OnAfterScr_MassDel.xml` (InfoPath `<event>`),
  `Bug-reports/API_GSOD_MassCreate.xml` (декларация UTF-8, байты CP1251)

---

## Findings (итоги парсинга)

### 1. UTF-16 → 0x00 в PostgreSQL

- `TC01_TechAddAgree_Status.sql`: 10554 байта, **5176 NUL, все на нечётных
  позициях**, BOM `FF FE`; декодирование UTF-16LE даёт валидный SQL с
  русскими комментариями (`-- pgonline version: release-2025.04.602`, блоки
  `do $$`).
- Ошибка воспроизводится только при первом парсинге файла (новый
  файл/ветка); дальше замалчивается пре-фильтром.
- `internal/encoding` не содержит UTF-16 вообще (код и тесты пакета —
  только CP866/CP1251/UTF-8 эвристика).

### 2. `<event>` InfoPath-формат

Образец (полностью — в фикстуре `OnAfterScr_MassDel.xml`):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?mso-infoPathSolution … href="http://dev/APINew/event_template.xsn" … ?>
<?mso-application progid="InfoPath.Document"?>
<event xmlns:my="…" xmlns:xd="…" …>
  <name>OnAfterScr_MassDel</name>
  <description>Событие информирует подписчиков…</description>
  <implemented>true</implemented><module-depend>false</module-depend>
  <input-params/><input-tables><table><name>pAPI_Scr_AfterDelScr</name>…
    <fields><field><name>ScoreID</name><name-rus>ИД оценки</name-rus>
    <type>DSIDENTIFIER</type><required>1</required></field>…</fields>
  </table></input-tables>
  <retvals/><descr-short>…</descr-short><output-params/><output-tables/>
  <architect-approval>OK</architect-approval>
  <return-resultset>RESULTSET_MULTIPLY</return-resultset><contexts/>
</event>
```

XML валиден (PI перед корнем допустимы) — это другой генератор, а не
повреждение. Маппинг на нашу модели полный:

| `<event>`-тег | модель (kind=event) |
|---|---|
| `name` | ContractName (ObjectName) |
| `descr-short` / `description` | ShortDescription / FullDescription |
| `implemented`, `module-depend` (`true/false`) | Implemented, ModuleDepend |
| `architect-approval=OK` | ArchApproval |
| `input-tables`/`output-tables` → `table` (`name`,`description`,`fields`) | APIContractTable |
| `field` → `name`,`name-rus`,`type`,`required`,`description` | APIContractTableField (RusName ← `name-rus`) |
| `retvals`, `contexts`, `return-resultset` | ReturnValues, Contexts, resultset — в сигнатуру/игнор |

Неизвестное: в образце `<input-params/>` пуст — структуру `<param>`
предположить по аналогии с `<field>`; уточнить при появлении файла с
непустыми params.

### 3. Ложная декларация UTF-8 (API_GSOD_MassCreate.xml)

- Файл 37882 байта, без BOM, начинается с корректной `<?xml … encoding="UTF-8"?>`.
- 9290 байтов ≥ 0x80; валидных UTF-8 кириллических пар (D0/D1 + 80-BF) — 0
  → содержимое целиком в однобайтовой кодировке; декодирование CP1251 даёт
  осмысленный русский текст (первое русское — строка 13,
  `<ShortDescription>Массовое добавение глобальных операций с рынка цб и
  операций перекодировки счетов</ShortDescription>`).
- `DetectXMLEncoding` (`encoding.go:109-136`) возвращает UTF8 по декларации
  без `utf8.Valid`; транскодирования нет → Go xml-декодер падает
  `invalid UTF-8` на строке 13.
- 2 копии: fa-stockmarket-11-01-11 и -11-01-12 (`API_Security/DSArchitectData/
  BObject/GlobalSecuritiesOperationsDeal/Service/`); очевидно, файл сохранён
  инструментом в CP1251 при сохранённой UTF-8-декларации.

### 4. Битый XML в fmcore (отдельный случай, к фиксам отношения не имеет)

`pAPI_DFA_BfrMassDelPmnt.xml` (3 чек-аута fa-fmcore): первый
`<Indexses><Index>` не закрыт, вместо `</Index>` вставлен дубликат блока
`<Indexses>` с тем же индексом; `</Table>` закрывает таблицу поверх
открытых элементов. Дефект исходника (плохой merge); ошибка парсера
корректна. Владельцам репозитория fmcore сообщил пользователь. Recovery
(автозакрытие до совпадающего тега) — опционально, вне обязательного объёма.

---

## Root Cause Analysis

1. **Детектор кодировок не знает UTF-16** (`encoding.go`): проверка BOM
   `FF FE`/`FE FF` отсутствует; при неоднозначности возвращается дефолтная
   однобайтовая кодировка.
2. **Декларациим encoding доверяют без проверки** (`DetectXMLEncoding`):
   при `encoding="UTF-8"` не вызывается `utf8.Valid` — файл с байтами
   CP1251 проходит как UTF-8 и падает уже в xml-декодере.
3. **NUL без санитизации доезжает до PostgreSQL**: парсер SQL работает и по
   «половинным» ASCII-символам, фрагменты набираются с 0x00.
4. **Жёсткий корень `<Object>`** в default-ветке dsxml (`parser.go:358-362`,
   `unmarshalXML` в `objectXML` с XMLName `Object`): любой другой корень —
   ошибка всего файла.
5. **Ошибки консервируются пре-фильтром** mtime+size: упавший парсинг не
   откатывает строку `files`, ретрая до изменения файла нет.

---

## Impact

- Потеря всех сущностей затронутых файлов (фрагменты/процедуры — UTF-16;
  контракт GSOD с русскими описаниями — ложная декларация; контракты-события
  с русскими описаниями — `<event>`-формат) во всех ветках, где лежат
  коммиты; errors_count прогона 2026-10-28: 3 fmcore + 8 ratings + 2 GSOD.
- Ошибка видна один раз, затем молча — потеря данных незаметна.
- Русские описания `<event>`-событий и GSOD-контракта не попадают в
  desc-search.

---

## Suggested Fix

Единый change **«устойчивость к экзотике исходников»**:

- **A. UTF-16**: BOM-детект `FF FE`/`FE FF` (заодно UTF-8 BOM `EF BB BF`)
  в `internal/encoding`; декодер `golang.org/x/text/encoding/unicode`
  (зависимость уже в go.mod); опционально эвристика без BOM по плотности
  NUL на чётных/нечётных позициях.
- **A2. Валидация декларации**: при `encoding="UTF-8"` проверять
  `utf8.Valid(data)`; при провале — fallback на WIN1251-эвристику
  (покрывает GSOD-кейс).
- **B. NUL-strip safety-net** в store: зачистка `0x00` из текстовых
  значений перед вставкой (`strings.ReplaceAll(s, "\x00", "")`).
- **C. `<event>`-формат**: отдельная структура `eventXML` (корень `event`,
  kebab-теги) в default-ветке dsxml; маппинг по таблице выше; булевы
  значения `true/false/1/0`; `name-rus` → RusName; `return-resultset` — в
  сигнатуру.
- **D. Graceful-skip неизвестных корней**: корень вне ожидаемого набора
  (`Object`, `event`, таблицы/параметры) → файл индексируется без сущностей
  (счётчик skip вместо errors_count) — защита от будущих дрейфов.
- **E. Recovery для битых тегов (опционально)**: при «element closed by»
  автозакрытие открытых элементов до совпадающего тега — покроет fmcore-кейс.

**Тесты:** фикстуры `TC01_TechAddAgree_Status.sql` (детект → декод →
фрагменты), `OnAfterScr_MassDel.xml` (контракт kind=event + таблица + поля
с name-rus), `API_GSOD_MassCreate.xml` (детект WIN1251 при UTF-8-декларации,
контракт с русскими описаниями), BE-вариант UTF-16, skip-сценарий
(неизвестный корень — не ошибка), integration — вставка текста без NUL;
регресс CP1251/CP866/UTF-8.

**Верификация:** форс-ретраи упавших копий (touch или DELETE строк `files`)
→ 0 ошибок, сущности на месте; прогон на рабочем дереве — errors_count=0
(после правки fmcore-файла владельцами).

**Upstream:** pgonline-тесты — писать UTF-8 (автор qtskt-808415);
fmcore-файл — починить `</Index>` (пользователь сообщил владельцам);
GSOD-файл — пересохранить в UTF-8 или честной кодировке (сообщить владельцам
fa-stockmarket-11-01-11/-12).

---

## Related

- `Bug-reports/TC01_TechAddAgree_Status.sql`, `Bug-reports/OnAfterScr_MassDel.xml`,
  `Bug-reports/API_GSOD_MassCreate.xml` — фикстуры.
- Коммит FA `9f1f086bcc89e720e631f0a345d840979115e43e` (qtskt-808415) — источник UTF-16 файлов.
- `Bug-reports/BUG-query-deadlock-init-schema-ddl-20261026.md` — соседний открытый баг.
- `Modifications/Code-review-dsinfo-integration.md` — ревью среза C, в ходе которого тема поднята.
