## Context

Semantic-слой обоих поисков строится одинаково: `Tokenize` → стемы →
`Vocab.ProjectQuery` → cosine с эмбеддингами. Токенизатор (`internal/specfts/tokenizer.go`)
принимает русские слова, техимена (`API_*`, `t[A-Z]`, `ALL_CAPS`) и гибриды
(`758П`), но молча отбрасывает строчную латиницу и прочие нераспознанные
единицы. `ProjectQuery` (`vocab_tfidf.go:113-149`) тоже молча пропускает стемы,
которых нет в `Vocab.Index`.

Проверка на живом индексе (см. proposal.md, Why) показала: «несуществующий» и
«запрос» есть в словарях обоих корпусов, поэтому метрика по OOV-стемам не
отличает мусорный запрос от осмысленного. Отбрасываются именно *нераспознанные
токенизатором* слова.

Контракты поисков различаются: `spec_search` возвращает объект с секциями и
`semantic_meta` (`SpecSearchMeta`, `specsvc.go:29-35`), а `SearchDescriptions`
(`internal/query/desc_query.go:74-113`) возвращает `[]DescriptionSearchResult`,
`hasMore`, `error`; CLI `printDescriptionResults` (`cmd/query_desc.go:55-92`)
кладёт массив в envelope, MCP `codebase_query_desc_search`
(`internal/mcp/registry.go:761-780`) возвращает массив напрямую.

Точки интеграции:

- `internal/query/desc_query.go:207-310` — `searchDescriptionsSemantic`;
  модель и эмбеддинги грузятся через `descSemanticCache` (лениво, на процесс).
- `internal/specsvc/specsvc.go:1293-1360` — `searchSpecSemantic`, там же
  `filterSemanticHits` и `buildSemanticMeta`.
- `internal/config/config.go:400-407` и `427-432` — валидация `lsa_min_cosine`
  и `lsa_relative_cutoff` в `[0,1]`; геттеры с nil-дефолтами — `config.go:129-169`.

## Goals / Non-Goals

**Goals:**

- Дать semantic-слою явный сигнал «запрос в основном не распознан» и
  подавлять выдачу до сравнения с эмбеддингами.
- Сделать порог настраиваемым в `codebase.toml` независимо для `[spec]` и
  `[desc_lsa]`, с явным отключением через `0`.
- Сохранить exact-слой и текущие фильтры cosine без изменений.
- Отдавать единообразную диагностику `semantic_meta` в `spec_search` и
  `desc_search` (CLI и MCP).

**Non-Goals:**

- Центрирование эмбеддингов и изменение `lsa_min_cosine` (отдельные change'ы).
- Расширение токенизатора на строчную латиницу или другие категории.
- Новая диагностика недоступной модели в `desc-search`: текущий fallback
  (exact-only без ошибки) сохраняется.
- Переобучение модели или изменение формата `LSAModel`/схемы БД.

## Decisions

### D1. Метрика по «сырым» лексическим словам, а не по стемам словаря

`QueryLexicalCoverage(query) (recognized, total int)`:

1. Разбить запрос на «сырые» единицы: максимальные последовательности букв,
   цифр и `_`, содержащие хотя бы одну букву, длиной ≥ 2 рун.
2. Для каждой единицы: `Tokenize(unit)` вернул хотя бы один токен **или**
   lower-case единица (её стем) входит в `stopWords`/`stopStems` → распознана.
3. `total == 0` → `coverage = 0`.

`UnrecognizedWords` возвращает те же единицы, не прошедшие проверку, — для
`OOVTerms` в метаданных.

Почему не `Vocab.Index`: minDF/maxDF меняют состав словаря, а слова вроде
«несуществующий» уже в нём есть — метрика перестаёт отражать распознанность.
Почему не переписывать `Tokenize` с сохранением спанов: для коротких запросов
повторный `Tokenize(unit)` проще и не меняет существующий парсер. Альтернатива
со спанами — возможная оптимизация позже, поведение не изменится.

Следствие: почти любое русское слово распознаётся (стеммер принимает любую
кириллицу), поэтому гейт срабатывает в основном на строчной латинице и
смешанных символах. Это ожидаемо и ограничивает ложные срабатывания.

### D2. Дефолт 0.6, строгое `<`, `0` отключает

Условие: `coverage < MinQueryCoverage()` → гейт. Порог 0.5 отвергнут: TC-037
даёт ровно 2/4 = 0.5, и при строгом `<` граница не срабатывает. 0.6 отсекает
TC-037 (0.5) и TC-119 (0.33), но пропускает запрос с двумя распознанными
словами из трёх (2/3 = 0.67, например `asdkjhasd аннуитетного платежа`) и все
осмысленные запросы (1.0). Порог 0.7 отвергнут: 2/3 уже не проходит.

`*float64` обязателен: `nil` → 0.6, явный `0` → проверка отключена. В
`ensureDefaults` нельзя применять общий паттерн `if value <= 0 { default }` —
он уничтожит явный ноль.

### D3. Стоп-слова считаются распознанными

Иначе «расчёт для платежа» получает 2/3 и может быть подавлен при высоком
пороге. Стоп-слова — легитимная часть запроса; они и так не попадают в
TF-IDF (`tokenizer.go:195`).

### D4. Где стоит гейт

- **`desc-search`**: в `searchDescriptionsSemantic` до `descSemanticCache.get`.
  Мусорный запрос не должен грузить 130-МБ кэш модели и эмбеддингов; exact-слой
  уже отработал. Диагностика собирается без модели: `Reason=query_coverage`,
  `Coverage`, `CoverageThreshold`, `OOVTerms`, `Hint`.
- **`spec-search`**: в `searchSpecSemantic` после `specSemanticCache.get`, но до
  `ProjectQuery`/`CosineSimilarity`. Порядок сохраняет существующую диагностику
  недоступной модели (сначала — она, затем гейт).
- `FilteredOut` заполняется только когда кандидаты уже посеяны и отсечены
  порогом cosine. Для `Reason=query_coverage` гейт срабатывает до скоринга,
  поэтому `FilteredOut` не заполняется в обоих поисках — одинаково.

### D5. Общий тип диагностики

`internal/specfts` получает `SemanticDiagnostics` с JSON-тегами:

```go
FilteredOut       int      `json:"filtered_out,omitempty"`        // только для threshold/oov
Threshold         float64  `json:"threshold,omitempty"`           // порог cosine
Coverage          *float64 `json:"coverage,omitempty"`            // доля распознанных слов
CoverageThreshold *float64 `json:"coverage_threshold,omitempty"`  // порог покрытия
Reason            string   `json:"reason,omitempty"`              // threshold | oov | query_coverage
OOVTerms          []string `json:"oov_terms,omitempty"`           // OOV-стемы или нераспознанные слова
Hint              string   `json:"hint,omitempty"`
```

`SpecSearchMeta` становится алиасом (`type SpecSearchMeta = specfts.SemanticDiagnostics`)
— существующие вызовы и JSON не меняются. `DescriptionSearchOutput` использует
тот же тип. Указатели на `float64` нужны, чтобы покрытие `0.0` сериализовалось
(не путалось с «поле не задано»). `Threshold` продолжает означать порог cosine;
переиспользовать его под coverage отвергнуто — неоднозначная семантика.

Правило заполнения (одинаковое для обоих поисков): `semantic_meta` присутствует,
если semantic-секция пуста или в запросе есть OOV-термины; при непустой секции
без OOV метаданные не возвращаются.

### D6. Контракт `desc-search`

```go
type DescriptionSearchOutput struct {
    Items        []DescriptionSearchResult       `json:"items"`
    HasMore      bool                            `json:"has_more"`
    SemanticMeta *specfts.SemanticDiagnostics    `json:"semantic_meta,omitempty"`
}
```

`SearchDescriptions` возвращает `(*DescriptionSearchOutput, error)`.

- CLI: envelope сохраняет `count`/`items`/`meta.has_more`; диагностика кладётся
  в `meta.semantic_meta` (аддитивно, без ломания существующих полей).
- MCP: `codebase_query_desc_search` возвращает объект
  `{items, has_more, semantic_meta}` вместо массива — **BREAKING** во имя
  единообразия с `codebase_query_spec_search`.
- Диагностика `threshold`/`oov` для desc повторяет логику
  `buildSemanticMeta`: `FilteredOut` — число semantic-кандидатов, отсечённых
  порогами; при отсутствии семантических хитов и наличии OOV-стем — `reason=oov`.

Альтернатива «оставить массив и не отдавать диагностику в MCP» отвергнута:
формы ответа двух поисков остались бы разными. Альтернатива «отдельный
MCP-инструмент для диагностики» отвергнута как лишний контракт.

### D7. Конфигурация

В `SpecConfig` и `DescLSAConfig`:

```go
LSAMinQueryCoverage *float64 `toml:"lsa_min_query_coverage"`
```

Геттеры `MinQueryCoverage()`; константа `DefaultLSAMinQueryCoverage = 0.6` —
одна на пакет (не дублировать литерал в двух геттерах). Валидация — рядом с
`lsa_min_cosine`: `value < 0 || value > 1` → ошибка с именем ключа
(`spec.lsa_min_query_coverage` / `desc_lsa.lsa_min_query_coverage`).

Оба дефолта — 0.6; отдельные значения для корпусов не вводятся. В
`codebase.toml` ключ добавляется **закомментированным** в обе секции: явное
значение зафиксировало бы текущий дефолт (та же проблема, что с
`lsa_min_cosine = 0.15`, который сейчас перекрывает Go-дефолт).

### D8. Тестовая стратегия

Unit (`specfts`): таблица кейсов из дельт, включая `0/0`, стоп-слова,
техимена, гибриды, границу 0.6, регистр.
Integration (`query`, `specsvc`, `cmd`, `mcp`): гейт при `layer=both`/`semantic`,
exact не затронут, `semantic_meta` в CLI и MCP, `0` отключает, независимость
секций `[spec]`/`[desc_lsa]`, новая форма MCP-ответа `desc-search`.
Контрольный прогон: TC-037 (`count=0`), TC-119 (`semantic=[]`), TC-032…036 и
TC-113…118 без регрессий.

## Risks / Trade-offs

- **BREAKING-форма MCP `desc-search`** → зафиксировать в release notes;
  клиенты, ожидающие массив, обновляются вместе с сервером; контрактный тест
  проверяет объект.
- **Ложное подавление легитимного латинского запроса** (`mass create`) →
  exact-слой продолжает работать; `0` отключает гейт; расширение токенизатора —
  отдельная задача.
- **Гейт не ловит русский мусор** (токенизатор примет любое русское слово за
  стем) → это вне scope: метрика про нераспознанные токены, а не про смысл.
- **Аддитивные поля CLI JSON** → старые клиенты игнорируют неизвестные поля;
  версия envelope не меняется.
- **Приоритет ошибок** → сначала существующая диагностика недоступной модели,
  затем гейт; зафиксировано сценариями.
- **Производительность** → повторный `Tokenize` по словам запроса; запросы
  короткие, стоимость пренебрежима; при необходимости — один проход со спанами.
- **Порог 0.6 может не подойти одному из корпусов** → секции независимы,
  значение настраивается без релиза.

## Migration Plan

- Схема БД и формат модели не меняются; переобучение не требуется.
- Существующие конфиги без ключа получают 0.6; поведение меняется только для
  запросов с покрытием ниже порога.
- CLI `desc-search` остаётся совместимым (диагностика аддитивна); MCP-клиенты
  `codebase_query_desc_search` должны перейти с массива на объект.
- Откат: `lsa_min_query_coverage = 0` в нужной секции или удаление ключа
  возвращает прежнее поведение semantic-слоя; форма MCP-ответа откатывается
  вместе с кодом.
- После релиза — прогон `Tests/TEST_CASES.md` (TC-032…037, TC-113…119).
