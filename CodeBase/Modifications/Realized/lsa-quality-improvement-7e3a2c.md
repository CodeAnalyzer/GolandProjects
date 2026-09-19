# Улучшение качества LSA-слоя семантического поиска спек

Повышение релевантности семантического поиска (`layer=semantic`) за счёт расширения training-данных, настройки порогов и фильтрации шумных терминов. Изменения не нарушают спеку `openspec/specs/query/spec-search/spec.md` — LSA остаётся на уровне capability, раздельные секции результата сохраняются.

---

## Контекст проблемы

Семантический поиск для запроса «Заметка по клиенту» возвращает шум: нерелевантные capability с cosine 0.43–0.50, тогда как релевантная capability `legal` не попадает в топ. Причины:

1. **Training-данные скудны**: документ для LSA = `title + purpose + notes` capability (`indexer_postprocess_spec_lsa.go:59`). Тексты requirements и scenarios не включены. Термин «заметка» встречается в requirements, но не в title/purpose/notes — его нет в векторе.
2. **Высокочастотный термин «клиент»**: встречается в сотнях capability, проходит maxDF-фильтр (0.3), получает низкий IDF. Запрос вырождается в «найди документы про клиентов».
3. **Порог 0.01 фиктивен**: `specsvc.go:882` пропускает практически всё — нет реального отсева по релевантности.

Спека (`spec-search/spec.md:27`) говорит: «LSA применяется на уровне capability (embed_level = 'spec')». Включение текста дочерних requirements в документ capability не нарушает это — документ всё ещё один на capability, embed_level не меняется.

---

## Затронутые файлы

- `internal/indexer/indexer_postprocess_spec_lsa.go` — расширение training-данных
- `internal/store/db_lsa.go` — загрузка requirements/scenarios для LSA
- `internal/specsvc/specsvc.go` — порог косинуса, относительный cutoff
- `internal/specfts/tokenizer.go` — доменные стоп-слова
- `internal/config/config.go` — новые параметры конфигурации
- `internal/specfts/vocab_tfidf.go` — без изменений (логика BuildVocab та же)
- `internal/indexer/indexer_postprocess_spec_lsa_test.go` — новые тесты
- `internal/specsvc/specsvc_test.go` — тесты порога

---

## План (3 шага)

### Шаг 1: Расширить training-данные LSA (включить requirements и scenarios)

**Файлы:** `internal/store/db_lsa.go`, `internal/indexer/indexer_postprocess_spec_lsa.go`

**Текущее поведение:** `LoadAllSpecCapabilitiesForLSA` (`db_lsa.go:13-31`) загружает только `id, title, purpose, notes`. Документ = `title + purpose + notes` (строка 59).

**Новое поведение:** загрузить также тексты requirements и scenarios для каждой capability. Документ = `title + purpose + notes + requirements.body_text + scenarios.given_text + scenarios.when_text + scenarios.then_text`.

**Изменения в `db_lsa.go`:**

1. Новая функция `LoadAllSpecCapabilitiesWithReqsForLSA` — загружает capability + агрегированный текст дочерних requirements/scenarios через `string_agg`:

```sql
SELECT c.id, c.title, c.purpose, c.notes,
       COALESCE((
           SELECT string_agg(req.requirement_name || ' ' || req.body_text, ' ')
           FROM spec_requirements req WHERE req.capability_id = c.id
       ), ''),
       COALESCE((
           SELECT string_agg(s.given_text || ' ' || s.when_text || ' ' || s.then_text, ' ')
           FROM spec_scenarios s
           JOIN spec_requirements req ON req.id = s.requirement_id
           WHERE req.capability_id = c.id
       ), '')
FROM spec_capabilities c
ORDER BY c.id
```

2. Возвращать `[]model.SpecCapability` с заполненным полем `RelatedCode` (переиспользуем существующее поле) или добавить новое поле `ReqText` в модель. Предпочтительно — новое поле `ReqText string` в `SpecCapability` (`model.go:673`), чтобы не путать с `related_code`.

**Изменения в `indexer_postprocess_spec_lsa.go`:**

Строка 59:
```go
// Было:
text := strings.Join([]string{c.Title, c.Purpose, c.Notes}, " ")
// Стало:
text := strings.Join([]string{c.Title, c.Purpose, c.Notes, c.ReqText}, " ")
```

Строка 129 (embed_text для БД):
```go
// Было:
embedText := strings.Join([]string{c.Title, c.Purpose, c.Notes}, " ")
// Стало:
embedText := strings.Join([]string{c.Title, c.Purpose, c.Notes, c.ReqText}, " ")
```

**Почему не нарушает спеку:** `embed_level` остаётся `'spec'`, одна запись на capability. Спека говорит «LSA на уровне capability» — документ capability теперь включает текст дочерних требований, но это всё ещё документ capability, не отдельный requirement-эмбеддинг. Спека не запрещает состав документа.

**Ожидаемый эффект:** термин «заметка» попадёт в вектор capability `legal` (через текст требования «Заметка по клиенту без срока»). Cosine для запроса «Заметка по клиенту» с `legal` вырастет, шумные capability уйдут из топа.

---

### Шаг 2: Порог косинуса и относительный cutoff

**Файл:** `internal/specsvc/specsvc.go` (строка 882), `internal/config/config.go`

**Текущее поведение:** `specsvc.go:882` — `if hit.Rank <= 0.01 { continue }`. Порог 0.01 пропускает практически всё.

**Новое поведение:** два уровня фильтрации:

1. **Абсолютный порог** — configurable, по умолчанию 0.15 (вместо 0.01):
   ```go
   if hit.Rank <= cfg.Spec.LSAMinCosine {
       continue
   }
   ```

2. **Относительный cutoff** — отсекать хиты с rank < `LSARelativeCutoff * maxRank` (по умолчанию 0.5):
   ```go
   // После сортировки hits по Rank DESC:
   if len(hits) > 0 {
       cutoff := cfg.Spec.LSARelativeCutoff * hits[0].Rank
       for i := len(hits) - 1; i >= 0; i-- {
           if hits[i].Rank < cutoff {
               hits = hits[:i]
           }
       }
   }
   ```

**Изменения в `config.go`:**

```go
type SpecConfig struct {
    // ... существующие поля ...
    LSAMinCosine        float64 `toml:"lsa_min_cosine"`         // минимальный cosine для LSA-хита (default: 0.15)
    LSARelativeCutoff   float64 `toml:"lsa_relative_cutoff"`   // относительный порог: отсекать < cutoff * maxRank (default: 0.5)
}
```

Дефолты в `ensureDefaults`:
```go
if cfg.Spec.LSAMinCosine <= 0 {
    cfg.Spec.LSAMinCosine = 0.15
}
if cfg.Spec.LSARelativeCutoff <= 0 {
    cfg.Spec.LSARelativeCutoff = 0.5
}
```

**Почему не нарушает спеку:** спека (`spec-search/spec.md:59`) говорит «семантически близкие» — не определяет порог. Текущий 0.01 — реализация. Новый порог фильтрует нерелевантные результаты, улучшая качество «семантически близких».

**Ожидаемый эффект:** шумные хиты с cosine 0.43–0.50 при maxRank 0.60 будут отсечены относительным cutoff (0.5 × 0.60 = 0.30 < 0.43 — пройдут, но если maxRank вырастет до 0.80 после Шага 1, cutoff = 0.40 — отсеет часть шума). Абсолютный порог 0.15 уберёт совсем слабые совпадения.

---

### Шаг 3: Доменные стоп-слова и понижение maxDF

**Файлы:** `internal/specfts/tokenizer.go`, `internal/config/config.go`

**Текущее поведение:** `stopWords` (`tokenizer.go:27-40`) содержит только общеупотребительные русские и английские стоп-слова. Доменные термины («клиент», «документ», «объект», «счёт») не отфильтруются. `maxDF = 0.3` — 30% корпуса.

**Новое поведение:**

1. **Доменные стоп-слова** — добавить в `stopWords` термины, слишком общие для корпуса спек Diasoft 5NT:
   ```go
   // Доменные стоп-слова (слишком общие для корпуса спек)
   "клиент": true, "клиента": true, "клиентов": true, "клиенту": true,
   "документ": true, "документа": true, "документов": true,
   "объект": true, "объекта": true, "объектов": true,
   "счет": true, "счета": true, "счетов": true,
   "операция": true, "операции": true, "операций": true,
   "параметр": true, "параметра": true, "параметров": true,
   ```

2. **Понижение maxDF** — с 0.3 до 0.15 (configurable):
   ```go
   // config.go, ensureDefaults:
   if cfg.Spec.LSAMaxDF <= 0 {
       cfg.Spec.LSAMaxDF = 0.15  // было 0.3
   }
   ```

**Почему не нарушает спеку:** спека (`spec-search/spec.md:27`) говорит «словарь с фильтрацией minDF/maxDF» — конкретные значения не указаны. Стоп-слова не упоминаются, но и не запрещаются. Токенизация с тремя категориями сохраняется.

**Ожидаемый эффект:** «клиент» перестанет доминировать в LSA-векторах. Термины с df > 15% корпуса отфильтруются. Запрос «Заметка по клиенту» будет сопоставляться по термину «заметк» (уникальному) и «клиент» (если он пройдёт фильтр — но с очень низким IDF, почти не влияя на ранжирование).

---

## Порядок выполнения

1. **Шаг 1** (training-данные) → `go build ./...` + `go test ./internal/indexer/... ./internal/store/...`
2. **Шаг 2** (порог) → `go build ./...` + `go test ./internal/specsvc/...`
3. **Шаг 3** (стоп-слова + maxDF) → `go build ./...` + `go test ./internal/specfts/...`
4. Ручная верификация: `codebase init` на FA → `codebase_query_spec_search` с `layer=semantic` для «Заметка по клиенту»

Каждый шаг независим. Шаг 1 даёт наибольший эффект (термин «заметка» появляется в векторе). Шаги 2-3 фильтруют шум.

---

## Ожидаемый результат

| Метрика | Было | Стало |
|---|---|---|
| «Заметка по клиенту» semantic — релевантная capability в топе | Нет | Да (термин «заметк» в векторе) |
| Шумные хиты с cosine 0.43–0.50 | В выдаче | Отсечены (абсолютный + относительный порог) |
| Термин «клиент» в словаре LSA | Да (низкий IDF) | Нет (доменное стоп-слово) |
| maxDF | 0.3 (30%) | 0.15 (15%) |
| Минимальный cosine для попадания в выдачу | 0.01 | 0.15 + относительный cutoff 0.5×maxRank |
| Training-данные на capability | title + purpose + notes | title + purpose + notes + requirements + scenarios |

---

## Совместимость со спекой

| Требование спеки | Изменение | Статус |
|---|---|---|
| LSA на уровне capability (embed_level = 'spec') | Шаг 1: документ включает текст требований, но embed_level = 'spec' | ✅ Не нарушает |
| requirement-уровень покрывается только лексическим слоем | Шаг 1: требования включены в документ capability, не отдельные эмбеддинги | ✅ Не нарушает |
| Раздельные секции результата без слияния | Шаг 2: порог в semantic-секции, exact не затронут | ✅ Не нарушает |
| Словарь с фильтрацией minDF/maxDF | Шаг 3: maxDF изменён, стоп-слова добавлены | ✅ В рамках спеки |
| Токенизация с тремя категориями | Шаг 3: стоп-слова фильтруются после токенизации | ✅ Не нарушает |
