# Acceptance: fix-varuseaftercursor-reassignment-fp (build 1717)

## 1. Билд и тесты (4.1)

- buildNumber 1716 → 1717 (`cmd/root.go:29`).
- `gofmt -l internal\review cmd` — пусто; `go test ./...` — все пакеты ok.
- Unit-тесты: 15 старых + 12 новых `TestCheckVarUseAfterCursor_*` зелёные
  (12-й добавлен по R1 ревью кода: `set @a+=1` без пробела).

## 2. Эталон FP (4.2)

`FA/fa-cards/Cards/SERVER/Card/pCardRegRelation_Load.sql`:
1716 → 4 findings (306/307/312/314); **1717 → 0**.
Kill от присваиваний 306–307 (многострочный select), литерал профайлера на 312
замаскирован (D6), 314 — чтение после kill.

## 3. Якоря регрессии (4.3)

- `InstrumentSO_LoadStates.sql`: ровно 1 varUseAfterCursor (@ChangedBrief, 652).
  A/B 1716 vs 1717 (все правила): total 9 = 9, per-rule идентично
  (execNotExistsProc=1, inSubQuery=4, useFuncInIndCol=3, varUseAfterCursor=1).
  Скан использований остаётся подстрочным: якорь @ChangedBrief ⊂ @ChangedBriefs не изменился.
- `Cons_BankPartnerInstPrepare.sql`: 0 varUseAfterCursor (на 1716 также 0 —
  якорь «нет новых находок», не «исчезновение»).

## 4. Corpus diff, fa-contracts 11991 .sql (4.4)

- Baseline 1716: **10068 findings / 238 файлов**.
- Новый 1717: **1614 findings / 125 файлов**.
- Дифф по уникальным ключам (file,line,object): old-uniq 5586, new-uniq 1442,
  **disappeared 4144, appeared 0**.
- Появившихся находок нет — в т.ч. класс «RHS на column-0 set/select» пуст.
- Верификация исчезновений — PS-оракул поверх expanded-дампа (реальный
  `replaceMacros` + SourceMap; дамп через временный Go-тест, удалён после прогона):
  - reassigned_before_read (переопределение между dealloc и чтением): 3870 из 4144 (93.4%);
  - остаток 274 (6.6%): no_dump 39 (2 файла не прочитались дампером), no_dealloc_expanded 54
    и no_reassign 181 — разобраны выборкой: киллы select-continuation и макро-расширения,
    которые упрощённый регекс-оракул не детектирует (в т.ч. найден баг самого оракула:
    строка-заголовок select закрывала список continuations); выборочная проверка контекстов
    показала реальные присваивания (например Cons_AccrDate_Fill2.sql:524 `@PayDate = PayDate,`).
  - Инструмент киллит только от настоящих присваиваний: EXEC-параметры `@X = @X output`
    и литералы профайлера `, @X = %1!` не являются источниками исчезновений (B1/B2;
    unit-тесты 3.8/3.9; в corpus-анализе таких случаев не выявлено).

## 5. FN-зонды (4.5)

- Sibling-ветки (мера trade-off D1): между киллом и чтением есть `^else\b` —
  3700 находок из 10068 (сырой оракул v5; расширенный v6 даёт 5578 — верхняя оценка,
  прокси без проверки вложенности). Кейсы перестают флаговаться — задокументированный
  trade-off: правило потоково-нечувствительно по дизайну (kill ниже dealloc снимает
  отслеживание независимо от веток). Пример: Cons_AccrDate_Fill2.sql — присваивание
  в then-ветке, чтение ниже.
- B1/B2-зонд: см. п.4 — источников исчезновений нет.

## 6. Артефакты

Все скрипты/выгрузки: `Tests\Temp\vuac_*` (baseline/classify/diff/probe, expanded\,
baseline_varuseaftercursor{,_full}.csv, vuac_classified{,_v4,_v5,_v6}.csv,
vuac_disappeared.csv, vuac_appeared.csv).

Вывод: изменение принято — FP устранены (10068 → 1614), новых находок нет,
регрессий по якорям нет, trade-off D1 измерен и задокументирован.
