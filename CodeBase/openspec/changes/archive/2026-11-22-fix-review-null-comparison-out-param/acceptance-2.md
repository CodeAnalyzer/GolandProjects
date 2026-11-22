# Приёмка: fix-review-null-comparison-out-param

Дата: 2026-11-22. Бинарь: CodeBase 0.9.5 build **1714** (до — build 1713, счётчики «до» взяты из прогона ревью change-review от того же дня).

## Unit-тесты

`go test ./internal/review/ -run TestCheckNullComparison` — **ok**, в т.ч.:

- `TestCheckNullComparison_NoFalsePositive` — все кейсы, включая 5 новых: `null out,` / `null out` / `NULL OUT,` / `null Out ,` / `null out, -- passthrough`;
- `TestCheckNullComparison_Detects` — без изменений, проходят.

## 2.1 Исходный файл-триггер

`FA\fa-cards\Cards\SERVER\Card\Card_OffB_CheckClient.sql`, `--rules nullComparison`:

| build | Findings |
|---|---|
| 1713 (до) | 2 (строки 9–10: `@Message ... = null out,`, `@CheckClient ... = null out`) |
| 1714 (после) | **0** |

## 2.2 Образцы корпуса с короткой формой

| Файл | до (1713) | после (1714) |
|---|---|---|
| `FA\fa-custody\...\SecSwift\SETPRTY_Generate.sql` | 1 | **0** |
| `FA\fa-custody\...\SecSwift\SecSwift_LongLine.sql` | 1 | **0** |
| `FA\fa-custody\...\SecSwift\ValueByBlockStr.sql` | 3 | **0** |
| `FA\fa-payments\...\UFEBS\CBRF_Maket_Unload.sql` | 1 | **0** |

## 2.3 Сохранение true-positive

В корпусе FA нет файла, содержащего одновременно `= null out`-объявление и настоящее сравнение `= NULL` в теле: пересечение списков файлов по двум паттернам пусто (ни одного общего файла среди 823 с объявлениями и 83 со сравнениями), поэтому использован synthetic-файл.

Особенность выполнения: `Tests\Temp` вне `root_path` (FA) — review требует индексированности и `ds_product_id`, поэтому файл временно размещался в `FA\fa-cards\__tmp_null_out_check.sql`, индексировался точечным `update -p`, после прогона удалён из дерева и из индекса (повторный `update -p`, `Files deleted: 1`).

Содержимое (DCL_PROC_BEGIN + два объявления `= null out` + `if @CheckClient = NULL` в теле), результат build 1714:

```
Findings: 1
- [2] nullComparison line=6 object=if @CheckClient = NULL
```

Ровно 1 finding на строке сравнения, 0 на строках объявлений (2–3). FN-регрессии нет.

## Оценка эффекта по корпусу

По эмпирической проверке регулярки из change-review: устраняется **2359** findings (2357 объявлений в `.sql` + 2 в `.h`; 4 вхождения целиком в комментариях findings не давали). 0 оставшихся срабатываний на объявлениях `= null out`, регрессий на истинных сравнениях (включая `OUTPUT`-клаузы UPDATE/SELECT) не найдено.

## Вывод

Приёмка пройдена: класс FP `= null out` устранён, детекция истинных сравнений с NULL сохранена (unit + synthetic + отсутствие регрессий в точечных кейсах ревью).
