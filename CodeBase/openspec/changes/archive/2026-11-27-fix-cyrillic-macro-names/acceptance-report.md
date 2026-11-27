# Приёмка: fix-cyrillic-macro-names (tasks 5.1-5.4)

Дата: 2026-11-27, build 1724 (базлайн до: build 1723, run_masked_1723).
Прогоны: `Tests\Temp\macro_budget_measure\` (run_1724.tsv, run_1724/, diff_228_1724.txt),
скрипты mask_dq (run_batch.ps1, compare_runs.ps1).

## 5.1 — Ранее виснущие файлы завершаются

dq228 (228 файлов, timeout 180 с): **228/228 PASS, 0 TIMEOUT**.
Все 20 ранее виснущих (16 hang16 + 4 маска-эры) завершились:

| Файл | Время | cnt |
|---|---|---|
| RPT_6406U_F711_ExportToXml.sql | 65.9 с | 0 |
| RPT_7047U_F711_ExportToXml.sql | 66.1 с | 0 |
| RPT_7404U_F711_ExportToXml.sql | 65.6 с | 0 |
| RPT_F711_ExportToXml.sql | 50.8 с | 0 |
| RPT_7047U_718 / RPT_718_ExportToXml | 10.0 / 9.9 с | 0 |
| остальные 14 (724/Delta_XML-семейство) | 2.0-4.7 с | 0-3 |

`GuardTrippedLines == 0` на всех 20 (маскированный замер, см. 5.2): висяга ушла
из-за корректной идентификации, не из-за бюджета. Бонус: медленный линейный
каскад тоже ушёл — RPT_F350_ExportToXml 48.3 с → 1.9 с, RPT_F128 18.4 с → 4.0 с.

## 5.2 — Дельты findings на 36 кириллических файлах

- **Критерий чистоты**: `GuardTrippedLines == 0` на всех 36 — masked-замер
  (последовательность runner: mask → replaceMacros): GuardTripped = none.
- **Параметры ASCII**: 303 `#define` по 36 файлам, 0 с не-ASCII параметрами
  (байтовый скан CP866).
- **16 не-виснущих** (до = PASS на 1723): findings идентичны — 0 добавлено /
  0 удалено (ключи rule|line|object, compare_runs по 208 both-PASS; коллизий
  basename среди них нет). Ускорение без изменения результата.
- **20 виснущих** (до = TIMEOUT, находок нет): после — PASS, суммарно **10 новых
  findings**, все `nullComparison` в 4 файлах `_724_ExportToXml` (строки
  476-703). Проверка по исходникам: строки — литеральный код
  `, '@TINБрокер' = null`, `, '@NUMБрокер' = null` в конструкциях FOR XML PATH
  (например, RPT_6406U_724_ExportToXml.sql:478-479). Находки заякорены на
  реальные исходные строки, не на макро-артефакты → **легитимны** (правила
  впервые видят реальный SQL; семантика правила не менялась).
- Удалённых findings нет (у 16 — идентичность, у 20 — нечему удаляться).

## 5.3 — Оставшиеся 9 файлов mixed45

до/после PASS, findings идентичны: filldatatopartition_tCardTranExt ×2 (4/4),
RPT_Exec (4/4), FCD_RPT_DWVit_ProcessCorrection (20/20), RPT_7047U_0409706 /
RPT_0409302 / RPT_F712 / RPT_0409120 / RPT_0409706 (0/0). 0 добавлено / 0 удалено.

## 5.4 — Основной контур эквивалентности: dq228 − 36 = 192 файла

compare_runs (208 both-PASS ⊇ 192): **findings removed: 0, findings ADDED: 0,
REGRESSION: 0**. `GuardTrippedLines = 0` по всему корпусу (raw и masked),
`PassesExhausted`: none — pre-existing усечения 32 проходов на корпусе не
выявлено (диагностическая сводка). Артефакт: diff_228_1724.txt.

## Итог

Все критерии 5.1-5.4 выполнены. Единственные изменения findings на корпусе —
10 легитимных nullComparison в ранее недоступных (виснущих) файлах.
