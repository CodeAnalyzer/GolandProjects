# Чек-лист контрольных запросов (задача 8.2)

Модель: k=512, algorithm=2, 2322 capability, обучена 2026-09-11 19:03 (update после чистки
spec-таблиц). Бинарник: build 1487+. Пороги: min_cosine 0.15, relative_cutoff 0.5.

| Запрос | Слой | Результат | Статус |
|---|---|---|---|
| Заметка по клиенту | semantic | **legal #5 (0.3247)** в топ-5; шум FORMS/385p сверху (0.35–0.52), но в выдаче; oov=[клиент] подсвечен | ✅ |
| Заметка по клиенту + product=fa-generalledger | semantic | **legal #1 (0.3247)** — единственный хит | ✅ |
| Заметка по клиенту | exact | legal requirement «ДПО «Заметка по клиенту без срока»» топ-1 (ts_rank 0.21) | ✅ |
| блокировка счёта | semantic | **card-block #1 (0.5420)**, далее api-debit-card; oov=[счет] | ✅ |
| лимит карточки | semantic | топ-5 целиком accrual-core/…/limit/* (0.42–0.56) | ✅ |
| обработка платежного документа | semantic | factoring-input-output-documents, card-cb-383p, card-transaction — доменно осмысленно | ✅ |
| параметры операции | semantic | accrual-core/…/operation/operation-sum #1–2; oov=[параметр] | ✅ |
| CON_STP_MassAccrual | exact | accrual-core (fa-contracts), tsvector | ✅ |
| API_CCred_BindClassifier | exact | consumer-credit (fa-contracts), tsvector | ✅ |

Динамика headline-кейса «Заметка по клиенту» → legal по итерациям change:

| Состояние | Позиция legal |
|---|---|
| До change (старая модель, title+purpose+notes) | отсутствует в выдаче (шум 0.43–0.51 другого продукта) |
| Состав документа + req/scn, k=128, SVD-баг | все косинусы 0.0 (термин вне обрезанной VT) |
| Фикс SVD, k=128 | #18 (0.2801) |
| **Фикс SVD, k=512 (итог)** | **#5 (0.3247), с фильтром продукта — #1** |

Шум: 2317 из 2322 кандидатов отсечены порогами (threshold 0.15 + cutoff 0.5×maxRank)
на каждом запросе; метаполе semantic_meta (filtered_out/threshold/oov_terms/hint)
возвращается при пустой секции и при OOV-терминах.

Наблюдение (не блокер): в сводке прогона 2194 capability за прогон при 2322 строках
в spec_capabilities — расхождение 128 не разбиралось (возможны capability из
не-specs директорий или дубли корней у 11 конфигов продуктов).
