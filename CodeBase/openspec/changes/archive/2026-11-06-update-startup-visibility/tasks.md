# Tasks: update-startup-visibility

## 1. Модель таймингов

- [x] 1.1 Добавить в `ScanStats` (`internal/model/`) поля `DiscoveryMs`,
      `SafetyNetMs`, `StateLoadMs`, `PrepMiscMs` (последний — CreateScanRun +
      FTS-backfill контрактов)
- [x] 1.2 Расширить `printPipelineTimings` (`cmd/init.go`): печать новых
      таймингов при значении > 0; поправить подпись `Walk+save files`
      (больше не включает подготовительные фазы); в комментарии/выводе
      отметить смену базы `Parallel efficiency` — знаменатель
      `(WalkSaveMs+ProcessMs)` меняет смысл, сравнение с историей прогонов
      требует пере-замера

## 2. Параллельная git-детекция (D2)

- [x] 2.1 Сигнатура `discoverGitSources(ctx, rootPath, detector, parallel)`
      (`internal/indexer/source_registry.go`); вызовы из `UpdateCtx` и
      `runInitPipeline` передают effective parallel; размер пула — через
      существующий `normalizeParallel` (indexer.go:199, защита от `-j 0`)
- [x] 2.2 Разделить discovery на фазы: сбор кандидатов (ReadDir + Stat(.git)
      последовательно) → пул воркеров детекции со сбором результатов по
      индексу → последовательные resolve/upsert/SetDefaultBranch в порядке
      ReadDir. Детектор принять за минимальным интерфейсом
      (`Detect(ctx, dir) (*GitSource, error)`), текущий `*gitDetector` —
      реализация (хук для подставного счётного детектора в тесте)
- [x] 2.3 Счётчик детекции (atomic) для стадии `git detection: N/M` —
      инкремент только по завершении `Detect` каталога; после детекции —
      стадия `source registry: upsert` (upsert'ы + SetDefaultBranch +
      bulk-backfill), без счётчика детекции
- [x] 2.4 Интеграционный тест с подставным счётным детектором: максимум
      одновременных детекций = размер пула и достигает его; результат
      (продукты/линии/commits) идентичен прогону с parallel=1; ошибка exec в
      одном воркере не валит остальные каталоги

## 3. Видимость подготовительных фаз (D1, D5)

- [x] 3.1 В `UpdateCtx` (`internal/indexer/runner.go`): создание collector +
      `startProgressReporter` перенести до `BackfillAPIContractSearchVectors`;
      в `runInitPipeline` — до discovery
- [x] 3.2 Установить стадии через collector, всегда в форме «фаза: деталь»
      (стадия без «: » отрисовуется с пустым префиксом):
      `git detection: N/M` (из 2.3), `source registry: upsert`,
      `safety-net: duplicates`, `load state: files`; сброс Stage при переходе
      к walk
- [x] 3.3 Тайминги D4: снять `DiscoveryMs`/`SafetyNetMs`/`StateLoadMs`/
      `PrepMiscMs` вокруг фаз; `WalkSaveMs` — от фактического старта
      `WalkParallelCtx`
- [x] 3.4 Тест рендера: смена фазы фиксирует строку, счётчик детекции растёт,
      переход к счётчикам walk штатный; кейс стадии с формой «фаза: деталь»
      без пустого префикса
- [x] 3.5 В `cmd/update.go` и `cmd/init.go`: перенести баннер прогона выше
      `store.NewDB`/`InitSchema`, перед `InitSchema` печатать строку
      индикации схемы (миграция v6 не создаёт тихое окно до баннера); время
      схемной работы учтено в `PrepMiscMs`

## 4. Индекс files(path) и sargable-предикаты (D3)

- [x] 4.1 `internal/store/db_schema.go`: DDL
      `CREATE INDEX IF NOT EXISTS idx_files_path ON files(path text_pattern_ops)`
      (колонка — `TEXT`), bump `CurrentSchemaVersion` v5 → v6
- [x] 4.2 `GetLatestFilesByRootPath` (`db_files.go`): убрать ветку `path = $1`
      (семантически мёртвая: root — каталог, file row всегда глубже), LIKE
      заключить в существующий `escapeLikeAll` (сегодня корень с `%`/`_`
      перематчивается)
- [x] 4.3 `FindDuplicateFileGenerations` (`db_reconcile_relations.go`):
      убрать ветку `path = $1`, оставить экранированный `path LIKE $2`
- [x] 4.4 `BackfillFilesFromSources` (`db_products.go`): переписать предикат
      `left(...)` на `(path LIKE prefix||'/%' OR path LIKE prefix||'\%')`
      с экранированием LIKE-метасимволов префикса; использовать общий хелпер
      `escapeLikeAll` (при необходимости вынести в общий файл пакета store)
- [x] 4.5 Юнит-тест экранирования: префиксы/корни с `%`, `_`, `\` дают
      точные совпадения границ и не матчат лишнее
- [x] 4.6 Интеграционные тесты: эквивалентность поведения предикатов до/после
      (rows affected на контрольных данных); миграция v5→v6 создаёт индекс
      (по образцу TestSymbolsOwnerName_BackfillIdempotent: ResetSchemaVersion);
      обязательная структурная EXPLAIN-проверка: узкий префикс при
      `SET enable_seqscan = off` → Index/Bitmap Index Scan по `idx_files_path`
      (планировщик на малых данных волен выбрать Seq Scan — проверяем
      доступность индекса, не выбор планировщика)

## 5. Верификация

- [x] 5.1 `gofmt`, `go vet`, `go build`, unit + integration тесты пакетов
      indexer/store/model/cmd — зелёные; бамп `buildNumber` «1672» → «1676»
      (`cmd/root.go:29`) и пересборка `go build -o CodeBase.exe`
- [x] 5.2 Локальный прогон `codebase update` по FA-корню: стадии видны в
      консоли (детекция со счётчиком, registry/safety-net/load state),
      `Stage timings` атрибутирует подготовительное окно всеми таймингами,
      ход прогона и итоговые Stats не изменились; зафиксировать до/после
      `WalkSaveMs` и `Parallel efficiency` на одном корне (смена базы)
      - Прогон build 1676, FA-корень (18 git-источников), БД `codebase`
        (миграция v6 применена первым прогоном — CREATE INDEX вошёл в
        Prep misc): стадии `git detection: 0/18…17/18` → `source registry:
        upsert` → `load state: files` видны в кадрах; `Stage timings`:
        Prep misc 6811 ms + Prep discovery 2588 ms + Prep safety-net 155 ms
        + Prep load state 4760 ms = 14314 ms подготовительного окна;
        `Walk+save files` 70582 ms — только фаза walk (старая семантика
        дала бы ~84.9 с на том же прогоне: сумма не изменилась, изменилась
        атрибуция). Ход не изменился: scanned=206042, pre-filtered=206035,
        indexed=0, errors=0. `Parallel efficiency` в no-change прогоне не
        печатается (SaveMs=ParseMs=0) — база знаменателя задокументирована
        в 1.2; сравнение требует содержательного прогона после замены бинаря.
- [x] 5.3 `openspec validate update-startup-visibility --strict` — без ошибок

## 6. UX-доводка вывода (по итогам рабочего прогона)

- [x] 6.1 Подготовительные стадии сведены в ОДНУ строку прогресса: общий
      префикс «preparing: » у всех prep-стадий (`preparing: git detection
      N/M` → `preparing: source registry upsert` → `preparing: safety-net
      duplicates` → `preparing: load state files`); репортер не менялся
      (фиксация `\n` только при смене фазы) + флаг «первый кадр» убирает
      ведущий `\n` перед prep-строкой
- [x] 6.2 Индикация схемы — первая деталь prep-строки: cmd печатает
      `preparing | schema check/migrations... ` без перевода строки, после
      `InitSchema` — `done` + очистка строки (`\r`+пробелы+`\r`; кадры
      репортера короче — без очистки `\r`-перезапись оставила бы видимый
      хвост), репортер продолжает строку
- [x] 6.3 Тесты и артефакты синхронизированы:
      TestProgressReporterStageRendering (ровно один `\n`: prep-строка →
      счётчики walk), TestDiscovery_StageSequence (формат «preparing: git
      detection N/4»), дельта progress-reporting + design D1/D5 + proposal
      - Прогон build 1676 (локальный FA-корень, no-change): одна prep-строка
        (`preparing / load state files` — детали детекции/registry на быстром
        диске пролетели между тиками 250 мс; на рабочем ПК видны все),
        затем счётчики walk, spec-lsa/desc-lsa на собственных строках;
        Prep misc 251 + discovery 2968 + safety-net 158 + load state 5346 ms.
