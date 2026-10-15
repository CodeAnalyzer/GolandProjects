## 1. Схема: FK с ON DELETE SET NULL

- [x] 1.1 В `internal/store/db_schema.go` изменить DDL `CREATE TABLE rti_client_events`: `server_call_id BIGINT REFERENCES rti_calls(id) ON DELETE SET NULL`. Проверка: integration-тест схемы создаёт FK с `confdeltype='n'` на пустой БД (TestClientEventsFK_FreshSchemaIsSetNull)
- [x] 1.2 Добавить после DDL `rti_client_events` guard-нутый DO-блок миграции (DROP+ADD при `confdeltype='a'`, см. design D3). Проверка: integration-тест «legacy FK → InitSchemaCtx → confdeltype='n'; повторный прогон — no-op (oid констрейнта неизменен)» (TestClientEventsFK_MigratesLegacyConstraint)
- [x] 1.3 Прогнать `go test -tags integration ./internal/store/...` — существующие schema-тесты (включая конкурентные из `schema_version_integration_test.go`) не сломаны (полный прогон ok, 115s)

## 2. Порядок batch-удаления

- [x] 2.1 В `internal/rti/store.go` `DeleteSession`: переставить batch-цикл `rti_client_events` перед batch-циклом `rti_calls` (остальной порядок без изменений). Проверка: integration-тест удаляет сессию с клиентским событием, ссылающимся на вызов, без ошибки FK (TestDeletePruneWithCorrelatedClientEvents)
- [x] 2.2 В `internal/rti/store.go` `PruneSessions`: та же перестановка в цикле по сессиям. Проверка: integration-тест prune удаляет сессию с коррелированным событием (в т.ч. несколько сессий, одна из которых чистая) (TestDeletePruneWithCorrelatedClientEvents)
- [x] 2.3 Integration-тест устойчивости к старому FK: вручную пересоздать `NO ACTION` FK и повторить delete/prune — порядок D1 работает без миграции. Проверка: тест проходит (TestDeletePruneWithLegacyClientEventsFK)

## 3. Проверки и документация

- [x] 3.1 `go build ./...`, `go vet ./...`, `go test ./...` — без ошибок и регрессий
- [x] 3.2 E2E на живой БД: воспроизвести исходный сценарий `codebase rti prune --keep-last 5` (в т.ч. добить частично удалённый хвост от упавшего prune). Проверка: команда завершается успешно, в `rti_sessions` остаётся 5 последних сессий (E2E 2026-10-15: 6 probe-сессий с коррелированными событиями → prune ok, count=5, legacy FK; затем `health` → FK `confdeltype='n'`; `prune --keep-last 0` — зачистка)
- [x] 3.3 Оформить баг-репорт `Bug-reports/Checked/BUG-rti-prune-client-events-fk-20261015.md` со статусом «Решено» (описание проблемы + реализованное решение). README не менялся: там не описываются порядок удаления/FK; Modifications-заметка не заводилась
- [x] 3.4 `openspec validate rti-prune-client-events-fk --strict` — valid
