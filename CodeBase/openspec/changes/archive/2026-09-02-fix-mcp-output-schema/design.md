## Context

Текущее состояние (см. proposal.md — Why): `registerSDKCoreTools` жёстко декларирует `OutputSchema: defaultToolOutputSchema` для каждого инструмента (`internal/mcp/server.go:62`), при этом ни один обработчик не возвращает `structuredContent` (grep по проекту — 0 упоминаний). Схема описывает конверт ответа (`content`/`isError`), а не payload — семантически неверна даже для заполнения. SDK go-sdk v1.6.0 для нетипизированных хендлеров (`ToolHandler`) переносит валидацию и заполнение полей результата на вызывающего (`mcp/server.go:231-234` SDK). Происхождение схемы — спекулятивная попытка починить регистрацию tools в IDE (коммит `09b2b61`), реальной функции не несёт. Легаси-путь `tools()` (`internal/mcp/tools.go`) используется только тестами; поле `toolDefinition.OutputSchema` других производственных использований не имеет.

## Goals / Non-Goals

**Goals:**

- Соответствие спецификации MCP 2025-06-18: инструменты без `outputSchema` возвращают text-only результат; валидирующие клиенты (opencode) принимают вызовы без `-32600`.
- Wire-level regression-тесты, фиксирующие контракт (tools/list без `outputSchema`; вызов и isError-путь без structuredContent).
- Минимальный диф: удаление мёртвой декларации, без изменения хендлеров и пагинации.

**Non-Goals:**

- Миграция на типизированные хендлеры SDK / честные payload-схемы (вариант B из bug report) — ответы гетерогенны (JSON-текст, пагинационные чанки, `rawMCPText` verbatim), единая payload-схема не строится.
- Изменения `mcp-pagination`, сервисного слоя (querysvc/rtisvc/trcsvc/reviewsvc), CLI.
- Введение per-tool схем для отдельных инструментов (возможно в будущем, контракт задокументирован в спеке).

## Decisions

1. **Убрать декларацию полностью (вариант A), а не заполнять `structuredContent` (вариант B).**
   Text-only результат без схемы валиден по спецификации. Вариант B требует переписать схему под payload каждого инструмента (их ~55 в 5 профилях), ломается на пагинации и `rawMCPText`, где ответ — текстовый чанк. Минимальный диф покрывает цель.

2. **Удалить `defaultToolOutputSchema`, подстановку в `tools()` и поле `toolDefinition.OutputSchema` целиком.**
   Альтернатива — оставить поле «на будущее» — открывает путь снова тихо задекларировать схему без plumbing. Удаление даёт compile-level гарантию: появление `outputSchema` потребует осознанного расширения типа. Поле и подстановка используются только тестами, имя `tools()` сохраняется (тесты продолжают работать без правок).

3. **Хендлеры и пути результата не меняются: `sdkToolPagedResult` / `sdkToolErrorResult` остаются text-only.**
   Без декларации `outputSchema` text-only и isError-результаты полностью валидны. isError-путь спецификацией для structured content не регламентирован однозначно — устранение декларации снимает вопрос.

4. **Regression-тесты — wire-level через `mcpsdk.NewInMemoryTransports()` (есть в SDK v1.6.0), без БД.**
   Паттерн: `mcpsdk.NewServer` + `registerSDKCoreTools(server, buildToolRegistry(nil), profile, nil)` + клиент поверх in-memory пары. БД не нужна: `buildToolRegistry(nil)` уже стандартно используется (в т.ч. package-level `toolRegistry`), `codebase_ping` не обращается к БД (см. `TestPingHandlerReturnsOK`). Три проверки:
   - `tools/list` для всех профилей (`""`, `query`, `rti`, `trc`, `review`): ни один tool не содержит `outputSchema`;
   - `CallTool("codebase_ping")` завершается без ошибки валидации, результат text-only;
   - isError-путь (например, `codebase_rti_parse` с несуществующим путём): `isError=true`, text-only, без протокольной ошибки.

## Risks / Trade-offs

- [Risk] Какой-то клиент рассчитывает на присутствие `outputSchema` в `tools/list` → Mitigation: таких клиентов не известно; схема была envelope-образной и не соответствовала реальным payload-ам, строгие клиенты её отвергают. Удаление только расширяет совместимость.
- [Risk] В будущем инструмент захочет structured output → Mitigation: контракт зафиксирован в дельта-спеке (декларация схемы ⇒ MUST вернуть валидируемый `structuredContent`); в SDK есть типизированный путь `AddTool` с `Out`-параметром, который маршалит и валидирует автоматически.
- [Trade-off] Ручной путь без единой схемы лишает клиентов статической типизации ответов — принято сознательно (ответы и так гетерогенный текст; клиенты — LLM-агенты, читающие текст).

## Migration Plan

1. Удаление кода + тесты (один PR/коммит).
2. Перезапуск MCP-серверов (все профили) — новое `tools/list` без `outputSchema`.
3. Проверка в валидирующем клиенте (opencode): вызов `codebase_ping` проходит без `-32600`.
4. Rollback — git revert единственного коммита; данных/конфигураций для миграции нет.
