## ADDED Requirements

### Requirement: FK rti_client_events.server_call_id с ON DELETE SET NULL

Система SHALL объявлять внешний ключ `rti_client_events.server_call_id → rti_calls(id)` с правилом `ON DELETE SET NULL`. На существующих БД, где ограничение создано без правила удаления, схема MUST идемпотентно пересоздавать его с `ON DELETE SET NULL` при очередном прогоне инициализации схемы, не требуя действий от пользователя; повторные прогоны на уже мигрированной схеме MUST быть no-op.

#### Scenario: Свежая БД

- **GIVEN** пустая БД
- **WHEN** выполняется инициализация схемы
- **THEN** FK `rti_client_events_server_call_id_fkey` создан с правилом `ON DELETE SET NULL`

#### Scenario: Миграция существующей БД

- **GIVEN** БД, где FK `rti_client_events.server_call_id` существует без правила удаления (`NO ACTION`)
- **WHEN** выполняется инициализация схемы
- **THEN** ограничение пересоздано с правилом `ON DELETE SET NULL`
- **AND** повторная инициализация схемы не пересоздаёт ограничение (no-op)

#### Scenario: Удаление вызова обнуляет ссылку события

- **GIVEN** клиентское событие с непустым `server_call_id`, ссылающимся на вызов
- **WHEN** вызов удаляется из `rti_calls`
- **THEN** строка клиентского события сохранена, `server_call_id` установлен в NULL
