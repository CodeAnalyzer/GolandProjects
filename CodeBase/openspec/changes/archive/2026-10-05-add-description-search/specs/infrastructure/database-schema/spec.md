# Дельта: infrastructure/database-schema

## ADDED Requirements

### Requirement: FTS-индексация описаний процедур и контрактов

Схема SHALL хранить описание SQL-процедуры в колонке `sql_procedures.description`
типа `TEXT`, добавляемой идемпотентной миграцией (`ADD COLUMN IF NOT EXISTS`).
Схема SHALL поддерживать полнотекстовые векторы:
`sql_procedures.search_vector` (имя процедуры — вес A, описание — вес B,
конфигурация `'russian'`) и `api_contracts.search_vector` (имя контракта — вес A,
краткое и полное описание — вес B). На оба вектора SHALL существовать GIN-индексы;
на `sql_procedures.description` SHALL существовать trgm-GIN-индекс для частичных
совпадений. Бэкфилл векторов SHALL быть идемпотентным SQL-обновлением без
переиндексации файлов (по образцу `EnsureSpecSearchVectors`).

#### Scenario: Миграция на существующей БД

- **GIVEN** проиндексированная БД предыдущей версии схемы
- **WHEN** выполняется `InitSchema`
- **THEN** колонка `sql_procedures.description` и FTS-индексы созданы,
  существующие данные не повреждены

#### Scenario: Бэкфилл вектора контрактов без переиндексации

- **GIVEN** проиндексированная БД с заполненными `api_contracts.full_description`
- **WHEN** выполняется бэкфилл `search_vector`
- **THEN** векторы контрактов заполнены и поиск по описаниям контрактов работает
  без переиндексации файлов

#### Scenario: Описание появляется после перепарсинга

- **GIVEN** БД после миграции, где `sql_procedures.description` пуст
- **WHEN** файл с процедурой перепарсивается (полная пересборка или инкрементальный
  update изменённого файла)
- **THEN** колонка `description` заполнена и вектор процедуры учитывает её

### Requirement: Хранение LSA-публикаций корпуса описаний

Схема SHALL хранить публикации LSA-поколений корпуса описаний в таблицах
`desc_vocab` (generation, term, doc_freq, idf) и `desc_embeddings` (generation,
entity_type, entity_id, embed_text, embedding, embed_method, embed_dim),
ключённых по generation, — по образцу `spec_vocab`/`spec_embeddings`.
Публикация нового поколения SHALL быть транзакционной (удаление поколения +
вставка атомарно); при смене поколения SHALL сохраняться текущее и предыдущее
поколение, остальные удаляться.

#### Scenario: Публикация поколения desc-модели

- **GIVEN** обученная desc-LSA-модель с набором терминов и эмбеддингов
- **WHEN** выполняется публикация поколения
- **THEN** `desc_vocab` и `desc_embeddings` содержат строки нового generation,
  записи атомарно заменены

#### Scenario: Удержание предыдущего поколения

- **GIVEN** опубликованы поколения G1, G2, G3 последовательно
- **WHEN** публикуется G4
- **THEN** остаются только G3 (текущее) и G2 (предыдущее), G1 удалена
