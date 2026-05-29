# Рекомендация по доработке CodeBase для поддержки подписчиков событий (callback_event)

## Проблема

CodeBase не находит подписчиков для событий при использовании команды `query api-publishers --event <Name>`. 

**Пример:**
- Событие: `OnBeforeRuleD_MassCreate` (контракт в fa-gaap/API_GAAP/DSArchitectData/BObject/RuleDoc/Event/)
- Подписчик: `CON_BeforeRuleD_MassCreate` (callback_event в fa-contracts/API_Credit/DSArchitectData/CallbackEvent/)
- Команда `query api-publishers --event OnBeforeRuleD_MassCreate` не возвращает подписчиков

## Анализ текущей ситуации

### XML структура контракта события
**Файл:** `fa-gaap\API_GAAP\DSArchitectData\BObject\RuleDoc\Event\OnBeforeRuleD_MassCreate.xml`

```xml
<Object>
    <ObjectTypeID>2</ObjectTypeID>
    <ObjectName>OnBeforeRuleD_MassCreate</ObjectName>
    ...
</Object>
```

- `ObjectTypeID=2` - событие
- `ContractKind` в парсере: "event"

### XML структура подписки на событие
**Файл:** `fa-contracts\API_Credit\DSArchitectData\CallbackEvent\CON_BeforeRuleD_MassCreate.xml`

```xml
<Object>
    <ObjectTypeID>5</ObjectTypeID>
    <ObjectName>CON_BeforeRuleD_MassCreate</ObjectName>
    <UsedObjectName>OnBeforeRuleD_MassCreate</UsedObjectName>
    <UsedModuleSysName>API_GAAP</UsedModuleSysName>
    ...
</Object>
```

- `ObjectTypeID=5` - callback_event
- `ContractKind` в парсере: "callback_event"
- `UsedObjectName` - имя события, на которое подписываются
- `UsedModuleSysName` - модуль-владелец события

### Текущая реализация парсера
**Файл:** `Source/internal/parser/dsxml/parser.go`

Парсер корректно извлекает поля `UsedObjectName` и `UsedModuleSysName`:

```go
type objectXML struct {
    ...
    UsedObjectName    string           `xml:"UsedObjectName"`
    UsedModuleSysName string           `xml:"UsedModuleSysName"`
    ...
}
```

Эти поля сохраняются в модель `APIContract` (строки 300-301):

```go
contract := &model.APIContract{
    ...
    UsedObjectName:    strings.TrimSpace(obj.UsedObjectName),
    UsedModuleSysName: strings.TrimSpace(obj.UsedModuleSysName),
    ...
}
```

### Модель данных
**Файл:** `Source/internal/model/model.go`

```go
type APIContract struct {
    ...
    UsedObjectName    string
    UsedModuleSysName string
    ...
}
```

Поля корректно сохраняются в БД (таблица `api_contracts`).

### Query слой
**Файл:** `Source/internal/query/api_query.go`

Функция `SearchAPIPublishers` (строки 310-312):

```go
func (q *Query) SearchAPIPublishers(name string, limit int) ([]APIRelatedProcedureResult, error) {
    return q.searchAPIRelatedProcedures(name, "publishes_event", limit)
}
```

Функция `searchAPIRelatedProcedures` (строки 318-362) ищет подписчиков через таблицу `relations`:

```go
SELECT c.id, c.contract_name, p.id, p.proc_name AS procedure_name, f.rel_path, r.relation_type,
       FALSE AS is_indirect, '' AS via_procedure
FROM relations r
JOIN api_contracts c ON c.id = r.target_id AND r.target_type = 'api_contract'
JOIN sql_procedures p ON p.id = r.source_id AND r.source_type = 'sql_procedure'
JOIN files f ON f.id = p.file_id
WHERE r.relation_type = $1  -- "publishes_event"
  AND c.contract_name ILIKE $2
```

## Корневая причина

**Проблема:** Отсутствуют relations между callback_event и событиями.

Парсер DSArchitect XML:
1. Корректно извлекает `UsedObjectName` и `UsedModuleSysName` из callback_event XML
2. Сохраняет эти поля в таблицу `api_contracts`
3. **НО не создает relations** между callback_event и соответствующим event

Query слой ищет подписчиков только через таблицу `relations`, поэтому callback_event не находятся.

## Рекомендация по доработке

### Вариант 1: Создание relations при индексации callback_event (Рекомендуемый)

Добавить логику создания relations в индексатор при обработке callback_event XML файлов.

**Место изменения:** `Source/internal/indexer/indexer.go`

**Алгоритм:**
1. После сохранения callback_event в БД, проверить поле `UsedObjectName`
2. Если `UsedObjectName` не пустое и `ContractKind == "callback_event"`:
   - Найти ID события в таблице `api_contracts` по критериям:
     - `contract_name = UsedObjectName`
     - `contract_kind = 'event'`
     - `owner_module = UsedModuleSysName` (если указан)
   - Если событие найдено, создать relation:
     - `source_type = 'api_contract'`
     - `source_id = ID callback_event`
     - `target_type = 'api_contract'`
     - `target_id = ID события`
     - `relation_type = 'subscribes_to_event'`

**Пример кода (псевдокод):**

```go
func (idx *Indexer) processAPIContract(contract *model.APIContract) error {
    // Сохранение контракта в БД
    contractID, err := idx.store.SaveAPIContract(contract)
    if err != nil {
        return err
    }
    
    // Создание relation для callback_event
    if contract.ContractKind == "callback_event" && contract.UsedObjectName != "" {
        eventID, err := idx.store.FindAPIContractID(contract.UsedObjectName, "event", contract.UsedModuleSysName)
        if err == nil && eventID > 0 {
            relation := &model.Relation{
                SourceType:   "api_contract",
                SourceID:     contractID,
                TargetType:   "api_contract",
                TargetID:     eventID,
                RelationType: "subscribes_to_event",
                LineNumber:   1,
            }
            if err := idx.store.SaveRelation(relation); err != nil {
                log.Printf("Warning: failed to create callback_event relation: %v", err)
            }
        }
    }
    
    return nil
}
```

**Дополнительно в store:** Добавить метод для поиска контракта по имени, kind и модулю:

```go
func (db *DB) FindAPIContractID(name string, kind string, ownerModule string) (int64, error) {
    var id int64
    var query string
    var args []interface{}
    
    if ownerModule != "" {
        query = `SELECT id FROM api_contracts WHERE LOWER(contract_name)=LOWER($1) AND LOWER(contract_kind)=LOWER($2) AND LOWER(owner_module)=LOWER($3) ORDER BY id DESC LIMIT 1`
        args = []interface{}{name, kind, ownerModule}
    } else {
        query = `SELECT id FROM api_contracts WHERE LOWER(contract_name)=LOWER($1) AND LOWER(contract_kind)=LOWER($2) ORDER BY id DESC LIMIT 1`
        args = []interface{}{name, kind}
    }
    
    err := db.QueryRow(query, args...).Scan(&id)
    if err != nil {
        return 0, err
    }
    return id, nil
}
```

### Вариант 2: Изменение query слоя для поиска через UsedObjectName

Изменить `SearchAPIPublishers` для поиска callback_event через поля `UsedObjectName` и `UsedModuleSysName`.

**Место изменения:** `Source/internal/query/api_query.go`

**Алгоритм:**
1. Добавить UNION к существующему запросу для поиска callback_event
2. Искать callback_event, где `UsedObjectName = имя события`
3. Возвращать callback_event как подписчиков

**Пример кода:**

```go
func (q *Query) SearchAPIPublishers(name string, limit int) ([]APIRelatedProcedureResult, error) {
    rows, err := q.db.Query(`
        SELECT *
        FROM (
            -- Существующая логика для publishes_event через relations
            SELECT c.id, c.contract_name, p.id, p.proc_name AS procedure_name, f.rel_path, r.relation_type,
                   FALSE AS is_indirect, '' AS via_procedure
            FROM relations r
            JOIN api_contracts c ON c.id = r.target_id AND r.target_type = 'api_contract'
            JOIN sql_procedures p ON p.id = r.source_id AND r.source_type = 'sql_procedure'
            JOIN files f ON f.id = p.file_id
            WHERE r.relation_type = 'publishes_event'
              AND c.contract_name ILIKE $1

            UNION

            -- Новая логика для callback_event через UsedObjectName
            SELECT cb.id, cb.contract_name, cb.id, cb.contract_name AS procedure_name, f.rel_path, 
                   'subscribes_to_event' AS relation_type,
                   FALSE AS is_indirect, '' AS via_procedure
            FROM api_contracts cb
            JOIN files f ON f.id = cb.file_id
            WHERE cb.contract_kind = 'callback_event'
              AND LOWER(cb.used_object_name) = LOWER($1)
        ) rel
        ORDER BY contract_name, is_indirect, procedure_name
        LIMIT $2
    `, name, limit)
    // ... обработка результатов
}
```

**Недостатки Варианта 2:**
- Не создает relations, что нарушает целостность графа связей
- callback_event возвращаются как "процедуры", что семантически неверно
- Не позволяет использовать callback_event в других query (например, relations)

### Вариант 3: Комбинированный подход

Реализовать Вариант 1 (создание relations) + изменить query слой для поддержки нового типа relation `subscribes_to_event`.

**Преимущества:**
- Сохраняется целостность графа связей
- callback_event можно использовать в других query
- Семантически корректное решение

## Рекомендуемый план действий

1. **Реализовать Вариант 1** - создание relations при индексации callback_event
2. **Добавить новый тип relation** `subscribes_to_event` в модель
3. **Обновить query слой** для поддержки поиска по `subscribes_to_event`
4. **Добавить тесты** для проверки:
   - Создание relations для callback_event
   - Поиск подписчиков через `query api-publishers`
   - Отображение callback_event в `query relations`

## Дополнительные улучшения

### 1. Логирование при индексации
Добавить логирование при создании/пропуске relations для callback_event:

```go
if contract.ContractKind == "callback_event" {
    if contract.UsedObjectName != "" {
        log.Printf("Processing callback_event %s -> %s (module: %s)", 
            contract.ContractName, contract.UsedObjectName, contract.UsedModuleSysName)
    } else {
        log.Printf("Warning: callback_event %s has empty UsedObjectName", contract.ContractName)
    }
}
```

### 2. Валидация при индексации
Добавить валидацию для callback_event:
- Проверить, что `UsedObjectName` указан
- Проверить, что событие существует (если указан `UsedModuleSysName`)
- Логировать предупреждения для невалидных callback_event

### 3. Обновить документацию
Добавить в README.md описание механизма callback_event и relations.

## Проверка после реализации

После реализации доработки проверить:

```powershell
# Поиск события
& ".\Tools\CodeBase\CodeBase.exe" query api-contract --name OnBeforeRuleD_MassCreate --json

# Поиск подписчиков (должен вернуть CON_BeforeRuleD_MassCreate)
& ".\Tools\CodeBase\CodeBase.exe" query api-publishers --event OnBeforeRuleD_MassCreate --json

# Проверка relations
& ".\Tools\CodeBase\CodeBase.exe" query relations --source-type api_contract --source-name CON_BeforeRuleD_MassCreate --json
```

## Заключение

Проблема отсутствия поиска подписчиков событий решается путем создания relations между callback_event и событиями при индексации. Рекомендуемый подход - Вариант 1 (создание relations) как наиболее семантически корректный и сохраняющий целостность графа связей CodeBase.
