## MODIFIED Requirements

### Requirement: Справочник Module ID → Product Name

Система SHALL содержать полный справочник Module ID → Product Name (`internal/rti/symbols.go`, `moduleIDMap`) и предоставлять функцию `ModuleNameByID(moduleID)` для разрешения числового module_id в имя продукта Diasoft в enrichment и сводках. Справочник пополняется вместе с набором модулей Diasoft 5NT; количество записей в требовании сознательно не фиксируется (фактический объём — см. `symbols.go`).

#### Scenario: Разрешение module_id

- **GIVEN** RTI-вызов с `module_id = 25`
- **WHEN** выполняется enrichment через `ModuleNameByID(25)`
- **THEN** возвращено имя продукта (например, «Credit»)
