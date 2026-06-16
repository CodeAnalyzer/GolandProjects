# Bug Report: excessProcParams produces false positives for API contracts

**Severity:** Medium (false positive lint findings)
**Rule:** `excessProcParams`
**Component:** `internal/review/review_lookup.go` — `lookupProcedureParams`

---

## Problem Summary

The `excessProcParams` reviewer rule incorrectly reports that parameters passed to an API contract procedure are "excess" because `lookupProcedureParams` only searches the `sql_procedures` table and ignores `api_contract_params`.

---

## Reproduction Steps

1. Run `codebase review` on any SQL file that calls an API contract procedure, e.g.:
   ```sql
   exec API_Loan_GetListEffRateByDate
          @ConstraintMode = 2
   ```

2. Observe the finding:
   ```
   - [2] excessProcParams line=456 object=API_Loan_GetListEffRateByDate
     Передача лишних параметров или дублирование параметров в вызове процедуры:
     процедура не принимает параметры, но передано 1
   ```

3. Verify the contract exists and has parameters:
   - `codebase query api-contract --name API_Loan_GetListEffRateByDate --json`
   - The contract has `context` / `input` / `output` params stored in `api_contract_params`.

---

## Root Cause

`lookupProcedureParams` (review_lookup.go:1180) queries **only** `sql_procedures`:

```go
func (r *Runner) lookupProcedureParams(procName string) ([]model.SQLParam, error) {
    SELECT parameters
    FROM sql_procedures
    WHERE LOWER(proc_name) = LOWER($1)
    ORDER BY id DESC
    LIMIT 1
}
```

API contract procedures are declared via the macro `__BEGIN_PROCEDURE__` (not a native `CREATE PROCEDURE` statement), so the SQL parser does **not** extract their parameters into `sql_procedures.parameters`. The parameters are instead indexed from DSArchitect XML into `api_contract_params`.

Because `lookupProcedureParams` returns `[]` (empty params), `validateExecArguments` treats any passed argument as an excess parameter.

---

## Expected Behavior

`lookupProcedureParams` should implement a **fallback**:

1. First, try `sql_procedures` as it does now.
2. If empty / not found, look up the contract in `api_contracts` by `contract_name`.
3. Load parameters from `api_contract_params` where `contract_id` matches.
4. Map `APIContractParam` fields to `SQLParam` for compatibility with `validateExecArguments`.

---

## Database Schema Reference

Table `api_contract_params` (db_schema.go:295):
```sql
CREATE TABLE IF NOT EXISTS api_contract_params (
    id BIGSERIAL PRIMARY KEY,
    contract_id BIGINT NOT NULL REFERENCES api_contracts(id),
    direction TEXT NOT NULL,     -- input | output | context
    param_name TEXT NOT NULL,
    type_name TEXT,
    required bool,
    ...
);
```

Model `APIContractParam` (model.go:454):
```go
type APIContractParam struct {
    ID         int64
    ContractID int64
    Direction  string // input, output, context
    ParamName  string
    TypeName   string
    Required   bool
    ...
}
```

---

## Affected Findings

- `excessProcParams` — false positives for any `exec API_*` call that passes scalar parameters.
- Potentially `maxProcParam` and `procParamDefValue` if they also use `lookupProcedureParams`.

---

## Suggested Fix

Add a new method (e.g., `lookupAPIContractParams`) in `review_lookup.go` and chain it inside `lookupProcedureParams` when `sql_procedures` yields no parameters.

Mapping logic (pseudocode):
```go
func (r *Runner) lookupAPIContractParams(procName string) ([]model.SQLParam, error) {
    var contractID int64
    err := r.db.QueryRow(`
        SELECT id FROM api_contracts
        WHERE LOWER(contract_name) = LOWER($1)
        ORDER BY id DESC LIMIT 1
    `, procName).Scan(&contractID)
    if err == sql.ErrNoRows {
        return nil, nil
    }
    if err != nil {
        return nil, err
    }
    // load params from api_contract_params
    // map each to model.SQLParam{ Name: param_name, Type: type_name, Direction: direction }
}
```

Then update `lookupProcedureParams`:
```go
func (r *Runner) lookupProcedureParams(procName string) ([]model.SQLParam, error) {
    params, err := r.lookupSQLProcedureParams(procName)
    if err == nil && len(params) > 0 {
        return params, nil
    }
    return r.lookupAPIContractParams(procName)
}
```

---

## Related Bug Report

- `BUG-procParamRe-varchar-not-parsed.md` — covers the SQL parser's inability to read parameters from `__BEGIN_PROCEDURE__` macros. The current bug is a **downstream consequence**: even if the parser is fixed, API contract parameters live in `api_contract_params`, so the reviewer still needs the fallback described here.
