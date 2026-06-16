# Bug: SQL Parser procParamRe Ignores Standard SQL Types (varchar, int, datetime, etc.)

## Summary

CodeBase SQL parser fails to extract procedure parameters whose data type is a standard SQL type (e.g., `varchar`, `int`, `datetime`, `numeric`, `decimal`) instead of a Diasoft `DS*` type. The hard-coded `procParamRe` regular expression only matches types starting with `DS`, causing parameters with SQL-native types to be silently dropped from the index.

## Affected Component

- **File**: `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\parser\sql\sql_parser.go`
- **Function**: `Parse()` method
- **Lines**: 424-425 (regexp initialization), 903-925 (parameter extraction loop)

## Root Cause

The regular expression for procedure parameters is defined as:

```go
procParamRe: regexp.MustCompile(`@([A-Za-z_][A-Za-z0-9_]*)\s+(DS[A-Za-z0-9_]*)(?:\s*=\s*([^,\s]+))?`),
```

It requires the type name to start with `DS` (`DS[A-Za-z0-9_]*`). When the parser encounters a parameter like:

```sql
@Message varchar(250) = null output
```

the type `varchar` does not match `DS[A-Za-z0-9_]*`, so the parameter is never captured and never added to `currentProc.Params`.

This happens during the parameter extraction block:

```go
if inProcSignature {
    paramMatches := p.procParamRe.FindAllStringSubmatch(trimmed, -1)
    for _, paramMatch := range paramMatches {
        // ...build SQLParam...
        currentProc.Params = append(currentProc.Params, model.SQLParam{...})
    }
}
```

Because `FindAllStringSubmatch` returns no match for `@Message varchar(...)`, the parameter is completely lost.

## Steps to Reproduce

1. Create or locate a SQL procedure that mixes `DS*` types with standard SQL types:

```sql
DCL_PROC_BEGIN(TestProc)
              @RetCode DSIDENTIFIER = null,
              @Message varchar(250) = null output
as
/* body */
__BEGIN_PROCEDURE__(TestProc)
__END_PROCEDURE__(TestProc)
go
```

2. Run CodeBase indexing: `codebase update`
3. Query the procedure: `codebase query symbol --name TestProc --type procedure --json`
4. Observe that `params` array contains only `@RetCode`; `@Message` is missing.

## Expected Result

The `params` array should contain **both** parameters:
- `@RetCode` (`DSIDENTIFIER`, direction = in, default = null)
- `@Message` (`varchar(250)`, direction = out, default = null)

## Actual Result

Only `@RetCode` is present in the index. `@Message` is silently omitted, causing downstream tools (review linter, API contract validation, etc.) to report false positives about excess or missing parameters.

## Example Affected Procedure

- **Procedure**: `FCD_Cons_GetRetMessage`
- **File**: `fa-contracts/API_Credit/Server/Facade/FCD_Cons_GetRetMessage.sql`
- **Expected parameters**: 2 (`@RetCode DSIDENTIFIER = null`, `@Message varchar(250) = null output`)
- **Actual parameters in index**: 1 (`@RetCode` only)
- **Downstream impact**: Linter rule `excessProcParams` falsely reports `@Message` as an extra parameter when analyzing calls such as:
  ```sql
  exec FCD_Cons_GetRetMessage @RetCode = @SomeCode, @Message = @msg output
  ```

## Proposed Fix

### Option 1: Extend regex to accept any type (generic SQL + DS)

Change `procParamRe` to allow any type identifier, optionally with a size/precision suffix in parentheses:

```go
procParamRe: regexp.MustCompile(`@([A-Za-z_][A-Za-z0-9_]*)\s+([A-Za-z_][A-Za-z0-9_]*(?:\([^)]*\))?)\s*(?:=\s*([^,\s]+))?`),
```

- Captures `varchar(250)` as a single type token.
- Captures `numeric(18,2)` as a single type token.
- Still compatible with `DSIDENTIFIER`, `DSINT_KEY`, etc.

### Option 2: Use two-pass parsing

1. First pass: match `@Name` and the rest of the line up to the next comma or `as` boundary.
2. Second pass: split the captured substring to extract type, default value, and `output` flag.

This avoids over-complicating a single regex and is easier to maintain.

### Option 3: Relax regex and post-process

```go
procParamRe: regexp.MustCompile(`@([A-Za-z_][A-Za-z0-9_]*)\s+([A-Za-z_][A-Za-z0-9_]*)(?:\([^)]*\))?\s*(?:=\s*([^,\s]+))?`),
```

Then in the loop, check if the line also contains `output` to set direction correctly (current code already does a string-contains check for `output`).

## Impact

- **Severity**: Medium-High
- **Scope**: All SQL procedures declared with `DCL_PROC_BEGIN` (or `create procedure`) that use standard SQL types for parameters
- **Data loss**: Parameter metadata incomplete in CodeBase index
- **Affected features**:
  - `codebase review sql` — false positive `excessProcParams` on every call to affected procedures
  - `codebase query procedure` — incomplete parameter list
  - API contract / caller validation — incorrect arity checks

## Related Code

- **Parser initialization**: Line 424 (`procParamRe`)
- **Parameter extraction**: Lines 903-925
- **Direction detection**: Line 916 (`strings.Contains(strings.ToLower(trimmed), "output")`)
- **Duplicate guard**: Lines 912-914 (`hasProcedureParam`)

## Testing Checklist

- [ ] Test with `DSIDENTIFIER` (existing behavior must remain intact)
- [ ] Test with `varchar(250)`
- [ ] Test with `varchar(250) = null output`
- [ ] Test with `int`, `datetime`, `numeric(18,2)`
- [ ] Test with mixed DS and SQL types in the same procedure signature
- [ ] Re-index `fa-contracts` and verify `FCD_Cons_GetRetMessage` has 2 parameters
- [ ] Re-run `codebase review sql` on `Cons_EffRateRecalcByPTable.sql` and confirm `excessProcParams` at line 558 disappears
- [ ] Verify no regressions in other linter rules

## Discovery Method

- **Procedure**: `FCD_Cons_GetRetMessage`
- **Command**: `codebase review sql C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Consumer\Cons_EffRateRecalcByPTable.sql`
- **Observation**: Linter reported `excessProcParams` for `FCD_Cons_GetRetMessage` at line 558, claiming `@Message` is an extra parameter
- **Verification**: Manual inspection of `FCD_Cons_GetRetMessage.sql` confirmed both parameters exist; `codebase query symbol --name FCD_Cons_GetRetMessage --type procedure --json` returned an empty `params` array (0 results), while `codebase query callers` correctly found 100+ callers, proving the procedure name is indexed but its signature is not.
- **Root-cause confirmation**: Traced to `sql_parser.go:424` where `procParamRe` requires `DS` prefix.
