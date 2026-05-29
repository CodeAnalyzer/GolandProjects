# Bug: SQL Procedure Parameters Not Parsed for DCL_PROC_BEGIN + __BEGIN_PROCEDURE__ Pattern

## Summary

CodeBase SQL parser fails to extract procedure parameters when a procedure uses both `DCL_PROC_BEGIN` and `__BEGIN_PROCEDURE__` macros. The parser incorrectly resets the `inProcSignature` flag to `false` when encountering `__BEGIN_PROCEDURE__`, which disables parameter parsing.

## Affected Component

- **File**: `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\parser\sql\sql_parser.go`
- **Function**: `Parse()` method
- **Lines**: 382-393

## Root Cause

The regular expression `procBeginRe` matches BOTH `DCL_PROC_BEGIN` and `__BEGIN_PROCEDURE__`:

```go
procBeginRe: regexp.MustCompile(`(?i)(?:DCL_PROC_BEGIN\s*[\(]?|__BEGIN_PROCEDURE__\s*\()([A-Za-z_][A-Za-z0-9_]*)[\)]?`)
```

When the parser encounters a procedure with the following pattern:

```sql
DCL_PROC_BEGIN(FCD_CON_ACM_MassInsertProtocol2)
                 @AccrualID        DSIDENTIFIER       ,
                 @BranchID         DSIDENTIFIER       ,
                 ...
as
__BEGIN_PROCEDURE__(FCD_CON_ACM_MassInsertProtocol2)
```

The execution flow is:

1. Line 14: `DCL_PROC_BEGIN(FCD_CON_ACM_MassInsertProtocol2)` detected
   - `inProcSignature = true` (parameters should be parsed)
   
2. Line 97: `__BEGIN_PROCEDURE__(FCD_CON_ACM_MassInsertProtocol2)` detected
   - `inProcSignature = false` (parameter parsing disabled)
   - This happens because line 387 sets: `inProcSignature = !strings.Contains(strings.ToUpper(trimmed), "__BEGIN_PROCEDURE__")`

## Steps to Reproduce

1. Create a SQL procedure using both `DCL_PROC_BEGIN` and `__BEGIN_PROCEDURE__` macros:

```sql
DCL_PROC_BEGIN(ExampleProc)
                 @Param1 DSIDENTIFIER,
                 @Param2 DSINT_KEY
as
-- Procedure body
__BEGIN_PROCEDURE__(ExampleProc)
  -- Implementation
__END_PROCEDURE__(ExampleProc)
```

2. Run CodeBase indexing: `codebase update`
3. Query the procedure: `codebase query procedure --name ExampleProc --json`
4. Observe that `params` array is empty

## Expected Result

The `params` array should contain all parameters declared between `DCL_PROC_BEGIN` and `__BEGIN_PROCEDURE__`.

## Actual Result

The `params` array is empty because parameter parsing is disabled when `__BEGIN_PROCEDURE__` is encountered.

## Example Affected Procedure

- **Procedure**: `FCD_CON_ACM_MassInsertProtocol2`
- **File**: `fa-contracts/API_Credit/Server/Facade/FCD_CON_ACM_MassInsertProtocol2.sql`
- **Expected parameters**: 11 parameters (@AccrualID, @BranchID, @FinOperID, @MetaDataID, @Number, @ObjectTypeID, @OperSetID, @ParentProtocolID, @StateID, @TransactFlag, @LinkErrorToAudit)
- **Actual parameters**: 0 (empty array)

## Proposed Fix

### Option 1: Skip __BEGIN_PROCEDURE__ when already inside procedure

```go
if matches := p.procBeginRe.FindStringSubmatch(trimmed); matches != nil {
    // If already inside procedure and this is __BEGIN_PROCEDURE__, skip
    if inProcedure && strings.Contains(strings.ToUpper(trimmed), "__BEGIN_PROCEDURE__") {
        continue
    }
    flushStatement(lineNum - 1)
    procName = matches[1]
    procLineStart = lineNum
    inProcedure = true
    inProcSignature = !strings.Contains(strings.ToUpper(trimmed), "__BEGIN_PROCEDURE__")
    currentProc = &model.SQLProcedure{
        ProcName:  procName,
        LineStart: procLineStart,
        Params:    make([]model.SQLParam, 0),
    }
    continue
}
```

### Option 2: Don't reset inProcSignature if already inside procedure

```go
if matches := p.procBeginRe.FindStringSubmatch(trimmed); matches != nil {
    flushStatement(lineNum - 1)
    procName = matches[1]
    procLineStart = lineNum
    inProcedure = true
    // Only set inProcSignature if not already inside procedure
    if !inProcedure {
        inProcSignature = !strings.Contains(strings.ToUpper(trimmed), "__BEGIN_PROCEDURE__")
    }
    currentProc = &model.SQLProcedure{
        ProcName:  procName,
        LineStart: procLineStart,
        Params:    make([]model.SQLParam, 0),
    }
    continue
}
```

### Option 3: Separate regex patterns for DCL_PROC_BEGIN and __BEGIN_PROCEDURE__

Create separate regex patterns and handle them differently:
- `procBeginRe` only for `DCL_PROC_BEGIN`
- `procBodyBeginRe` for `__BEGIN_PROCEDURE__` (only sets inProcSignature to false, doesn't create new procedure)

## Impact

- **Severity**: Medium
- **Scope**: All SQL procedures using the `DCL_PROC_BEGIN` + `__BEGIN_PROCEDURE__` pattern
- **Data loss**: Parameter metadata not available in CodeBase index
- **Affected features**: 
  - `codebase query procedure --name <PROC>` shows empty params
  - API contract validation may miss parameter mismatches
  - Relation building may be incomplete

## Related Code

- **Parser initialization**: Line 87 (`procBeginRe`)
- **Procedure start detection**: Lines 382-393
- **Parameter parsing**: Lines 396-416
- **Procedure end detection**: Lines 424-437

## Testing Checklist

- [ ] Test with procedures using only `DCL_PROC_BEGIN`
- [ ] Test with procedures using only `__BEGIN_PROCEDURE__`
- [ ] Test with procedures using both macros (current bug)
- [ ] Test with standard `CREATE PROCEDURE` syntax
- [ ] Verify parameter types are correctly extracted
- [ ] Verify parameter directions (in/out) are correctly detected
- [ ] Re-index affected codebase after fix
- [ ] Verify query results show correct parameters

## Discovery Method

- **Procedure**: `FCD_CON_ACM_MassInsertProtocol2`
- **Command**: `codebase query procedure --name FCD_CON_ACM_MassInsertProtocol2 --json`
- **Observation**: `params` array empty despite 11 parameters in source file
- **Verification**: Manual inspection of source file confirmed parameters exist between lines 15-36
