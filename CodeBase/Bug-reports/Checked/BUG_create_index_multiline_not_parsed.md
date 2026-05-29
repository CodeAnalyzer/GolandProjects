# Bug: CREATE INDEX Statements Not Parsed in Multiline Format

## Summary

CodeBase SQL parser fails to extract index definitions when CREATE INDEX statements use multiline format. The parser's regular expression expects the entire CREATE INDEX statement (including the ON clause and field list) to be on a single line, but Diasoft SQL scripts often use multiline formatting for better readability.

## Affected Component

- **File**: `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\parser\sql\sql_parser.go`
- **Function**: `Parse()` method
- **Lines**: 98 (createIndexRe regex), 500-509 (parsing logic)

## Root Cause

The regular expression for CREATE INDEX expects single-line format:

```go
createIndexRe: regexp.MustCompile(`(?i)^\s*create\s+(unique\s+)?index\s+([A-Za-z_#][A-Za-z0-9_#]*)\s+on\s+([A-Za-z_#][A-Za-z0-9_#]*)\s*\(([^\)]*)\)`)
```

This regex requires:
1. `CREATE INDEX index_name` at start of line
2. `ON table_name` on the same line after index name
3. `(field1, field2, ...)` on the same line after table name

However, Diasoft SQL scripts (especially in `index.sql` files) use multiline format:

```sql
if not CHECK_INDEX(tCtrCtrRelation,XE1tCtrCtrRelation)
 CREATE  INDEX XE1tCtrCtrRelation
   ON tCtrCtrRelation
 (
        ParentContractID,
        TypeLink 
 )
```

In this format:
- Line 1: `CREATE INDEX XE1tCtrCtrRelation`
- Line 2: `ON tCtrCtrRelation`
- Line 3-5: Field list in parentheses

The regex fails because:
- `\s+on\s+` expects `ON` on the same line after index name
- `\s*\(([^\)]*)\)` expects opening parenthesis on the same line after table name

## Steps to Reproduce

1. Create a SQL file with multiline CREATE INDEX:

```sql
CREATE  INDEX XE1tCtrCtrRelation
   ON tCtrCtrRelation
 (
        ParentContractID,
        TypeLink 
 )
```

2. Run CodeBase indexing: `codebase update`
3. Query the table indices: `codebase query table-index --name tCtrCtrRelation --json`
4. Observe that the index is not found

## Expected Result

The index should be extracted and available via `codebase query table-index`.

## Actual Result

The index is not found because the regex does not match multiline CREATE INDEX statements.

## Example Affected Index

- **Table**: `tCtrCtrRelation`
- **File**: `fa-contracts/Consumer/SERVER/DB/index.sql`
- **Missing indices**:
  - `XPKtCtrCtrRelation` on (CtrCtrRelationID) - UNIQUE
  - `XE1tCtrCtrRelation` on (ParentContractID, TypeLink)
  - `XE2tCtrCtrRelation` on (ContractID, TypeLink)
- **Result**: `codebase query table-index --name tCtrCtrRelation --json` returns empty array

## Proposed Fix

### Option 1: Use multiline regex mode

Modify the parser to use multiline regex mode and adjust the pattern:

```go
// Use multiline mode with (?m) flag
createIndexRe: regexp.MustCompile(`(?im)^\s*create\s+(unique\s+)?index\s+([A-Za-z_#][A-Za-z0-9_#]*)\s+on\s+([A-Za-z_#][A-Za-z0-9_#]*)\s*\(([^\)]*)\)`)
```

However, this alone won't solve the issue because the regex still expects all components on one line.

### Option 2: Stateful parsing for multiline CREATE INDEX

Implement stateful parsing that tracks when CREATE INDEX is started and collects subsequent lines until the closing parenthesis:

```go
// Add state variable
var inCreateIndex bool
var currentIndexName string
var currentTableName string
var currentIndexFields strings.Builder

// In parsing loop
if matches := p.createIndexRe.FindStringSubmatch(trimmed); matches != nil {
    isUnique := strings.TrimSpace(matches[1]) != ""
    indexName := strings.TrimSpace(matches[2])
    tableName := strings.TrimSpace(matches[3])
    indexFields := strings.TrimSpace(matches[4])
    
    // If fields are found on same line, process immediately
    if indexFields != "" {
        appendIndexDefinition(tableName, indexName, indexFields, indexType, "create_index", isUnique, lineNum)
    } else {
        // Start multiline collection
        inCreateIndex = true
        currentIndexName = indexName
        currentTableName = tableName
        currentIndexFields.Reset()
    }
}

// If in multiline CREATE INDEX mode
if inCreateIndex {
    currentIndexFields.WriteString(trimmed)
    // Check for closing parenthesis
    if strings.Contains(trimmed, ")") {
        indexFields := currentIndexFields.String()
        // Extract fields from collected text
        // ... parsing logic
        appendIndexDefinition(currentTableName, currentIndexName, indexFields, indexType, "create_index", isUnique, lineNum)
        inCreateIndex = false
    }
    continue
}
```

### Option 3: Preprocess multiline statements

Before regex matching, join multiline CREATE INDEX statements into single lines:

```go
// Preprocess: join lines that are part of multiline CREATE INDEX
lines := strings.Split(content, "\n")
for i := 0; i < len(lines); i++ {
    line := strings.TrimSpace(lines[i])
    if strings.HasPrefix(strings.ToUpper(line), "CREATE INDEX") || 
       strings.HasPrefix(strings.ToUpper(line), "CREATE UNIQUE INDEX") {
        // Join subsequent lines until closing parenthesis
        for j := i + 1; j < len(lines); j++ {
            if strings.Contains(lines[j], ")") {
                lines[i] += " " + strings.TrimSpace(lines[j])
                lines[j] = "" // Clear joined line
                break
            }
            lines[i] += " " + strings.TrimSpace(lines[j])
            lines[j] = "" // Clear joined line
        }
    }
}
content = strings.Join(lines, "\n")
```

## Impact

- **Severity**: Medium
- **Scope**: All CREATE INDEX statements using multiline format (common in Diasoft index.sql files)
- **Data loss**: Index metadata not available in CodeBase index
- **Affected features**:
  - `codebase query table-index --name <TABLE>` returns incomplete results
  - SQL inspection cannot verify index existence via CodeBase
  - Relation building may miss index usage patterns
  - Developers cannot use CodeBase to find index definitions

## Related Code

- **Parser initialization**: Line 98 (`createIndexRe`)
- **Index parsing**: Lines 500-509
- **Index definition appending**: Lines 215-233

## Testing Checklist

- [ ] Test with single-line CREATE INDEX (should still work)
- [ ] Test with multiline CREATE INDEX (current bug)
- [ ] Test with CREATE UNIQUE INDEX in multiline format
- [ ] Test with multiline field lists
- [ ] Verify index names are correctly extracted
- [ ] Verify table names are correctly extracted
- [ ] Verify index fields are correctly extracted
- [ ] Verify UNIQUE flag is correctly detected
- [ ] Re-index affected codebase after fix
- [ ] Verify query results show correct indices for tCtrCtrRelation

## Discovery Method

- **Table**: `tCtrCtrRelation`
- **Command**: `codebase query table-index --name tCtrCtrRelation --json`
- **Observation**: Empty array despite 3 indices defined in `fa-contracts/Consumer/SERVER/DB/index.sql`
- **Verification**: Manual inspection of `index.sql` confirmed indices exist at lines 289-305
- **Root cause analysis**: Regex pattern expects single-line format, but script uses multiline format
