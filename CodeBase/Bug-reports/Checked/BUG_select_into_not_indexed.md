# Bug: Table Columns Created via SELECT INTO Not Indexed

## Summary

CodeBase SQL parser fails to extract table columns when a table is created using `SELECT ... INTO` syntax. The parser only indexes columns from explicit `CREATE TABLE` definitions and `M_ADD_FIELD` macros, but not columns created dynamically through `SELECT ... INTO` statements.

## Affected Component

- **Component**: SQL Table Schema Indexer
- **Parser**: SQL table creation detection
- **Pattern**: `SELECT ... INTO table_name`

## Root Cause

CodeBase indexes table columns from:
1. Explicit `CREATE TABLE` statements
2. `M_ADD_FIELD` macro calls (schema patches)
3. `ALTER TABLE ... ADD` statements

However, it does NOT parse and index columns from:
- `SELECT ... INTO table_name` statements
- `INSERT ... INTO table_name SELECT ...` when table doesn't exist

This is a common pattern in Diasoft patch files where tables are populated during creation.

## Steps to Reproduce

1. Create a table using SELECT ... INTO:

```sql
select i.InstitutionID,
       i.Brief,
       i.MainMember as Resident
  into tConsInstitutionSync
  from tInstitution i
```

2. Run CodeBase indexing: `codebase update`
3. Query the table schema: `codebase query table-schema --name tConsInstitutionSync --json`
4. Observe that columns are missing (only M_ADD_FIELD columns appear)

## Expected Result

The `table-schema` query should return all columns defined in the SELECT ... INTO statement, including:
- InstitutionID
- Brief
- Resident (aliased from MainMember)

## Actual Result

The `table-schema` query returns only columns added via M_ADD_FIELD macros (e.g., IsBankrupt). Columns from SELECT ... INTO are not indexed.

## Example Affected Table

- **Table**: `tConsInstitutionSync`
- **File**: `fa-contracts/Consumer/SERVER/Patch/patch7_2_1056.sql`
- **Creation method**: SELECT ... INTO (lines 124-154)
- **Expected columns**: InstitutionID, Brief, PropDealPart, Name, Name1, Name2, Resident, INN, BranchID, PORTAL, ExternalID, InDateTime
- **Actual indexed columns**: Only IsBankrupt (from M_ADD_FIELD in patch7_2_2026.sql)

**Query result:**
```json
{
  "table_name": "tConsInstitutionSync",
  "column_name": "IsBankrupt",
  "data_type": "DSTINYINT",
  "definition_kind": "macro_add_field",
  "file": "fa-contracts/Consumer/SERVER/Patch/patch7_2_2026.sql",
  "line_number": 41
}
```

**Missing columns:** Resident (critical for data type validation)

## Impact

- **Severity**: Medium
- **Scope**: All tables created via SELECT ... INTO pattern
- **Data loss**: Column metadata not available in CodeBase index
- **Affected features**:
  - `codebase query table-schema --name <TABLE>` returns incomplete column list
  - Data type validation may miss type mismatches (e.g., Resident: DSTINYINT vs DSINT_KEY)
  - Relation building may be incomplete
  - Code inspector tools cannot detect type mismatches for non-indexed columns

## Proposed Fix

### Option 1: Parse SELECT ... INTO statements

Add regex pattern to detect SELECT ... INTO and extract column names:

```go
// Add to parser initialization
selectIntoRe: regexp.MustCompile(`(?i)select\s+.+?\s+into\s+([A-Za-z_][A-Za-z0-9_]*)`)

// In parser logic
if matches := p.selectIntoRe.FindStringSubmatch(trimmed); matches != nil {
    tableName := matches[1]
    // Parse SELECT clause to extract column expressions
    // Create table entry with inferred column types
    columns := extractSelectColumns(trimmed)
    for _, col := range columns {
        indexTableColumn(tableName, col)
    }
}
```

### Option 2: Use query_fragments as fallback

When table-schema query returns incomplete results, automatically search for query_fragments that reference the table with SELECT ... INTO pattern.

### Option 3: Add manual annotation support

Allow developers to add comments to specify table structure for SELECT ... INTO tables:

```sql
-- @codebase table: tConsInstitutionSync
-- @codebase columns: InstitutionID:DSIDENTIFIER, Brief:DSBRIEFNAME, Resident:DSTINYINT
select i.InstitutionID, i.Brief, i.MainMember as Resident
  into tConsInstitutionSync
  from tInstitution i
```

## Related Code

- **Table creation detection**: SQL parser
- **M_ADD_FIELD handling**: Schema patch indexing
- **Query fragment indexing**: query_fragments table

## Testing Checklist

- [ ] Test with tables created via CREATE TABLE
- [ ] Test with tables created via SELECT ... INTO
- [ ] Test with tables using both CREATE TABLE and SELECT ... INTO
- [ ] Test with aliased columns (e.g., i.MainMember as Resident)
- [ ] Verify column types are inferred or annotated
- [ ] Re-index affected codebase after fix
- [ ] Verify query results show complete column list

## Discovery Method

- **Table**: `tConsInstitutionSync`
- **Command**: `codebase query table-schema --name tConsInstitutionSync --json`
- **Observation**: Only IsBankunt column returned, Resident column missing
- **Verification**: Manual inspection of patch7_2_1056.sql confirmed Resident column exists (line 142: `i.MainMember as Resident`)
- **Impact**: Data type mismatch error (DSINT_KEY → DSTINYINT) not detected by code inspector due to missing Resident column metadata
