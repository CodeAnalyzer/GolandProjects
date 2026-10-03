package sql

import (
	"testing"

	"github.com/codebase/internal/model"
)

// Тесты по дельте change fix-sql-tables-update-pollution:
// LHS-идентификаторы UPDATE ... SET, хинт-макросы M_*/#M_* и макро-плейсхолдеры
// ##...## не попадают в sql_tables; колонки многострочного SET — в sql_columns.

// tableNames собирает множество имён таблиц из результата парсинга
func tableNames(result *ParseResult) map[string]bool {
	names := make(map[string]bool)
	for _, tbl := range result.Tables {
		names[tbl.TableName] = true
	}
	return names
}

// assertTableNames проверяет, что множество имён таблиц совпадает с ожидаемым
func assertTableNames(t *testing.T, result *ParseResult, want []string) {
	t.Helper()
	got := tableNames(result)
	if len(got) != len(want) {
		t.Fatalf("table count: got=%v want=%v", got, want)
	}
	for _, name := range want {
		if !got[name] {
			t.Fatalf("table %q not found, got=%v", name, got)
		}
	}
}

// assertNoTables проверяет, что указанные имена отсутствуют среди таблиц
func assertNoTables(t *testing.T, result *ParseResult, names ...string) {
	t.Helper()
	got := tableNames(result)
	for _, name := range names {
		if got[name] {
			t.Fatalf("table %q should not be extracted, got=%v", name, got)
		}
	}
}

// findColumn ищет колонку по имени, привязанную к таблице
func findColumn(result *ParseResult, tableName, columnName string) *model.SQLColumn {
	for _, col := range result.Columns {
		if col.TableName == tableName && col.ColumnName == columnName {
			return col
		}
	}
	return nil
}

// TestParseContent_MultilineUpdateSet_Repro — воспроизведение кейса
// Cons_GetDocToProcess.sql:813-821 (сценарий «Многострочный UPDATE SET»)
func TestParseContent_MultilineUpdateSet_Repro(t *testing.T) {
	parser := NewParser()
	content := `update pCreditDocument
      set OperationType   = f.Condition,
          TemplateSysName = f.UserTag
     from pCreditDocument       p M_UPDLOCK_INDEX(XIE0pCreditDocument)
    inner join pAPI_FO_Template f M_NOLOCK_INDEX(XPKpAPI_FO_Template)
            on f.Spid = @@Spid
           and f.TemplateID = p.OperTemplateID
    where p.Spid = @@Spid
   M_FORCEORDER
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	assertTableNames(t, result, []string{"pCreditDocument", "pAPI_FO_Template"})
	assertNoTables(t, result, "TemplateSysName", "OperationType", "M_FORCEORDER", "set")

	if col := findColumn(result, "pCreditDocument", "OperationType"); col == nil {
		t.Fatal("column OperationType not saved to sql_columns for pCreditDocument")
	}
	if col := findColumn(result, "pCreditDocument", "TemplateSysName"); col == nil {
		t.Fatal("column TemplateSysName not saved to sql_columns for pCreditDocument")
	}
}

// TestParseContent_UpdateSetSameLine — UPDATE и SET в одной строке
// (сценарий «UPDATE и SET в одной строке»)
func TestParseContent_UpdateSetSameLine(t *testing.T) {
	parser := NewParser()
	content := `update t set a = 1,
b = 2
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	assertTableNames(t, result, []string{"t"})
	assertNoTables(t, result, "a", "b")

	if col := findColumn(result, "t", "a"); col == nil {
		t.Fatal("column a not saved to sql_columns for t")
	}
	if col := findColumn(result, "t", "b"); col == nil {
		t.Fatal("column b not saved to sql_columns for t")
	}
}

// TestParseContent_HintMacroAfterJoin — хинт-макрос после списка JOIN
// (сценарий «Хинт-макрос после списка JOIN»)
func TestParseContent_HintMacroAfterJoin(t *testing.T) {
	tests := []struct {
		name string
		hint string
	}{
		{name: "M_FORCEORDER", hint: "M_FORCEORDER"},
		{name: "hashM_FORCEORDER", hint: "#M_FORCEORDER"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parser := NewParser()
			content := `update tDoc
     from xref x
inner join yTab y on y.ID = tDoc.ID
   ` + tc.hint + `
`

			result, err := parser.ParseContent(content)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}

			assertTableNames(t, result, []string{"tDoc", "xref", "yTab"})
			assertNoTables(t, result, tc.hint)
		})
	}
}

// TestIsIgnoredTableName — фильтры хинт-макросов и плейсхолдеров
// (сценарии «Хинт-макрос после списка JOIN», «Макро-плейсхолдер в шаблоне DDL»)
func TestIsIgnoredTableName(t *testing.T) {
	parser := NewParser()

	ignored := []string{
		"M_FORCEORDER", "#M_FORCEORDER", "M_KEEPPLAN", "M_ISOLAT",
		"##M_TABNAME##", "##OUT_COMMIS_TABLE", "##_TABLENAME_##", "##mTable##",
	}
	for _, name := range ignored {
		if !parser.isIgnoredTableName(name) {
			t.Fatalf("%q should be ignored", name)
		}
	}

	legit := []string{"tContract", "pCreditDocument", "pAPI_FO_Template", "#Temp", "tOwnerPayment"}
	for _, name := range legit {
		if parser.isIgnoredTableName(name) {
			t.Fatalf("%q should not be ignored", name)
		}
	}
}

// TestParseContent_MultilineSetColumns — колонки многострочного SET
// сохраняются в sql_columns (сценарий «Колонки многострочного SET»)
func TestParseContent_MultilineSetColumns(t *testing.T) {
	parser := NewParser()
	content := `update t set a = 1,
b = 2
from t2
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	assertTableNames(t, result, []string{"t", "t2"})

	if col := findColumn(result, "t", "a"); col == nil {
		t.Fatal("column a not saved to sql_columns for t")
	}
	if col := findColumn(result, "t", "b"); col == nil {
		t.Fatal("column b not saved to sql_columns for t")
	}
}

// TestParseContent_FromCommaList — регресс: список FROM через запятую
// (сценарий «Регресс — список FROM через запятую»)
func TestParseContent_FromCommaList(t *testing.T) {
	parser := NewParser()
	content := `select *
  from t1,
       t2,
       t3
 where 1 = 1
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	assertTableNames(t, result, []string{"t1", "t2", "t3"})
}

// TestParseContent_InsertIntoValuesNextLine — регресс: INSERT INTO с VALUES
// на следующей строке (сценарий «Регресс — INSERT INTO с VALUES»)
func TestParseContent_InsertIntoValuesNextLine(t *testing.T) {
	parser := NewParser()
	content := `insert into t (a, b)
values (@a, @b)
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	assertTableNames(t, result, []string{"t"})
	assertNoTables(t, result, "values", "a", "b")
}

// TestParseContent_WhereCommentContinuation — `where--` без пробела должен
// завершать режим списка таблиц (остаточный вектор из верификации на FA)
func TestParseContent_WhereCommentContinuation(t *testing.T) {
	parser := NewParser()
	content := `update tRaceCommentCard
   set Comment = @Comment
  from tRaceCommentCard M_UPDLOCK_INDEX(XIE0tRaceCommentCard)
 where-- Spid = @@spid
         MBBufferID = @MBBufferID
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	assertTableNames(t, result, []string{"tRaceCommentCard"})
	assertNoTables(t, result, "MBBufferID", "where")

	if col := findColumn(result, "tRaceCommentCard", "Comment"); col == nil {
		t.Fatal("column Comment not saved to sql_columns")
	}
}

// TestParseContent_InsertDeleteIgnorePlaceholders — плейсхолдеры и хинты
// не извлекаются в insert/delete контекстах; lowercase m_table (легитимная
// мем-таблица FA) сохраняется
func TestParseContent_InsertDeleteIgnorePlaceholders(t *testing.T) {
	parser := NewParser()
	content := `insert into ##OUTTABLE (a) values (1)
delete ##ERRTABLE where a = 1
delete m_table where a = 2
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	assertNoTables(t, result, "##OUTTABLE", "##ERRTABLE", "M_FORCEORDER")

	found := false
	for _, tbl := range result.Tables {
		if tbl.TableName == "m_table" {
			found = true
		}
	}
	if !found {
		t.Fatalf("legit m_table should be kept, got=%v", tableNames(result))
	}
}

// TestParseContent_MacroPlaceholderDDL — макро-плейсхолдеры в шаблонах DDL
// не сохраняются в sql_tables (сценарий «Макро-плейсхолдер в шаблоне DDL»)
func TestParseContent_MacroPlaceholderDDL(t *testing.T) {
	parser := NewParser()
	content := `create table ##M_TABNAME## (ID int NULL)
go
select * from ##OUT_COMMIS_TABLE
go
create table ##_TABLENAME_## (ID int NULL)
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if len(result.Tables) != 0 {
		t.Fatalf("placeholders should not be extracted as tables, got=%v", tableNames(result))
	}
	if len(result.ColumnDefinitions) != 0 {
		t.Fatalf("placeholder columns should not be extracted, got=%d", len(result.ColumnDefinitions))
	}
}

// TestParseContent_LocalTempTableUntouched — локальная временная таблица
// (одно #) не задета фильтром плейсхолдеров (сценарий «Локальная временная
// таблица сохраняется»)
func TestParseContent_LocalTempTableUntouched(t *testing.T) {
	parser := NewParser()
	content := `select Col1, Col2 into #Temp from tContract
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	found := false
	for _, tbl := range result.Tables {
		if tbl.TableName == "#Temp" {
			found = true
			if !tbl.IsTemporary {
				t.Fatal("#Temp should have is_temporary = true")
			}
		}
	}
	if !found {
		t.Fatalf("#Temp not found in tables, got=%v", tableNames(result))
	}
}
