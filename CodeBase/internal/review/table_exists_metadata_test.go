package review

import (
	"context"
	"testing"

	sqlparser "github.com/codebase/internal/parser/sql"
)

// newTableExistsTestRunner создаёт Runner с активным exec-контекстом (правило
// читает expanded-контент через fileContent) и распарсенным содержимым.
func newTableExistsTestRunner(t *testing.T, content string, cache map[string]bool) (*Runner, *sqlparser.ParseResult, *indexedFile) {
	t.Helper()
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	r := &Runner{tableCreatorCache: cache}
	r.exec = &reviewExecContext{filePath: "/virtual/review/test.sql", content: []byte(content)}
	file := &indexedFile{Path: "/virtual/review/test.sql", DsProductID: 42}
	return r, parsed, file
}

// TestCheckTableExistsInMetadata_CreatedTables_NoFinding: таблицы с источником
// создания в индексе (DDL-патч, TmpTbl, API-объявление, SELECT INTO из другого
// файла) не дают finding.
func TestCheckTableExistsInMetadata_CreatedTables_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  insert pTmpTbl (SPID, ID)
  select @@spid, @ID
  select ID from tCreatedByDDL
  select ID from pApiContractTable
  select ID from tCreatedBySelectInto
`
	r, parsed, file := newTableExistsTestRunner(t, content, map[string]bool{
		"tcreatedbyddl":        true,
		"ptmptbl":              true,
		"papicontracttable":    true,
		"tcreatedbyselectinto": true,
	})

	// Guard: все четыре имени реально извлечены парсером в DML-контексте —
	// иначе тест проверял бы только кэш и был бы вакуумным.
	seenDML := map[string]bool{}
	for _, tbl := range parsed.Tables {
		if tableExistsDMLContexts[tbl.Context] {
			seenDML[normalizeIdentifier(tbl.TableName)] = true
		}
	}
	for _, name := range []string{"tcreatedbyddl", "ptmptbl", "papicontracttable", "tcreatedbyselectinto"} {
		if !seenDML[name] {
			t.Fatalf("guard failed: %q должен быть извлечён парсером в DML-контексте, parsed: %v", name, seenDML)
		}
	}

	findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

// TestCheckTableExistsInMetadata_MissingTable_Finding: DML-обращение к таблице
// без источника создания — severity 1, имя и строка обращения в finding.
func TestCheckTableExistsInMetadata_MissingTable_Finding(t *testing.T) {
	content := `create proc TestProc
as
  insert pAPI_Account_MassClose (SPID, AccountID)
  select @@spid, @AccountID
`
	r, parsed, file := newTableExistsTestRunner(t, content, map[string]bool{
		"papi_account_massclose": false, // негативная запись prewarm
	})
	findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d: %v", len(findings), findings)
	}
	f := findings[0]
	if f.Rule != RuleTableExistsInMetadata || f.Severity != SeverityDeployStopper {
		t.Fatalf("rule/severity = %v/%d, want %v/%d", f.Rule, f.Severity, RuleTableExistsInMetadata, SeverityDeployStopper)
	}
	if f.Object != "pAPI_Account_MassClose" {
		t.Fatalf("object = %q, want pAPI_Account_MassClose", f.Object)
	}
	if f.Line != 3 {
		t.Fatalf("line = %d, want 3 (строка INSERT)", f.Line)
	}
	if f.File != file.Path {
		t.Fatalf("file = %q, want %q", f.File, file.Path)
	}
}

// TestCheckTableExistsInMetadata_TempTable_Skipped: временные таблицы сессии
// (#/##) не проверяются.
func TestCheckTableExistsInMetadata_TempTable_Skipped(t *testing.T) {
	content := `create proc TestProc
as
  select ID from #Rest
  select ID from ##GlobalRest
`
	r, parsed, file := newTableExistsTestRunner(t, content, nil)
	findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

// TestCheckTableExistsInMetadata_SameFileCreateTable_NoFinding: CREATE TABLE в
// самом файле закрывает обращение при устаревшем индексе.
func TestCheckTableExistsInMetadata_SameFileCreateTable_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  create table tLocal (SPID int, ID int)
  insert tLocal (SPID, ID) select @@spid, 1
`
	// Кэш без tlocal — создание видно только из самого файла.
	r, parsed, file := newTableExistsTestRunner(t, content, map[string]bool{})
	findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

// TestCheckTableExistsInMetadata_SameFileSelectInto_NoFinding: SELECT ... INTO
// со списком проекции в самом файле создаёт таблицу (column-определения
// select_into) даже при устаревшем индексе.
func TestCheckTableExistsInMetadata_SameFileSelectInto_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  select ts.AccountID as AccountID, ts.OpenDate as OpenDate
    into tReportData
    from tSource ts
  select AccountID from tReportData
`
	// Кэш содержит только источник, но не tReportData.
	r, parsed, file := newTableExistsTestRunner(t, content, map[string]bool{
		"tsource": true,
	})
	findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

// TestCheckTableExistsInMetadata_CursorDeclareOpen_NoFinding: объявление и
// открытие курсора не являются DML-обращением.
func TestCheckTableExistsInMetadata_CursorDeclareOpen_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  declare cur1 cursor for select ID from tContract
  open cur1
  close cur1
  deallocate cur1
`
	r, parsed, file := newTableExistsTestRunner(t, content, map[string]bool{
		"tcontract": true, // select из declare-части — обращение к реальной таблице
	})
	findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

// TestCheckTableExistsInMetadata_CursorFetchAndCurrentOf_NoFinding: парсер
// даёт курсорным именам DML-контекст (FETCH NEXT FROM — select; FETCH после
// многострочного UPDATE — update, воспроизведение замера по tag17_2_APRebuild),
// но курсор, объявленный в файле, таблицей не считается.
func TestCheckTableExistsInMetadata_CursorFetchAndCurrentOf_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  DECLARE DateSelectCursor CURSOR FOR
  select AccountID, OpenDate
    from #Contract
  OPEN DateSelectCursor
  FETCH NEXT FROM DateSelectCursor
  INTO @AccountID, @OpenDate
  WHILE @@FETCH_STATUS = 0
  BEGIN
  update tP
     set SPID = -SPID
    from tP
   where spid = @@spid
    FETCH NEXT FROM DateSelectCursor
    INTO  @AccountID, @OpenDate
  END
  CLOSE DateSelectCursor
  DEALLOCATE DateSelectCursor
`
	r, parsed, file := newTableExistsTestRunner(t, content, map[string]bool{
		"tp": true,
		// dateselectcursor НЕ в кэше — у курсора нет и не должно быть источника создания
	})

	// Guard: парсер действительно помещает имя курсора в DML-контекст —
	// иначе тест стал бы вакантным, а фильтр курсоров бесполезным.
	dmlCursorContexts := map[string]bool{}
	for _, tbl := range parsed.Tables {
		if normalizeIdentifier(tbl.TableName) == "dateselectcursor" && tableExistsDMLContexts[tbl.Context] {
			dmlCursorContexts[tbl.Context] = true
		}
	}
	if !dmlCursorContexts["select"] || !dmlCursorContexts["update"] {
		t.Fatalf("guard failed: курсор должен попадать в DML-контексты select и update, got %v", dmlCursorContexts)
	}

	findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

// TestCheckTableExistsInMetadata_TriggerAndSystemNames_Skipped: триггерные
// pseudo-таблицы и системные каталоги не проверяются.
func TestCheckTableExistsInMetadata_TriggerAndSystemNames_Skipped(t *testing.T) {
	content := `create trigger TestTrigger on tContract
for insert
as
  select ID from inserted
  select ID from deleted
  select name from sysobjects
  select name from syscolumns
`
	r, parsed, file := newTableExistsTestRunner(t, content, map[string]bool{
		"tcontract": true,
	})
	findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

// TestCheckTableExistsInMetadata_StringLiteralNoise_Skipped: DML-глаголы в
// строковых литералах профилировочных вызовов (PROFILE_TIME_EX('update
// contract')) дают парсеру ложные имена — имя, встречающееся на строке только
// внутри кавычек, таблицей не считается.
func TestCheckTableExistsInMetadata_StringLiteralNoise_Skipped(t *testing.T) {
	content := `create proc TestProc
as
  select @MinID = min(b.ID)
    from pCons_MassImport b
   where b.SPID = @@SPID
  PROFILE_TIME_EX('update contract')
`
	r, parsed, file := newTableExistsTestRunner(t, content, map[string]bool{
		"pcons_massimport": true,
	})

	// Guard: парсер действительно извлекает 'update contract' из строкового
	// литерала как DML-таблицу — иначе тест вакуумен.
	quotedNoiseSeen := false
	for _, tbl := range parsed.Tables {
		if normalizeIdentifier(tbl.TableName) == "contract" && tableExistsDMLContexts[tbl.Context] {
			quotedNoiseSeen = true
		}
	}
	if !quotedNoiseSeen {
		t.Fatal("guard failed: парсер должен извлечь contract из PROFILE_TIME_EX('update contract')")
	}

	findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, f := range findings {
		if f.Object == "contract" {
			t.Fatalf("имя из строкового литерала не должно давать finding: %v", f)
		}
	}
}

// TestContainsWord_Boundaries: word-boundary проверка вхождения имени —
// подстроки внутри других токенов не считаются вхождением.
func TestContainsWord_Boundaries(t *testing.T) {
	cases := []struct {
		line string
		name string
		want bool
	}{
		{"update contract", "contract", true},
		{"select [contract].id from t", "contract", true},
		{"update subcontracting", "contract", false},
		{"update contract2", "contract", false},
		{"update _contract", "contract", false},
		{"select contract_id from t", "contract", false},
	}
	for _, c := range cases {
		if got := containsWord(c.line, c.name); got != c.want {
			t.Fatalf("containsWord(%q, %q) = %v, want %v", c.line, c.name, got, c.want)
		}
	}
}

// TestCachedLookupTableCreator_CacheAndFallback: положительная/негативная
// записи читаются из кэша; при промахе (без БД) возвращается false и общий
// кэш не пополняется — запись из горутины правила была бы data race.
func TestCachedLookupTableCreator_CacheAndFallback(t *testing.T) {
	r := &Runner{tableCreatorCache: map[string]bool{
		"tcreated": true,
		"pmissing": false,
	}}
	if !r.cachedLookupTableCreator(context.Background(), "TCreated") {
		t.Fatal("tcreated: want true (регистронезависимый ключ)")
	}
	if r.cachedLookupTableCreator(context.Background(), "pMissing") {
		t.Fatal("pmissing: want false (негативная запись)")
	}
	if r.cachedLookupTableCreator(context.Background(), "pAbsent") {
		t.Fatal("pabsent: want false (промах без БД)")
	}
	if len(r.tableCreatorCache) != 2 {
		t.Fatalf("ленивый путь не должен писать в общий кэш: size = %d, want 2", len(r.tableCreatorCache))
	}
}

// TestCheckTableExistsInMetadata_PlaceholderTokens_Skipped: placeholder-токены
// генераторов (имена из подчёркиваний, __-префикс, ##) не проверяются.
func TestCheckTableExistsInMetadata_PlaceholderTokens_Skipped(t *testing.T) {
	content := `create proc TestProc
as
  select ID from __table__
  select ID from _tablename_
  select ID from t##__table__##
  select ID from __api_table_in__
`
	r, parsed, file := newTableExistsTestRunner(t, content, nil)
	findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}
