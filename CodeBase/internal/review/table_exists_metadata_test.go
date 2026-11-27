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

// assertDMLReference — guard: имя реально извлечено парсером в DML-контексте,
// иначе тест свидетельств проверял бы только кэш.
func assertDMLReference(t *testing.T, parsed *sqlparser.ParseResult, name string) {
	t.Helper()
	for _, tbl := range parsed.Tables {
		if tableExistsDMLContexts[tbl.Context] && normalizeIdentifier(tbl.TableName) == name {
			return
		}
	}
	t.Fatalf("guard failed: %q должен быть извлечён парсером в DML-контексте", name)
}

// TestCheckTableExistsInMetadata_Evidence_NoFinding: таблица без создающего
// источника, но со свидетельством изменения структуры в индексе (M_ADD_FIELD,
// ALTER TABLE ADD, индексный патч) finding не даёт.
func TestCheckTableExistsInMetadata_Evidence_NoFinding(t *testing.T) {
	cases := []struct {
		name     string
		content  string
		table    string
		evidence string // ключ записи в кэше свидетельств
	}{
		{
			name: "M_ADD_FIELD",
			content: `M_ADD_FIELD('tRequisitParticipantSB', 'InstitutionID DSIDENTIFIER')
select InstitutionID from tRequisitParticipantSB
`,
			table:    "trequisitparticipantsb",
			evidence: "trequisitparticipantsb",
		},
		{
			name: "ALTER TABLE ADD",
			content: `alter table tClientService add NewCol DSIDENTIFIER
update tClientService set NewCol = 1
`,
			table:    "tclientservice",
			evidence: "tclientservice",
		},
		{
			name: "индексный патч",
			content: `M_CRT_INDEX('XIE1tWebUser', 'tWebUser', 'WebUserID')
delete from tWebUser
`,
			table:    "twebuser",
			evidence: "twebuser",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, parsed, file := newTableExistsTestRunner(t, c.content, map[string]bool{c.table: false})
			assertDMLReference(t, parsed, c.table)
			r.tableEvidenceCache = map[string]bool{c.evidence: true}
			findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(findings) != 0 {
				t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
			}
		})
	}
}

// TestCheckTableExistsInMetadata_NoEvidence_FindingNewMessage: без создателя и
// без свидетельств — finding с перечислением расширенного набора источников.
func TestCheckTableExistsInMetadata_NoEvidence_FindingNewMessage(t *testing.T) {
	content := `create proc TestProc
as
  insert pAPI_Account_MassClose (SPID, AccountID)
  select @@spid, @AccountID
`
	r, parsed, file := newTableExistsTestRunner(t, content, map[string]bool{
		"papi_account_massclose": false,
	})
	r.tableEvidenceCache = map[string]bool{"papi_account_massclose": false}
	findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d: %v", len(findings), findings)
	}
	want := "Таблица pAPI_Account_MassClose отсутствует в метаданных: не найден создающий DDL, изменение структуры патчем, реестр таблиц продукта, SELECT INTO или объявление DSArchitect"
	if findings[0].Message != want {
		t.Fatalf("message = %q, want %q", findings[0].Message, want)
	}
}

// TestCheckTableExistsInMetadata_RegistryEvidence_NoFinding: таблица без
// создающего источника и патчей, но зарегистрированная в реестре продукта
// (reg_table_name → та же ветка tableEvidenceCache), finding не даёт.
// Специфика реестра: свидетельство существует только в индексе — в тексте
// файла нет ни DDL, ни патча (кейс tKey из Payments ProductTable.sql).
func TestCheckTableExistsInMetadata_RegistryEvidence_NoFinding(t *testing.T) {
	content := `create proc TestProc
as
  update tKey
     set KeyValue = 1
   where SPID = @@spid
`
	r, parsed, file := newTableExistsTestRunner(t, content, map[string]bool{
		"tkey": false, // негативная запись prewarm создателей
	})
	assertDMLReference(t, parsed, "tkey")
	r.tableEvidenceCache = map[string]bool{"tkey": true} // регистрация реестра
	findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
	}
}

// TestCheckTableExistsInMetadata_SameFileEvidence: свидетельство берётся из
// снимка индекса, а не из текста файла. Файл сам добавляет колонку
// используемой таблице: при записи в индексе (кэш true) finding подавлен;
// при устаревшем индексе (записи нет, db == nil) — finding остаётся.
func TestCheckTableExistsInMetadata_SameFileEvidence(t *testing.T) {
	content := `M_ADD_FIELD('tLegacyRef', 'NewCol DSIDENTIFIER')
update tLegacyRef set NewCol = 1
`
	t.Run("fresh index: evidence cached", func(t *testing.T) {
		r, parsed, file := newTableExistsTestRunner(t, content, map[string]bool{"tlegacyref": false})
		assertDMLReference(t, parsed, "tlegacyref")
		r.tableEvidenceCache = map[string]bool{"tlegacyref": true}
		findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(findings) != 0 {
			t.Fatalf("expected 0 findings, got %d: %v", len(findings), findings)
		}
	})
	t.Run("stale index: no evidence record", func(t *testing.T) {
		r, parsed, file := newTableExistsTestRunner(t, content, map[string]bool{"tlegacyref": false})
		assertDMLReference(t, parsed, "tlegacyref")
		r.tableEvidenceCache = map[string]bool{"tlegacyref": false}
		findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(findings) != 1 || findings[0].Object != "tLegacyRef" {
			t.Fatalf("expected 1 finding for tLegacyRef, got %v", findings)
		}
	})
}

// TestCachedLookupTableEvidence_CacheAndFallback: положительная/негативная
// записи читаются из кэша; при промахе (без БД) возвращается false и общий
// кэш не пополняется.
func TestCachedLookupTableEvidence_CacheAndFallback(t *testing.T) {
	r := &Runner{tableEvidenceCache: map[string]bool{
		"tevidenced":  true,
		"tnoevidence": false,
	}}
	if !r.cachedLookupTableEvidence(context.Background(), "TEvidenced") {
		t.Fatal("tevidenced: want true (регистронезависимый ключ)")
	}
	if r.cachedLookupTableEvidence(context.Background(), "tNoEvidence") {
		t.Fatal("tnoeevidence: want false (негативная запись)")
	}
	if r.cachedLookupTableEvidence(context.Background(), "tAbsent") {
		t.Fatal("tabsent: want false (промах без БД)")
	}
	if len(r.tableEvidenceCache) != 2 {
		t.Fatalf("ленивый путь не должен писать в общий кэш: size = %d, want 2", len(r.tableEvidenceCache))
	}
}

// TestDMLTableNames: уникальные нормализованные имена только из DML-контекстов.
func TestDMLTableNames(t *testing.T) {
	content := `create table tCreated (ID DSIDENTIFIER)
select ID from tSelected
insert tSelected (ID) values (1)
update tUpdated set ID = 1
delete from tDeleted
`
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	got := dmlTableNames(parsed)
	want := []string{"tselected", "tupdated", "tdeleted"}
	if len(got) != len(want) {
		t.Fatalf("dmlTableNames = %v, want %v (parsed contexts: %v)", got, want, parsed.Tables)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dmlTableNames[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if dmlTableNames(nil) != nil {
		t.Fatal("dmlTableNames(nil) must return nil")
	}
}
