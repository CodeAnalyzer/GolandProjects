//go:build integration

package review

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sqlparser "github.com/codebase/internal/parser/sql"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// reviewScopeFixture — веточная фикстура review: продукт fa-rv
// (develop default + sup-линия), fa-rv-other (develop) и EXT-продукт
// fa-rv-ext (develop + prj/bank-a + prj/bank-b). SP-ветка продукта fa-rv
// содержит два файла (для дублирования процедур внутри области).
type reviewScopeFixture struct {
	scanID      int64
	productID   int64
	otherID     int64
	extID       int64
	devBranch   int64
	spBranch    int64
	otherBranch int64
	extDev      int64
	extPrjA     int64
	extPrjB     int64
	devFileA    int64
	spFileA     int64
	spFileA2    int64
	otherFile   int64
	extDevFile  int64
	extPrjAFile int64
	extPrjBFile int64
}

func seedReviewScopeFixture(t *testing.T, db *store.DB) *reviewScopeFixture {
	t.Helper()
	ctx := context.Background()
	f := &reviewScopeFixture{}

	scanID, err := db.CreateScanRun(ctx, "/repo-review-scope")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	f.scanID = scanID
	for _, p := range []struct {
		name string
		id   *int64
	}{
		{"fa-rv", &f.productID},
		{"fa-rv-other", &f.otherID},
		{"fa-rv-ext", &f.extID},
	} {
		if *p.id, err = db.GetOrCreateDSProductIDByName(ctx, p.name); err != nil {
			t.Fatalf("GetOrCreateDSProductIDByName %s: %v", p.name, err)
		}
	}
	insertBranch := func(product int64, productName, name, key string, isDefault bool) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
			VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			product, name, key, "/repo-review-scope/"+productName+"/"+key, isDefault).Scan(&id); err != nil {
			t.Fatalf("insert branch %s: %v", key, err)
		}
		return id
	}
	f.devBranch = insertBranch(f.productID, "fa-rv", "develop", "develop", true)
	f.spBranch = insertBranch(f.productID, "fa-rv", "FA-Rv-11-01-7", "fa-rv-11-01-7", false)
	f.otherBranch = insertBranch(f.otherID, "fa-rv-other", "develop", "develop", true)
	f.extDev = insertBranch(f.extID, "fa-rv-ext", "develop", "develop", true)
	f.extPrjA = insertBranch(f.extID, "fa-rv-ext", "prj bank a", "prj/bank-a", false)
	f.extPrjB = insertBranch(f.extID, "fa-rv-ext", "prj bank b", "prj/bank-b", false)

	insertFile := func(productID, branchID int64, path string) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, ds_product_id, ds_product_branch_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
			VALUES ($1, NULLIF($2, 0), NULLIF($3, 0), $4, $4, 'sql', 1, 'rv', NOW(), 'CP866', 'SQL')
			RETURNING id
		`, f.scanID, productID, branchID, path).Scan(&id); err != nil {
			t.Fatalf("insert file %s: %v", path, err)
		}
		return id
	}
	f.devFileA = insertFile(f.productID, f.devBranch, "/repo-review-scope/fa-rv/develop/Dev.sql")
	f.spFileA = insertFile(f.productID, f.spBranch, "/repo-review-scope/fa-rv-11017/Sp1.sql")
	f.spFileA2 = insertFile(f.productID, f.spBranch, "/repo-review-scope/fa-rv-11017/Sp2.sql")
	f.otherFile = insertFile(f.otherID, f.otherBranch, "/repo-review-scope/fa-rv-other/develop/Other.sql")
	f.extDevFile = insertFile(f.extID, f.extDev, "/repo-review-scope/fa-rv-ext/develop/Ext.sql")
	f.extPrjAFile = insertFile(f.extID, f.extPrjA, "/repo-review-scope/fa-rv-ext/prj-bank-a/ExtA.sql")
	f.extPrjBFile = insertFile(f.extID, f.extPrjB, "/repo-review-scope/fa-rv-ext/prj-bank-b/ExtB.sql")

	insertProc := func(fileID int64, procName string) {
		if _, err := db.Exec(`INSERT INTO sql_procedures (file_id, proc_name, line_start, line_end) VALUES ($1, $2, 1, 2)`, fileID, procName); err != nil {
			t.Fatalf("insert procedure %s: %v", procName, err)
		}
	}
	insertProc(f.devFileA, "RV_DevOnly")
	insertProc(f.spFileA, "RV_SPOnly")
	insertProc(f.devFileA, "RV_Both")
	insertProc(f.spFileA, "RV_Both")
	insertProc(f.otherFile, "RV_ForeignUtil")
	insertProc(f.otherFile, "API_RvForeign")
	insertProc(f.spFileA, "API_RvOwn")
	insertProc(f.extDevFile, "RV_ExtProc")
	insertProc(f.spFileA, "RV_DupInSP")
	insertProc(f.spFileA2, "RV_DupInSP")
	insertProc(f.extDevFile, "RV_ExtDup")
	insertProc(f.extPrjAFile, "RV_ExtDup")

	insertTableCreate := func(fileID int64, table string) {
		if _, err := db.Exec(`INSERT INTO sql_tables (file_id, table_name, context, line_number) VALUES ($1, $2, 'create', 1)`, fileID, table); err != nil {
			t.Fatalf("insert sql_tables %s: %v", table, err)
		}
	}
	insertTableCreate(f.devFileA, "tRvDevOnly")
	insertTableCreate(f.spFileA, "tRvSpOnly")
	insertTableCreate(f.extDevFile, "tRvExtDev")
	insertTableCreate(f.extPrjBFile, "tRvExtPrjB")

	insertMacro := func(fileID int64, name, value string) {
		if _, err := db.Exec(`INSERT INTO h_files_defines (file_id, define_name, define_value, define_type, line_number) VALUES ($1, $2, $3, 'macro', 1)`, fileID, name, value); err != nil {
			t.Fatalf("insert macro %s: %v", name, err)
		}
	}
	insertMacro(f.devFileA, "RV_Macro", "DSVARCHAR")
	insertMacro(f.spFileA, "RV_Macro", "DSINT")

	return f
}

// spFile — проверяемый файл SP-ветки продукта fa-rv.
func (f *reviewScopeFixture) spFile(path string) *indexedFile {
	return &indexedFile{ID: f.spFileA, Path: path, RelPath: path, DsProductID: f.productID, DsProductBranchID: f.spBranch}
}

// scopedRunner — Runner с вручную заданной областью SP-файла fa-rv
// (чужой EXT без профиля: default ∪ prj).
func scopedRunner(db *store.DB, f *reviewScopeFixture) *Runner {
	r := NewRunner(db)
	r.scope = ReviewScope{
		FileID:    f.spFileA,
		ProductID: f.productID,
		BranchID:  f.spBranch,
		ProductBranch: map[int64][]int64{
			f.productID: {f.spBranch},
			f.otherID:   {f.otherBranch},
			f.extID:     {f.extDev, f.extPrjA, f.extPrjB},
		},
	}
	return r
}

func parseContent(t *testing.T, content string) *sqlparser.ParseResult {
	t.Helper()
	parsed, err := sqlparser.NewParser().ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	return parsed
}

func findingObjects(findings []Finding) map[string]bool {
	objects := make(map[string]bool, len(findings))
	for _, fnd := range findings {
		objects[fnd.Object] = true
	}
	return objects
}

// TestCheckProcDuplicateScope: дублирование — в пределах области своего
// продукта; копии в других ветках/продуктах — не дубли; finding на имя.
func TestCheckProcDuplicateScope(t *testing.T) {
	db := testutil.Open(t)
	f := seedReviewScopeFixture(t, db)
	ctx := context.Background()
	r := scopedRunner(db, f)

	file := f.spFile("/repo-review-scope/fa-rv-11017/Sp1.sql")
	// Файл объявляет RV_DupInSP: копия в Sp1.sql (свой файл) + копия в Sp2.sql
	// той же SP-ветки — межфайловый дубль; RV_Both в develop и SP (область
	// SP = {sp}) — копия вне области, не дубль.
	content := `create proc RV_DupInSP
as
  select 1
  return`
	parsed := parseContent(t, content)
	r.exec = &reviewExecContext{filePath: normalizePath(file.Path), content: []byte(content), lines: strings.Split(content, "\n"), macroResult: replaceMacros(content)}

	findings, err := r.checkProcDuplicate(ctx, parsed, file)
	if err != nil {
		t.Fatalf("checkProcDuplicate: %v", err)
	}
	if got := findingObjects(findings); !got["RV_DupInSP"] {
		t.Fatalf("findings = %v, want RV_DupInSP (два файла одной ветки)", got)
	}
	for _, fnd := range findings {
		if fnd.Object == "RV_Both" {
			t.Fatal("RV_Both: копии в разных ветках одного продукта не должны считаться дублированием")
		}
	}

	// Within-file: два CREATE PROCEDURE одного имени в одном файле.
	dupContent := `create proc RV_Review_Dup
as
  select 1
  return

create proc RV_Review_Dup
as
  select 2
  return`
	r.exec = &reviewExecContext{filePath: normalizePath(file.Path), content: []byte(dupContent), lines: strings.Split(dupContent, "\n"), macroResult: replaceMacros(dupContent)}
	findings, err = r.checkProcDuplicate(ctx, parseContent(t, dupContent), file)
	if err != nil {
		t.Fatalf("checkProcDuplicate (within-file): %v", err)
	}
	if got := findingObjects(findings); !got["RV_Review_Dup"] {
		t.Fatalf("findings = %v, want RV_Review_Dup (within-file дубль)", got)
	}
	if len(findings) != 1 {
		t.Fatalf("findings = %d, want 1 (finding на имя, не на каждое вхождение)", len(findings))
	}

	// EXT-union: файл prj-ветки — develop и prj одной поставки дают дубль.
	extRunner := NewRunner(db)
	extRunner.scope = ReviewScope{
		FileID:    f.extPrjAFile,
		ProductID: f.extID,
		BranchID:  f.extPrjA,
		ProductBranch: map[int64][]int64{
			f.productID: {f.spBranch},
			f.otherID:   {f.otherBranch},
			f.extID:     {f.extPrjA, f.extDev},
		},
	}
	extContent := `create proc RV_ExtDup
as
  select 1
  return`
	extRunner.exec = &reviewExecContext{filePath: normalizePath("/repo-review-scope/fa-rv-ext/prj-bank-a/ExtA.sql"), content: []byte(extContent), lines: strings.Split(extContent, "\n"), macroResult: replaceMacros(extContent)}
	findings, err = extRunner.checkProcDuplicate(ctx, parseContent(t, extContent), &indexedFile{ID: f.extPrjAFile, Path: "/repo-review-scope/fa-rv-ext/prj-bank-a/ExtA.sql", DsProductID: f.extID, DsProductBranchID: f.extPrjA})
	if err != nil {
		t.Fatalf("checkProcDuplicate (EXT): %v", err)
	}
	if got := findingObjects(findings); !got["RV_ExtDup"] {
		t.Fatalf("EXT findings = %v, want RV_ExtDup (prj + develop одной поставки)", got)
	}

	// Продукт-scope {0}: копии во всех ветках продукта — дубль (RV_Both).
	productRunner := NewRunner(db)
	productRunner.scope = ReviewScope{
		FileID: f.spFileA, ProductID: f.productID, BranchID: 0,
		ProductBranch: map[int64][]int64{f.productID: {0}},
	}
	bothContent := `create proc RV_Both
as
  select 1
  return`
	productRunner.exec = &reviewExecContext{filePath: normalizePath("p"), content: []byte(bothContent), lines: strings.Split(bothContent, "\n"), macroResult: replaceMacros(bothContent)}
	findings, err = productRunner.checkProcDuplicate(ctx, parseContent(t, bothContent), &indexedFile{ID: f.spFileA, Path: "p", DsProductID: f.productID})
	if err != nil {
		t.Fatalf("checkProcDuplicate (product scope): %v", err)
	}
	if got := findingObjects(findings); !got["RV_Both"] {
		t.Fatalf("product scope findings = %v, want RV_Both (обе ветки в области)", got)
	}
}

// TestCheckExecNotExistsProceduresScope: existence-режим — имя должно иметь
// копию в разрешённой области владельца.
func TestCheckExecNotExistsProceduresScope(t *testing.T) {
	db := testutil.Open(t)
	f := seedReviewScopeFixture(t, db)
	ctx := context.Background()
	r := scopedRunner(db, f)
	file := f.spFile("/repo-review-scope/fa-rv-11017/Sp1.sql")

	content := `create proc RV_Exec_Review
as
  exec RV_SPOnly
  exec RV_DevOnly
  exec RV_ForeignUtil
  return`
	findings, err := r.checkExecNotExistsProcedures(ctx, parseContent(t, content), file)
	if err != nil {
		t.Fatalf("checkExecNotExistsProcedures: %v", err)
	}
	got := findingObjects(findings)
	if got["RV_SPOnly"] {
		t.Fatal("RV_SPOnly: копия в SP-области — finding не нужен")
	}
	if !got["RV_DevOnly"] {
		t.Fatal("RV_DevOnly: копия только в develop — нужен finding execNotExistsProc")
	}
	if got["RV_ForeignUtil"] {
		t.Fatal("RV_ForeignUtil: копия в разрешённой области чужого продукта — finding не нужен")
	}

	// EXT-union: процедура ext-develop вызывается из prj-файла — существует.
	extRunner := NewRunner(db)
	extRunner.scope = ReviewScope{
		FileID: f.extPrjAFile, ProductID: f.extID, BranchID: f.extPrjA,
		ProductBranch: map[int64][]int64{
			f.productID: {f.spBranch},
			f.otherID:   {f.otherBranch},
			f.extID:     {f.extPrjA, f.extDev},
		},
	}
	findings, err = extRunner.checkExecNotExistsProcedures(ctx,
		parseContent(t, `create proc RV_Ext_Review
as
  exec RV_ExtProc
  return`),
		&indexedFile{ID: f.extPrjAFile, Path: "p", DsProductID: f.extID, DsProductBranchID: f.extPrjA})
	if err != nil {
		t.Fatalf("checkExecNotExistsProcedures (EXT): %v", err)
	}
	if got := findingObjects(findings); got["RV_ExtProc"] {
		t.Fatal("RV_ExtProc: develop EXT входит в область prj-файла (union) — finding не нужен")
	}
}

// TestCheckForeignProceduresOwnFirst: identity-режим — свой продукт раньше
// чужого; чужая процедура даёт finding с продуктом-владельцем.
func TestCheckForeignProceduresOwnFirst(t *testing.T) {
	db := testutil.Open(t)
	f := seedReviewScopeFixture(t, db)
	ctx := context.Background()
	r := scopedRunner(db, f)
	file := f.spFile("/repo-review-scope/fa-rv-11017/Sp1.sql")

	content := `create proc RV_Foreign_Review
as
  exec RV_Both
  exec RV_ForeignUtil
  return`
	findings, err := r.checkForeignProcedures(ctx, parseContent(t, content), file)
	if err != nil {
		t.Fatalf("checkForeignProcedures: %v", err)
	}
	got := findingObjects(findings)
	if got["RV_Both"] {
		t.Fatal("RV_Both: своя копия (в develop) перекрывает чужие объявления — finding не нужен")
	}
	if !got["RV_ForeignUtil"] {
		t.Fatal("RV_ForeignUtil: чужая процедура — нужен finding foreignProcedureUsing")
	}
	for _, fnd := range findings {
		if fnd.Object == "RV_ForeignUtil" && fnd.TargetProductID != f.otherID {
			t.Fatalf("RV_ForeignUtil: TargetProductID = %d, want %d", fnd.TargetProductID, f.otherID)
		}
	}
}

// TestCheckAPIProcDirectCallScope: прямой exec чужой API-процедуры —
// deploy-stopper apiProcDirectCall с дедупом на имя (первое вхождение);
// своя API-процедура — чисто; foreignProcedureUsing для тех же вызовов
// не формируется (change add-review-api-proc-direct-call-rule).
func TestCheckAPIProcDirectCallScope(t *testing.T) {
	db := testutil.Open(t)
	f := seedReviewScopeFixture(t, db)
	ctx := context.Background()
	r := scopedRunner(db, f)
	file := f.spFile("/repo-review-scope/fa-rv-11017/Sp1.sql")

	content := `create proc RV_API_Review
as
  exec API_RvForeign
  exec API_RvForeign
  exec API_RvOwn
  return`
	findings, err := r.checkAPIProcDirectCall(ctx, parseContent(t, content), file)
	if err != nil {
		t.Fatalf("checkAPIProcDirectCall: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("want 1 apiProcDirectCall finding, got %d: %+v", len(findings), findings)
	}
	fnd := findings[0]
	if fnd.Object != "API_RvForeign" {
		t.Fatalf("object = %s, want API_RvForeign", fnd.Object)
	}
	if fnd.Severity != SeverityDeployStopper {
		t.Fatalf("severity = %d, want %d", fnd.Severity, SeverityDeployStopper)
	}
	if fnd.TargetProductID != f.otherID || fnd.CurrentProductID != f.productID {
		t.Fatalf("products: current=%d target=%d, want %d/%d", fnd.CurrentProductID, fnd.TargetProductID, f.productID, f.otherID)
	}
	if fnd.Line != 3 {
		t.Fatalf("line = %d, want 3 (первое вхождение)", fnd.Line)
	}

	// foreignProcedureUsing не дублирует API-вызовы.
	foreignFindings, err := r.checkForeignProcedures(ctx, parseContent(t, content), file)
	if err != nil {
		t.Fatalf("checkForeignProcedures: %v", err)
	}
	for _, ff := range foreignFindings {
		if isAPIProcedure(ff.Object) {
			t.Fatalf("foreignProcedureUsing вернул API-имя %s — юрисдикция apiProcDirectCall", ff.Object)
		}
	}
}

// TestCheckAPIExecOutsideFacadeScope: API_EXEC в не-фасадном файле —
// deploy-stopper; lowercase-фасад — чисто (регистронезависимость).
func TestCheckAPIExecOutsideFacadeScope(t *testing.T) {
	db := testutil.Open(t)
	f := seedReviewScopeFixture(t, db)
	ctx := context.Background()
	r := scopedRunner(db, f)

	content := `create proc RV_ApiExec_Review
as
  API_EXEC(API_RvForeign, @RetVal, @ID)
  return`

	file := f.spFile("/repo-review-scope/fa-rv-11017/Sp1.sql")
	r.exec = &reviewExecContext{filePath: normalizePath(file.Path), content: []byte(content), lines: strings.Split(content, "\n"), macroResult: replaceMacros(content)}
	findings, err := r.checkAPIExecOutsideFacade(ctx, file)
	if err != nil {
		t.Fatalf("checkAPIExecOutsideFacade: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("want 1 apiExecOutsideFacade finding, got %d: %+v", len(findings), findings)
	}
	if findings[0].Severity != SeverityDeployStopper || findings[0].Line != 3 {
		t.Fatalf("severity/line = %d/%d, want %d/3", findings[0].Severity, findings[0].Line, SeverityDeployStopper)
	}

	facadePath := "/repo-review-scope/fa-rv-11017/fcd_RvTest.sql"
	facadeFile := f.spFile(facadePath)
	r.exec = &reviewExecContext{filePath: normalizePath(facadePath), content: []byte(content), lines: strings.Split(content, "\n"), macroResult: replaceMacros(content)}
	findings, err = r.checkAPIExecOutsideFacade(ctx, facadeFile)
	if err != nil {
		t.Fatalf("checkAPIExecOutsideFacade (facade): %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("lowercase fcd_ фасад — finding не нужен, got %+v", findings)
	}
}

// TestCheckTableExistsInMetadataScope: existence-режим для таблиц — создатель
// в другой ветке своего продукта не подтверждает; EXT-union подтверждает.
func TestCheckTableExistsInMetadataScope(t *testing.T) {
	db := testutil.Open(t)
	f := seedReviewScopeFixture(t, db)
	ctx := context.Background()
	r := scopedRunner(db, f)
	file := f.spFile("/repo-review-scope/fa-rv-11017/Sp1.sql")

	content := `create proc RV_Tables_Review
as
  select ID from tRvSpOnly
  select ID from tRvDevOnly
  return`
	findings, err := r.checkTableExistsInMetadata(ctx, parseContent(t, content), file)
	if err != nil {
		t.Fatalf("checkTableExistsInMetadata: %v", err)
	}
	got := findingObjects(findings)
	if got["tRvSpOnly"] {
		t.Fatal("tRvSpOnly: создатель в SP — finding не нужен")
	}
	if !got["tRvDevOnly"] {
		t.Fatal("tRvDevOnly: создатель только в develop — нужен finding tableExistsInMetadata")
	}

	// EXT: prj-файл ссылается на таблицу develop — union; prj другого банка — нет.
	extRunner := NewRunner(db)
	extRunner.scope = ReviewScope{
		FileID: f.extPrjAFile, ProductID: f.extID, BranchID: f.extPrjA,
		ProductBranch: map[int64][]int64{
			f.productID: {f.spBranch},
			f.otherID:   {f.otherBranch},
			f.extID:     {f.extPrjA, f.extDev},
		},
	}
	findings, err = extRunner.checkTableExistsInMetadata(ctx,
		parseContent(t, `create proc RV_Ext_Review
as
  select ID from tRvExtDev
  select ID from tRvExtPrjB
  return`),
		&indexedFile{ID: f.extPrjAFile, Path: "p", DsProductID: f.extID, DsProductBranchID: f.extPrjA})
	if err != nil {
		t.Fatalf("checkTableExistsInMetadata (EXT): %v", err)
	}
	got = findingObjects(findings)
	if got["tRvExtDev"] {
		t.Fatal("tRvExtDev: develop входит в область prj-файла (union) — finding не нужен")
	}
	if !got["tRvExtPrjB"] {
		t.Fatal("tRvExtPrjB: prj-ветка другого банка вне области — нужен finding")
	}
}

// TestIndexLookupsScope: индексы существующей области; first-wins дедуп полей
// не смешивает ветки; ленивый lookupIndexFieldsByName даёт поля целевой ветки.
func TestIndexLookupsScope(t *testing.T) {
	db := testutil.Open(t)
	f := seedReviewScopeFixture(t, db)
	ctx := context.Background()

	// Индексы: XIE1tRvIdx в develop (DevCol) и SP (SpCol) — разные поля;
	// XAK1tRvIdx только в develop.
	newIndex := func(fileID int64, table, name string, fields ...string) {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO sql_index_definitions (file_id, table_name, index_name, index_fields, definition_kind)
			VALUES ($1, $2, $3, '', 'create_index') RETURNING id`, fileID, table, name).Scan(&id); err != nil {
			t.Fatalf("insert index %s: %v", name, err)
		}
		for i, fld := range fields {
			if _, err := db.Exec(`INSERT INTO sql_index_definition_fields (table_index_id, field_name, field_order) VALUES ($1, $2, $3)`, id, fld, i+1); err != nil {
				t.Fatalf("insert index field %s: %v", fld, err)
			}
		}
	}
	newIndex(f.devFileA, "tRvIdx", "XIE1tRvIdx", "DevCol")
	newIndex(f.spFileA, "tRvIdx", "XIE1tRvIdx", "SpCol")
	newIndex(f.devFileA, "tRvIdx", "XAK1tRvIdx", "OtherCol")

	r := scopedRunner(db, f)
	exists, err := r.lookupIndexExists(ctx, "tRvIdx", "XIE1tRvIdx")
	if err != nil {
		t.Fatalf("lookupIndexExists XIE1: %v", err)
	}
	if !exists {
		t.Fatal("XIE1tRvIdx: копия в SP-области должна подтверждать существование")
	}
	exists, err = r.lookupIndexExists(ctx, "tRvIdx", "XAK1tRvIdx")
	if err != nil {
		t.Fatalf("lookupIndexExists XAK1: %v", err)
	}
	if exists {
		t.Fatal("XAK1tRvIdx: копия только в develop не должна подтверждать существование")
	}

	fields, err := r.lookupIndexFieldsByName(ctx, "XIE1tRvIdx")
	if err != nil {
		t.Fatalf("lookupIndexFieldsByName: %v", err)
	}
	if len(fields) != 1 || fields[0] != "spcol" {
		t.Fatalf("XIE1tRvIdx fields = %v, want [spcol] (поля целевой ветки)", fields)
	}

	// Ленивый путь (пустой кэш) — тот же результат.
	lazy := scopedRunner(db, f)
	fields, err = lazy.lookupIndexFieldsByName(ctx, "XIE1tRvIdx")
	if err != nil {
		t.Fatalf("lookupIndexFieldsByName (lazy): %v", err)
	}
	if len(fields) != 1 || fields[0] != "spcol" {
		t.Fatalf("lazy XIE1tRvIdx fields = %v, want [spcol]", fields)
	}
}

// TestLookupMacroSignatureScope: тип макроса — value-режим, сигнатура из
// целевой ветки (существование #define глобально — исключение спеки).
func TestLookupMacroSignatureScope(t *testing.T) {
	db := testutil.Open(t)
	f := seedReviewScopeFixture(t, db)
	ctx := context.Background()
	r := scopedRunner(db, f)

	sig, err := r.lookupMacroSignature(ctx, "RV_Macro")
	if err != nil {
		t.Fatalf("lookupMacroSignature: %v", err)
	}
	if sig != "DSINT" {
		t.Fatalf("macro signature = %q, want DSINT (SP-копия)", sig)
	}
}

// TestPrewarmLazyEquivalence: batch-предзагрузка и ленивые lookup'ы дают
// одинаковые результаты при одинаковой области (design D3/D4).
func TestPrewarmLazyEquivalence(t *testing.T) {
	db := testutil.Open(t)
	f := seedReviewScopeFixture(t, db)
	ctx := context.Background()

	content := `create proc RV_Equiv_Review
as
  exec RV_Both
  exec RV_DevOnly
  exec RV_ForeignUtil
  select ID from tRvSpOnly
  select ID from tRvDevOnly
  return`
	parsed := parseContent(t, content)

	prewarmRunner := scopedRunner(db, f)
	lazyRunner := scopedRunner(db, f)

	// Prewarm-пути.
	prewarmRunner.prewarmProcCaches(ctx, parsed)
	prewarmRunner.prewarmTableCreators(ctx, parsed)
	if err := prewarmRunner.prewarmColTypeCache(ctx, parsed, strings.Split(content, "\n")); err != nil {
		t.Fatalf("prewarmColTypeCache: %v", err)
	}

	// Процедуры: identity/existence.
	for _, name := range []string{"RV_Both", "RV_DevOnly", "RV_ForeignUtil"} {
		prewarmInfo, ok := prewarmRunner.cachedLookupProcedureScopeInfo(ctx, name)
		if !ok {
			t.Fatalf("prewarm: %s не найден", name)
		}
		lazyInfo, ok := lazyRunner.cachedLookupProcedureScopeInfo(ctx, name)
		if !ok {
			t.Fatalf("lazy: %s не найден", name)
		}
		if prewarmInfo != lazyInfo {
			t.Fatalf("%s: prewarm %+v != lazy %+v", name, prewarmInfo, lazyInfo)
		}
	}

	// Создатели таблиц.
	for _, table := range []string{"tRvSpOnly", "tRvDevOnly"} {
		prewarmCreator := prewarmRunner.cachedLookupTableCreator(ctx, table)
		lazyCreator := lazyRunner.cachedLookupTableCreator(ctx, table)
		if prewarmCreator != lazyCreator {
			t.Fatalf("%s: prewarm creator = %v, lazy = %v", table, prewarmCreator, lazyCreator)
		}
	}
	if !prewarmRunner.cachedLookupTableCreator(ctx, "tRvSpOnly") {
		t.Fatal("tRvSpOnly: создатель в SP должен быть найден")
	}
	if prewarmRunner.cachedLookupTableCreator(ctx, "tRvDevOnly") {
		t.Fatal("tRvDevOnly: создатель в develop не должен подтверждать существование")
	}
}

// TestGetIndexedFileReturnsBranch: getIndexedFile возвращает ветку файла.
func TestGetIndexedFileReturnsBranch(t *testing.T) {
	db := testutil.Open(t)
	f := seedReviewScopeFixture(t, db)
	ctx := context.Background()

	file, err := NewRunner(db).getIndexedFile(ctx, "/repo-review-scope/fa-rv-11017/Sp1.sql")
	if err != nil {
		t.Fatalf("getIndexedFile: %v", err)
	}
	if file.DsProductID != f.productID || file.DsProductBranchID != f.spBranch {
		t.Fatalf("file = product %d / branch %d, want %d / %d", file.DsProductID, file.DsProductBranchID, f.productID, f.spBranch)
	}
}

// TestBuildReviewScopeFromDB: построение области по индексу и профилю dsinfo
// (карта областей, EXT-union, fallback default).
func TestBuildReviewScopeFromDB(t *testing.T) {
	db := testutil.Open(t)
	f := seedReviewScopeFixture(t, db)
	ctx := context.Background()
	t.Setenv("CODEBASE_DSINFO_ID", "")

	// Профиль: fa-rv-other → develop, fa-rv-ext → prj/bank-a.
	var dsinfoID int64
	if err := db.QueryRow(`INSERT INTO dsinfo (file_path, session_name, file_hash, parser_version) VALUES ('/dsinfo/rv.csv', 'rv-profile', 'rvhash', 1) RETURNING id`).Scan(&dsinfoID); err != nil {
		t.Fatalf("insert dsinfo: %v", err)
	}
	for _, row := range []struct {
		line int
		name string
		key  string
	}{
		{1, "fa-rv-other", "develop"},
		{2, "fa-rv-ext", "prj/bank-a"},
	} {
		if _, err := db.Exec(`INSERT INTO dsinfo_products (dsinfo_id, line_no, raw_name, product_name, branch_key) VALUES ($1, $2, $3, $4, $5)`, dsinfoID, row.line, row.name, row.name, row.key); err != nil {
			t.Fatalf("insert dsinfo_products %s: %v", row.name, err)
		}
	}

	r := NewRunner(db)
	file := f.spFile("/repo-review-scope/fa-rv-11017/Sp1.sql")

	// Без профиля: чужой EXT — default ∪ все prj.
	scope, err := r.buildReviewScope(ctx, file, 0)
	if err != nil {
		t.Fatalf("buildReviewScope (без профиля): %v", err)
	}
	assertAreaEqual(t, scope.ProductBranch, f.productID, []int64{f.spBranch})
	assertAreaEqual(t, scope.ProductBranch, f.extID, []int64{f.extDev, f.extPrjA, f.extPrjB})
	assertAreaEqual(t, scope.ProductBranch, f.otherID, []int64{f.otherBranch})

	// С профилем: fa-rv-ext → {prj/bank-a, develop}, свой продукт не меняется.
	scope, err = r.buildReviewScope(ctx, file, dsinfoID)
	if err != nil {
		t.Fatalf("buildReviewScope (профиль): %v", err)
	}
	assertAreaEqual(t, scope.ProductBranch, f.productID, []int64{f.spBranch})
	assertAreaEqual(t, scope.ProductBranch, f.extID, []int64{f.extPrjA, f.extDev})
	assertAreaEqual(t, scope.ProductBranch, f.otherID, []int64{f.otherBranch})

	// Несуществующий профиль — ошибка без fallback.
	if _, err = r.buildReviewScope(ctx, file, 999); err == nil || !strings.Contains(err.Error(), "dsinfo profile 999 not found") {
		t.Fatalf("buildReviewScope (999) err = %v, want dsinfo profile 999 not found", err)
	}
}

func assertAreaEqual(t *testing.T, areas map[int64][]int64, productID int64, want []int64) {
	t.Helper()
	got := areas[productID]
	if len(got) != len(want) {
		t.Fatalf("область продукта %d = %v, want %v", productID, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("область продукта %d = %v, want %v", productID, got, want)
		}
	}
}

// TestRunSQLFileCtxScopedEndToEnd: полный прогон SP-файла — existence- и
// value-правила работают в области, identity не даёт foreign-шума.
func TestRunSQLFileCtxScopedEndToEnd(t *testing.T) {
	db := testutil.Open(t)
	f := seedReviewScopeFixture(t, db)
	ctx := context.Background()

	content := `create proc Cons_RV_Check
as
  exec RV_DevOnly
  select ID from tRvDevOnly
  return`
	dir := t.TempDir()
	path := filepath.Join(dir, "Sp1.sql")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp sql: %v", err)
	}
	// Файл в индексе — по фактическому пути на диске.
	if _, err := db.Exec(`UPDATE files SET path = $1, rel_path = $1 WHERE id = $2`, path, f.spFileA); err != nil {
		t.Fatalf("update file path: %v", err)
	}

	result, err := NewRunner(db).RunSQLFileCtx(ctx, path, Options{Rules: []RuleID{
		RuleTableExistsInMetadata, RuleExecNotExistsProc, RuleForeignTablesUsing, RuleForeignProcedureUsing,
	}})
	if err != nil {
		t.Fatalf("RunSQLFileCtx: %v", err)
	}
	objects := map[RuleID]map[string]bool{}
	for _, fnd := range result.Findings {
		if objects[fnd.Rule] == nil {
			objects[fnd.Rule] = map[string]bool{}
		}
		objects[fnd.Rule][fnd.Object] = true
	}
	if !objects[RuleTableExistsInMetadata]["tRvDevOnly"] {
		t.Fatalf("tableExistsInMetadata: ожидался finding tRvDevOnly, findings = %+v", result.Findings)
	}
	if !objects[RuleExecNotExistsProc]["RV_DevOnly"] {
		t.Fatalf("execNotExistsProc: ожидался finding RV_DevOnly, findings = %+v", result.Findings)
	}
	if objects[RuleForeignTablesUsing]["tRvDevOnly"] {
		t.Fatal("foreignTablesUsing: таблица своего продукта из другой ветки не чужая")
	}
}
