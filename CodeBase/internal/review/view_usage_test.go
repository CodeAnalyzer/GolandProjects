package review

import (
	"context"
	"testing"

	sqlparser "github.com/codebase/internal/parser/sql"
)

// newViewUsageTestRunner создаёт Runner без БД с активным exec-контекстом и
// распарсенным контентом; при viewNames != nil кэш имён view предзаполнен
// (флаг загруженности установлен — ленивая загрузка не срабатывает).
func newViewUsageTestRunner(t *testing.T, content string, viewNames map[string]struct{}, creatorCache map[string]bool) (*Runner, *sqlparser.ParseResult, *indexedFile) {
	t.Helper()
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	r := &Runner{tableCreatorCache: creatorCache}
	if viewNames != nil {
		r.viewNameCache = viewNames
		r.viewNamesLoaded = true
	}
	r.exec = &reviewExecContext{filePath: "/virtual/review/test.sql", content: []byte(content)}
	file := &indexedFile{Path: "/virtual/review/test.sql", DsProductID: 42}
	return r, parsed, file
}

// TestIsViewTable_NoDB_FailOpen: без БД множество лениво «загружается»
// пустым, флаг загруженности ставится, повторный вызов не штормит запросами
// и остаётся false.
func TestIsViewTable_NoDB_FailOpen(t *testing.T) {
	r := &Runner{}
	if r.isViewTable(context.Background(), "tfund") {
		t.Fatal("isViewTable without DB must be false")
	}
	if !r.viewNamesLoaded {
		t.Fatal("viewNamesLoaded must be set after first call")
	}
	if r.viewNameCache == nil {
		t.Fatal("viewNameCache must be initialized after first call")
	}
	if r.isViewTable(context.Background(), "TFUND") {
		t.Fatal("repeat isViewTable call must stay false")
	}
	if !r.viewNamesLoaded {
		t.Fatal("viewNamesLoaded must stay set")
	}
}

// TestIsViewTable_LoadedCache_HitAndMiss: предзаполненный кэш отвечает без
// БД; регистронезависимость и схемный префикс нормализуются.
func TestIsViewTable_LoadedCache_HitAndMiss(t *testing.T) {
	r := &Runner{
		viewNameCache:   map[string]struct{}{"tfund": {}, "tinstracclink": {}},
		viewNamesLoaded: true,
	}
	for _, name := range []string{"tfund", "TFund", "dbo.tFund", "public.tinstracclink"} {
		if !r.isViewTable(context.Background(), name) {
			t.Fatalf("isViewTable(%q) must be true", name)
		}
	}
	for _, name := range []string{"tcontract", "tFundX", ""} {
		if r.isViewTable(context.Background(), name) {
			t.Fatalf("isViewTable(%q) must be false", name)
		}
	}
}

// TestRuleViewUsageInServerCode_DefaultEnabled: правило регистрируется в
// каталоге и включается DefaultEnabledRules автоматически.
func TestRuleViewUsageInServerCode_DefaultEnabled(t *testing.T) {
	meta, ok := ruleCatalog[RuleViewUsageInServerCode]
	if !ok {
		t.Fatal("ruleViewUsageInServerCode must be registered in ruleCatalog")
	}
	if meta.Severity != SeverityDeployStopper {
		t.Fatalf("severity = %d, want SeverityDeployStopper", meta.Severity)
	}
	enabled := DefaultEnabledRules()
	if !enabled[RuleViewUsageInServerCode] {
		t.Fatal("DefaultEnabledRules must include viewUsageInServerCode")
	}
}

// TestCheckViewUsageInServerCode: SELECT/UPDATE по view — findings severity 1
// с именем и строкой; обычная таблица, #-таблица и курсор с именем view —
// без findings.
func TestCheckViewUsageInServerCode(t *testing.T) {
	content := `create proc TestProc
as
begin
  update Batch set Brief = 'y' where BatchID = 1
  declare curViewName cursor for select ID from tContract
  open curViewName
  fetch next from curViewName
  select f.Brief from tfund f where f.Name = 'x'
  select ID from #tfund
end
`
	// curviewname в кэше view: единственная защита от finding — фильтр
	// курсоров файла (cursorDecls), как у tableExistsInMetadata.
	r, parsed, file := newViewUsageTestRunner(t, content,
		map[string]struct{}{"tfund": {}, "batch": {}, "curviewname": {}}, nil)

	// Guard: ссылки реально извлечены парсером в DML-контексте.
	seenDML := map[string]int{}
	for _, tbl := range parsed.Tables {
		if tableExistsDMLContexts[tbl.Context] {
			seenDML[normalizeIdentifier(tbl.TableName)] = tbl.LineNumber
		}
	}
	for _, name := range []string{"batch", "tfund", "curviewname"} {
		if _, ok := seenDML[name]; !ok {
			t.Fatalf("guard failed: %q должен быть извлечён парсером, seen: %v", name, seenDML)
		}
	}

	findings, err := r.checkViewUsageInServerCode(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 2 {
		t.Fatalf("expected 2 findings (tfund select, batch update), got %d: %+v", len(findings), findings)
	}
	for _, f := range findings {
		if f.Rule != RuleViewUsageInServerCode || f.Severity != SeverityDeployStopper {
			t.Fatalf("unexpected finding: %+v", f)
		}
	}
	byObj := map[string]Finding{}
	for _, f := range findings {
		byObj[f.Object] = f
	}
	tf, ok := byObj["tfund"]
	if !ok {
		t.Fatalf("finding for tfund missing: %+v", findings)
	}
	if tf.Line != seenDML["tfund"] || tf.CurrentProductID != file.DsProductID {
		t.Fatalf("tfund finding line/product mismatch: %+v", tf)
	}
	if _, ok := byObj["Batch"]; !ok {
		t.Fatalf("finding for Batch missing: %+v", findings)
	}
}

// TestCheckViewUsageInServerCode_DefinitionNotViolation: тело
// create view над обычными таблицами не даёт findings (имя самого view в
// parsed.Tables не попадает — парсер не извлекает create view).
func TestCheckViewUsageInServerCode_DefinitionNotViolation(t *testing.T) {
	content := `create view Batch
  select p.PropertyUsrID as BatchID,
         p.Brief,
         p.Name
    from tPropertyUsr p M_NOLOCK
   where p.PropertyType = 6
`
	r, parsed, file := newViewUsageTestRunner(t, content,
		map[string]struct{}{"batch": {}}, nil)

	findings, err := r.checkViewUsageInServerCode(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 findings, got %d: %+v", len(findings), findings)
	}
}

// TestCheckViewUsageInServerCode_ViewOnView: обращение тела одного view к
// другому view флагается.
func TestCheckViewUsageInServerCode_ViewOnView(t *testing.T) {
	content := `create view vA
  select b.Brief from vB b
`
	r, parsed, file := newViewUsageTestRunner(t, content,
		map[string]struct{}{"vb": {}}, nil)

	findings, err := r.checkViewUsageInServerCode(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 1 || findings[0].Object != "vB" {
		t.Fatalf("expected 1 finding for vB, got %+v", findings)
	}
}

// TestCheckTableExistsInMetadata_ViewSuppressed: имя-view без
// создателя-таблицы не даёт tableExistsInMetadata (атрибуция
// viewUsageInServerCode).
func TestCheckTableExistsInMetadata_ViewSuppressed(t *testing.T) {
	content := `create proc TestProc
as
  select ID from tfund
`
	r, parsed, file := newViewUsageTestRunner(t, content,
		map[string]struct{}{"tfund": {}}, map[string]bool{"tfund": false})

	findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 tableExistsInMetadata findings for view name, got %+v", findings)
	}

	viewFindings, err := r.checkViewUsageInServerCode(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(viewFindings) != 1 || viewFindings[0].Object != "tfund" {
		t.Fatalf("expected 1 viewUsageInServerCode finding, got %+v", viewFindings)
	}
}

// TestCheckViewUsageInServerCode_LocalTableShadowsView: таблица, созданная
// в самом файле (CREATE TABLE / SELECT INTO) с именем view, — ссылка идёт на
// локальную таблицу, finding не формируется.
func TestCheckViewUsageInServerCode_LocalTableShadowsView(t *testing.T) {
	content := `create proc TestProc
as
begin
  create table Account (ID int)
  select ID into Balance from tSrc
  select ID from Account
  select ID from Balance
end
`
	r, parsed, file := newViewUsageTestRunner(t, content,
		map[string]struct{}{"account": {}, "balance": {}}, nil)

	findings, err := r.checkViewUsageInServerCode(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("local tables shadowing view names must not fire, got %+v", findings)
	}
}

// TestCheckViewUsageInServerCode_QualifiedReference: qualified-ссылка
// (`from dbo.tfund`) распознаётся парсером по последнему сегменту
// dotted-имени (change table-existence-source-fixes) — обращение к view
// со схемным префиксом даёт finding, прежний FN закрыт.
func TestCheckViewUsageInServerCode_QualifiedReference(t *testing.T) {
	content := `create proc TestProc
as
begin
  select ID from dbo.tfund
end
`
	r, parsed, file := newViewUsageTestRunner(t, content,
		map[string]struct{}{"tfund": {}}, nil)

	// Парсер извлекает tfund (последний сегмент), а не префикс dbo.
	sawTfund, sawDbo := false, false
	for _, tbl := range parsed.Tables {
		if !tableExistsDMLContexts[tbl.Context] {
			continue
		}
		switch normalizeIdentifier(tbl.TableName) {
		case "tfund":
			sawTfund = true
		case "dbo":
			sawDbo = true
		}
	}
	if !sawTfund {
		t.Fatalf("parser must extract %q from qualified reference, parsed: %+v", "tfund", parsed.Tables)
	}
	if sawDbo {
		t.Fatalf("parser must not extract schema prefix %q, parsed: %+v", "dbo", parsed.Tables)
	}

	findings, err := r.checkViewUsageInServerCode(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("qualified reference must fire viewUsageInServerCode, got %+v", findings)
	}
}

// TestCheckTableExistsInMetadata_ViewCacheNotLoaded: при незагруженном
// множестве view (nil-кэш, как при ошибке БД) подавление не срабатывает —
// tableExistsInMetadata работает как до изменения.
func TestCheckTableExistsInMetadata_ViewCacheNotLoaded(t *testing.T) {
	content := `create proc TestProc
as
  select ID from pMissingTable
`
	r, parsed, file := newViewUsageTestRunner(t, content, nil, map[string]bool{"pmissingtable": false})

	findings, err := r.checkTableExistsInMetadata(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 1 || findings[0].Rule != RuleTableExistsInMetadata {
		t.Fatalf("expected 1 tableExistsInMetadata finding, got %+v", findings)
	}

	viewFindings, err := r.checkViewUsageInServerCode(context.Background(), parsed, file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(viewFindings) != 0 {
		t.Fatalf("expected 0 viewUsageInServerCode findings, got %+v", viewFindings)
	}
}
