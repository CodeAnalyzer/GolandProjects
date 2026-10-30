package dsxml

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const fmcoreTablePath = "fa-fmcore/API_DFA/DSArchitectData/BObject/DigitalFinancialAsset/Table/pAPI_DFA_BfrMassDelPmnt.xml"

func fixtureFmcorePath() string {
	return filepath.Join("..", "..", "..", "Bug-reports", "pAPI_DFA_BfrMassDelPmnt.xml")
}

// TestRecoverUnclosedElements_FmcorePattern — дублированный <Indexses> при
// незакрытом <Index> (фикстура Bug-reports/pAPI_DFA_BfrMassDelPmnt.xml):
// перед закрывающим </Table> вставляются </Index></Indexses>.
func TestRecoverUnclosedElements_FmcorePattern(t *testing.T) {
	data, err := os.ReadFile(fixtureFmcorePath())
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	fixed, fixes, ok := recoverUnclosedElements(string(data))
	if !ok {
		t.Fatal("recoverUnclosedElements = !ok, want ok")
	}
	if fixes != 2 {
		t.Fatalf("fixes = %d, want 2 (</Index> and </Indexses>)", fixes)
	}
	if !strings.Contains(fixed, "</Index></Indexses></Table>") {
		t.Fatalf("fixed content misses auto-closed tags before </Table>:\n...%s", tail(fixed, 120))
	}
	if strings.Count(fixed, "<Table>") != 1 || strings.Count(fixed, "</Table>") != 1 {
		t.Fatal("fixed content must not add or remove Table elements")
	}
}

// TestRecoverUnclosedElements_ValidXMLUntouched — корректный контент
// возвращается без изменений и без признака фикса.
func TestRecoverUnclosedElements_ValidXMLUntouched(t *testing.T) {
	content := "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Table>\n<ParamName>p1</ParamName>\n<Indexses><Index><IndexName>i</IndexName></Index></Indexses>\n</Table>\n"
	fixed, fixes, ok := recoverUnclosedElements(content)
	if ok || fixes != 0 || fixed != content {
		t.Fatalf("recoverUnclosedElements on valid xml = (%q, %d, %v), want same content, 0, false", fixed, fixes, ok)
	}
}

// TestRecoverUnclosedElements_LimitExceeded — слишком глубокий стек
// незакрытых элементов не лечится (лимит автозакрытий).
func TestRecoverUnclosedElements_LimitExceeded(t *testing.T) {
	var b strings.Builder
	b.WriteString("<root>")
	for i := 0; i < maxRecoverFixes+8; i++ {
		b.WriteString("<i>")
	}
	b.WriteString("</root>")
	fixed, fixes, ok := recoverUnclosedElements(b.String())
	if ok || fixes != 0 {
		t.Fatalf("recoverUnclosedElements over limit = (%d, %v), want 0, false", fixes, ok)
	}
	if fixed != b.String() {
		t.Fatal("content must stay untouched when over limit")
	}
}

// TestRecoverUnclosedElements_OrphanClose — закрывающий тег без пары сам
// по себе не порождает вставок (окончательный вердикт — за encoding/xml).
func TestRecoverUnclosedElements_OrphanClose(t *testing.T) {
	content := "<root></b></root>"
	fixed, _, ok := recoverUnclosedElements(content)
	if ok {
		t.Fatalf("recoverUnclosedElements on orphan close = ok, want false (fixed=%q)", fixed)
	}
}

// TestParseContent_FmcoreFixture_AutoClosed — битая фикстура fmcore
// индексируется после автозакрытия: Recovered, таблица с русским именем,
// оба поля и индекс XPK с полями SPID/PaymentID.
func TestParseContent_FmcoreFixture_AutoClosed(t *testing.T) {
	data, err := os.ReadFile(fixtureFmcorePath())
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	res, err := NewParser().ParseContent(fmcoreTablePath, string(data))
	if err != nil {
		t.Fatalf("ParseContent: %v", err)
	}
	if res.Skipped {
		t.Fatalf("fixture must not be skipped: %s", res.SkipReason)
	}
	if !res.Recovered || res.RecoveryNote == "" {
		t.Fatalf("Recovered = %v, note = %q, want recovery with note", res.Recovered, res.RecoveryNote)
	}
	if len(res.BusinessObjectTables) != 1 {
		t.Fatalf("business object tables = %d, want 1", len(res.BusinessObjectTables))
	}
	tbl := res.BusinessObjectTables[0]
	if tbl.TableName != "pAPI_DFA_BfrMassDelPmnt" || tbl.BusinessObject != "DigitalFinancialAsset" {
		t.Fatalf("table = %+v", tbl)
	}
	if !strings.Contains(tbl.RusName, "выплат") {
		t.Fatalf("RusName is not readable Russian: %q", tbl.RusName)
	}
	if len(res.BusinessTableFields) != 2 {
		t.Fatalf("fields = %d, want 2", len(res.BusinessTableFields))
	}
	if res.BusinessTableFields[0].FieldName != "PaymentID" || res.BusinessTableFields[1].FieldName != "DigitalFinancialAssetID" {
		t.Fatalf("fields = %s, %s", res.BusinessTableFields[0].FieldName, res.BusinessTableFields[1].FieldName)
	}
	if len(res.BusinessTableIndexes) != 1 {
		t.Fatalf("indexes = %d, want 1 (duplicate block is nested, not a second index)", len(res.BusinessTableIndexes))
	}
	if res.BusinessTableIndexes[0].IndexName != "XPKpAPI_DFA_BfrMassDelPmnt" {
		t.Fatalf("index name = %q", res.BusinessTableIndexes[0].IndexName)
	}
	if len(res.BusinessIndexFields) != 2 || res.BusinessIndexFields[0].FieldName != "SPID" || res.BusinessIndexFields[1].FieldName != "PaymentID" {
		t.Fatalf("index fields = %+v", res.BusinessIndexFields)
	}
}

// TestParseContent_OrphanCloseStillFails — ошибка, не поддающаяся
// автозакрытию, остаётся ошибкой файла.
func TestParseContent_OrphanCloseStillFails(t *testing.T) {
	content := "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Table>\n<ParamName>x</ParamName>\n</WrongClose>\n</Table>\n"
	res, err := NewParser().ParseContent(fmcoreTablePath, content)
	if err == nil {
		t.Fatalf("ParseContent unexpectedly succeeded: %+v", res)
	}
	if res != nil && res.Recovered {
		t.Fatal("no recovery expected for orphan close")
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
