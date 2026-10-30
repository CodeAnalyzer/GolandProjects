package dsxml

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// eventFixtureXML — структура InfoPath event-формата (fa-ratings) с
// фикстуры Bug-reports/OnAfterScr_MassDel.xml.
const eventFixtureXML = `<?xml version="1.0" encoding="UTF-8"?>
<?mso-infoPathSolution PIVersion="1.0.0.0" href="http://dev/APINew/event_template.xsn" name="urn:schemas-microsoft-com:office:infopath:event-template:" language="ru" productVersion="11.0.8165" solutionVersion="1.0.0.428" ?>
<?mso-application progid="InfoPath.Document"?>
<event xmlns:my="http://schemas.microsoft.com/office/infopath/2003/myXSD/2008-05-08T06:18:44"><name>OnAfterScr_MassDel</name><description>Событие информирует подписчиков о факте изменения рейтинговой оценки.</description><implemented>true</implemented><module-depend>false</module-depend><input-params/><input-tables><table><name>pAPI_Scr_AfterDelScr</name><description>Набор данный, возвращающий список удаленных оценок</description><fields><field><name>ScoreID</name><name-rus>ИД оценки</name-rus><type>DSIDENTIFIER</type><description>Идентификатор рейтинговой оценки</description><required>1</required></field><field><name>Date</name><name-rus>Дата события</name-rus><type>DSDATETIME</type><description>Дата формирования события</description><required>0</required></field></fields><required>true</required></table></input-tables><retvals/><descr-short>Событие после изменения рейтинговой оценки</descr-short><output-params>
	</output-params><output-tables/><architect-approval>OK</architect-approval><return-resultset>RESULTSET_MULTIPLY</return-resultset><contexts/></event>`

func TestParseContent_EventXML(t *testing.T) {
	parser := NewParser()
	path := "/API_RTG/DSArchitectData/BObject/Rating/Event/OnAfterScr_MassDel.xml"

	result, err := parser.ParseContent(path, eventFixtureXML)
	if err != nil {
		t.Fatalf("ParseContent event: %v", err)
	}
	if result.Skipped {
		t.Fatalf("event root must not be skipped: %s", result.SkipReason)
	}
	if len(result.Contracts) != 1 {
		t.Fatalf("contracts = %d, want 1", len(result.Contracts))
	}
	c := result.Contracts[0]
	if c.ContractName != "OnAfterScr_MassDel" {
		t.Fatalf("ContractName = %q", c.ContractName)
	}
	if c.ContractKind != "event" {
		t.Fatalf("ContractKind = %q, want event", c.ContractKind)
	}
	if !c.Implemented {
		t.Fatal("Implemented = false, want true")
	}
	if c.ArchApproval != 1 {
		t.Fatalf("ArchApproval = %d, want 1 (OK)", c.ArchApproval)
	}
	if c.ShortDescription != "Событие после изменения рейтинговой оценки" {
		t.Fatalf("ShortDescription = %q", c.ShortDescription)
	}
	if !strings.Contains(c.FullDescription, "рейтинговой оценки") {
		t.Fatalf("FullDescription = %q", c.FullDescription)
	}

	if len(result.Tables) != 1 {
		t.Fatalf("tables = %d, want 1", len(result.Tables))
	}
	tbl := result.Tables[0]
	if tbl.Direction != "input" || tbl.TableName != "pAPI_Scr_AfterDelScr" || !tbl.Required {
		t.Fatalf("table = %+v", tbl)
	}
	if !strings.Contains(tbl.Description, "список удаленных оценок") {
		t.Fatalf("table Description = %q", tbl.Description)
	}

	if len(result.TableFields) != 2 {
		t.Fatalf("table fields = %d, want 2", len(result.TableFields))
	}
	scoreID, dateField := result.TableFields[0], result.TableFields[1]
	if scoreID.FieldName != "ScoreID" || scoreID.TypeName != "DSIDENTIFIER" || !scoreID.Required {
		t.Fatalf("ScoreID = %+v", scoreID)
	}
	if scoreID.RusName != "ИД оценки" {
		t.Fatalf("ScoreID RusName = %q", scoreID.RusName)
	}
	if dateField.FieldName != "Date" || dateField.Required {
		t.Fatalf("Date = %+v", dateField)
	}
	if dateField.RusName != "Дата события" {
		t.Fatalf("Date RusName = %q", dateField.RusName)
	}

	// Символ контракта попадает в unified index.
	if len(result.Symbols) != 1 || result.Symbols[0].SymbolName != "OnAfterScr_MassDel" {
		t.Fatalf("symbols = %+v", result.Symbols)
	}
}

func TestParseFile_EventFixture(t *testing.T) {
	// Реальная фикстура: CP866-байты с декларацией UTF-8 (ложная) —
	// DetectXMLEncoding отвергает декларацию, контент декодируется.
	path := filepath.Join("..", "..", "..", "Bug-reports", "OnAfterScr_MassDel.xml")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("fixture not found: %v", err)
	}
	result, err := NewParser().ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile event fixture: %v", err)
	}
	if result.Skipped {
		t.Fatalf("fixture skipped: %s", result.SkipReason)
	}
	if len(result.Contracts) != 1 || result.Contracts[0].ContractName != "OnAfterScr_MassDel" {
		t.Fatalf("contracts = %+v", result.Contracts)
	}
	if !strings.Contains(result.Contracts[0].FullDescription, "рейтинговой оценки") {
		t.Fatalf("FullDescription = %q (Cyrillic lost)", result.Contracts[0].FullDescription)
	}
}

func TestParseContent_EventMixedRequired(t *testing.T) {
	// required в обеих записях: true/false у table, 1/0 у field;
	// implemented=false; architect-approval не OK.
	xmlDoc := `<event>
<name>EvMixed</name><descr-short>Короткое</descr-short><description>Полное описание</description>
<implemented>false</implemented><module-depend>true</module-depend>
<input-tables><table><name>tIn</name><description>Описание таблицы</description>
<fields><field><name>F1</name><name-rus>Поле один</name-rus><type>DSINT</type><required>1</required></field>
<field><name>F2</name><name-rus>Поле два</name-rus><type>DSCHAR</type><required>0</required></field></fields>
<required>false</required></table></input-tables>
<output-tables><table><name>tOut</name><description>Выход</description><fields></fields><required>true</required></table></output-tables>
<architect-approval>NOK</architect-approval></event>`
	result, err := NewParser().ParseContent("/API_X/DSArchitectData/BObject/BO/Event/EvMixed.xml", xmlDoc)
	if err != nil {
		t.Fatalf("ParseContent: %v", err)
	}
	c := result.Contracts[0]
	if c.Implemented {
		t.Fatal("Implemented = true, want false")
	}
	if c.ArchApproval != 0 {
		t.Fatalf("ArchApproval = %d, want 0 (NOK)", c.ArchApproval)
	}
	in, out := result.Tables[0], result.Tables[1]
	if in.Required || !out.Required {
		t.Fatalf("table required flags: in=%v out=%v, want false/true", in.Required, out.Required)
	}
	if !result.TableFields[0].Required || result.TableFields[1].Required {
		t.Fatalf("field required flags = %v/%v, want true/false", result.TableFields[0].Required, result.TableFields[1].Required)
	}
	if result.TableFields[0].RusName != "Поле один" {
		t.Fatalf("F1 RusName = %q", result.TableFields[0].RusName)
	}
}

func TestParseContent_GracefulSkip(t *testing.T) {
	parser := NewParser()

	// Неизвестный корень — skip, не ошибка.
	res, err := parser.ParseContent("/repo/DSArchitectData/Module/BObject/BO/Service/NewFormat.xml",
		`<?xml version="1.0"?><somethingNew><data>x</data></somethingNew>`)
	if err != nil {
		t.Fatalf("unknown root returned error: %v", err)
	}
	if !res.Skipped {
		t.Fatal("unknown root must set Skipped")
	}
	if len(res.Contracts) != 0 || len(res.Tables) != 0 {
		t.Fatal("skipped result must be empty")
	}

	// Известный корень вне своей path-ветки: <Table> в default-ветке — skip.
	res, err = parser.ParseContent("/repo/DSArchitectData/Module/BObject/BO/Service/StrayTable.xml",
		`<?xml version="1.0"?><Table><ParamName>t1</ParamName></Table>`)
	if err != nil {
		t.Fatalf("table outside /Table/ returned error: %v", err)
	}
	if !res.Skipped {
		t.Fatal("<Table> outside /Table/ must set Skipped")
	}

	// Несоответствие ветки: <event> в /Table/-пути — skip.
	res, err = parser.ParseContent("/repo/DSArchitectData/Module/BObject/BO/Table/EvMisplaced.xml",
		`<?xml version="1.0"?><event><name>Ev</name></event>`)
	if err != nil {
		t.Fatalf("event in /Table/ returned error: %v", err)
	}
	if !res.Skipped {
		t.Fatal("<event> in /Table/ must set Skipped")
	}

	// message/fasdocument — прежний skip, теперь с признаком.
	res, err = parser.ParseContent("/repo/msg.xml", `<?xml version="1.0"?><message><x/></message>`)
	if err != nil || !res.Skipped {
		t.Fatalf("message root: err=%v skipped=%v", err, res.Skipped)
	}
	res, err = parser.ParseContent("/repo/fas.xml", `<?xml version="1.0"?><fasdocument><x/></fasdocument>`)
	if err != nil || !res.Skipped {
		t.Fatalf("fasdocument root: err=%v skipped=%v", err, res.Skipped)
	}

	// Битый XML (незакрытый элемент) — ошибка, не skip.
	_, err = parser.ParseContent("/repo/DSArchitectData/Module/BObject/BO/Service/Broken.xml",
		`<?xml version="1.0"?><Object><ObjectName>Test`)
	if err == nil {
		t.Fatal("broken XML must return error, not skip")
	}

	// Валидный <Object> в default-ветке — не skip (регресс).
	res, err = parser.ParseContent("/repo/DSArchitectData/Module/BObject/BO/Service/OK.xml",
		`<?xml version="1.0"?><Object><ObjectName>OK</ObjectName></Object>`)
	if err != nil || res.Skipped {
		t.Fatalf("valid object: err=%v skipped=%v", err, res.Skipped)
	}

	// Валидный <Table> в /Table/-ветке — не skip (регресс).
	res, err = parser.ParseContent("/repo/DSArchitectData/Module/BObject/BO/Table/OK.xml",
		`<?xml version="1.0"?><Table><ParamName>t</ParamName></Table>`)
	if err != nil || res.Skipped {
		t.Fatalf("valid table in /Table/: err=%v skipped=%v", err, res.Skipped)
	}
}

func TestParseBytes_UTF16EventXML(t *testing.T) {
	// UTF-16LE с декларацией UTF-16: детект возвращает UTF16LE, DecodeBytes
	// декодирует (BOM удалён), CharsetReader обслуживает utf-16 pass-through.
	enc := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewEncoder()
	encoded, _, err := transform.Bytes(enc, []byte(`<?xml version="1.0" encoding="UTF-16"?><event><name>EvUTF16</name><descr-short>Событие</descr-short><implemented>true</implemented></event>`))
	if err != nil {
		t.Fatalf("encode UTF-16LE: %v", err)
	}
	withBOM := append([]byte{0xFF, 0xFE}, encoded...)

	result, err := NewParser().ParseBytes(withBOM, "/API_RTG/DSArchitectData/BObject/Rating/Event/EvUTF16.xml")
	if err != nil {
		t.Fatalf("ParseBytes UTF-16LE: %v", err)
	}
	if result.Skipped {
		t.Fatalf("UTF-16 event skipped: %s", result.SkipReason)
	}
	if len(result.Contracts) != 1 || result.Contracts[0].ContractName != "EvUTF16" {
		t.Fatalf("contracts = %+v", result.Contracts)
	}
	if result.Contracts[0].ShortDescription != "Событие" {
		t.Fatalf("ShortDescription = %q (Cyrillic lost)", result.Contracts[0].ShortDescription)
	}
}

func TestXMLCharsetReader_PassThrough(t *testing.T) {
	// Контент предекодирован в UTF-8, декларация осталась cp866/utf-16 —
	// pass-through без ошибки unsupported xml charset.
	for _, decl := range []string{`encoding="cp866"`, `encoding="UTF-16"`, `encoding="utf-16le"`, `encoding="utf-16be"`, `encoding="ucs-2"`, `encoding="ibm866"`} {
		content := `<?xml version="1.0" ` + decl + `?><Object><ObjectName>T</ObjectName></Object>`
		var obj objectXML
		if err := unmarshalXML(content, &obj); err != nil {
			t.Fatalf("unmarshalXML with %s: %v", decl, err)
		}
		if obj.ObjectName != "T" {
			t.Fatalf("ObjectName = %q", obj.ObjectName)
		}
	}
}
