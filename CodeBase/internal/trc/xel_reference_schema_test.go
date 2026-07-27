package trc

import (
	"bufio"
	"encoding/xml"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// refRoot — логический контейнер результата разбора STP3_1.reference.xml.
// Файл, полученный через `sys.fn_xe_file_target_read_file` + `FOR XML
// PATH(”)`, представляет собой последовательность соседних
// <event>...</event> элементов БЕЗ единого корневого узла (не является
// самостоятельным well-formed документом с одним корнем), поэтому
// разбирается не через dec.Decode(&root), а потоково: dec.Token() до
// каждого StartElement "event", затем dec.DecodeElement (см.
// loadReferenceXML). Файл большой (~188 МБ на полном STP3_1.xel) —
// streaming обязателен, полная загрузка в память нежелательна.
type refRoot struct {
	Events []refEvent
}

type refEvent struct {
	Name      string     `xml:"name,attr"`
	Package   string     `xml:"package,attr"`
	Timestamp string     `xml:"timestamp,attr"`
	Data      []refField `xml:"data"`
	Action    []refField `xml:"action"`
}

// refField — общая структура для <data name="..."><value>...</value>
// [<text>...</text>]</data> и <action name="..." package="..."><value>...
// </value></action>. Text заполняется только для map-полей (числовой код +
// разрешённое человекочитаемое имя, см. wait_type/SOS_SCHEDULER_YIELD).
type refField struct {
	Name    string `xml:"name,attr"`
	Package string `xml:"package,attr"`
	Value   string `xml:"value"`
	Text    string `xml:"text"`
}

// loadReferenceXML потоково читает Modifications/STP3_1.reference.xml и
// разбирает его как последовательность соседних <event> элементов (без
// единого корня), не загружая файл в память целиком. limit ограничивает
// число распознанных событий (0 = без ограничения) — полезно для быстрых
// прогонов на 188 МБ файле.
func loadReferenceXML(t *testing.T, limit int) *refRoot {
	t.Helper()
	path := filepath.Join(modificationsDir(t), "STP3_1.reference.xml")
	skipIfMissing(t, path)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	dec := xml.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	var root refRoot
	for {
		if limit > 0 && len(root.Events) >= limit {
			break
		}
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("token %s (event #%d): %v", path, len(root.Events), err)
		}
		start, ok := tok.(xml.StartElement)
		if !ok || start.Name.Local != "event" {
			continue
		}
		var ev refEvent
		if err := dec.DecodeElement(&ev, &start); err != nil {
			t.Fatalf("decode event #%d in %s: %v", len(root.Events), path, err)
		}
		root.Events = append(root.Events, ev)
	}
	return &root
}

// TestXEL_ExtractReferenceSchema — служебный (не golden) тест: строит и
// печатает (t.Logf) полный реестр реально встречающихся в STP3_1.reference.xml
// имён event/data/action, чтобы использовать их как точный список "known
// values" для реверс-инжиниринга бинарного STP3_1.xel (см. Phase 0 плана
// Modifications/xel-extended-events-support-a1b2c3.md и
// C:\Users\Александр\.windsurf\plans\xel-parser-impl-2640ba.md).
//
// Запуск: go test ./internal/trc/... -run TestXEL_ExtractReferenceSchema -v -timeout 120s
func TestXEL_ExtractReferenceSchema(t *testing.T) {
	root := loadReferenceXML(t, 0)
	t.Logf("Всего событий в эталоне: %d", len(root.Events))

	type dataFieldInfo struct {
		hasText bool
	}

	// eventName -> dataFieldName -> info
	eventDataFields := map[string]map[string]dataFieldInfo{}
	eventCount := map[string]int{}
	actionNames := map[string]struct{}{}
	packageNames := map[string]struct{}{}

	for _, ev := range root.Events {
		eventCount[ev.Name]++
		packageNames[ev.Package] = struct{}{}

		fields, ok := eventDataFields[ev.Name]
		if !ok {
			fields = map[string]dataFieldInfo{}
			eventDataFields[ev.Name] = fields
		}
		for _, d := range ev.Data {
			info := fields[d.Name]
			if d.Text != "" {
				info.hasText = true
			}
			fields[d.Name] = info
		}
		for _, a := range ev.Action {
			actionNames[a.Name] = struct{}{}
		}
	}

	eventNames := make([]string, 0, len(eventCount))
	for name := range eventCount {
		eventNames = append(eventNames, name)
	}
	sort.Strings(eventNames)

	t.Logf("=== Уникальные packages: %v ===", sortedKeys(packageNames))
	t.Logf("=== Уникальные event names (%d) ===", len(eventNames))
	for _, name := range eventNames {
		fields := eventDataFields[name]
		fieldNames := make([]string, 0, len(fields))
		for fn, info := range fields {
			if info.hasText {
				fieldNames = append(fieldNames, fn+"(+text)")
			} else {
				fieldNames = append(fieldNames, fn)
			}
		}
		sort.Strings(fieldNames)
		t.Logf("event=%-30s count=%-6d data_fields=%v", name, eventCount[name], fieldNames)
	}

	t.Logf("=== Уникальные action names (%d) ===", len(actionNames))
	for _, name := range sortedKeys(actionNames) {
		t.Logf("action=%s", name)
	}

	// Печатаем первые 5 событий полностью (все известные значения) — это
	// прямые "known values" для needle-поиска в бинарном .xel (Phase 0 п.3
	// плана xel-parser-impl-2640ba.md).
	limit := 5
	if limit > len(root.Events) {
		limit = len(root.Events)
	}
	t.Logf("=== Первые %d событий целиком (known values для needle-поиска) ===", limit)
	for i := 0; i < limit; i++ {
		ev := root.Events[i]
		t.Logf("--- event[%d]: name=%s package=%s timestamp=%s ---", i, ev.Name, ev.Package, ev.Timestamp)
		for _, d := range ev.Data {
			t.Logf("  data name=%-20s value=%-30q text=%q", d.Name, d.Value, d.Text)
		}
		for _, a := range ev.Action {
			t.Logf("  action name=%-20s value=%q", a.Name, a.Value)
		}
	}
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
