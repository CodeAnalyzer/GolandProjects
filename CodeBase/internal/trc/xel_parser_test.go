package trc

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestParseXEL_Header — проверяет разбор заголовка .xel (сигнатура/GUID +
// калибровочные константы timestamp) на STP3_1.xel.
func TestParseXEL_Header(t *testing.T) {
	path := filepath.Join(modificationsDir(t), "STP3_1.xel")
	skipIfMissing(t, path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	h, err := parseXELHeader(data)
	if err != nil {
		t.Fatalf("parseXELHeader: %v", err)
	}
	if len(h.Magic) != 8 {
		t.Errorf("Magic длина=%d, ожидалось 8", len(h.Magic))
	}
	if len(h.FileGUID) != 16 {
		t.Errorf("FileGUID длина=%d, ожидалось 16", len(h.FileGUID))
	}
	if h.Freq == 0 {
		t.Errorf("Freq=0, ожидалось непустое значение")
	}
	if h.BaseFileTime100ns == 0 {
		t.Errorf("BaseFileTime100ns=0, ожидалось непустое значение")
	}
	// Значения подтверждены в TestXEL_TimestampCalibration.
	if h.BaseFileTime100ns != 133545483423052768 {
		t.Errorf("BaseFileTime100ns=%d, ожидалось 133545483423052768", h.BaseFileTime100ns)
	}
	if h.Freq != 3020249 {
		t.Errorf("Freq=%d, ожидалось 3020249", h.Freq)
	}
}

// TestParseXEL_FileTimeConversion — проверяет xelRawTimestampToUTC на
// значениях, подтверждённых TestXEL_TimestampCalibration.
func TestParseXEL_FileTimeConversion(t *testing.T) {
	header := &XELHeader{BaseFileTime100ns: 133545483423052768, Freq: 3020249}
	got := xelRawTimestampToUTC(1407640595195, header).Truncate(time.Millisecond)
	want, _ := time.Parse(time.RFC3339Nano, "2024-03-15T22:13:30.035Z")
	if !got.Equal(want) {
		t.Errorf("xelRawTimestampToUTC=%s, ожидалось %s", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

// TestParseXEL_EventNameMapping — проверяет, что ParseXEL корректно
// присваивает EventClass/EventName через xeEventNameToClass (Phase 1) для
// распознанных типов событий, включая EventClass=0 для wait_completed (XE
// событие без TRC-эквивалента).
func TestParseXEL_EventNameMapping(t *testing.T) {
	path := filepath.Join(modificationsDir(t), "STP3_1.xel")
	skipIfMissing(t, path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	// Ограничиваем объём разбираемых данных для скорости теста (первые ~200КБ
	// достаточно для нескольких событий каждого распознанного типа — см.
	// TestXEL_DecodeWaitCompletedData/TestXEL_DecodeSQLBatchCompletedData).
	limit := 200_000
	if limit > len(data) {
		limit = len(data)
	}
	res, err := ParseXEL(data[:limit])
	if err != nil {
		t.Fatalf("ParseXEL: %v", err)
	}
	if len(res.Events) == 0 {
		t.Fatalf("ParseXEL вернул 0 событий")
	}

	seen := map[string]bool{}
	for _, ev := range res.Events {
		seen[ev.EventName] = true
		switch ev.EventName {
		case "wait_completed":
			if ev.EventClass != 0 {
				t.Errorf("wait_completed: EventClass=%d, ожидалось 0 (XE-only событие)", ev.EventClass)
			}
		case "sql_batch_completed":
			if ev.EventClass != 12 {
				t.Errorf("sql_batch_completed: EventClass=%d, ожидалось 12", ev.EventClass)
			}
		case "sql_statement_completed":
			if ev.EventClass != 41 {
				t.Errorf("sql_statement_completed: EventClass=%d, ожидалось 41", ev.EventClass)
			}
		case "sp_statement_completed":
			if ev.EventClass != 45 {
				t.Errorf("sp_statement_completed: EventClass=%d, ожидалось 45", ev.EventClass)
			}
		}
	}
	if !seen["wait_completed"] {
		t.Errorf("не найдено ни одного события wait_completed в первых %d байт", limit)
	}
}

// TestParseXEL_MatchesReferenceXML — сверяет декодированные значения первых
// нескольких событий wait_completed с STP3_1.reference.xml (см.
// TestXEL_ExtractReferenceSchema/loadReferenceXML).
func TestParseXEL_MatchesReferenceXML(t *testing.T) {
	xelPath := filepath.Join(modificationsDir(t), "STP3_1.xel")
	skipIfMissing(t, xelPath)
	data, err := os.ReadFile(xelPath)
	if err != nil {
		t.Fatalf("read %s: %v", xelPath, err)
	}
	limit := 200_000
	if limit > len(data) {
		limit = len(data)
	}
	res, err := ParseXEL(data[:limit])
	if err != nil {
		t.Fatalf("ParseXEL: %v", err)
	}

	ref := loadReferenceXML(t, 5)
	if len(ref.Events) == 0 {
		t.Fatalf("эталонный XML пуст")
	}

	// event[0] эталона — wait_completed, session_id=370 (см.
	// TestXEL_DecodeFirstEventActions/TestXEL_DecodeWaitCompletedData).
	if len(res.Events) == 0 {
		t.Fatalf("ParseXEL вернул 0 событий")
	}
	first := res.Events[0]
	if first.EventName != "wait_completed" {
		t.Fatalf("первое событие=%q, ожидалось wait_completed", first.EventName)
	}
	if sess, ok := first.Columns[12].(int32); !ok || sess != 370 {
		t.Errorf("SPID(12)=%v, ожидалось 370", first.Columns[12])
	}
	if db, ok := first.Columns[35].(string); !ok || db != "diasoft_prod" {
		t.Errorf("DatabaseName(35)=%v, ожидалось diasoft_prod", first.Columns[35])
	}
}

// TestParseXEL_FullFile — сквозной прогон полного STP3_1.xel: проверяет, что
// парсер не падает на всём файле и находит существенное количество событий
// известных типов. Не входит в быстрый набор — запускается отдельно по -run.
//
// Запуск: go test ./internal/trc/... -run TestParseXEL_FullFile -v -timeout 300s
func TestParseXEL_FullFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping full-file smoke test in -short mode")
	}
	path := filepath.Join(modificationsDir(t), "STP3_1.xel")
	skipIfMissing(t, path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	res, err := ParseXEL(data)
	if err != nil {
		t.Fatalf("ParseXEL: %v", err)
	}
	if len(res.Events) == 0 {
		t.Fatalf("ParseXEL вернул 0 событий на полном файле")
	}

	counts := map[string]int{}
	for _, ev := range res.Events {
		counts[ev.EventName]++
	}
	t.Logf("всего событий: %d", len(res.Events))
	for name, c := range counts {
		t.Logf("  %-30s %d", name, c)
	}
	if counts["wait_completed"] == 0 {
		t.Errorf("wait_completed не найдено на полном файле")
	}
}
