package trc

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// TestReverseEngineerEventBody — служебный (не golden) тест, не проверяющий
// assert-ы, а печатающий (t.Logf) найденные байтовые смещения известных
// значений первого информативного события DIAPR-391.trc (EventClass 37,
// SP:Recompile, см. Modifications/DIAPR-391.utf8full.txt строки 394-420)
// относительно TraceHeader.EventsOffset. Используется как инструмент
// Phase 0 п.1-2: вместо ручного hex-разбора в shell ищем байтовые паттерны
// известных значений программно и по получившимся смещениям восстанавливаем
// раскладку полей тела события.
//
// Запуск: go test ./internal/trc/... -run TestReverseEngineerEventBody -v -timeout 60s
func TestReverseEngineerEventBody(t *testing.T) {
	dir := modificationsDir(t)
	trcPath := filepath.Join(dir, "DIAPR-391.trc")
	skipIfMissing(t, trcPath)
	data, err := os.ReadFile(trcPath)
	if err != nil {
		t.Fatalf("read %s: %v", trcPath, err)
	}
	h, err := ParseHeader(data)
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	t.Logf("EventsOffset = %d (0x%X), file size = %d", h.EventsOffset, h.EventsOffset, len(data))

	// Ищем во всём файле (события начинаются вскоре после EventsOffset, но
	// для первого проходa не ограничиваем окно, чтобы не пропустить смещение
	// из-за неверной гипотезы о начале потока).
	body := data[h.EventsOffset:]

	type candidate struct {
		field   string
		pattern []byte
		note    string
	}

	u16 := func(s string) []byte {
		out := make([]byte, 0, len(s)*2)
		for _, r := range s {
			out = append(out, byte(r), 0)
		}
		return out
	}
	i16 := func(v int16) []byte {
		b := make([]byte, 2)
		binary.LittleEndian.PutUint16(b, uint16(v))
		return b
	}
	i32 := func(v int32) []byte {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, uint32(v))
		return b
	}
	i64 := func(v int64) []byte {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(v))
		return b
	}
	hexBytes := func(s string) []byte {
		b, err := hex.DecodeString(s)
		if err != nil {
			t.Fatalf("hex.DecodeString(%q): %v", s, err)
		}
		return b
	}

	cands := []candidate{
		{"TextData(utf16)", u16("while 1 = 1"), "строка"},
		{"ClientProcessID=18920(i16)", i16(18920), ""},
		{"ClientProcessID=18920(i32)", i32(18920), ""},
		{"DatabaseID=11(i16)", i16(11), "ОСТОРОЖНО частое значение"},
		{"DatabaseID=11(i32)", i32(11), "ОСТОРОЖНО частое значение"},
		{"LoginName(utf16)", u16("diasoft"), "строка"},
		{"LineNumber=143(i16)", i16(143), ""},
		{"LineNumber=143(i32)", i32(143), ""},
		{"HostName(utf16)", u16("SRV-CO-RDS-05"), "строка"},
		{"ApplicationName(utf16)", u16("5NT(e)"), "строка"},
		{"SPID=99(i16)", i16(99), "ОСТОРОЖНО частое значение"},
		{"SPID=99(i32)", i32(99), "ОСТОРОЖНО частое значение"},
		{"EventSubClass=4(i16)", i16(4), "ОСТОРОЖНО частое значение"},
		{"EventSubClass=4(i32)", i32(4), "ОСТОРОЖНО частое значение"},
		{"ObjectID=1486145921(i32)", i32(1486145921), ""},
		{"ObjectID=1486145921(i64)", i64(1486145921), ""},
		{"ObjectType=8272(i16)", i16(8272), ""},
		{"ObjectType=8272(i32)", i32(8272), ""},
		{"ObjectName(utf16)", u16("Process_Init"), "строка"},
		{"DatabaseName(utf16)", u16("testday3"), "строка"},
		{"LoginSid(raw16)", hexBytes("3F28485733559D42A2631F19875EC9B4"), "GUID/binary как есть"},
		{"EventSequence=23821372(i32)", i32(23821372), ""},
		{"EventSequence=23821372(i64)", i64(23821372), ""},
		{"IntegerData2=11554(i16)", i16(11554), ""},
		{"IntegerData2=11554(i32)", i32(11554), ""},
		{"Offset=11534(i16)", i16(11534), ""},
		{"Offset=11534(i32)", i32(11534), ""},
		{"SqlHandle(raw)", hexBytes("03000B0081C9945800B69D00FCB2000001000000000000000000000000000000000000000000000000000000"), "binary как есть"},
		{"SessionLoginName(utf16)", u16("diasoft"), "строка, совпадает с LoginName"},
		{"ServerName(utf16)", u16("re_test2016"), "строка, уже встречалась в заголовке"},
	}

	type found struct {
		field  string
		offset int
		length int
		note   string
	}
	var results []found
	for _, c := range cands {
		idx := bytes.Index(body, c.pattern)
		if idx < 0 {
			t.Logf("NOT FOUND: %-40s (len=%d) %s", c.field, len(c.pattern), c.note)
			continue
		}
		results = append(results, found{c.field, idx, len(c.pattern), c.note})
	}

	sort.Slice(results, func(i, j int) bool { return results[i].offset < results[j].offset })

	t.Logf("=== Найденные поля, отсортированные по смещению относительно EventsOffset ===")
	for i, r := range results {
		gap := ""
		if i > 0 {
			prevEnd := results[i-1].offset + results[i-1].length
			gap = fmt.Sprintf(" gap_from_prev_end=%d", r.offset-prevEnd)
		}
		t.Logf("offset=%6d (abs=%d) len=%3d field=%-40s%s %s",
			r.offset, h.EventsOffset+r.offset, r.length, r.field, gap, r.note)
	}

	// Проверяем гипотезу TLV (PropID uint16 LE + Type byte + Length uint32 LE
	// + Value): ищем ВСЕ вхождения строки "while 1 = 1" (повторяющийся
	// TextData у нескольких SP:Recompile событий в XML) во всём body, чтобы
	// понять, повторяется ли обнаруженная раскладка периодически (то есть
	// является форматом КАЖДОГО события), либо это разовый блок.
	needle := u16("while 1 = 1")
	var allIdx []int
	for start := 0; ; {
		idx := bytes.Index(body[start:], needle)
		if idx < 0 {
			break
		}
		allIdx = append(allIdx, start+idx)
		start += idx + 1
	}
	firstN := allIdx
	if len(firstN) > 6 {
		firstN = firstN[:6]
	}
	t.Logf("ALL_OCCURRENCES count=%d first6=%v", len(allIdx), firstN)

	// Ищем ВСЕ вхождения "SQL Server Profiler" (utf16) в body и печатаем
	// 16 байт непосредственно ПЕРЕД каждым вхождением — это заголовок
	// блока (маркер + служебные поля), который предшествует строке.
	// Делается программно (не вручную), чтобы избежать ошибок смещения
	// при чтении одного hex-дампа глазами.
	profilerNeedle := u16("SQL Server Profiler")
	var profilerIdx []int
	for start := 0; ; {
		idx := bytes.Index(body[start:], profilerNeedle)
		if idx < 0 {
			break
		}
		profilerIdx = append(profilerIdx, start+idx)
		start += idx + 1
	}
	t.Logf("=== \"SQL Server Profiler\" occurrences: %d ===", len(profilerIdx))
	for i, idx := range profilerIdx {
		if i >= 5 {
			break
		}
		pre := 16
		if idx < pre {
			pre = idx
		}
		t.Logf("occurrence %d at body_offset=%d abs=%d preceding_bytes=% X", i, idx, h.EventsOffset+idx, body[idx-pre:idx])
	}

	// Дополнительно: печатаем сырой hex-дамп первых 512 байт потока событий
	// для визуальной перепроверки найденных смещений.
	dumpLen := 512
	if dumpLen > len(body) {
		dumpLen = len(body)
	}
	t.Logf("=== Hex-дамп первых %d байт body (offset 0 = EventsOffset=%d) ===", dumpLen, h.EventsOffset)
	for off := 0; off < dumpLen; off += 16 {
		end := off + 16
		if end > dumpLen {
			end = dumpLen
		}
		t.Logf("%6d: % X", off, body[off:end])
	}
}
