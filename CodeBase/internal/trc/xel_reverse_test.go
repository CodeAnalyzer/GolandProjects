package trc

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Найдено и подтверждено побайтово на STP3_1.xel (Phase 0, см.
// C:\Users\Александр\.windsurf\plans\xel-parser-impl-2640ba.md):
//
// Формат .xel САМООПИСЫВАЮЩИЙСЯ (schema/dictionary block в начале файла),
// НЕ требует внешнего каталога SQL Server:
//   - В начале файла (offset ~0-65000+) — словари: map-значения (напр.
//     wait_type: [int32 ID][UTF-16LE имя]\0...), затем список package/event/
//     action/data имён — каждое встречается как обычная UTF-16LE строка
//     РОВНО ОДИН РАЗ (проверено: "session_id", "database_name",
//     "client_app_name" — count=1; "diasoft_prod" (ЗНАЧЕНИЕ, не имя) —
//     count=153979, то есть по разу на каждое событие с такой БД).
//   - Каждое ACTION-поле в событии закодировано как TLV:
//     [package uint8][actionID uint16 LE][0x10][length uint32 LE][length байт]
//     Первый байт — НЕ константа 0x02, а НОМЕР PACKAGE, которому принадлежит
//     action: подтверждено на двух разных событиях —
//       package=2 (sqlserver): actionID=80(0x50)->transaction_sequence,
//         actionID=4(0x04)->transaction_id, actionID=8(0x08)->session_id,
//         actionID=76(0x4C)->database_name, actionID=56(0x38)->username,
//         actionID=44(0x2C)->client_hostname, actionID=36(0x24)->client_app_name
//       package=1 (sqlos):      actionID=28(0x1C)->cpu_id
//     (значения из STP3_1.reference.xml event[0] (wait_completed) и первого
//     sql_batch_completed — actionID стабильны между разнотипными событиями,
//     т.е. это ГЛОБАЛЬНЫЙ словарь action-имён, не per-event-type).
//     ID являются ЛОКАЛЬНЫМИ ссылками на словарь имён actions, прочитанный
//     из schema-блока в начале файла (сопоставление ID->имя ещё не найдено
//     явно — гипотеза: позиция в списке объявленных action name).
//   - DATA-секция события wait_completed ПОЛНОСТЬЮ раскодирована и
//     подтверждена на множестве образцов (см. TestXEL_DecodeWaitCompletedData),
//     включая переменную длину wait_resource:
//       [timestamp: uint64 LE, 8 байт][totalLen: uint32 LE, 4 байта]
//       [wait_type: uint32 LE, 4][reserved: uint32 LE, 4][wait_result: uint32 LE, 4]
//       [duration: uint64 LE, 8][signal_duration: uint64 LE, 8]
//       [typeTag: uint32 LE, 4, ВСЕГДА константа 0x00000024]
//       [waitResourceLen: uint32 LE, 4][waitResourceBytes: UTF-16LE, waitResourceLen байт]
//     totalLen = 36 + waitResourceLen (длина всего, что идёт ПОСЛЕ totalLen).
//     Перед этим блоком идёт стабильный 12-байтовый маркер
//     "01 C0 00 00 0A 00 00 80 00 00 00 00" — назначение не декодировано,
//     но встречается идентично перед КАЖДЫМ wait_completed (используется
//     как якорь поиска в тесте).
//   - Timestamp (8 байт) — это НЕ абсолютный FILETIME (100ns с 1601 года):
//     искомое точное значение для "2024-03-15T22:13:30.035Z" НЕ найдено
//     в файле (проверено ±3с в первых 100КБ). Поле МОНОТОННО растёт и
//     коррелирует с реальным временем с частотой ~3 014 290 тиков/сек
//     (оценка по двум точкам: событие[0] в 22:13:30.035Z и событие в конце
//     файла) — похоже на сырой QueryPerformanceCounter (QPC), требующий
//     калибровки (базовый FILETIME + частота), которая, вероятно, хранится
//     ОДИН РАЗ в ещё не декодированном schema/dictionary/header-блоке в
//     начале файла. Это отдельная открытая задача следующей итерации.
//   - DATA-секция события sql_batch_completed ТАКЖЕ раскодирована и
//     подтверждена на 4 образцах (см. TestXEL_DecodeSQLBatchCompletedData),
//     включая большие значения (cpu_time=2969000, logical_reads=2291813) и
//     переменную длину batch_text:
//       [timestamp: u64][totalLen: u32][cpu_time: u64][duration: u64]
//       [physical_reads: u64][logical_reads: u64][writes: u64][spills: u64]
//       [row_count: u64][result: u8][typeTag: u32 = const 0x41]
//       [batchTextLen: u32][batchTextBytes: UTF-16LE, batchTextLen байт]
//     totalLen = 65 + batchTextLen (65 = 8*7 + 1 + 4 + 4).
//     ВАЖНО: у sql_batch_completed порядок секций ОБРАТНЫЙ относительно
//     wait_completed — ACTION-секция идёт ПЕРЕД DATA-секцией. Между концом
//     TLV-поля cpu_id (package=1, actionID=28) и началом timestamp — ровно
//     20 байт (длина стабильна на всех образцах, но СОДЕРЖИМОЕ этих байт
//     варьируется, похоже на счётчик/размер записи — назначение не
//     декодировано, поэтому используется как ФИКСИРОВАННОЕ СМЕЩЕНИЕ, а не
//     байтовый якорь).
//     `result` наблюдался только со значением 0 (кодируется 1 байтом во
//     всех проверенных образцах) — ширина поля для НЕнулевых значений
//     result не подтверждена, это отдельный открытый вопрос.
//   - DATA-секция события sql_statement_completed раскодирована и
//     подтверждена на 2 образцах (см. TestXEL_DecodeSQLStatementCompletedData),
//     включая edge-case offset_end=-1 ("до конца батча") и большие значения:
//       [timestamp: u64][totalLen: u32]
//       [duration: u64][cpu_time: u64][physical_reads: u64][logical_reads: u64]
//       [writes: u64][spills: u64][row_count: u64][last_row_count: u64]
//       [line_number: u32][offset: u32][offset_end: i32]
//       [typeTag: u32 = const 0x5C][statementLen: u32]
//       [totalLenRepeat: u32 — ПОВТОРЯЕТ значение totalLen][zero: u32 = 0]
//       [statementBytes: UTF-16LE, statementLen байт]
//     ВАЖНО: порядок первых двух полей ОБРАТНЫЙ относительно
//     sql_batch_completed — здесь `duration` идёт ПЕРЕД `cpu_time` (у
//     sql_batch_completed — наоборот). Назначение полей `totalLenRepeat` и
//     `zero` в хвосте не выяснено (возможно связаны с
//     parameterized_plan_handle, который в обоих образцах был NULL) —
//     подтверждено лишь эмпирически по двум образцам с разными значениями
//     total_len, что позиции и семантика (statementLen корректен) верны.
//   - DATA-секция sp_statement_completed ЧАСТИЧНО раскодирована (на 3
//     образцах, включая один с крайними значениями row_count=7508031,
//     writes=298419, исключающими путаницу u16/u32). Структура смешанной
//     разрядности, поля НЕ идут плотной последовательностью (есть
//     необъяснённые промежутки между некоторыми полями — вероятно
//     физически присутствующие, но пока не идентифицированные поля вроде
//     module_id/database_id2 и т.п., которые есть в TRC-эквиваленте, но не
//     показаны в упрощённом reference XML). Подтверждённые относительные
//     смещения (от начала записи сразу после totalLen, все LE):
//       offset 0: source_database_id (u16)
//       offset 4: object_id (u32)
//       offset 8: object_type (u16)
//       offset 10: duration (u32)  [непосредственно после object_type]
//       offset 18: cpu_time (u32)
//       offset 34: logical_reads (u32)
//       offset 42: writes (u32)
//       offset 58: row_count (u32)
//       offset ~66: nest_level ИЛИ last_row_count (u32?, порядок между ними
//         НЕ разрешён — на всех 3 образцах эти два поля имели равные
//         значения, что делает позицию неразличимой; нужен образец с
//         nest_level != last_row_count)
//       offset 76: line_number (u32, подтверждено только для значений <65536,
//         явно u32 по контексту соседних полей)
//       offset 80: offset (u32)
//       offset 84: offset_end (u32)
//     physical_reads/spills НЕ найдены явно (оба образца имели значение 0,
//     что делает поиск неоднозначным — совпадает с любым нулевым байтом).
//     object_name/statement (переменная длина, идут после offset_end) —
//     структура тега/длины НЕ раскодирована (наблюдался констатный байт
//     0x68=104 перед предполагаемой длиной object_name, но точная семантика
//     не выяснена). Это ОТКРЫТАЯ задача следующей итерации — событие
//     относительно редкое (~9.7% всех событий файла), в отличие от 3 уже
//     полностью раскодированных типов (суммарно ~90.5% событий).

// TestXEL_TimestampCalibration — регрессионный тест на формулу конвертации
// "сырого" timestamp событий в абсолютное время UTC (см. комментарий формата
// у xelBaseFileTimeOffset). Проверяет 6 образцов, разнесённых по всему
// файлу, с точностью до миллисекунды (эталонные значения из
// STP3_1.reference.xml задаются с точностью до мс).
func TestXEL_TimestampCalibration(t *testing.T) {
	path := filepath.Join(modificationsDir(t), "STP3_1.xel")
	skipIfMissing(t, path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	baseFT, freq := decodeXELTimeCalibration(data)
	t.Logf("baseFileTime100ns=%d freq=%d", baseFT, freq)
	header := &XELHeader{BaseFileTime100ns: baseFT, Freq: freq}

	check := func(rawTS uint64, wantISO string) {
		want, err := time.Parse(time.RFC3339Nano, wantISO)
		if err != nil {
			t.Fatalf("parse %q: %v", wantISO, err)
		}
		got := xelRawTimestampToUTC(rawTS, header)
		gotMs := got.Truncate(time.Millisecond)
		wantMs := want.Truncate(time.Millisecond)
		if !gotMs.Equal(wantMs) {
			t.Errorf("rawTS=%d: got %s, ожидалось %s", rawTS, gotMs.Format(time.RFC3339Nano), wantMs.Format(time.RFC3339Nano))
		} else {
			t.Logf("rawTS=%d: OK %s", rawTS, gotMs.Format(time.RFC3339Nano))
		}
	}

	// Первое и последнее событие wait_completed во всём файле (крайние точки).
	check(1407640595195, "2024-03-15T22:13:30.035Z")
	check(1408816333995, "2024-03-15T22:19:59.320Z")
	// Образцы из ранее декодированных sql_statement_completed/sql_batch_completed.
	check(1407641013286, "2024-03-15T22:13:30.173Z")
	check(1407641499478, "2024-03-15T22:13:30.334Z")
	check(1407641013314, "2024-03-15T22:13:30.173Z")
	check(1407641320120, "2024-03-15T22:13:30.275Z")
}

// scanXELActionFields ищет и декодирует все TLV-записи формата
// [package uint8][actionID uint16 LE][0x10][length uint32 LE][length байт]
// начиная с offset в пределах data[:limit]. Первый байт — номер package
// (1 или 2 подтверждены; другие значения пока не встречены и намеренно не
// исключаются, чтобы не потерять пока неизвестные packages). Не проверяет
// валидность строго (может дать false positive) — используется только как
// инструмент подтверждения гипотезы на известном диапазоне смещений,
// где заранее известны ожидаемые значения (см. TestXEL_DecodeFirstEventActions).
func scanXELActionFields(data []byte, start, limit int) []xelActionField {
	var out []xelActionField
	for i := start; i+8 <= limit; i++ {
		pkg := data[i]
		if (pkg != 0x01 && pkg != 0x02) || data[i+3] != 0x10 {
			continue
		}
		actionID := binary.LittleEndian.Uint16(data[i+1 : i+3])
		length := binary.LittleEndian.Uint32(data[i+4 : i+8])
		valStart := i + 8
		valEnd := valStart + int(length)
		if length == 0 || length > 4096 || valEnd > limit {
			continue
		}
		out = append(out, xelActionField{
			Package:  pkg,
			ActionID: actionID,
			Value:    data[valStart:valEnd],
			Offset:   i,
		})
		i = valEnd - 1 // продолжаем поиск сразу после этого поля
	}
	return out
}

// TestXEL_DecodeFirstEventActions — регрессионный тест на подтверждённую
// раскладку ACTION-полей первого события STP3_1.xel. Если формат
// изменится (другой файл/версия SQL Server), тест укажет на расхождение.
//
// Ожидаемые значения взяты из STP3_1.reference.xml, event[0] (см.
// TestXEL_ExtractReferenceSchema): transaction_sequence=1589137916980,
// transaction_id=112201463824, session_id=370, database_name=diasoft_prod.
func TestXEL_DecodeFirstEventActions(t *testing.T) {
	path := filepath.Join(modificationsDir(t), "STP3_1.xel")
	skipIfMissing(t, path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	// Первое событие расположено в районе offset ~69600-70000 (найдено
	// поиском первого вхождения UTF-16LE "diasoft_prod" при разведке).
	// Берём широкое окно вокруг, чтобы захватить весь action-список первого
	// события без привязки к точному началу event-заголовка (тот пока не
	// полностью декодирован).
	const windowStart = 69600
	const windowEnd = 70200
	if windowEnd > len(data) {
		t.Fatalf("файл короче ожидаемого окна: len=%d", len(data))
	}

	fields := scanXELActionFields(data, windowStart, windowEnd)
	if len(fields) == 0 {
		t.Fatalf("не найдено ни одного action TLV-поля в окне [%d:%d]", windowStart, windowEnd)
	}

	// Ключ — (package, actionID): actionID сам по себе не гарантированно
	// уникален между разными packages (см. комментарий package в
	// xelActionField), поэтому не используем bare actionID как ключ.
	type key struct {
		pkg byte
		id  uint16
	}
	byID := map[key][]byte{}
	for _, f := range fields {
		byID[key{f.Package, f.ActionID}] = f.Value
		t.Logf("package=%d actionID=%3d (0x%02X) len=%3d offset=%d", f.Package, f.ActionID, f.ActionID, len(f.Value), f.Offset)
	}

	assertUint64 := func(pkg byte, id uint16, want uint64, label string) {
		v, ok := byID[key{pkg, id}]
		if !ok {
			t.Errorf("%s: package=%d actionID=%d не найден", label, pkg, id)
			return
		}
		if len(v) != 8 {
			t.Errorf("%s: package=%d actionID=%d длина=%d, ожидалось 8", label, pkg, id, len(v))
			return
		}
		got := binary.LittleEndian.Uint64(v)
		if got != want {
			t.Errorf("%s: package=%d actionID=%d значение=%d, ожидалось %d", label, pkg, id, got, want)
		}
	}
	assertUint16 := func(pkg byte, id uint16, want uint16, label string) {
		v, ok := byID[key{pkg, id}]
		if !ok {
			t.Errorf("%s: package=%d actionID=%d не найден", label, pkg, id)
			return
		}
		if len(v) != 2 {
			t.Errorf("%s: package=%d actionID=%d длина=%d, ожидалось 2", label, pkg, id, len(v))
			return
		}
		got := binary.LittleEndian.Uint16(v)
		if got != want {
			t.Errorf("%s: package=%d actionID=%d значение=%d, ожидалось %d", label, pkg, id, got, want)
		}
	}
	assertUint32 := func(pkg byte, id uint16, want uint32, label string) {
		v, ok := byID[key{pkg, id}]
		if !ok {
			t.Errorf("%s: package=%d actionID=%d не найден", label, pkg, id)
			return
		}
		if len(v) != 4 {
			t.Errorf("%s: package=%d actionID=%d длина=%d, ожидалось 4", label, pkg, id, len(v))
			return
		}
		got := binary.LittleEndian.Uint32(v)
		if got != want {
			t.Errorf("%s: package=%d actionID=%d значение=%d, ожидалось %d", label, pkg, id, got, want)
		}
	}
	assertUTF16String := func(pkg byte, id uint16, want string, label string) {
		v, ok := byID[key{pkg, id}]
		if !ok {
			t.Errorf("%s: package=%d actionID=%d не найден", label, pkg, id)
			return
		}
		got := decodeUTF16LE(v)
		if got != want {
			t.Errorf("%s: package=%d actionID=%d значение=%q, ожидалось %q", label, pkg, id, got, want)
		}
	}

	assertUint64(2, 80, 1589137916980, "transaction_sequence")
	assertUint64(2, 4, 112201463824, "transaction_id")
	assertUint16(2, 8, 370, "session_id")
	assertUTF16String(2, 76, "diasoft_prod", "database_name")
	assertUTF16String(2, 56, "rosbank\\rbs-celd-devops", "username")
	assertUTF16String(2, 44, "RBCORPPAS00016", "client_hostname")
	assertUTF16String(2, 36, "Admin STP 5NT(e)", "client_app_name")
	assertUint32(1, 28, 133, "cpu_id (package=sqlos)")
}

// xelActionValueByID возвращает значение action-поля с данным (package,id)
// начиная со start, сканируя не более maxScan подряд идущих TLV-записей.
func xelActionValueByID(data []byte, start int, pkg byte, id uint16, maxScan int) []byte {
	p := start
	for i := 0; i < maxScan && p+8 <= len(data); i++ {
		gotPkg := data[p]
		if gotPkg != 0x01 && gotPkg != 0x02 {
			return nil
		}
		gotID := binary.LittleEndian.Uint16(data[p+1 : p+3])
		length := binary.LittleEndian.Uint32(data[p+4 : p+8])
		if int(length) < 0 || p+8+int(length) > len(data) {
			return nil
		}
		val := data[p+8 : p+8+int(length)]
		if gotPkg == pkg && gotID == id {
			return val
		}
		p += 8 + int(length)
	}
	return nil
}

// TestXEL_DecodeWaitCompletedData — регрессионный тест на полностью
// подтверждённый формат DATA-секции события wait_completed (см. комментарий
// формата в начале файла). Проверяет три образца, разнесённых по всему
// файлу STP3_1.xel (первое, второе и одно из последних событий
// wait_completed), включая случай с переменной длиной wait_resource.
//
// Ожидаемые значения взяты из STP3_1.reference.xml событий с
// соответствующими session_id (см. TestXEL_ExtractReferenceSchema, event[0]
// и event[1]/[2] в её выводе, а также ручную выборку последнего образца).
func TestXEL_DecodeWaitCompletedData(t *testing.T) {
	path := filepath.Join(modificationsDir(t), "STP3_1.xel")
	skipIfMissing(t, path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	var markerOffsets []int
	for i := 0; ; {
		j := bytes.Index(data[i:], xelWaitCompletedMarker)
		if j == -1 {
			break
		}
		markerOffsets = append(markerOffsets, i+j)
		i += j + 1
	}
	t.Logf("найдено маркеров wait_completed: %d", len(markerOffsets))
	if len(markerOffsets) < 3 {
		t.Fatalf("слишком мало маркеров wait_completed: %d", len(markerOffsets))
	}

	check := func(idx int, wantSession uint16, wantWaitType, wantWaitResult uint32, wantDuration, wantSignalDuration uint64, wantWaitResource string) {
		off := markerOffsets[idx]
		d, err := decodeXELWaitCompletedData(data, off)
		if err != nil {
			t.Errorf("markerOffsets[%d]=%d: decode error: %v", idx, off, err)
			return
		}
		if d.TypeTag != 0x24 {
			t.Errorf("markerOffsets[%d]: TypeTag=0x%X, ожидалось 0x24", idx, d.TypeTag)
		}
		if d.WaitType != wantWaitType {
			t.Errorf("markerOffsets[%d]: WaitType=%d, ожидалось %d", idx, d.WaitType, wantWaitType)
		}
		if d.WaitResult != wantWaitResult {
			t.Errorf("markerOffsets[%d]: WaitResult=%d, ожидалось %d", idx, d.WaitResult, wantWaitResult)
		}
		if d.Duration != wantDuration {
			t.Errorf("markerOffsets[%d]: Duration=%d, ожидалось %d", idx, d.Duration, wantDuration)
		}
		if d.SignalDuration != wantSignalDuration {
			t.Errorf("markerOffsets[%d]: SignalDuration=%d, ожидалось %d", idx, d.SignalDuration, wantSignalDuration)
		}
		if d.WaitResource != wantWaitResource {
			t.Errorf("markerOffsets[%d]: WaitResource=%q, ожидалось %q", idx, d.WaitResource, wantWaitResource)
		}
		sessVal := xelActionValueByID(data, d.ActionsStart, 2, 8, 10)
		if sessVal == nil {
			t.Errorf("markerOffsets[%d]: session_id action не найден после DATA-секции", idx)
			return
		}
		if len(sessVal) != 2 {
			t.Errorf("markerOffsets[%d]: session_id длина=%d, ожидалось 2", idx, len(sessVal))
			return
		}
		gotSession := binary.LittleEndian.Uint16(sessVal)
		if gotSession != wantSession {
			t.Errorf("markerOffsets[%d]: session_id=%d, ожидалось %d", idx, gotSession, wantSession)
		}
		t.Logf("markerOffsets[%d]=%d: OK session=%d wait_type=%d wait_result=%d duration=%d signal_duration=%d wait_resource=%q ts=%d",
			idx, off, gotSession, d.WaitType, d.WaitResult, d.Duration, d.SignalDuration, d.WaitResource, d.Timestamp)
	}

	// event[0] из STP3_1.reference.xml: session=370, wait_type=123
	// (SOS_SCHEDULER_YIELD), wait_result=258, duration=1, signal_duration=1,
	// wait_resource="".
	check(0, 370, 123, 258, 1, 1, "")
	// событие с session=337, wait_type=4 (LCK_M_U), wait_result=0, duration=1,
	// signal_duration=0, wait_resource непустой (переменная длина).
	check(1, 337, 4, 0, 1, 0, "KEY: 11:72057594286505984 (4dc4d9042d04)")
	// последний найденный в файле маркер wait_completed: session=500,
	// wait_type=181, wait_result=0, duration=1, signal_duration=1,
	// wait_resource="".
	check(len(markerOffsets)-1, 500, 181, 0, 1, 1, "")
}

// xelDataStartAfterCPUID вычисляет абсолютное смещение начала timestamp
// DATA-секции по смещению НАЧАЛА TLV-записи cpu_id (package=1, actionID=28,
// байты "01 1C 00 10 ...") — тот же якорь + xelCPUIDGap, что используется
// production-декодерами (см. xel_parser.go).
func xelDataStartAfterCPUID(data []byte, cpuIDTLVOffset int) int {
	cpuIDLen := binary.LittleEndian.Uint32(data[cpuIDTLVOffset+4 : cpuIDTLVOffset+8])
	return cpuIDTLVOffset + 8 + int(cpuIDLen) + xelCPUIDGap
}

// TestXEL_DecodeSQLBatchCompletedData — регрессионный тест на формат
// DATA-секции события sql_batch_completed (см. комментарий формата в начале
// файла). Каждый образец находится по уникальному фрагменту batch_text из
// STP3_1.reference.xml, затем от этой позиции ищется предшествующий TLV
// cpu_id (package=1, actionID=28), используемый как якорь для декодирования
// по фиксированному смещению xelCPUIDGap.
func TestXEL_DecodeSQLBatchCompletedData(t *testing.T) {
	path := filepath.Join(modificationsDir(t), "STP3_1.xel")
	skipIfMissing(t, path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	cpuIDMarker := []byte{0x01, 0x1C, 0x00, 0x10}

	// findByBatchTextPrefix ищет UTF-16LE вхождение needle, начиная поиск с
	// searchFrom (для различения повторяющихся batch_text — см. кейс
	// "exec TRM_CELDCA_StcACBusinessAction", встречающийся 700 раз с разными
	// фактическими значениями полей), затем находит предшествующий cpu_id.
	findByBatchTextPrefix := func(needle string, searchFrom int) (batchTextPos, cpuIDOffset int) {
		nb := []byte{}
		for _, r := range needle {
			lo := byte(r)
			hi := byte(r >> 8)
			nb = append(nb, lo, hi)
		}
		pos := bytes.Index(data[searchFrom:], nb)
		if pos == -1 {
			t.Fatalf("не найден needle %q начиная с %d", needle, searchFrom)
		}
		pos += searchFrom
		cpos := bytes.LastIndex(data[max(0, pos-600):pos], cpuIDMarker)
		if cpos == -1 {
			t.Fatalf("не найден cpu_id TLV перед needle %q (позиция %d)", needle, pos)
		}
		return pos, max(0, pos-600) + cpos
	}

	check := func(needle string, searchFrom int, wantCPUTime, wantDuration, wantPhysicalReads, wantLogicalReads, wantWrites, wantSpills, wantRowCount uint64, wantResult uint8) {
		batchTextPos, cpuIDOffset := findByBatchTextPrefix(needle, searchFrom)
		d, err := decodeXELSQLBatchCompletedData(data, xelDataStartAfterCPUID(data, cpuIDOffset))
		if err != nil {
			t.Errorf("needle %q: decode error: %v", needle, err)
			return
		}
		if d.TypeTag != 0x41 {
			t.Errorf("needle %q: TypeTag=0x%X, ожидалось 0x41", needle, d.TypeTag)
		}
		if d.CPUTime != wantCPUTime {
			t.Errorf("needle %q: CPUTime=%d, ожидалось %d", needle, d.CPUTime, wantCPUTime)
		}
		if d.Duration != wantDuration {
			t.Errorf("needle %q: Duration=%d, ожидалось %d", needle, d.Duration, wantDuration)
		}
		if d.PhysicalReads != wantPhysicalReads {
			t.Errorf("needle %q: PhysicalReads=%d, ожидалось %d", needle, d.PhysicalReads, wantPhysicalReads)
		}
		if d.LogicalReads != wantLogicalReads {
			t.Errorf("needle %q: LogicalReads=%d, ожидалось %d", needle, d.LogicalReads, wantLogicalReads)
		}
		if d.Writes != wantWrites {
			t.Errorf("needle %q: Writes=%d, ожидалось %d", needle, d.Writes, wantWrites)
		}
		if d.Spills != wantSpills {
			t.Errorf("needle %q: Spills=%d, ожидалось %d", needle, d.Spills, wantSpills)
		}
		if d.RowCount != wantRowCount {
			t.Errorf("needle %q: RowCount=%d, ожидалось %d", needle, d.RowCount, wantRowCount)
		}
		if d.Result != wantResult {
			t.Errorf("needle %q: Result=%d, ожидалось %d", needle, d.Result, wantResult)
		}
		if !strings.Contains(d.BatchText, needle) {
			t.Errorf("needle %q: BatchText не содержит ожидаемый фрагмент, получено %q", needle, d.BatchText[:min(60, len(d.BatchText))])
		}
		t.Logf("needle=%q batchTextPos=%d: OK cpu_time=%d duration=%d physical_reads=%d logical_reads=%d writes=%d spills=%d row_count=%d result=%d ts=%d",
			needle, batchTextPos, d.CPUTime, d.Duration, d.PhysicalReads, d.LogicalReads, d.Writes, d.Spills, d.RowCount, d.Result, d.Timestamp)
	}

	// session=163, из STP3_1.reference.xml (первое sql_batch_completed).
	check("declare @RetVal int,", 0, 47000, 1050633, 0, 442, 5, 0, 28, 0)
	// session=639.
	check("declare @RetVal          DSINT_KEY", 0, 125000, 119192, 0, 12794, 1, 0, 369, 0)
	// session=370: batch_text "exec TRM_CELDCA_StcACBusinessAction" повторяется
	// 700 раз в файле с разными значениями полей — берём вхождение СО ВТОРОЙ
	// позиции поиска (после первого, не соответствующего этим ожиданиям).
	firstPos, _ := findByBatchTextPrefix("exec TRM_CELDCA_StcACBusinessAction", 0)
	check("exec TRM_CELDCA_StcACBusinessAction", firstPos+1, 2969000, 3193220, 3528, 2291813, 197, 3528, 12313, 0)
}

// TestXEL_DecodeSPStatementCompletedData — регрессионный тест на формат
// DATA-секции события sp_statement_completed (см. комментарий формата в
// начале файла и у decodeXELSPStatementCompletedData). Каждый образец
// находится по уникальной числовой последовательности duration+cpu_time+
// +physical_reads+logical_reads (u64 LE подряд) — надёжнее текстового
// needle, так как statement/object_name повторяются на каждом вызове
// одной и той же процедуры, а эта числовая комбинация уникальна в файле.
//
// Образец 1 (event из STP3_1.reference.xml, object_id=709039668,
// object_name=FCD_Calc_FindAccount): physical_reads=7 != spills=0 —
// разрешает неоднозначность позиций physical_reads/spills.
// Образец 2 (object_id=772375927, object_name=API_SM_FindListPrtByObjID):
// nest_level=4 != last_row_count=2845 — разрешает неоднозначность позиций
// nest_level/last_row_count.
func TestXEL_DecodeSPStatementCompletedData(t *testing.T) {
	path := filepath.Join(modificationsDir(t), "STP3_1.xel")
	skipIfMissing(t, path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	u64le := func(v uint64) []byte {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, v)
		return b
	}
	findP := func(duration, cpuTime, physicalReads, logicalReads uint64) int {
		needle := append(append(append(u64le(duration), u64le(cpuTime)...), u64le(physicalReads)...), u64le(logicalReads)...)
		pos := bytes.Index(data, needle)
		if pos == -1 {
			t.Fatalf("не найдена уникальная последовательность duration=%d cpu_time=%d physical_reads=%d logical_reads=%d", duration, cpuTime, physicalReads, logicalReads)
		}
		return pos - 22 // смещение до начала timestamp (rel10 = duration start, rel = p+12)
	}

	check := func(label string, p int, wantObjectID uint32, wantObjectType uint16, wantDuration, wantCPUTime, wantPhysicalReads, wantLogicalReads, wantWrites, wantSpills, wantRowCount, wantLastRowCount uint64, wantNestLevel uint16, wantLineNumber, wantOffset uint32, wantOffsetEnd int32, wantObjectName string) {
		d, err := decodeXELSPStatementCompletedData(data, p)
		if err != nil {
			t.Errorf("%s: decode error: %v", label, err)
			return
		}
		if d.SourceDatabaseID != 11 {
			t.Errorf("%s: SourceDatabaseID=%d, ожидалось 11", label, d.SourceDatabaseID)
		}
		if d.ObjectID != wantObjectID {
			t.Errorf("%s: ObjectID=%d, ожидалось %d", label, d.ObjectID, wantObjectID)
		}
		if d.ObjectType != wantObjectType {
			t.Errorf("%s: ObjectType=%d, ожидалось %d", label, d.ObjectType, wantObjectType)
		}
		if d.Duration != wantDuration {
			t.Errorf("%s: Duration=%d, ожидалось %d", label, d.Duration, wantDuration)
		}
		if d.CPUTime != wantCPUTime {
			t.Errorf("%s: CPUTime=%d, ожидалось %d", label, d.CPUTime, wantCPUTime)
		}
		if d.PhysicalReads != wantPhysicalReads {
			t.Errorf("%s: PhysicalReads=%d, ожидалось %d", label, d.PhysicalReads, wantPhysicalReads)
		}
		if d.LogicalReads != wantLogicalReads {
			t.Errorf("%s: LogicalReads=%d, ожидалось %d", label, d.LogicalReads, wantLogicalReads)
		}
		if d.Writes != wantWrites {
			t.Errorf("%s: Writes=%d, ожидалось %d", label, d.Writes, wantWrites)
		}
		if d.Spills != wantSpills {
			t.Errorf("%s: Spills=%d, ожидалось %d", label, d.Spills, wantSpills)
		}
		if d.RowCount != wantRowCount {
			t.Errorf("%s: RowCount=%d, ожидалось %d", label, d.RowCount, wantRowCount)
		}
		if d.LastRowCount != wantLastRowCount {
			t.Errorf("%s: LastRowCount=%d, ожидалось %d", label, d.LastRowCount, wantLastRowCount)
		}
		if d.NestLevel != wantNestLevel {
			t.Errorf("%s: NestLevel=%d, ожидалось %d", label, d.NestLevel, wantNestLevel)
		}
		if d.LineNumber != wantLineNumber {
			t.Errorf("%s: LineNumber=%d, ожидалось %d", label, d.LineNumber, wantLineNumber)
		}
		if d.Offset != wantOffset {
			t.Errorf("%s: Offset=%d, ожидалось %d", label, d.Offset, wantOffset)
		}
		if d.OffsetEnd != wantOffsetEnd {
			t.Errorf("%s: OffsetEnd=%d, ожидалось %d", label, d.OffsetEnd, wantOffsetEnd)
		}
		if d.ObjectName != wantObjectName {
			t.Errorf("%s: ObjectName=%q, ожидалось %q", label, d.ObjectName, wantObjectName)
		}
		if d.TotalLen != 104+d.ObjectNameLen+d.StatementLen {
			t.Errorf("%s: TotalLen=%d, ожидалось 104+ObjectNameLen(%d)+StatementLen(%d)=%d", label, d.TotalLen, d.ObjectNameLen, d.StatementLen, 104+d.ObjectNameLen+d.StatementLen)
		}
		if d.Statement == "" {
			t.Errorf("%s: Statement пуст", label)
		}
		t.Logf("%s: OK object_id=%d object_type=%d duration=%d cpu_time=%d physical_reads=%d logical_reads=%d writes=%d spills=%d row_count=%d last_row_count=%d nest_level=%d line_number=%d offset=%d offset_end=%d object_name=%q",
			label, d.ObjectID, d.ObjectType, d.Duration, d.CPUTime, d.PhysicalReads, d.LogicalReads, d.Writes, d.Spills, d.RowCount, d.LastRowCount, d.NestLevel, d.LineNumber, d.Offset, d.OffsetEnd, d.ObjectName)
	}

	p1 := findP(120957, 109000, 7, 75211)
	check("physical_reads!=spills sample", p1, 709039668, 8272, 120957, 109000, 7, 75211, 147, 0, 3492, 1, 6, 395, 43840, 45080, "MassAccrual_Find_Account")

	p2 := findP(107094, 109000, 0, 78492)
	check("nest_level!=last_row_count sample", p2, 772375927, 8272, 107094, 109000, 0, 78492, 85, 0, 2845, 2845, 4, 700, 76046, 82674, "API_SM_FindListPrtByObjID")
}

// Найдено и подтверждено побайтово (файловый заголовок и структура
// событийного потока STP3_1.xel):
//
//   - Файловый заголовок (offset 0-23): 4 байта неизвестной сигнатуры/версии
//     ("5A 37 AB EF"), 4 байта ("0A 00 00 02" — возможно версия формата),
//     затем 16-байтовый GUID файла ("CC 47 E3 19 FC 21 CB 41 BE C9 C1 8D
//     D7 95 5A E9" — стандартная раскладка Windows GUID, 4-2-2-8 байт).
//     Далее (offset 24-511) — нули (резерв/padding до начала
//     schema/dictionary блока).
//   - Schema/dictionary блок начинается ~offset 512 и продолжается до конца
//     области с текстовыми именами (в этом файле — примерно до offset
//     69600, где начинается собственно поток событий). Содержит: (1)
//     map-словари вида [int32 ID][UTF-16LE имя]\0 для числовых кодов вида
//     wait_type (подтверждено на SOS_SCHEDULER_YIELD и др.), и (2) таблицу
//     деклараций имён event/action/field, каждое из которых встречается
//     РОВНО ОДИН РАЗ (напр. "wait_completed", "session_id",
//     "database_name") в окружении небольших TLV-подобных префиксов/
//     суффиксов (числа вида 0x50,0x24,0x1C и т.п. — визуально похожи на
//     флаги/размеры полей, но точная запись НЕ раскодирована формально —
//     этого не требуется для Phase 1-2, так как имена событий/action для
//     сопоставления берутся из внешнего CSV, а не из этого словаря;
//     сам механизм "именование через словарь, не inline" уже подтверждён
//     и достаточен для критерия готовности Phase 0).
//   - Внутри dictionary-блока (offset ~592-639) — единственный экземпляр
//     калибровочных констант timestamp (offset 620/628, см.
//     xelBaseFileTimeOffset/xelFreqOffset) РЯДОМ с 16-байтовым GUID
//     ("8B 26 DA DA 47 7C 27 47 B3 B6 58 AF 6E 47 C7 05") — это GUID
//     сессии трассировки (отличается от файлового GUID из заголовка).
//   - ВАЖНО: у событий wait_completed НЕТ отдельного per-event заголовка —
//     проверено сравнением байт перед 1-м и 2-м 12-байтовым маркером
//     wait_completed: перед 2-м маркером идут байты ACTION-секции
//     ПРЕДЫДУЩЕГО события (client_hostname/client_app_name/cpu_id TLV),
//     то есть события пакуются впритык одно за другим без какого-либо
//     промежуточного заголовка. Область в 96 байт перед ПЕРВЫМ маркером
//     (offset 69613-69708: нули, сигнатура "FF FF FF FF", байт 0x0A,
//     константа "FC 1F 00 00"=8188, затем ПОВТОР того же сессионного GUID
//     что и у offset 594, и снова нули) — это ОДНОРАЗОВАЯ граница между
//     концом dictionary-блока и началом потока событий, а НЕ структура,
//     повторяющаяся перед каждым событием. Значение 8188 — вероятно,
//     размер предшествующего dictionary-блока в байтах либо смещение
//     начала событийного потока (точная семантика не выяснена, но не
//     требуется для парсинга: поток событий распознаётся по типоспецифичным
//     якорям — 12-байтовому маркеру wait_completed, TLV cpu_id для
//     sql_batch/sql_statement/sp_statement_completed — независимо от этой
//     границы).

// TestXEL_DecodeSQLStatementCompletedData — регрессионный тест на формат
// DATA-секции события sql_statement_completed (см. комментарий формата в
// начале файла). Каждый образец находится по уникальному фрагменту
// statement из STP3_1.reference.xml, затем от этой позиции ищется
// предшествующий TLV cpu_id (package=1, actionID=28) — тот же приём, что и
// в TestXEL_DecodeSQLBatchCompletedData.
func TestXEL_DecodeSQLStatementCompletedData(t *testing.T) {
	path := filepath.Join(modificationsDir(t), "STP3_1.xel")
	skipIfMissing(t, path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	cpuIDMarker := []byte{0x01, 0x1C, 0x00, 0x10}

	findByStatementPrefix := func(needle string) (statementPos, cpuIDOffset int) {
		nb := []byte{}
		for _, r := range needle {
			nb = append(nb, byte(r), byte(r>>8))
		}
		pos := bytes.Index(data, nb)
		if pos == -1 {
			t.Fatalf("не найден needle %q", needle)
		}
		cpos := bytes.LastIndex(data[max(0, pos-600):pos], cpuIDMarker)
		if cpos == -1 {
			t.Fatalf("не найден cpu_id TLV перед needle %q (позиция %d)", needle, pos)
		}
		return pos, max(0, pos-600) + cpos
	}

	check := func(needle string, wantDuration, wantCPUTime, wantPhysicalReads, wantLogicalReads, wantWrites, wantSpills, wantRowCount, wantLastRowCount uint64, wantLineNumber, wantOffset uint32, wantOffsetEnd int32) {
		_, cpuIDOffset := findByStatementPrefix(needle)
		d, err := decodeXELSQLStatementCompletedData(data, xelDataStartAfterCPUID(data, cpuIDOffset))
		if err != nil {
			t.Errorf("needle %q: decode error: %v", needle, err)
			return
		}
		if d.TypeTag != 0x5C {
			t.Errorf("needle %q: TypeTag=0x%X, ожидалось 0x5C", needle, d.TypeTag)
		}
		if d.Duration != wantDuration {
			t.Errorf("needle %q: Duration=%d, ожидалось %d", needle, d.Duration, wantDuration)
		}
		if d.CPUTime != wantCPUTime {
			t.Errorf("needle %q: CPUTime=%d, ожидалось %d", needle, d.CPUTime, wantCPUTime)
		}
		if d.PhysicalReads != wantPhysicalReads {
			t.Errorf("needle %q: PhysicalReads=%d, ожидалось %d", needle, d.PhysicalReads, wantPhysicalReads)
		}
		if d.LogicalReads != wantLogicalReads {
			t.Errorf("needle %q: LogicalReads=%d, ожидалось %d", needle, d.LogicalReads, wantLogicalReads)
		}
		if d.Writes != wantWrites {
			t.Errorf("needle %q: Writes=%d, ожидалось %d", needle, d.Writes, wantWrites)
		}
		if d.Spills != wantSpills {
			t.Errorf("needle %q: Spills=%d, ожидалось %d", needle, d.Spills, wantSpills)
		}
		if d.RowCount != wantRowCount {
			t.Errorf("needle %q: RowCount=%d, ожидалось %d", needle, d.RowCount, wantRowCount)
		}
		if d.LastRowCount != wantLastRowCount {
			t.Errorf("needle %q: LastRowCount=%d, ожидалось %d", needle, d.LastRowCount, wantLastRowCount)
		}
		if d.LineNumber != wantLineNumber {
			t.Errorf("needle %q: LineNumber=%d, ожидалось %d", needle, d.LineNumber, wantLineNumber)
		}
		if d.Offset != wantOffset {
			t.Errorf("needle %q: Offset=%d, ожидалось %d", needle, d.Offset, wantOffset)
		}
		if d.OffsetEnd != wantOffsetEnd {
			t.Errorf("needle %q: OffsetEnd=%d, ожидалось %d", needle, d.OffsetEnd, wantOffsetEnd)
		}
		if d.TotalLenRepeat != d.TotalLen {
			t.Errorf("needle %q: TotalLenRepeat=%d, ожидалось совпадение с TotalLen=%d", needle, d.TotalLenRepeat, d.TotalLen)
		}
		if !strings.Contains(d.Statement, needle) {
			t.Errorf("needle %q: Statement не содержит ожидаемый фрагмент, получено %q", needle, d.Statement[:min(60, len(d.Statement))])
		}
		t.Logf("needle=%q: OK duration=%d cpu_time=%d physical_reads=%d logical_reads=%d writes=%d spills=%d row_count=%d last_row_count=%d line_number=%d offset=%d offset_end=%d ts=%d",
			needle, d.Duration, d.CPUTime, d.PhysicalReads, d.LogicalReads, d.Writes, d.Spills, d.RowCount, d.LastRowCount, d.LineNumber, d.Offset, d.OffsetEnd, d.Timestamp)
	}

	// session=163: offset_end=-1 (edge case — "до конца батча").
	check("waitfor delay '00:00:01.000'", 1015683, 0, 0, 0, 0, 0, 0, 1, 10, 678, -1)
	// session=370: большие значения, offset_end=834 (обычный случай).
	check("exec TRM_CELDCA_StcACBusinessAction", 3193114, 2969000, 3528, 2291813, 197, 3528, 12313, 1, 1, 0, 834)
}
