package rti

import (
	"regexp"
	"strconv"
	"strings"
)

// Regex-паттерны для разбора клиентского (толстый клиент d5nt) трейс-лога.
// Все статические регулярные выражения (не зависящие от параметров) вынесены
// в package-level var-блок и компилируются один раз при инициализации пакета.
var (
	reClientHeader = regexp.MustCompile(
		`^(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}:\d{2}\.\d{3})\t(\w+)\t([\w.]+)\t([^\t]*)\t([^\t]*)\t(\d+)\t(\d+)\t\t(-?\d+)\t(-?\d+)\s*$`)

	reBPLSeparator   = regexp.MustCompile(`^=+\s*$`)
	reBPLColumnSplit = regexp.MustCompile(`\s{2,}`)

	reNewConnectionSPID = regexp.MustCompile(`^---\s*New Connection\s*:\s*SPID\s*=\s*(\d+)`)
	reConnServer        = regexp.MustCompile(`^---\s*Server\s*:\s*(.*?)\.?\s*$`)
	reConnDatabase      = regexp.MustCompile(`^---\s*Database\s*:\s*(.*?)\.?\s*$`)
	reConnUser          = regexp.MustCompile(`^---\s*User\s*:\s*(.*?)\.?\s*$`)
	reConnAppName       = regexp.MustCompile(`^---\s*Application Name:\s*(.*?)\.?\s*$`)

	reSQLSpidLine     = regexp.MustCompile(`^SPID\s*=\s*(\d+)\s*$`)
	reSQLServerLine   = regexp.MustCompile(`^SERVER\s*=\s*'(.*)'\s*$`)
	reSQLDatabaseLine = regexp.MustCompile(`^DATABASE\s*=\s*'(.*)'\s*$`)
	reSQLPreparedLine = regexp.MustCompile(`^PREPARED\s*$`)
	reSQLStartedLine  = regexp.MustCompile(`^STARTED\s*$`)
	reDurationLine    = regexp.MustCompile(`^Duration\s*=\s*'([\d.]+)'\s*$`)

	reTranCountLine = regexp.MustCompile(`^trancount\s*=\s*(\d+)\s*$`)

	reMemDelphiLine      = regexp.MustCompile(`\(Delphi manager\)\s*:\s*(\d+)`)
	reMemWinAPILine      = regexp.MustCompile(`\(WINAPI manager\)\s*:\s*(\d+)`)
	reMemDescriptorsLine = regexp.MustCompile(`:\s*(\d+);.*?:\s*(\d+);.*?:\s*(\d+)`)

	reExecCallLine  = regexp.MustCompile(`(?im)^\s*exec\s+(\w+)\s*(.*)$`)
	reExecParamLine = regexp.MustCompile(`(?i)@(\w+)\s*=\s*([^,\r\n]+)`)

	reBareEnterLine = regexp.MustCompile(`^Enter\s*$`)
	reBareExitLine  = regexp.MustCompile(`^Exit\s*$`)
)

// clientBodyState — временное состояние накопления тела текущего клиентского события
// между строкой-заголовком и следующим заголовком (клиентским или серверным).
type clientBodyState struct {
	bplSeparatorCount int
	bplDone           bool

	sqlMode      string // "", "prepare", "duration"
	sqlTextLines []string

	errorLines []string
	generic    []string
}

// classifyClientKind определяет тип (Kind) клиентского события по категории/классу/методу
// заголовка строки лога.
func classifyClientKind(category, class, method string) string {
	switch {
	case category == "Debug.d5ntsys" && strings.EqualFold(class, "TDBLFileHandler") && strings.EqualFold(method, "WriteAllBPL2Log"):
		return "bpl_list"
	case category == "Debug.d5ntsys" && strings.EqualFold(method, "Open"):
		return "recordset_open"
	case category == "Debug.d5ntsys" && strings.Contains(strings.ToLower(class), "writecurrentprocessmemoryusage"):
		return "memory_usage"
	case category == "SQL" && strings.EqualFold(class, "DSConnectorADO") && strings.EqualFold(method, "DoConnect"):
		return "connection"
	case category == "SQL":
		return "sql_block"
	case category == "SQL_TranCount":
		return "trancount"
	case strings.HasPrefix(category, "Error."):
		return "error"
	default:
		return "generic"
	}
}

// looksLikeNewRecordHeader проверяет, является ли строка началом нового события
// (клиентского или серверного) — используется для определения границы тела
// текущего накапливаемого клиентского события.
func looksLikeNewRecordHeader(line string) bool {
	return reClientHeader.MatchString(line) ||
		reTrace.MatchString(line) ||
		reBLogHeader.MatchString(line) ||
		reEnter.MatchString(line) ||
		reExit.MatchString(line) ||
		reBLogTableBound.MatchString(line)
}

// feedClientBody маршрутизирует строку тела в обработчик, соответствующий Kind события.
func feedClientBody(ev *RTIClientEvent, st *clientBodyState, line string) {
	switch ev.Kind {
	case "bpl_list":
		feedBPLBody(ev, st, line)
	case "connection":
		feedConnectionBody(ev, line)
	case "sql_block":
		feedSQLBody(ev, st, line)
	case "trancount":
		feedTranCountBody(ev, line)
	case "memory_usage":
		feedMemoryBody(ev, line)
	case "recordset_open":
		feedBareEnterExitBody(ev, line)
	case "error":
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			st.errorLines = append(st.errorLines, trimmed)
		}
	default:
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			st.generic = append(st.generic, line)
		}
	}
}

func feedBPLBody(ev *RTIClientEvent, st *clientBodyState, line string) {
	if st.bplDone {
		return
	}
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		// Пустая строка завершает список модулей только после того, как начался
		// захват строк таблицы (после второго разделителя). Пустые строки до
		// первого разделителя — это просто отступы вокруг "Текущий PID: N" и
		// заголовка списка, их нужно игнорировать, а не считать концом блока.
		if st.bplSeparatorCount >= 2 {
			st.bplDone = true
		}
		return
	}
	if reBPLSeparator.MatchString(trimmed) {
		st.bplSeparatorCount++
		return
	}
	// Строки до второго разделителя — информационные (PID процесса, заголовок
	// списка, шапка таблицы колонок) и не содержат данных о модуле.
	if st.bplSeparatorCount < 2 {
		return
	}
	cols := reBPLColumnSplit.Split(strings.TrimRight(line, " \t\r"), -1)
	filtered := make([]string, 0, len(cols))
	for _, c := range cols {
		if v := strings.TrimSpace(c); v != "" {
			filtered = append(filtered, v)
		}
	}
	if len(filtered) == 0 {
		return
	}
	mod := RTIBPLModule{File: filtered[0]}
	if len(filtered) > 1 {
		mod.Version = filtered[1]
	}
	if len(filtered) > 2 {
		mod.Title = filtered[2]
	}
	if len(filtered) > 3 {
		mod.Comment = strings.Join(filtered[3:], " ")
	}
	ev.BPL = append(ev.BPL, mod)
}

func feedConnectionBody(ev *RTIClientEvent, line string) {
	trimmed := strings.TrimSpace(line)
	if ev.Connection == nil {
		ev.Connection = &RTIConnectionInfo{}
	}
	switch {
	case reNewConnectionSPID.MatchString(trimmed):
		if m := reNewConnectionSPID.FindStringSubmatch(trimmed); m != nil {
			ev.Connection.SPID, _ = strconv.Atoi(m[1])
		}
	case reConnServer.MatchString(trimmed):
		if m := reConnServer.FindStringSubmatch(trimmed); m != nil {
			ev.Connection.Server = strings.TrimSpace(m[1])
		}
	case reConnDatabase.MatchString(trimmed):
		if m := reConnDatabase.FindStringSubmatch(trimmed); m != nil {
			ev.Connection.Database = strings.TrimSpace(m[1])
		}
	case reConnUser.MatchString(trimmed):
		if m := reConnUser.FindStringSubmatch(trimmed); m != nil {
			ev.Connection.User = strings.TrimSpace(m[1])
		}
	case reConnAppName.MatchString(trimmed):
		if m := reConnAppName.FindStringSubmatch(trimmed); m != nil {
			ev.Connection.AppName = strings.TrimSpace(m[1])
		}
	}
}

func feedSQLBody(ev *RTIClientEvent, st *clientBodyState, line string) {
	trimmed := strings.TrimSpace(line)
	if ev.SQL == nil {
		ev.SQL = &RTISQLBlock{}
	}
	switch {
	case reSQLSpidLine.MatchString(trimmed):
		if m := reSQLSpidLine.FindStringSubmatch(trimmed); m != nil {
			ev.SQL.SPID, _ = strconv.Atoi(m[1])
		}
		st.sqlMode = "prepare"
	case reSQLServerLine.MatchString(trimmed):
		if m := reSQLServerLine.FindStringSubmatch(trimmed); m != nil {
			ev.SQL.Server = m[1]
		}
	case reSQLDatabaseLine.MatchString(trimmed):
		if m := reSQLDatabaseLine.FindStringSubmatch(trimmed); m != nil {
			ev.SQL.Database = m[1]
		}
	case reSQLPreparedLine.MatchString(trimmed):
		ev.SQL.State = "PREPARED"
	case reDurationLine.MatchString(trimmed):
		if m := reDurationLine.FindStringSubmatch(trimmed); m != nil {
			ev.SQL.DurationSec, _ = strconv.ParseFloat(m[1], 64)
		}
		st.sqlMode = "duration"
	case reSQLStartedLine.MatchString(trimmed):
		ev.SQL.State = "STARTED"
	default:
		if st.sqlMode == "prepare" {
			if trimmed == "" && len(st.sqlTextLines) == 0 {
				return
			}
			st.sqlTextLines = append(st.sqlTextLines, line)
			ev.SQL.Text = strings.TrimSpace(strings.Join(st.sqlTextLines, "\n"))
		}
	}
}

func feedTranCountBody(ev *RTIClientEvent, line string) {
	trimmed := strings.TrimSpace(line)
	if m := reTranCountLine.FindStringSubmatch(trimmed); m != nil {
		v, _ := strconv.Atoi(m[1])
		ev.TranCount = &v
	}
}

func feedMemoryBody(ev *RTIClientEvent, line string) {
	if ev.Memory == nil {
		ev.Memory = &RTIMemoryUsage{}
	}
	trimmed := strings.TrimSpace(line)
	if m := reMemDelphiLine.FindStringSubmatch(trimmed); m != nil {
		ev.Memory.DelphiKB, _ = strconv.Atoi(m[1])
		return
	}
	if m := reMemWinAPILine.FindStringSubmatch(trimmed); m != nil {
		ev.Memory.WinAPIKB, _ = strconv.Atoi(m[1])
		return
	}
	if m := reMemDescriptorsLine.FindStringSubmatch(trimmed); m != nil {
		ev.Memory.Descriptors, _ = strconv.Atoi(m[1])
		ev.Memory.ObjectsUser, _ = strconv.Atoi(m[2])
		ev.Memory.ObjectsGDI, _ = strconv.Atoi(m[3])
		return
	}
}

func feedBareEnterExitBody(ev *RTIClientEvent, line string) {
	trimmed := strings.TrimSpace(line)
	if reBareEnterLine.MatchString(trimmed) {
		ev.RawBody = "Enter"
	} else if reBareExitLine.MatchString(trimmed) {
		ev.RawBody = "Exit"
	}
}

// extractExecSegment вырезает из полного текста SQL-блока фрагмент, начинающийся
// со строки "exec ProcName" и заканчивающийся первой пустой строкой (или концом
// текста) — чтобы разбор @param = value не подхватывал переменные из соседних
// операторов (declare/select), предшествующих или следующих за exec-вызовом.
func extractExecSegment(text string) string {
	loc := reExecCallLine.FindStringIndex(text)
	if loc == nil {
		return ""
	}
	rest := text[loc[0]:]
	if idx := strings.Index(rest, "\n\n"); idx >= 0 {
		return rest[:idx]
	}
	return rest
}

// finalizeClientEvent выполняет пост-обработку события после завершения накопления
// его тела (вызывается при обнаружении следующего заголовка или в конце файла).
func finalizeClientEvent(ev *RTIClientEvent, st *clientBodyState) {
	switch ev.Kind {
	case "sql_block":
		if ev.SQL == nil || ev.SQL.Text == "" {
			return
		}
		segment := extractExecSegment(ev.SQL.Text)
		if segment == "" {
			return
		}
		if m := reExecCallLine.FindStringSubmatch(segment); m != nil {
			ev.SQL.ExecProcedure = m[1]
		}
		for _, pm := range reExecParamLine.FindAllStringSubmatch(segment, -1) {
			ev.SQL.ExecParams = append(ev.SQL.ExecParams, RTIParam{
				Name:  pm[1],
				Value: strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(pm[2]), ",")),
			})
		}
	case "error":
		ev.ErrorText = strings.TrimSpace(strings.Join(st.errorLines, "\n"))
	case "generic":
		ev.RawBody = strings.TrimSpace(strings.Join(st.generic, "\n"))
	}
}

// clientSlowSQLThresholdSec — порог (в секундах) для подсчёта "медленных" клиентских
// SQL-блоков в сводке, аналогично порогу 100мс, используемому для серверных вызовов.
const clientSlowSQLThresholdSec = 0.1

// FillClientSummary заполняет клиентские поля сводки на основе загруженных событий.
// Используется как при парсинге файла, так и при загрузке сессии из БД.
func FillClientSummary(s *RTISummary, events []*RTIClientEvent) {
	s.ClientEventsCount = len(events)
	s.ClientErrorsCount = countClientErrors(events)
	s.ClientSlowSQLCount = countClientSlowSQL(events, clientSlowSQLThresholdSec)
	s.TopSlowClientSQL = topSlowClientSQLEvents(events, 10)
}

func countClientErrors(events []*RTIClientEvent) int {
	n := 0
	for _, e := range events {
		if e.Kind == "error" && e.ErrorText != "" {
			n++
		}
	}
	return n
}

func countClientSlowSQL(events []*RTIClientEvent, thresholdSec float64) int {
	n := 0
	for _, e := range events {
		if e.Kind == "sql_block" && e.SQL != nil && e.SQL.DurationSec >= thresholdSec {
			n++
		}
	}
	return n
}

func topSlowClientSQLEvents(events []*RTIClientEvent, n int) []RTIClientEvent {
	slow := make([]*RTIClientEvent, 0, len(events))
	for _, e := range events {
		if e.Kind == "sql_block" && e.SQL != nil && e.SQL.DurationSec > 0 {
			slow = append(slow, e)
		}
	}
	sortClientEventsByDuration(slow)
	if n > len(slow) {
		n = len(slow)
	}
	result := make([]RTIClientEvent, 0, n)
	for i := 0; i < n; i++ {
		result = append(result, *slow[i])
	}
	return result
}

func sortClientEventsByDuration(events []*RTIClientEvent) {
	for i := 1; i < len(events); i++ {
		for j := i; j > 0 && events[j-1].SQL.DurationSec < events[j].SQL.DurationSec; j-- {
			events[j-1], events[j] = events[j], events[j-1]
		}
	}
}
