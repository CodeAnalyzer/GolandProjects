package rti

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/codebase/internal/encoding"
	"golang.org/x/text/transform"
)

// Пакетные переменные для параметризации из конфига.
var (
	rtiSlowThresholdMs = 100
	rtiTopSlowCount    = 10
)

// SetSlowThresholdMs устанавливает порог медленности серверных вызовов (мс).
func SetSlowThresholdMs(ms int) {
	if ms > 0 {
		rtiSlowThresholdMs = ms
	}
}

// GetSlowThresholdMs возвращает текущий порог медленности серверных вызовов (мс).
func GetSlowThresholdMs() int {
	return rtiSlowThresholdMs
}

// SetTopSlowCount устанавливает количество Top-N медленных вызовов в summary.
func SetTopSlowCount(n int) {
	if n > 0 {
		rtiTopSlowCount = n
	}
}

// Regex patterns для распознавания строк RTI-лога
var (
	reEnter = regexp.MustCompile(
		`^Enter\s+(\w+)\s+@@TranCount\s*=\s*(\d+)\s+@@NestLevel\s*=\s*(\d+)\s+@@DsSysModuleID\s*=\s*(\d+)`)

	reExit = regexp.MustCompile(
		`^Exit\s+(\w+)\s+@@TranCount\s*=\s*(\d+)\s+@@NestLevel\s*=\s*(\d+)@BeginCnt\s*=\s*(\d+)\s+@@DsSysModuleID\s*=\s*(\d+)`)

	reTrace = regexp.MustCompile(
		`^(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}:\d{2}\.\d{3})\tINFO\tTrace\.Server\.(Proc|Trace)\t\t(\w+)\t(\d+)\t(\d+)\t\t(\d+)\t(\d+)`)

	reRetVal     = regexp.MustCompile(`^RetVal\s*=\s*(-?\d+)#(.*)`)
	reElapsed    = regexp.MustCompile(`^Elapsed,\s*ms:\s*(\d+)`)
	reReturn     = regexp.MustCompile(`^Return\s+(-?\d+)`)
	reParam      = regexp.MustCompile(`^@(\w+)\s*:\s*(\w+)\s+=\s*(.*)$`)
	reBLogParam  = regexp.MustCompile(`^BLogParam:@(\w+)\s*:\s*(\w+)\s+=\s*(.*)$`)
	reCheckpoint = regexp.MustCompile(`^(\w+)_Begin_(\d+)$`)

	reBLogHeader      = regexp.MustCompile(`^(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}:\d{2}\.\d{3})\tINFO\tTrace\.Server\.BusinessLog\t\t\t(\d+)\t(\d+)\t\t(\d+)\t(\d+)`)
	reBLogEnter       = regexp.MustCompile(`^Enter\s+@@TranCount`)
	reBLogExit        = regexp.MustCompile(`^Exit\s+@@TranCount`)
	reBLogTableBound  = regexp.MustCompile(`^BusinessLog:\s+Data\s+from\s+(\S+)\s+(begin|end)`)
	reBLogTableHeader = regexp.MustCompile(`^Table\s+header\s+(.+)`)
)

type stackEntry struct {
	call *RTICall
}

// ParseFile парсит RTI-лог и возвращает структурированный результат.
func ParseFile(path string) (*RTIParseResult, error) {
	// Читаем весь файл для определения кодировки — RTI-логи могут начинаться
	// с ASCII-трассировки, а кириллица появляться позже в RetValContext
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read RTI file: %w", err)
	}
	enc := encoding.DetectFromBytes(data)

	fi, _ := os.Stat(path)
	fileSize := int64(0)
	if fi != nil {
		fileSize = fi.Size()
	}

	content, err := decodeData(data, enc)
	if err != nil {
		return nil, fmt.Errorf("failed to decode RTI file: %w", err)
	}

	return parseContent(content, path, fileSize)
}

// parseContent парсит декодированный контент RTI-лога.
func parseContent(content, filePath string, fileSize int64) (*RTIParseResult, error) {
	result := &RTIParseResult{}
	result.Summary.FilePath = filePath
	result.Summary.FileSize = fileSize

	stacks := make(map[int][]*stackEntry)
	var allCalls []*RTICall
	callIDCounter := int64(0)
	currentSPID := 0
	var lastExited *RTICall

	// Client (толстый клиент d5nt) event parsing state
	var pendingClient *RTIClientEvent
	var clientBody clientBodyState
	var allClientEvents []*RTIClientEvent
	clientIDCounter := int64(0)

	// BusinessLog block state
	var (
		pendingBLog        bool
		pendingBLogTS      time.Time
		pendingBLogSPID    int
		pendingBLogSrcLine int
		pendingBLogIsEnter *bool
	)

	// Checkpoint timestamp state
	var (
		pendingTraceTS  time.Time
		hasPendingTrace bool
	)

	// Table capture state
	var (
		captureTable bool
		currentTable *RTIBLogTable
	)

	scanner := bufio.NewScanner(strings.NewReader(content))
	buf := make([]byte, 0, 1<<20)
	scanner.Buffer(buf, 1<<20)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Клиентское событие: если сейчас накапливается тело клиентского события
		// (pendingClient != nil), и текущая строка не похожа на начало нового
		// события (клиентского или серверного) — это строка тела, передаём её
		// в соответствующий обработчик и переходим к следующей строке.
		if pendingClient != nil {
			if !looksLikeNewRecordHeader(line) {
				feedClientBody(pendingClient, &clientBody, line)
				continue
			}
			// Текущая строка — начало нового события: завершаем накопление
			// текущего клиентского события и даём этой строке пройти через
			// обычную цепочку проверок ниже (она может оказаться серверным
			// или новым клиентским заголовком).
			finalizeClientEvent(pendingClient, &clientBody)
			allClientEvents = append(allClientEvents, pendingClient)
			pendingClient = nil
			clientBody = clientBodyState{}
		}

		// BusinessLog table boundary (begin / end)
		if m := reBLogTableBound.FindStringSubmatch(line); m != nil {
			tableName := m[1]
			kind := strings.ToLower(m[2])
			if kind == "begin" {
				captureTable = true
				currentTable = &RTIBLogTable{
					TableName: tableName,
					EnterLine: lineNum,
				}
			} else {
				if captureTable && currentTable != nil {
					currentTable.RowCount = len(currentTable.Rows)
					stack := stacks[currentSPID]
					if len(stack) > 0 {
						stack[len(stack)-1].call.BLogTables = append(
							stack[len(stack)-1].call.BLogTables, *currentTable)
					}
				}
				captureTable = false
				currentTable = nil
			}
			continue
		}

		// Table header / row capture
		if captureTable && currentTable != nil {
			if m := reBLogTableHeader.FindStringSubmatch(line); m != nil {
				currentTable.Columns = strings.Split(m[1], "_|_")
			} else if strings.TrimSpace(line) != "" {
				currentTable.Rows = append(currentTable.Rows, line)
			}
			continue
		}

		// Прескрининг по первому байту: направляет строку только в релевантные
		// regex-проверки, исключая 85-95% вызовов на нерелевантных строках.
		if len(line) == 0 {
			continue
		}

		switch line[0] {
		case 'E':
			// BusinessLog Enter/Exit (only when pendingBLog is active) —
			// проверяются до reEnter/reExit, т.к. "Enter @@TranCount" не матчит
			// reEnter (требует имя процедуры после Enter), но проверка дёргает
			// regex, поэтому ставим первой, чтобы сразу continue при pendingBLog.
			if pendingBLog {
				if reBLogEnter.MatchString(line) {
					t := true
					pendingBLogIsEnter = &t
					continue
				}
				if reBLogExit.MatchString(line) {
					f := false
					pendingBLogIsEnter = &f
					continue
				}
			}
			// reEnter: "Enter <ProcName> @@TranCount=..."
			if m := reEnter.FindStringSubmatch(line); m != nil {
				procName := m[1]
				tranCount, _ := strconv.Atoi(m[2])
				nestLevel, _ := strconv.Atoi(m[3])
				moduleID, _ := strconv.Atoi(m[4])

				callIDCounter++
				call := &RTICall{
					ID:         callIDCounter,
					Procedure:  procName,
					EnterLine:  lineNum,
					NestLevel:  nestLevel,
					ModuleID:   moduleID,
					TranCount:  tranCount,
					ModuleName: ModuleNameByID(moduleID),
					SPID:       currentSPID,
				}
				lastExited = nil

				stack := stacks[currentSPID]
				if len(stack) > 0 {
					parent := stack[len(stack)-1]
					if parent.call.NestLevel == nestLevel-1 {
						pid := parent.call.ID
						call.ParentID = &pid
						parent.call.Children = append(parent.call.Children, call.ID)
					}
				}

				stacks[currentSPID] = append(stacks[currentSPID], &stackEntry{call: call})
				allCalls = append(allCalls, call)
				continue
			}
			// reExit: "Exit <ProcName> @@TranCount=..."
			if m := reExit.FindStringSubmatch(line); m != nil {
				procName := m[1]
				nestLevel, _ := strconv.Atoi(m[3])
				beginCnt, _ := strconv.Atoi(m[4])

				stack := stacks[currentSPID]
				if len(stack) > 0 {
					top := stack[len(stack)-1]
					if top.call.Procedure == procName && top.call.NestLevel == nestLevel {
						top.call.ExitLine = lineNum
						top.call.BeginCnt = beginCnt
						lastExited = top.call
						stacks[currentSPID] = stack[:len(stack)-1]
					}
				}
				continue
			}
			// reElapsed: "Elapsed, ms: <N>"
			if m := reElapsed.FindStringSubmatch(line); m != nil {
				ms, _ := strconv.Atoi(m[1])
				if lastExited != nil {
					lastExited.ElapsedMs = ms
				} else {
					stack := stacks[currentSPID]
					if len(stack) > 0 {
						stack[len(stack)-1].call.ElapsedMs = ms
					}
				}
				continue
			}

		case 'R':
			// reRetVal: "RetVal = <N>#<ctx>"
			if m := reRetVal.FindStringSubmatch(line); m != nil {
				val, _ := strconv.Atoi(m[1])
				ctx := strings.TrimSpace(m[2])

				// M_LOG entry: pendingBLog active but no Enter/Exit prefix seen.
				// This is a plain log message (not a block begin/end) — skip it.
				if pendingBLog && pendingBLogIsEnter == nil {
					_ = val
					_ = ctx
					pendingBLog = false
					continue
				}

				if pendingBLog && pendingBLogIsEnter != nil {
					stack := stacks[pendingBLogSPID]
					if len(stack) > 0 {
						top := stack[len(stack)-1]
						if *pendingBLogIsEnter {
							top.call.BLogBlocks = append(top.call.BLogBlocks, RTIBLogBlock{
								BlockName: ctx,
								EnterTime: pendingBLogTS,
								EnterLine: pendingBLogSrcLine,
							})
						} else {
							for i := len(top.call.BLogBlocks) - 1; i >= 0; i-- {
								b := &top.call.BLogBlocks[i]
								if b.BlockName == ctx && b.ExitTime.IsZero() {
									b.ExitTime = pendingBLogTS
									b.ExitLine = pendingBLogSrcLine
									if !b.EnterTime.IsZero() {
										b.ElapsedMs = int(pendingBLogTS.Sub(b.EnterTime).Milliseconds())
									}
									break
								}
							}
						}
					}
					_ = val
					pendingBLog = false
					pendingBLogIsEnter = nil
					continue
				}

				stack := stacks[currentSPID]
				if len(stack) > 0 {
					top := stack[len(stack)-1]
					if top.call.RetVal == nil {
						v := val
						top.call.RetVal = &v
						top.call.RetValContext = ctx
					}
				}
				continue
			}
			// reReturn: "Return <N>"
			if m := reReturn.FindStringSubmatch(line); m != nil {
				val, _ := strconv.Atoi(m[1])
				if lastExited != nil {
					if lastExited.RetVal == nil {
						v := val
						lastExited.RetVal = &v
					}
					lastExited = nil
				} else {
					stack := stacks[currentSPID]
					if len(stack) > 0 {
						top := stack[len(stack)-1]
						if top.call.RetVal == nil {
							v := val
							top.call.RetVal = &v
						}
					}
				}
				continue
			}

		case '@':
			// reParam: "@<Name> : <Type> = <Value>"
			if m := reParam.FindStringSubmatch(line); m != nil {
				stack := stacks[currentSPID]
				if len(stack) > 0 {
					top := stack[len(stack)-1]
					top.call.Params = append(top.call.Params, RTIParam{
						Name:  m[1],
						Type:  m[2],
						Value: strings.TrimSpace(m[3]),
					})
				}
				continue
			}

		case 'B':
			// reBLogParam: "BLogParam:@<Name> : <Type> = <Value>"
			if m := reBLogParam.FindStringSubmatch(line); m != nil {
				stack := stacks[currentSPID]
				if len(stack) > 0 {
					top := stack[len(stack)-1]
					top.call.Params = append(top.call.Params, RTIParam{
						Name:  m[1],
						Type:  m[2],
						Value: strings.TrimSpace(m[3]),
					})
				}
				continue
			}

		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			// reBLogHeader: "DD.MM.YYYY ... Trace.Server.BusinessLog ..."
			if m := reBLogHeader.FindStringSubmatch(line); m != nil {
				tsStr := m[1]
				spid, _ := strconv.Atoi(m[2])
				srcLine, _ := strconv.Atoi(m[5])
				if ts, err := time.ParseInLocation("02.01.2006 15:04:05.000", tsStr, time.Local); err == nil {
					pendingBLogTS = ts
				}
				pendingBLogSPID = spid
				pendingBLogSrcLine = srcLine
				pendingBLog = true
				pendingBLogIsEnter = nil
				currentSPID = spid
				continue
			}
			// reTrace: "DD.MM.YYYY ... Trace.Server.Proc|Trace ..."
			if m := reTrace.FindStringSubmatch(line); m != nil {
				tsStr := m[1]
				traceKind := m[2]
				spid, _ := strconv.Atoi(m[4])

				if ts, err := time.ParseInLocation("02.01.2006 15:04:05.000", tsStr, time.Local); err == nil {
					stack := stacks[spid]
					if len(stack) > 0 {
						top := stack[len(stack)-1]
						if top.call.EnterTime.IsZero() {
							top.call.EnterTime = ts
						}
					}
					if traceKind == "Trace" {
						pendingTraceTS = ts
						hasPendingTrace = true
					}
				}
				currentSPID = spid
				continue
			}
			// reClientHeader: "DD.MM.YYYY ... <Level> <Category> ..."
			if m := reClientHeader.FindStringSubmatch(line); m != nil {
				tsStr := m[1]
				level := m[2]
				category := m[3]
				class := m[4]
				method := m[5]
				pid, _ := strconv.Atoi(m[6])
				seq, _ := strconv.Atoi(m[7])

				var ts time.Time
				if parsed, err := time.ParseInLocation("02.01.2006 15:04:05.000", tsStr, time.Local); err == nil {
					ts = parsed
				}

				clientIDCounter++
				pendingClient = &RTIClientEvent{
					ID:         clientIDCounter,
					Timestamp:  ts,
					Level:      level,
					Category:   category,
					ClassName:  class,
					MethodName: method,
					PID:        pid,
					SeqNo:      seq,
					Line:       lineNum,
					Kind:       classifyClientKind(category, class, method),
				}
				clientBody = clientBodyState{}
				continue
			}

		default:
			// reCheckpoint: "<ProcName>_Begin_<N>" — имя процедуры может начинаться
			// с любой заглавной буквы, не только E/R/B/T/@/digit.
			if m := reCheckpoint.FindStringSubmatch(line); m != nil {
				procName := m[1]
				checkpointNum, _ := strconv.Atoi(m[2])
				stack := stacks[currentSPID]
				if len(stack) > 0 {
					top := stack[len(stack)-1]
					cp := RTICheckpoint{
						Label:     fmt.Sprintf("%s_Begin_%d", procName, checkpointNum),
						LineNo:    lineNum,
						ElapsedMs: top.call.ElapsedMs,
					}
					if hasPendingTrace {
						cp.Timestamp = pendingTraceTS
						hasPendingTrace = false
					}
					top.call.Checkpoints = append(top.call.Checkpoints, cp)
				}
				continue
			}
		}

		// Unparsed — строки, не матчиющие ни один паттерн.
		// continue внутри switch выше пропускает эту проверку (строка обработана).
		// Сюда попадают только строки, выпавшие из switch без continue.
		if strings.TrimSpace(line) != "" {
			result.UnparsedLines++
		}
	}

	if pendingClient != nil {
		finalizeClientEvent(pendingClient, &clientBody)
		allClientEvents = append(allClientEvents, pendingClient)
	}

	LinkClientServerCalls(allCalls, allClientEvents)

	result.Calls = allCalls
	result.ClientEvents = allClientEvents
	result.Summary.TotalCalls = len(allCalls)
	result.Summary.UnparsedLines = result.UnparsedLines
	result.Summary.ErrorsCount = countErrors(allCalls)
	result.Summary.MaxNestLevel = maxNestLevel(allCalls)
	result.Summary.SlowCallsCount = countSlowCalls(allCalls, rtiSlowThresholdMs)
	result.Summary.TopSlow = topSlowCalls(allCalls, rtiTopSlowCount)
	FillClientSummary(&result.Summary, allClientEvents)

	// HRTI: авто-детект и декодирование строковых полей
	if isHRTIContent(allCalls) {
		DecodeHRTIResult(result)
	}

	return result, nil
}

func countErrors(calls []*RTICall) int {
	count := 0
	for _, c := range calls {
		if c.RetVal != nil && *c.RetVal != 0 {
			count++
		}
	}
	return count
}

func maxNestLevel(calls []*RTICall) int {
	max := 0
	for _, c := range calls {
		if c.NestLevel > max {
			max = c.NestLevel
		}
	}
	return max
}

func countSlowCalls(calls []*RTICall, thresholdMs int) int {
	count := 0
	for _, c := range calls {
		if c.ElapsedMs >= thresholdMs {
			count++
		}
	}
	return count
}

func TopSlowCallsFromLoaded(calls []*RTICall, n int) []RTICall {
	return topSlowCalls(calls, n)
}

func CountSlowCalls(calls []*RTICall, thresholdMs int) int {
	return countSlowCalls(calls, thresholdMs)
}

func topSlowCalls(calls []*RTICall, n int) []RTICall {
	sorted := make([]*RTICall, len(calls))
	copy(sorted, calls)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ElapsedMs > sorted[j].ElapsedMs
	})
	if n > len(sorted) {
		n = len(sorted)
	}
	result := make([]RTICall, 0, n)
	for i := 0; i < n; i++ {
		c := *sorted[i]
		c.BLogTables = nil
		c.BLogBlocks = nil
		result = append(result, c)
	}
	return result
}

func decodeData(data []byte, enc encoding.Encoding) (string, error) {
	if enc == encoding.UTF8 {
		return string(data), nil
	}
	decoder := encoding.GetDecoder(enc)
	if decoder == nil {
		return string(data), nil
	}
	decoded, err := io.ReadAll(transform.NewReader(bytes.NewReader(data), decoder))
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}
