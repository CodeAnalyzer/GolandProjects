package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codebase/internal/trc"
	"github.com/codebase/internal/trcsvc"
	"github.com/spf13/cobra"
)

var (
	trcOutputJSON    bool
	trcSlowThreshold int
	trcProcedure     string
	trcSessionID     int64
	trcKeepLast      int
	trcListLimit     int
	trcSPID          int
	trcMaxDepth      int
	trcTreeLimit     int
	trcLimit         int

	trcSPIDsRaw      string
	trcEventNamesRaw string
	trcAfterID       int64
	trcTimeFrom      string
	trcTimeTo        string
	trcMinDuration   int64
	trcFormat        string
	trcTop           int
	trcSortBy        string
	trcGroupBySPID   bool

	trcFocusSPID      int
	trcCompareSPIDsRaw string
	trcSpidsLimit     int
	trcSpidsSortBy    string
)

var trcCmd = &cobra.Command{
	Use:   "trc",
	Short: "Binary .trc (SQL Server Profiler) trace analyzer",
	Long: `Analyze binary SQL Server Profiler .trc trace files.

Subcommands:
  parse      - parse .trc file, save session, print summary
  summary    - print summary info
  events     - list decoded events (filters: --spid/--spids, --proc, --event-names,
               --time-from/--time-to, --min-duration-ms; pagination: --after-id)
  procedures - aggregate procedure calls (default SP:Completed; filters: --spids,
               --event-names; --top, --sort, --group-by-spid)
  compare-procedures - Top-N focus-SPID procedures vs the same procedures in peer
               SPIDs (--focus-spid, --compare-spids; --top, --event-names, --sort)
  spids      - SPID activity summary without raw events (--spids, --time-from,
               --time-to, --sort, --limit)
  tree       - print call trees grouped by SPID
  errors     - print events with non-zero Error column
  slow       - print slowest events (threshold --slow-ms)
  list       - list saved sessions
  delete     - delete a session by ID
  prune      - prune old sessions`,
}

var trcParseCmd = &cobra.Command{
	Use:   "parse <file.trc>",
	Short: "Parse .trc file and print summary",
	Args:  cobra.ExactArgs(1),
	RunE:  runTRCParse,
}

var trcSummaryCmd = &cobra.Command{
	Use:   "summary [<file.trc>]",
	Short: "Print summary info",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runTRCSummary,
}

var trcEventsCmd = &cobra.Command{
	Use:   "events [<file.trc>]",
	Short: "List decoded events",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runTRCEvents,
}

var trcProceduresCmd = &cobra.Command{
	Use:   "procedures [<file.trc>]",
	Short: "Aggregate events by procedure",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runTRCProcedures,
}

var trcCompareProceduresCmd = &cobra.Command{
	Use:   "compare-procedures [<file.trc>]",
	Short: "Compare Top-N focus-SPID procedures against peer SPIDs",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runTRCCompareProcedures,
}

var trcSpidsCmd = &cobra.Command{
	Use:   "spids [<file.trc>]",
	Short: "Summarize SPID activity without raw events",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runTRCSpids,
}

var trcTreeCmd = &cobra.Command{
	Use:   "tree [<file.trc>]",
	Short: "Print call trees grouped by SPID",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runTRCTree,
}

var trcErrorsCmd = &cobra.Command{
	Use:   "errors [<file.trc>]",
	Short: "Print events with non-zero Error column",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runTRCErrors,
}

var trcSlowCmd = &cobra.Command{
	Use:   "slow [<file.trc>]",
	Short: "Print slowest events",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runTRCSlow,
}

var trcListCmd = &cobra.Command{
	Use:   "list",
	Short: "List saved trc sessions",
	Args:  cobra.NoArgs,
	RunE:  runTRCList,
}

var trcDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a saved trc session",
	Args:  cobra.NoArgs,
	RunE:  runTRCDelete,
}

var trcPruneCmd = &cobra.Command{
	Use:   "prune",
	Short: "Prune old trc sessions",
	Args:  cobra.NoArgs,
	RunE:  runTRCPrune,
}

func trcSource(args []string) trcsvc.SessionSource {
	var filePath string
	if len(args) > 0 {
		filePath = args[0]
	}
	return trcsvc.SessionSource{SessionID: trcSessionID, FilePath: filePath}
}

func runTRCParse(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := trcsvc.ExecuteParse(ctx, db, args[0])
	if err != nil {
		return err
	}
	if trcOutputJSON {
		return printJSON(result)
	}
	fmt.Printf("TRC file: %s\n", args[0])
	fmt.Printf("Total events: %d\n", result.TotalEvents)
	if result.SessionID > 0 {
		fmt.Printf("Saved session: %d\n", result.SessionID)
	} else {
		fmt.Fprintf(os.Stderr, "Warning: database unavailable, session not saved\n")
	}
	return nil
}

func runTRCSummary(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := trcsvc.ExecuteSummary(ctx, db, trcSource(args))
	if err != nil {
		return err
	}
	if trcOutputJSON {
		return printJSON(result)
	}
	if trcSessionID > 0 {
		fmt.Printf("Session: %d\n", trcSessionID)
		if result.Session != nil {
			fmt.Printf("File: %s\n", result.Session.FilePath)
		}
	} else if len(args) > 0 {
		fmt.Printf("TRC file: %s\n", args[0])
	}
	fmt.Printf("Total events: %d\n", result.TotalEvents)
	if result.Header != nil {
		fmt.Printf("Provider: %s  Server: %s  Version: %d.%d build %d\n",
			result.Header.ProviderName, result.Header.ServerName,
			result.Header.MajorVersion, result.Header.MinorVersion, result.Header.BuildNumber)
	}
	return nil
}

// parseSPIDCSV разбирает CSV-список SPID: пустые элементы, нечисловые,
// нулевые и отрицательные значения отклоняются с именем флага и значением.
func parseSPIDCSV(raw, flagName string) ([]int, error) {
	items, err := parseStringCSV(raw, flagName)
	if err != nil {
		return nil, err
	}
	if items == nil {
		return nil, nil
	}
	out := make([]int, 0, len(items))
	for i, s := range items {
		n, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("%s[%d]: invalid SPID %q", flagName, i, s)
		}
		if n <= 0 {
			return nil, fmt.Errorf("%s[%d]: SPID must be positive, got %d", flagName, i, n)
		}
		out = append(out, n)
	}
	return out, nil
}

// parseStringCSV разбирает CSV-список непустых строк с именем флага в ошибке.
func parseStringCSV(raw, flagName string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, fmt.Errorf("%s[%d]: must be non-empty", flagName, i)
		}
		out = append(out, p)
	}
	return out, nil
}

// parseRFC3339Flag парсит временной флаг строго в RFC3339.
func parseRFC3339Flag(raw, flagName string) (*time.Time, error) {
	if raw == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, fmt.Errorf("%s (expected RFC3339): %w", flagName, err)
	}
	return &t, nil
}

// resolveTRCSPIDsFilter объединяет legacy --spid и массив --spids с запретом
// одновременной передачи.
func resolveTRCSPIDsFilter(cmd *cobra.Command, spids []int) ([]int, error) {
	if cmd.Flags().Changed("spid") && cmd.Flags().Changed("spids") {
		return nil, fmt.Errorf("--spid and --spids are mutually exclusive: use --spids")
	}
	if trcSPID != 0 {
		spids = append([]int{trcSPID}, spids...)
	}
	return spids, nil
}

func formatTRCEventsHeader(result *trcsvc.EventsResult) string {
	total := 0
	if result.TotalCount != nil {
		total = *result.TotalCount
	}
	header := fmt.Sprintf("%d event(s) returned (%d matched, %d total, limit %d):\n\n", result.ReturnedCount, result.FilteredCount, total, result.Limit)
	if result.NextAfterID > 0 {
		header += fmt.Sprintf("next page: --after-id %d\n\n", result.NextAfterID)
	}
	return header
}

func runTRCEvents(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)

	spids, err := parseSPIDCSV(trcSPIDsRaw, "--spids")
	if err != nil {
		return err
	}
	spids, err = resolveTRCSPIDsFilter(cmd, spids)
	if err != nil {
		return err
	}
	eventNames, err := parseStringCSV(trcEventNamesRaw, "--event-names")
	if err != nil {
		return err
	}
	timeFrom, err := parseRFC3339Flag(trcTimeFrom, "--time-from")
	if err != nil {
		return err
	}
	timeTo, err := parseRFC3339Flag(trcTimeTo, "--time-to")
	if err != nil {
		return err
	}
	var minDuration *int64
	if cmd.Flags().Changed("min-duration-ms") {
		minDuration = &trcMinDuration
	}
	var afterID *int64
	if cmd.Flags().Changed("after-id") {
		afterID = &trcAfterID
	}

	result, err := trcsvc.ExecuteEvents(ctx, db, trcsvc.EventsParams{
		Source:        trcSource(args),
		SPIDs:         spids,
		Procedure:     trcProcedure,
		EventNames:    eventNames,
		TimeFrom:      timeFrom,
		TimeTo:        timeTo,
		MinDurationMs: minDuration,
		AfterID:       afterID,
		Format:        trcFormat,
		Limit:         applyQueryLimit(trcLimit),
	})
	if err != nil {
		return err
	}
	if trcOutputJSON {
		return printJSON(result)
	}
	fmt.Print(formatTRCEventsHeader(result))
	if result.Short {
		for _, v := range result.Views {
			printEventViewLine(v)
		}
	} else {
		for _, ev := range result.Events {
			printEventLine(ev)
		}
	}
	return nil
}

func printEventLine(ev trc.TRCEvent) {
	proc := ""
	if ev.Procedure != "" {
		proc = " exec " + ev.Procedure
	}
	duration := ""
	if ev.DurationMs > 0 {
		duration = fmt.Sprintf(" [%dms]", ev.DurationMs)
	}
	spid := ""
	if s, ok := ev.Columns[12].(int32); ok {
		spid = fmt.Sprintf(" SPID=%d", s)
	}
	fmt.Printf("  %s%s%s%s\n", ev.EventName, proc, duration, spid)
}

func printEventViewLine(v trcsvc.TRCEventView) {
	proc := ""
	if v.Procedure != "" {
		proc = " exec " + v.Procedure
	}
	duration := ""
	if v.DurationMs > 0 {
		duration = fmt.Sprintf(" [%dms]", v.DurationMs)
	}
	spid := ""
	if v.SPID != 0 {
		spid = fmt.Sprintf(" SPID=%d", v.SPID)
	}
	fmt.Printf("  %s%s%s%s\n", v.EventName, proc, duration, spid)
}

func runTRCProcedures(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)

	spids, err := parseSPIDCSV(trcSPIDsRaw, "--spids")
	if err != nil {
		return err
	}
	eventNames, err := parseStringCSV(trcEventNamesRaw, "--event-names")
	if err != nil {
		return err
	}
	result, err := trcsvc.ExecuteProcedures(ctx, db, trcsvc.ProceduresParams{
		Source:      trcSource(args),
		SPIDs:       spids,
		EventNames:  eventNames,
		Top:         trcTop,
		SortBy:      trcSortBy,
		GroupBySPID: trcGroupBySPID,
	})
	if err != nil {
		return err
	}
	if trcOutputJSON {
		return printJSON(result)
	}
	fmt.Printf("%d procedure(s):\n\n", result.Count)
	for _, a := range result.Procedures {
		spid := ""
		if a.SPID != 0 {
			spid = fmt.Sprintf(" SPID=%-5d", a.SPID)
		}
		fmt.Printf("  %s%-40s count=%-5d total=%dms min=%dms max=%dms avg=%.1fms",
			spid, a.Procedure, a.Count, a.TotalMs, a.MinMs, a.MaxMs, a.AvgMs)
		if a.SourceFile != "" {
			fmt.Printf("  → %s", a.SourceFile)
		}
		fmt.Println()
	}
	return nil
}

func runTRCCompareProcedures(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)

	compareSPIDs, err := parseSPIDCSV(trcCompareSPIDsRaw, "--compare-spids")
	if err != nil {
		return err
	}
	eventNames, err := parseStringCSV(trcEventNamesRaw, "--event-names")
	if err != nil {
		return err
	}
	result, err := trcsvc.ExecuteCompareProcedures(ctx, db, trcsvc.CompareParams{
		Source:       trcSource(args),
		FocusSPID:    trcFocusSPID,
		CompareSPIDs: compareSPIDs,
		EventNames:   eventNames,
		Top:          trcTop,
		SortBy:       trcSortBy,
	})
	if err != nil {
		return err
	}
	if trcOutputJSON {
		return printJSON(result)
	}
	fmt.Printf("focus SPID %d vs peers %v, top %d by %s: %d procedure(s)\n\n",
		result.FocusSPID, result.CompareSPIDs, result.Top, result.SortBy, len(result.Procedures))
	for _, p := range result.Procedures {
		fmt.Printf("%3d. %-45s focus: count=%-5d total=%dms avg=%.1fms\n",
			p.Rank, p.Procedure, p.Focus.Count, p.Focus.TotalMs, p.Focus.AvgMs)
		for _, peer := range p.Peers {
			peerLine := fmt.Sprintf("     peer %-6d count=%-5d total=%dms", peer.SPID, peer.Count, peer.TotalMs)
			if peer.AvgMs != nil {
				peerLine += fmt.Sprintf(" avg=%.1fms", *peer.AvgMs)
			} else {
				peerLine += " avg=null"
			}
			fmt.Println(peerLine)
		}
		if p.Ratios.AvgVsPeers != nil {
			fmt.Printf("     peer_combined: count=%d avg=%.1fms | ratios: avg_vs_peers=%.4f",
				p.PeerCombined.Count, derefFloat(p.PeerCombined.AvgMs), *p.Ratios.AvgVsPeers)
			if p.Ratios.CountVsPeers != nil {
				fmt.Printf(" count_vs_peers=%.4f", *p.Ratios.CountVsPeers)
			}
			fmt.Println()
		} else {
			fmt.Printf("     peer_combined: count=%d | ratios: null (no peer calls)\n", p.PeerCombined.Count)
		}
	}
	for _, w := range result.Warnings {
		fmt.Printf("warning: %s\n", w)
	}
	return nil
}

func derefFloat(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

func runTRCSpids(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)

	spids, err := parseSPIDCSV(trcSPIDsRaw, "--spids")
	if err != nil {
		return err
	}
	timeFrom, err := parseRFC3339Flag(trcTimeFrom, "--time-from")
	if err != nil {
		return err
	}
	timeTo, err := parseRFC3339Flag(trcTimeTo, "--time-to")
	if err != nil {
		return err
	}
	result, err := trcsvc.ExecuteSpids(ctx, db, trcsvc.SpidsParams{
		Source:   trcSource(args),
		SPIDs:    spids,
		TimeFrom: timeFrom,
		TimeTo:   timeTo,
		SortBy:   trcSpidsSortBy,
		Limit:    applyQueryLimit(trcSpidsLimit),
	})
	if err != nil {
		return err
	}
	if trcOutputJSON {
		return printJSON(result)
	}
	fmt.Printf("%d spid(s):\n\n", len(result.Spids))
	for _, s := range result.Spids {
		fmt.Printf("  SPID=%-6d events=%-6d SP=%-5d RPC=%-5d batch=%-5d errors=%-4d max=%dms",
			s.SPID, s.EventCount, s.SPCompletedCount, s.RPCCompletedCount, s.BatchCompletedCount, s.ErrorCount, s.MaxDurationMs)
		if s.FirstTime != nil {
			fmt.Printf("  %s → %s", s.FirstTime.Format("15:04:05"), s.LastTime.Format("15:04:05"))
		}
		if s.ApplicationName != "" || s.LoginName != "" || s.HostName != "" {
			fmt.Printf("  [%s|%s@%s]", s.ApplicationName, s.LoginName, s.HostName)
		}
		fmt.Println()
	}
	for _, w := range result.Warnings {
		fmt.Printf("warning: %s\n", w)
	}
	return nil
}

func runTRCTree(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := trcsvc.ExecuteTree(ctx, db, trcsvc.TreeParams{
		Source:    trcSource(args),
		SPID:      trcSPID,
		MaxDepth:  trcMaxDepth,
		Limit:     trcTreeLimit,
		Procedure: trcProcedure,
	})
	if err != nil {
		return err
	}
	if trcOutputJSON {
		return printJSON(result)
	}
	if len(result.Trees) == 0 {
		fmt.Println("No events found.")
		return nil
	}
	fmt.Print(trc.FormatTrees(result.Trees))
	return nil
}

func runTRCErrors(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := trcsvc.ExecuteErrors(ctx, db, trcsvc.ErrorsParams{
		Source: trcSource(args),
		Limit:  applyQueryLimit(trcLimit),
	})
	if err != nil {
		return err
	}
	if trcOutputJSON {
		return printJSON(result)
	}
	if result.Count == 0 {
		fmt.Println("No errors found.")
		return nil
	}
	fmt.Printf("Found %d error event(s) (limit %d):\n\n", result.Count, result.Limit)
	for _, ev := range result.Events {
		printEventLine(ev)
	}
	return nil
}

func runTRCSlow(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := trcsvc.ExecuteSlow(ctx, db, trcsvc.SlowParams{
		Source:      trcSource(args),
		ThresholdMs: trcSlowThreshold,
		Limit:       applyQueryLimit(trcLimit),
	})
	if err != nil {
		return err
	}
	if trcOutputJSON {
		return printJSON(result)
	}
	if result.Count == 0 {
		fmt.Printf("No events slower than %dms found.\n", result.Threshold)
		return nil
	}
	fmt.Printf("Found %d slow event(s) (>= %dms, limit %d):\n\n", result.Count, result.Threshold, result.Limit)
	for _, ev := range result.Events {
		printEventLine(ev)
	}
	return nil
}

func runTRCList(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := trcsvc.ExecuteList(ctx, db, trcListLimit)
	if err != nil {
		return err
	}
	if len(result.Sessions) == 0 {
		fmt.Println("No saved sessions.")
		return nil
	}
	if trcOutputJSON {
		return printJSON(result)
	}
	fmt.Printf("%d session(s):\n\n", len(result.Sessions))
	for _, s := range result.Sessions {
		fmt.Printf("  %d  %s  events=%d  size=%d  parsed=%s\n",
			s.ID, s.FilePath, s.TotalEvents, s.FileSize,
			s.ParsedAt.Format("2006-01-02 15:04:05"))
	}
	return nil
}

func runTRCDelete(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if trcSessionID <= 0 {
		return fmt.Errorf("--session is required for delete command")
	}
	db := openDB()
	defer closeDB(db)
	result, err := trcsvc.ExecuteDelete(ctx, db, trcSessionID)
	if err != nil {
		return err
	}
	if trcOutputJSON {
		return printJSON(result)
	}
	fmt.Printf("Deleted session %d (file: %s)\n", result.SessionID, result.FilePath)
	return nil
}

func runTRCPrune(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if trcKeepLast < 0 {
		return fmt.Errorf("--keep-last must be >= 0")
	}
	db := openDB()
	defer closeDB(db)
	result, err := trcsvc.ExecutePrune(ctx, db, trcKeepLast)
	if err != nil {
		return err
	}
	if trcOutputJSON {
		return printJSON(result)
	}
	fmt.Printf("Deleted %d session(s), kept last %d\n", result.DeletedCount, result.KeptLast)
	return nil
}

func init() {
	trcCmd.PersistentFlags().BoolVar(&trcOutputJSON, "json", false, "output as JSON")

	trcEventsCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")
	trcEventsCmd.Flags().IntVar(&trcSPID, "spid", 0, "legacy SPID filter (0 = all); mutually exclusive with --spids")
	trcEventsCmd.Flags().StringVar(&trcProcedure, "proc", "", "filter by procedure name (exact match)")
	trcEventsCmd.Flags().IntVar(&trcLimit, "limit", 100, "max events to return (max 1000)")
	trcEventsCmd.Flags().StringVar(&trcSPIDsRaw, "spids", "", "comma-separated SPID filter (e.g. 728,700)")
	trcEventsCmd.Flags().StringVar(&trcEventNamesRaw, "event-names", "", "comma-separated event name filter (e.g. SP:Completed,RPC:Completed)")
	trcEventsCmd.Flags().Int64Var(&trcAfterID, "after-id", 0, "keyset cursor: next_after_id of the previous page")
	trcEventsCmd.Flags().StringVar(&trcTimeFrom, "time-from", "", "RFC3339 lower bound of start_time (inclusive)")
	trcEventsCmd.Flags().StringVar(&trcTimeTo, "time-to", "", "RFC3339 upper bound of start_time (exclusive)")
	trcEventsCmd.Flags().Int64Var(&trcMinDuration, "min-duration-ms", 0, "minimum duration_ms filter")
	trcEventsCmd.Flags().StringVar(&trcFormat, "format", "full", "output format: full (params+columns) or short")

	trcProceduresCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")
	trcProceduresCmd.Flags().StringVar(&trcSPIDsRaw, "spids", "", "comma-separated SPID filter (e.g. 728,700)")
	trcProceduresCmd.Flags().StringVar(&trcEventNamesRaw, "event-names", "", "comma-separated event names to aggregate (default: SP:Completed only)")
	trcProceduresCmd.Flags().IntVar(&trcTop, "top", 0, "max procedures to return after aggregation (0 = all, max 1000)")
	trcProceduresCmd.Flags().StringVar(&trcSortBy, "sort", "total_ms", "sort metric: total_ms, avg_ms, max_ms, count")
	trcProceduresCmd.Flags().BoolVar(&trcGroupBySPID, "group-by-spid", false, "group aggregates by (spid, procedure)")

	trcCompareProceduresCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")
	trcCompareProceduresCmd.Flags().IntVar(&trcFocusSPID, "focus-spid", 0, "SPID whose statistics define the Top-N (required)")
	trcCompareProceduresCmd.Flags().StringVar(&trcCompareSPIDsRaw, "compare-spids", "", "comma-separated peer SPIDs (required, focus excluded)")
	trcCompareProceduresCmd.Flags().StringVar(&trcEventNamesRaw, "event-names", "", "comma-separated event names to aggregate (default: SP:Completed only)")
	trcCompareProceduresCmd.Flags().IntVar(&trcTop, "top", 0, "max procedures in the ranking (default 20, max 100)")
	trcCompareProceduresCmd.Flags().StringVar(&trcSortBy, "sort", "total_ms", "focus Top-N metric: total_ms, avg_ms, max_ms, count")

	trcSpidsCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")
	trcSpidsCmd.Flags().StringVar(&trcSPIDsRaw, "spids", "", "comma-separated subset of SPIDs")
	trcSpidsCmd.Flags().StringVar(&trcTimeFrom, "time-from", "", "RFC3339 lower bound of start_time (inclusive)")
	trcSpidsCmd.Flags().StringVar(&trcTimeTo, "time-to", "", "RFC3339 upper bound of start_time (exclusive)")
	trcSpidsCmd.Flags().StringVar(&trcSpidsSortBy, "sort", "event_count", "sort: event_count, first_time, last_time, max_duration_ms, error_count")
	trcSpidsCmd.Flags().IntVar(&trcSpidsLimit, "limit", 100, "max SPIDs to return (max 1000)")

	trcTreeCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")
	trcTreeCmd.Flags().IntVar(&trcSPID, "spid", 0, "filter by SPID (0 = all)")
	trcTreeCmd.Flags().IntVar(&trcMaxDepth, "max-depth", 0, "maximum tree depth (0 = unlimited)")
	trcTreeCmd.Flags().IntVar(&trcTreeLimit, "limit", 0, "maximum root nodes and children per node (0 = unlimited)")
	trcTreeCmd.Flags().StringVar(&trcProcedure, "proc", "", "filter tree by procedure name (exact match)")

	trcErrorsCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")
	trcErrorsCmd.Flags().IntVar(&trcLimit, "limit", 100, "max events to return (max 1000)")

	trcSlowCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")
	trcSlowCmd.Flags().IntVar(&trcSlowThreshold, "slow-ms", 100, "threshold in milliseconds")
	trcSlowCmd.Flags().IntVar(&trcLimit, "limit", 100, "max events to return (max 1000)")

	trcSummaryCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")

	trcListCmd.Flags().IntVar(&trcListLimit, "limit", 20, "max sessions to list")
	trcDeleteCmd.Flags().Int64Var(&trcSessionID, "session", 0, "session ID to delete")
	trcPruneCmd.Flags().IntVar(&trcKeepLast, "keep-last", 0, "keep only last N sessions (0 = delete all)")

	trcCmd.AddCommand(trcParseCmd)
	trcCmd.AddCommand(trcSummaryCmd)
	trcCmd.AddCommand(trcEventsCmd)
	trcCmd.AddCommand(trcProceduresCmd)
	trcCmd.AddCommand(trcCompareProceduresCmd)
	trcCmd.AddCommand(trcSpidsCmd)
	trcCmd.AddCommand(trcTreeCmd)
	trcCmd.AddCommand(trcErrorsCmd)
	trcCmd.AddCommand(trcSlowCmd)
	trcCmd.AddCommand(trcListCmd)
	trcCmd.AddCommand(trcDeleteCmd)
	trcCmd.AddCommand(trcPruneCmd)
	rootCmd.AddCommand(trcCmd)
}
