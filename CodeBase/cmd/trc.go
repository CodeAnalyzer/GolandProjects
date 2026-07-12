package cmd

import (
	"fmt"
	"os"
	"sort"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/query"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/trc"
	"github.com/spf13/cobra"
)

var (
	trcOutputJSON     bool
	trcSlowThreshold  int
	trcProcedure      string
	trcSessionID      int64
	trcKeepLast       int
	trcListLimit      int
	trcSPID           int
	trcMaxDepth       int
	trcTreeLimit      int
)

var trcCmd = &cobra.Command{
	Use:   "trc",
	Short: "Binary .trc (SQL Server Profiler) trace analyzer",
	Long: `Analyze binary SQL Server Profiler .trc trace files.

Subcommands:
  parse      - parse .trc file, save session, print summary
  summary    - print summary info
  events     - list decoded events (filters: --spid, --proc)
  procedures - aggregate events by procedure (count/min/max/avg/total duration)
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

func loadTRCResult(args []string) (*trc.TRCParseResult, error) {
	if trcSessionID > 0 {
		cfg := config.Get()
		if cfg == nil {
			return nil, fmt.Errorf("no config available")
		}
		db, err := store.NewDB(cfg.DB)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to DB: %w", err)
		}
		defer db.Close()

		session, err := trc.GetSession(db, trcSessionID)
		if err != nil {
			return nil, fmt.Errorf("session %d not found: %w", trcSessionID, err)
		}
		events, err := trc.LoadEvents(db, trcSessionID)
		if err != nil {
			return nil, fmt.Errorf("failed to load events: %w", err)
		}
		return &trc.TRCParseResult{
			Header: &trc.TraceHeader{
				ProviderName: session.ProviderName,
				ServerName:   session.ServerName,
				MajorVersion: session.MajorVersion,
				MinorVersion: session.MinorVersion,
				BuildNumber:  session.BuildNumber,
			},
			Events: events,
		}, nil
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("either <file.trc> or --session is required")
	}
	return trc.ParseFile(args[0])
}

func runTRCParse(cmd *cobra.Command, args []string) error {
	result, err := trc.ParseFile(args[0])
	if err != nil {
		return fmt.Errorf("failed to parse trc file: %w", err)
	}

	var sessionID int64
	cfg := config.Get()
	if cfg != nil {
		if db, dbErr := store.NewDB(cfg.DB); dbErr == nil {
			defer db.Close()
			if err := db.InitSchema(); err != nil {
				return fmt.Errorf("failed to init schema: %w", err)
			}
			fi, statErr := os.Stat(args[0])
			var fileSize int64
			if statErr == nil {
				fileSize = fi.Size()
			}
			sessionID, err = trc.SaveSession(db, result, args[0], fileSize)
			if err != nil {
				return fmt.Errorf("failed to save session: %w", err)
			}
		}
	}

	if trcOutputJSON {
		return printJSON(map[string]interface{}{
			"total_events": len(result.Events),
			"session_id":   sessionID,
		})
	}
	printTRCSummary(args[0], result)
	if sessionID > 0 {
		fmt.Printf("Saved session: %d\n", sessionID)
	}
	return nil
}

func runTRCSummary(cmd *cobra.Command, args []string) error {
	result, err := loadTRCResult(args)
	if err != nil {
		return err
	}
	if trcOutputJSON {
		return printJSON(map[string]interface{}{
			"total_events": len(result.Events),
			"header":       result.Header,
		})
	}
	filePath := ""
	if len(args) > 0 {
		filePath = args[0]
	}
	printTRCSummary(filePath, result)
	return nil
}

func printTRCSummary(filePath string, result *trc.TRCParseResult) {
	if filePath != "" {
		fmt.Printf("TRC file: %s\n", filePath)
	}
	fmt.Printf("Total events: %d\n", len(result.Events))
	if result.Header != nil {
		fmt.Printf("Provider: %s  Server: %s  Version: %d.%d build %d\n",
			result.Header.ProviderName, result.Header.ServerName,
			result.Header.MajorVersion, result.Header.MinorVersion, result.Header.BuildNumber)
	}
}

func runTRCEvents(cmd *cobra.Command, args []string) error {
	result, err := loadTRCResult(args)
	if err != nil {
		return err
	}
	var filtered []trc.TRCEvent
	for _, ev := range result.Events {
		if trcSPID > 0 {
			if spid, ok := ev.Columns[12].(int32); !ok || int(spid) != trcSPID {
				continue
			}
		}
		if trcProcedure != "" && ev.Procedure != trcProcedure {
			continue
		}
		filtered = append(filtered, ev)
	}
	if trcOutputJSON {
		return printJSON(map[string]interface{}{
			"events":         filtered,
			"total_count":    len(result.Events),
			"filtered_count": len(filtered),
		})
	}
	fmt.Printf("%d event(s) (of %d total):\n\n", len(filtered), len(result.Events))
	for _, ev := range filtered {
		printEventLine(ev)
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

func runTRCProcedures(cmd *cobra.Command, args []string) error {
	result, err := loadTRCResult(args)
	if err != nil {
		return err
	}
	aggs := trc.AggregateByProcedure(result.Events)
	cfg := config.Get()
	if cfg != nil && len(aggs) > 0 {
		if db, dbErr := store.NewDB(cfg.DB); dbErr == nil {
			defer db.Close()
			q := query.New(db)
			enrichMap := trc.EnrichEvents(q, result.Events)
			trc.EnrichAggregates(aggs, enrichMap)
		}
	}
	if trcOutputJSON {
		return printJSON(aggs)
	}
	fmt.Printf("%d procedure(s):\n\n", len(aggs))
	for _, a := range aggs {
		fmt.Printf("  %-40s count=%-5d total=%dms min=%dms max=%dms avg=%.1fms",
			a.Procedure, a.Count, a.TotalMs, a.MinMs, a.MaxMs, a.AvgMs)
		if a.SourceFile != "" {
			fmt.Printf("  → %s", a.SourceFile)
		}
		fmt.Println()
	}
	return nil
}

func runTRCTree(cmd *cobra.Command, args []string) error {
	result, err := loadTRCResult(args)
	if err != nil {
		return err
	}
	trees := trc.BuildTreesWithDepth(result.Events, trcMaxDepth)
	if trcSPID > 0 {
		if t, ok := trees[trcSPID]; ok {
			trees = map[int][]*trc.TRCTreeNode{trcSPID: t}
		} else {
			trees = map[int][]*trc.TRCTreeNode{}
		}
	}
	trc.LimitTrees(trees, trcTreeLimit)
	if trcOutputJSON {
		return printJSON(trees)
	}
	if len(trees) == 0 {
		fmt.Println("No events found.")
		return nil
	}
	fmt.Print(trc.FormatTrees(trees))
	return nil
}

func runTRCErrors(cmd *cobra.Command, args []string) error {
	result, err := loadTRCResult(args)
	if err != nil {
		return err
	}
	var errs []trc.TRCEvent
	for _, ev := range result.Events {
		if code, ok := ev.Columns[31].(int32); ok && code != 0 {
			errs = append(errs, ev)
		}
	}
	if trcOutputJSON {
		return printJSON(map[string]interface{}{
			"events": errs,
			"count":  len(errs),
		})
	}
	if len(errs) == 0 {
		fmt.Println("No errors found.")
		return nil
	}
	fmt.Printf("Found %d error event(s):\n\n", len(errs))
	for _, ev := range errs {
		printEventLine(ev)
	}
	return nil
}

func runTRCSlow(cmd *cobra.Command, args []string) error {
	result, err := loadTRCResult(args)
	if err != nil {
		return err
	}
	threshold := trcSlowThreshold
	if threshold <= 0 {
		threshold = 100
	}
	var slow []trc.TRCEvent
	for _, ev := range result.Events {
		if ev.DurationMs >= int64(threshold) {
			slow = append(slow, ev)
		}
	}
	sort.Slice(slow, func(i, j int) bool { return slow[i].DurationMs > slow[j].DurationMs })

	if trcOutputJSON {
		return printJSON(map[string]interface{}{
			"events":    slow,
			"threshold": threshold,
		})
	}
	if len(slow) == 0 {
		fmt.Printf("No events slower than %dms found.\n", threshold)
		return nil
	}
	fmt.Printf("Found %d slow event(s) (>= %dms):\n\n", len(slow), threshold)
	for _, ev := range slow {
		printEventLine(ev)
	}
	return nil
}

func runTRCList(cmd *cobra.Command, args []string) error {
	cfg := config.Get()
	if cfg == nil {
		return fmt.Errorf("no config available")
	}
	db, err := store.NewDB(cfg.DB)
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer db.Close()

	sessions, err := trc.ListSessions(db, trcListLimit)
	if err != nil {
		return fmt.Errorf("failed to list sessions: %w", err)
	}
	if len(sessions) == 0 {
		fmt.Println("No saved sessions.")
		return nil
	}
	if trcOutputJSON {
		return printJSON(sessions)
	}
	fmt.Printf("%d session(s):\n\n", len(sessions))
	for _, s := range sessions {
		fmt.Printf("  %d  %s  events=%d  size=%d  parsed=%s\n",
			s.ID, s.FilePath, s.TotalEvents, s.FileSize,
			s.ParsedAt.Format("2006-01-02 15:04:05"))
	}
	return nil
}

func runTRCDelete(cmd *cobra.Command, args []string) error {
	if trcSessionID <= 0 {
		return fmt.Errorf("--session is required for delete command")
	}
	cfg := config.Get()
	if cfg == nil {
		return fmt.Errorf("no config available")
	}
	db, err := store.NewDB(cfg.DB)
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer db.Close()

	session, err := trc.GetSession(db, trcSessionID)
	if err != nil {
		return fmt.Errorf("session %d not found: %w", trcSessionID, err)
	}
	if err := trc.DeleteSession(db, trcSessionID); err != nil {
		return fmt.Errorf("failed to delete session: %w", err)
	}
	if trcOutputJSON {
		return printJSON(map[string]interface{}{
			"deleted":    true,
			"session_id": trcSessionID,
			"file_path":  session.FilePath,
		})
	}
	fmt.Printf("Deleted session %d (file: %s, %d events)\n",
		trcSessionID, session.FilePath, session.TotalEvents)
	return nil
}

func runTRCPrune(cmd *cobra.Command, args []string) error {
	if trcKeepLast <= 0 {
		return fmt.Errorf("--keep-last is required for prune command")
	}
	cfg := config.Get()
	if cfg == nil {
		return fmt.Errorf("no config available")
	}
	db, err := store.NewDB(cfg.DB)
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer db.Close()

	deleted, err := trc.PruneSessions(db, trcKeepLast)
	if err != nil {
		return fmt.Errorf("failed to prune sessions: %w", err)
	}
	if trcOutputJSON {
		return printJSON(map[string]interface{}{
			"deleted_count": deleted,
			"kept_last":     trcKeepLast,
		})
	}
	fmt.Printf("Deleted %d session(s), kept last %d\n", deleted, trcKeepLast)
	return nil
}

func init() {
	trcCmd.PersistentFlags().BoolVar(&trcOutputJSON, "json", false, "output as JSON")

	trcEventsCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")
	trcEventsCmd.Flags().IntVar(&trcSPID, "spid", 0, "filter by SPID (0 = all)")
	trcEventsCmd.Flags().StringVar(&trcProcedure, "proc", "", "filter by procedure name (exact match)")

	trcProceduresCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")

	trcTreeCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")
	trcTreeCmd.Flags().IntVar(&trcSPID, "spid", 0, "filter by SPID (0 = all)")
	trcTreeCmd.Flags().IntVar(&trcMaxDepth, "max-depth", 0, "maximum tree depth (0 = unlimited)")
	trcTreeCmd.Flags().IntVar(&trcTreeLimit, "limit", 0, "maximum root nodes and children per node (0 = unlimited)")

	trcErrorsCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")

	trcSlowCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")
	trcSlowCmd.Flags().IntVar(&trcSlowThreshold, "slow-ms", 100, "threshold in milliseconds")

	trcSummaryCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")

	trcListCmd.Flags().IntVar(&trcListLimit, "limit", 20, "max sessions to list")
	trcDeleteCmd.Flags().Int64Var(&trcSessionID, "session", 0, "session ID to delete")
	trcPruneCmd.Flags().IntVar(&trcKeepLast, "keep-last", 0, "keep only last N sessions")

	trcCmd.AddCommand(trcParseCmd)
	trcCmd.AddCommand(trcSummaryCmd)
	trcCmd.AddCommand(trcEventsCmd)
	trcCmd.AddCommand(trcProceduresCmd)
	trcCmd.AddCommand(trcTreeCmd)
	trcCmd.AddCommand(trcErrorsCmd)
	trcCmd.AddCommand(trcSlowCmd)
	trcCmd.AddCommand(trcListCmd)
	trcCmd.AddCommand(trcDeleteCmd)
	trcCmd.AddCommand(trcPruneCmd)
	rootCmd.AddCommand(trcCmd)
}
