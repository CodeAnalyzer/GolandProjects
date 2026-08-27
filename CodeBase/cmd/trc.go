package cmd

import (
	"fmt"
	"os"

	"github.com/codebase/internal/trc"
	"github.com/codebase/internal/trcsvc"
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
	trcLimit          int
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

func runTRCEvents(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := trcsvc.ExecuteEvents(ctx, db, trcsvc.EventsParams{
		Source:    trcSource(args),
		SPID:      trcSPID,
		Procedure: trcProcedure,
		Limit:     applyQueryLimit(trcLimit),
	})
	if err != nil {
		return err
	}
	if trcOutputJSON {
		return printJSON(result)
	}
	fmt.Printf("%d event(s) (of %d total, limit %d):\n\n", result.FilteredCount, result.TotalCount, result.Limit)
	for _, ev := range result.Events {
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
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := trcsvc.ExecuteProcedures(ctx, db, trcSource(args))
	if err != nil {
		return err
	}
	if trcOutputJSON {
		return printJSON(result)
	}
	fmt.Printf("%d procedure(s):\n\n", result.Count)
	for _, a := range result.Procedures {
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
	trcEventsCmd.Flags().IntVar(&trcSPID, "spid", 0, "filter by SPID (0 = all)")
	trcEventsCmd.Flags().StringVar(&trcProcedure, "proc", "", "filter by procedure name (exact match)")
	trcEventsCmd.Flags().IntVar(&trcLimit, "limit", 100, "max events to return (max 1000)")

	trcProceduresCmd.Flags().Int64Var(&trcSessionID, "session", 0, "load from saved session ID instead of file")

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
	trcCmd.AddCommand(trcTreeCmd)
	trcCmd.AddCommand(trcErrorsCmd)
	trcCmd.AddCommand(trcSlowCmd)
	trcCmd.AddCommand(trcListCmd)
	trcCmd.AddCommand(trcDeleteCmd)
	trcCmd.AddCommand(trcPruneCmd)
	rootCmd.AddCommand(trcCmd)
}
