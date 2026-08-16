package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/codebase/internal/rti"
	"github.com/codebase/internal/rtisvc"
	"github.com/spf13/cobra"
)

var (
	rtiOutputJSON    bool
	rtiSlowThreshold int
	rtiMaxDepth      int
	rtiProcedure     string
	rtiSessionID     int64
	rtiKeepLast      int
	rtiListLimit     int
	rtiPID           int
	rtiLimit         int
)

var rtiCmd = &cobra.Command{
	Use:   "rti",
	Short: "RTI log analyzer",
	Long: `Analyze Diasoft 5NT RTI trace logs.

Subcommands:
  parse   - parse RTI log and print summary
  summary - print summary statistics
  tree    - print call tree for a procedure
  errors  - print calls with non-zero RetVal
  slow    - print slowest calls (threshold --slow-ms)
  details - show enriched details for a procedure
  list    - list saved sessions
  delete  - delete a session by ID
  prune   - prune old sessions`,
}

var rtiParseCmd = &cobra.Command{
	Use:   "parse <file.rti>",
	Short: "Parse RTI log and print summary",
	Args:  cobra.ExactArgs(1),
	RunE:  runRTIParse,
}

var rtiSummaryCmd = &cobra.Command{
	Use:   "summary [<file.rti>]",
	Short: "Print summary statistics",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runRTISummary,
}

var rtiTreeCmd = &cobra.Command{
	Use:   "tree [<file.rti>]",
	Short: "Print call tree for a procedure",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runRTITree,
}

var rtiErrorsCmd = &cobra.Command{
	Use:   "errors [<file.rti>]",
	Short: "Print calls with non-zero RetVal",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runRTIErrors,
}

var rtiSlowCmd = &cobra.Command{
	Use:   "slow [<file.rti>]",
	Short: "Print slowest calls",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runRTISlow,
}

var rtiDetailsCmd = &cobra.Command{
	Use:   "details [<file.rti>]",
	Short: "Show enriched details for a procedure",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runRTIDetails,
}

var rtiClientTreeCmd = &cobra.Command{
	Use:   "client-tree [<file.rti>]",
	Short: "Print client (thick client d5nt) events tree grouped by PID",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runRTIClientTree,
}

var rtiTimelineCmd = &cobra.Command{
	Use:   "timeline [<file.rti>]",
	Short: "Print unified chronological timeline of server calls and client events",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runRTITimeline,
}

var rtiBlogCmd = &cobra.Command{
	Use:   "blog [<file.rti>]",
	Short: "Show business log (blocks, checkpoints, tables) for a procedure",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runRTIBlog,
}

var rtiListCmd = &cobra.Command{
	Use:   "list",
	Short: "List saved RTI sessions",
	Args:  cobra.NoArgs,
	RunE:  runRTIList,
}

var rtiDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a saved RTI session",
	Args:  cobra.NoArgs,
	RunE:  runRTIDelete,
}

var rtiPruneCmd = &cobra.Command{
	Use:   "prune",
	Short: "Prune old RTI sessions",
	Args:  cobra.NoArgs,
	RunE:  runRTIPrune,
}

func runRTIParse(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := rtisvc.ExecuteParse(ctx, db, args[0])
	if err != nil {
		return err
	}
	if rtiOutputJSON {
		return printJSON(result)
	}
	printSummaryDirect(&result.Summary)
	if result.SessionID > 0 {
		fmt.Printf("Saved session: %d\n", result.SessionID)
	} else {
		fmt.Fprintf(os.Stderr, "Warning: database unavailable, session not saved\n")
	}
	return nil
}

func rtiSource(args []string) rtisvc.SessionSource {
	var filePath string
	if len(args) > 0 {
		filePath = args[0]
	}
	return rtisvc.SessionSource{SessionID: rtiSessionID, FilePath: filePath}
}

func runRTISummary(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := rtisvc.ExecuteSummary(ctx, db, rtiSource(args))
	if err != nil {
		return err
	}
	if rtiOutputJSON {
		return printJSON(result)
	}
	printSummaryDirect(&result.Summary)
	return nil
}

func runRTITree(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := rtisvc.ExecuteTree(ctx, db, rtisvc.TreeParams{
		Source:    rtiSource(args),
		Procedure: rtiProcedure,
		MaxDepth:  rtiMaxDepth,
	})
	if err != nil {
		return err
	}
	if result.Tree == nil {
		return fmt.Errorf("procedure %q not found in RTI log", rtiProcedure)
	}
	if rtiOutputJSON {
		return printJSON(result)
	}
	fmt.Print(rti.FormatTreeEnriched(result.Tree, result.Enrichment))
	return nil
}

func runRTIClientTree(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	var filter rtisvc.TimelineFilter
	if rtiPID > 0 {
		p := rtiPID
		filter.PID = &p
	}
	result, err := rtisvc.ExecuteClientTree(ctx, db, rtisvc.ClientTreeParams{
		Source: rtiSource(args),
		Filter: filter,
		Limit:  applyQueryLimit(rtiLimit),
	})
	if err != nil {
		return err
	}
	if rtiOutputJSON {
		return printJSON(result)
	}
	nodes, ok := result.Nodes.([]*rti.RTIClientTreeNode)
	if !ok || len(nodes) == 0 {
		fmt.Println("No client events found.")
		return nil
	}
	if result.Enrichment != nil {
		fmt.Print(rti.FormatClientTreeEnriched(nodes, result.Enrichment))
	} else {
		fmt.Print(rti.FormatClientTree(nodes))
	}
	return nil
}

func runRTITimeline(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	var filter rtisvc.TimelineFilter
	if rtiPID > 0 {
		p := rtiPID
		filter.PID = &p
	}
	result, err := rtisvc.ExecuteTimeline(ctx, db, rtisvc.TimelineParams{
		Source: rtiSource(args),
		Filter: filter,
		Limit:  applyQueryLimit(rtiLimit),
	})
	if err != nil {
		return err
	}
	if rtiOutputJSON {
		return printJSON(result)
	}
	calls, _ := result.Calls.([]*rti.RTICall)
	events, _ := result.ClientEvents.([]*rti.RTIClientEvent)
	if result.Enrichment != nil {
		fmt.Print(rti.FormatUnifiedTimelineEnriched(calls, events, result.Enrichment))
	} else {
		fmt.Print(rti.FormatUnifiedTimeline(calls, events))
	}
	return nil
}

func runRTIErrors(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := rtisvc.ExecuteErrors(ctx, db, rtisvc.ErrorsParams{
		Source: rtiSource(args),
		Limit:  applyQueryLimit(rtiLimit),
	})
	if err != nil {
		return err
	}
	if rtiOutputJSON {
		return printJSON(result)
	}
	if result.ServerErrorCount == 0 && result.ClientErrorCount == 0 {
		fmt.Println("No errors found.")
		return nil
	}
	if result.ServerErrorCount > 0 {
		fmt.Printf("Found %d server error(s):\n\n", result.ServerErrorCount)
		for _, c := range result.ServerErrors {
			fmt.Printf("  [server] Line %d: %s [RetVal=%d] %s ← %s (%d)\n",
				c.EnterLine, c.Procedure, *c.RetVal, c.RetValContext,
				c.ModuleName, c.ModuleID)
			if c.ElapsedMs > 0 {
				fmt.Printf("    Elapsed: %dms, NestLevel: %d\n", c.ElapsedMs, c.NestLevel)
			}
			if c.RetValMeaning != "" {
				fmt.Printf("    Error code: %s (proc: %s)\n", c.RetValMeaning, c.ErrorConstant)
			}
			if result.ServerEnrichment != nil {
				if enrich, ok := result.ServerEnrichment[c.Procedure]; ok && enrich != nil && enrich.Found {
					fmt.Printf("    Source: %s:%d\n", enrich.SourceFile, enrich.LineStart)
				}
			}
		}
	}
	if result.ClientErrorCount > 0 {
		fmt.Printf("\nFound %d client error(s):\n\n", result.ClientErrorCount)
		for _, ev := range result.ClientErrors {
			fmt.Printf("  [client] Line %d: %s.%s: %s\n", ev.Line, ev.ClassName, ev.MethodName, ev.ErrorText)
			if result.ClientEnrichment != nil {
				key := ev.ClassName + "." + ev.MethodName
				if enrich, ok := result.ClientEnrichment[key]; ok && enrich != nil && enrich.Found {
					fmt.Printf("    Source: %s:%d\n", enrich.SourceFile, enrich.LineNumber)
				}
			}
		}
	}
	return nil
}

func runRTIDetails(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if rtiProcedure == "" {
		return fmt.Errorf("--proc is required for details command")
	}
	db := openDB()
	defer closeDB(db)
	result, err := rtisvc.ExecuteDetails(ctx, db, rtisvc.DetailsParams{
		Source:    rtiSource(args),
		Procedure: rtiProcedure,
		Limit:     applyQueryLimit(rtiLimit),
	})
	if err != nil {
		return err
	}
	if result.Count == 0 {
		return fmt.Errorf("procedure %q not found in RTI log", rtiProcedure)
	}
	if rtiOutputJSON {
		return printJSON(result)
	}
	fmt.Printf("Procedure: %s\n", result.Procedure)
	fmt.Printf("Calls in log: %d\n", result.Count)
	if result.Enrichment != nil && result.Enrichment.Found {
		fmt.Printf("Source: %s:%d-%d\n", result.Enrichment.SourceFile, result.Enrichment.LineStart, result.Enrichment.LineEnd)
		if len(result.Enrichment.Params) > 0 {
			fmt.Println("Parameters:")
			for _, p := range result.Enrichment.Params {
				fmt.Printf("  %s %s (%s)\n", p.Direction, p.Name, p.Type)
			}
		}
	} else {
		fmt.Println("Source: (not found)")
	}
	fmt.Println()
	for i, c := range result.Calls {
		retVal := ""
		if c.RetVal != nil {
			retVal = fmt.Sprintf("RetVal=%d", *c.RetVal)
		}
		fmt.Printf("  Call %d: Line %d [%dms] %s NestLevel=%d ← %s (%d)\n",
			i+1, c.EnterLine, c.ElapsedMs, retVal, c.NestLevel,
			c.ModuleName, c.ModuleID)
		if c.RetValContext != "" {
			fmt.Printf("    Context: %s\n", c.RetValContext)
		}
		if c.RetValMeaning != "" {
			fmt.Printf("    Error: %s (proc: %s)\n", c.RetValMeaning, c.ErrorConstant)
		}
		if len(c.Params) > 0 {
			fmt.Printf("    Params: %d\n", len(c.Params))
		}
	}
	return nil
}

func runRTIBlog(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if rtiProcedure == "" {
		return fmt.Errorf("--proc is required for blog command")
	}
	db := openDB()
	defer closeDB(db)
	result, err := rtisvc.ExecuteBlog(ctx, db, rtisvc.BlogParams{
		Source:    rtiSource(args),
		Procedure: rtiProcedure,
		Limit:     applyQueryLimit(rtiLimit),
	})
	if err != nil {
		return err
	}
	if result.Count == 0 {
		return fmt.Errorf("procedure %q not found in RTI log", rtiProcedure)
	}
	if rtiOutputJSON {
		return printJSON(result)
	}
	fmt.Printf("Business log: %s  (%d call(s))\n\n", result.Procedure, result.Count)
	for i, c := range result.Calls {
		fmt.Printf("  Call %d: Line %d [%dms]\n", i+1, c.EnterLine, c.ElapsedMs)
		for _, b := range c.BLogBlocks {
			fmt.Printf("    Block %-40s  Enter: %s  Exit: %s  [%dms]\n",
				b.BlockName,
				formatOptTime(b.EnterTime),
				formatOptTime(b.ExitTime),
				b.ElapsedMs)
		}
		for _, cp := range c.Checkpoints {
			timePart := ""
			if !cp.Timestamp.IsZero() {
				timePart = "  @ " + cp.Timestamp.Format("15:04:05.000")
			}
			fmt.Printf("    Checkpoint %-40s [%dms]%s\n", cp.Label, cp.ElapsedMs, timePart)
		}
		for _, tbl := range c.BLogTables {
			fmt.Printf("    Table %-30s  rows=%d\n", tbl.TableName, tbl.RowCount)
			if len(tbl.Columns) > 0 {
				fmt.Printf("      Columns: %s\n", joinStrings(tbl.Columns, ", "))
			}
		}
	}
	return nil
}

func formatOptTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format("15:04:05.000")
}

func joinStrings(ss []string, sep string) string {
	result := ""
	for i, s := range ss {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}

func runRTIList(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := rtisvc.ExecuteList(ctx, db, rtiListLimit)
	if err != nil {
		return err
	}
	if len(result.Sessions) == 0 {
		fmt.Println("No saved sessions.")
		return nil
	}
	if rtiOutputJSON {
		return printJSON(result)
	}
	fmt.Printf("%d session(s):\n\n", len(result.Sessions))
	for _, s := range result.Sessions {
		fmt.Printf("  %d  %s  calls=%d  errors=%d  size=%d  parsed=%s\n",
			s.ID, s.FilePath, s.TotalCalls, s.ErrorsCount, s.FileSize,
			s.ParsedAt.Format("2006-01-02 15:04:05"))
	}
	return nil
}

func runRTIDelete(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if rtiSessionID <= 0 {
		return fmt.Errorf("--session is required for delete command")
	}
	db := openDB()
	defer closeDB(db)
	result, err := rtisvc.ExecuteDelete(ctx, db, rtiSessionID)
	if err != nil {
		return err
	}
	if rtiOutputJSON {
		return printJSON(result)
	}
	fmt.Printf("Deleted session %d (file: %s)\n", result.SessionID, result.FilePath)
	return nil
}

func runRTIPrune(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if rtiKeepLast < 0 {
		return fmt.Errorf("--keep-last must be >= 0")
	}
	db := openDB()
	defer closeDB(db)
	result, err := rtisvc.ExecutePrune(ctx, db, rtiKeepLast)
	if err != nil {
		return err
	}
	if rtiOutputJSON {
		return printJSON(result)
	}
	fmt.Printf("Deleted %d session(s), kept last %d\n", result.DeletedCount, result.KeptLast)
	return nil
}

func runRTISlow(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := rtisvc.ExecuteSlow(ctx, db, rtisvc.SlowParams{
		Source:      rtiSource(args),
		ThresholdMs: rtiSlowThreshold,
		Limit:       applyQueryLimit(rtiLimit),
	})
	if err != nil {
		return err
	}
	if rtiOutputJSON {
		return printJSON(result)
	}
	if result.ServerCallCount == 0 && result.ClientSQLCount == 0 {
		fmt.Printf("No calls slower than %dms found.\n", result.Threshold)
		return nil
	}
	if result.ServerCallCount > 0 {
		fmt.Printf("Found %d slow server call(s) (>= %dms):\n\n", result.ServerCallCount, result.Threshold)
		for _, c := range result.ServerCalls {
			fmt.Printf("  [server] %s [%dms] Line %d ← %s (%d) NestLevel=%d\n",
				c.Procedure, c.ElapsedMs, c.EnterLine,
				c.ModuleName, c.ModuleID, c.NestLevel)
			if c.RetVal != nil {
				fmt.Printf("    RetVal=%d %s\n", *c.RetVal, c.RetValContext)
			}
		}
	}
	if result.ClientSQLCount > 0 {
		fmt.Printf("\nFound %d slow client SQL block(s) (>= %dms):\n\n", result.ClientSQLCount, result.Threshold)
		for _, ev := range result.ClientSQLBlocks {
			proc := ev.SQL.ExecProcedure
			if proc == "" {
				proc = "(raw SQL)"
			}
			fmt.Printf("  [client] %s [%.3fs] Line %d ← %s.%s\n",
				proc, ev.SQL.DurationSec, ev.Line, ev.ClassName, ev.MethodName)
			if result.ClientEnrichment != nil {
				key := ev.ClassName + "." + ev.MethodName
				if enrich, ok := result.ClientEnrichment[key]; ok && enrich != nil {
					if enrich.Found {
						fmt.Printf("    Source: %s:%d\n", enrich.SourceFile, enrich.LineNumber)
					}
					if enrich.QueryFragmentFile != "" {
						fmt.Printf("    SQL origin: %s:%d", enrich.QueryFragmentFile, enrich.QueryFragmentLine)
						if enrich.OriginMethod != "" {
							fmt.Printf(" [%s]", enrich.OriginMethod)
						}
						fmt.Println()
					}
				}
			}
		}
	}
	return nil
}

func printSummaryDirect(s *rti.RTISummary) {
	fmt.Printf("RTI Log: %s\n", s.FilePath)
	fmt.Printf("File size: %d bytes\n", s.FileSize)
	fmt.Printf("Total calls: %d\n", s.TotalCalls)
	fmt.Printf("Errors: %d\n", s.ErrorsCount)
	fmt.Printf("Max nest level: %d\n", s.MaxNestLevel)
	fmt.Printf("Unparsed lines: %d\n", s.UnparsedLines)
	if s.ClientEventsCount > 0 {
		fmt.Printf("Client events: %d (errors: %d, slow SQL: %d)\n",
			s.ClientEventsCount, s.ClientErrorsCount, s.ClientSlowSQLCount)
	}
	if len(s.TopSlow) > 0 {
		fmt.Println("\nTop 10 slowest calls:")
		for i, c := range s.TopSlow {
			fmt.Printf("  %d. %s [%dms] ← %s (%d)\n", i+1, c.Procedure, c.ElapsedMs, c.ModuleName, c.ModuleID)
		}
	}
	if len(s.TopSlowClientSQL) > 0 {
		fmt.Println("\nTop slowest client SQL blocks:")
		for i, c := range s.TopSlowClientSQL {
			proc := ""
			if c.SQL != nil && c.SQL.ExecProcedure != "" {
				proc = " exec " + c.SQL.ExecProcedure
			}
			dur := 0.0
			if c.SQL != nil {
				dur = c.SQL.DurationSec
			}
			fmt.Printf("  %d. [%.3fs]%s ← %s.%s\n", i+1, dur, proc, c.ClassName, c.MethodName)
		}
	}
}

func printJSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

func init() {
	rtiCmd.PersistentFlags().BoolVar(&rtiOutputJSON, "json", false, "output as JSON")
	rtiTreeCmd.Flags().StringVar(&rtiProcedure, "proc", "", "root procedure name (default: first NestLevel=1)")
	rtiTreeCmd.Flags().IntVar(&rtiMaxDepth, "depth", 0, "max tree depth (0 = unlimited)")
	rtiDetailsCmd.Flags().StringVar(&rtiProcedure, "proc", "", "procedure name (required)")
	rtiSlowCmd.Flags().IntVar(&rtiSlowThreshold, "slow-ms", 100, "threshold in milliseconds")
	rtiSlowCmd.Flags().IntVar(&rtiLimit, "limit", 100, "max results to return (max 1000)")
	rtiErrorsCmd.Flags().IntVar(&rtiLimit, "limit", 100, "max results to return (max 1000)")
	rtiDetailsCmd.Flags().IntVar(&rtiLimit, "limit", 100, "max call instances to return (max 1000)")
	rtiBlogCmd.Flags().IntVar(&rtiLimit, "limit", 100, "max call instances to return (max 1000)")
	rtiTreeCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "load from saved session ID instead of file")
	rtiErrorsCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "load from saved session ID instead of file")
	rtiSlowCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "load from saved session ID instead of file")
	rtiDetailsCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "load from saved session ID instead of file")
	rtiDeleteCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "session ID to delete")
	rtiPruneCmd.Flags().IntVar(&rtiKeepLast, "keep-last", 0, "keep only last N sessions (0 = delete all)")
	rtiListCmd.Flags().IntVar(&rtiListLimit, "limit", 20, "max sessions to list")
	rtiSummaryCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "load from saved session ID instead of file")

	rtiCmd.AddCommand(rtiParseCmd)
	rtiCmd.AddCommand(rtiSummaryCmd)
	rtiCmd.AddCommand(rtiTreeCmd)
	rtiCmd.AddCommand(rtiErrorsCmd)
	rtiCmd.AddCommand(rtiSlowCmd)
	rtiBlogCmd.Flags().StringVar(&rtiProcedure, "proc", "", "procedure name (required)")
	rtiBlogCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "load from saved session ID instead of file")
	rtiCmd.AddCommand(rtiBlogCmd)
	rtiClientTreeCmd.Flags().IntVar(&rtiPID, "pid", 0, "filter by client PID (0 = all)")
	rtiClientTreeCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "load from saved session ID instead of file")
	rtiClientTreeCmd.Flags().IntVar(&rtiLimit, "limit", 100, "max events to return (max 1000)")
	rtiCmd.AddCommand(rtiClientTreeCmd)
	rtiTimelineCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "load from saved session ID instead of file")
	rtiTimelineCmd.Flags().IntVar(&rtiLimit, "limit", 100, "max items per type to return (max 1000)")
	rtiTimelineCmd.Flags().IntVar(&rtiPID, "pid", 0, "filter by client PID (0 = all)")
	rtiCmd.AddCommand(rtiTimelineCmd)
	rtiCmd.AddCommand(rtiDetailsCmd)
	rtiCmd.AddCommand(rtiListCmd)
	rtiCmd.AddCommand(rtiDeleteCmd)
	rtiCmd.AddCommand(rtiPruneCmd)
	rootCmd.AddCommand(rtiCmd)
}
