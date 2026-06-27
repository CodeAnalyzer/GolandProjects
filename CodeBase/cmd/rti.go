package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/query"
	"github.com/codebase/internal/rti"
	"github.com/codebase/internal/store"
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
	result, err := rti.ParseFile(args[0])
	if err != nil {
		return fmt.Errorf("failed to parse RTI file: %w", err)
	}

	// Save to DB if available
	var sessionID int64
	cfg := config.Get()
	if cfg != nil {
		if db, dbErr := store.NewDB(cfg.DB); dbErr == nil {
			defer db.Close()
			if err := db.InitSchema(); err != nil {
				return fmt.Errorf("failed to init schema: %w", err)
			}
			sessionID, err = rti.SaveSession(db, result, args[0])
			if err != nil {
				return fmt.Errorf("failed to save session: %w", err)
			}
		}
	}

	if rtiOutputJSON {
		return printJSON(map[string]interface{}{
			"summary":    result.Summary,
			"session_id": sessionID,
		})
	}
	printSummary(result)
	if sessionID > 0 {
		fmt.Printf("Saved session: %d\n", sessionID)
	}
	return nil
}

func loadRTICalls(args []string) (*rti.RTIParseResult, error) {
	if rtiSessionID > 0 {
		cfg := config.Get()
		if cfg == nil {
			return nil, fmt.Errorf("no config available")
		}
		db, err := store.NewDB(cfg.DB)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to DB: %w", err)
		}
		defer db.Close()

		session, err := rti.GetSession(db, rtiSessionID)
		if err != nil {
			return nil, fmt.Errorf("session %d not found: %w", rtiSessionID, err)
		}
		calls, err := rti.LoadCalls(db, rtiSessionID)
		if err != nil {
			return nil, fmt.Errorf("failed to load calls: %w", err)
		}
		return &rti.RTIParseResult{
			Calls: calls,
			Summary: rti.RTISummary{
				FilePath:    session.FilePath,
				FileSize:    session.FileSize,
				TotalCalls:  session.TotalCalls,
				ErrorsCount: session.ErrorsCount,
			},
		}, nil
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("either <file.rti> or --session is required")
	}
	return rti.ParseFile(args[0])
}

func runRTISummary(cmd *cobra.Command, args []string) error {
	result, err := loadRTICalls(args)
	if err != nil {
		return err
	}
	if rtiOutputJSON {
		return printJSON(result.Summary)
	}
	printSummary(result)
	return nil
}

func runRTITree(cmd *cobra.Command, args []string) error {
	result, err := loadRTICalls(args)
	if err != nil {
		return err
	}
	// BuildTree with empty --proc auto-selects root with most descendants
	tree := rti.BuildTree(result.Calls, rtiProcedure, rtiMaxDepth)
	if tree == nil {
		return fmt.Errorf("procedure %q not found in RTI log", rtiProcedure)
	}

	// Enrich with source files from CodeBase DB
	var enrichMap map[string]*rti.ProcedureEnrichment
	cfg := config.Get()
	if cfg != nil {
		if db, dbErr := store.NewDB(cfg.DB); dbErr == nil {
			defer db.Close()
			q := query.New(db)
			enrichMap = rti.EnrichCalls(q, result.Calls)
		}
	}

	if rtiOutputJSON {
		return printJSON(tree)
	}
	fmt.Print(rti.FormatTreeEnriched(tree, enrichMap))
	return nil
}

func runRTIErrors(cmd *cobra.Command, args []string) error {
	result, err := loadRTICalls(args)
	if err != nil {
		return err
	}
	var errors []*rti.RTICall
	for _, c := range result.Calls {
		if c.RetVal != nil && *c.RetVal != 0 {
			errors = append(errors, c)
		}
	}

	// Try to enrich with ds_return_codes and source files from DB
	var retCodeMap map[int64]*store.RetCodeLookup
	var enrichMap map[string]*rti.ProcedureEnrichment
	cfg := config.Get()
	if cfg != nil {
		if db, dbErr := store.NewDB(cfg.DB); dbErr == nil {
			defer db.Close()
			codes := make([]int64, 0, len(errors))
			for _, c := range errors {
				codes = append(codes, int64(*c.RetVal))
			}
			retCodeMap, _ = db.LookupRetCodes(codes)
			q := query.New(db)
			enrichMap = rti.EnrichCalls(q, errors)
		}
	}

	if rtiOutputJSON {
		return printJSON(errors)
	}
	if len(errors) == 0 {
		fmt.Println("No errors found.")
		return nil
	}
	fmt.Printf("Found %d error(s):\n\n", len(errors))
	for _, c := range errors {
		fmt.Printf("  Line %d: %s [RetVal=%d] %s ← %s (%d)\n",
			c.EnterLine, c.Procedure, *c.RetVal, c.RetValContext,
			c.ModuleName, c.ModuleID)
		if c.ElapsedMs > 0 {
			fmt.Printf("    Elapsed: %dms, NestLevel: %d\n", c.ElapsedMs, c.NestLevel)
		}
		if retCodeMap != nil {
			if lookup, ok := retCodeMap[int64(*c.RetVal)]; ok && lookup != nil {
				fmt.Printf("    Error code: %s (proc: %s)\n", lookup.Message, lookup.ProcName)
			}
		}
		if enrichMap != nil {
			if enrich, ok := enrichMap[c.Procedure]; ok && enrich != nil && enrich.Found {
				fmt.Printf("    Source: %s:%d\n", enrich.SourceFile, enrich.LineStart)
			}
		}
	}
	return nil
}

func runRTIDetails(cmd *cobra.Command, args []string) error {
	if rtiProcedure == "" {
		return fmt.Errorf("--proc is required for details command")
	}
	result, err := loadRTICalls(args)
	if err != nil {
		return err
	}

	// Find all calls for the specified procedure
	var calls []*rti.RTICall
	for _, c := range result.Calls {
		if c.Procedure == rtiProcedure {
			calls = append(calls, c)
		}
	}
	if len(calls) == 0 {
		return fmt.Errorf("procedure %q not found in RTI log", rtiProcedure)
	}

	// Enrich from CodeBase DB
	var enrich *rti.ProcedureEnrichment
	var retCodeMap map[int64]*store.RetCodeLookup
	cfg := config.Get()
	if cfg != nil {
		if db, dbErr := store.NewDB(cfg.DB); dbErr == nil {
			defer db.Close()
			q := query.New(db)
			enrich, _ = rti.EnrichProcedure(q, rtiProcedure)
			// Lookup retcodes for all error calls
			codes := make([]int64, 0)
			for _, c := range calls {
				if c.RetVal != nil && *c.RetVal != 0 {
					codes = append(codes, int64(*c.RetVal))
				}
			}
			if len(codes) > 0 {
				retCodeMap, _ = db.LookupRetCodes(codes)
			}
		}
	}

	if rtiOutputJSON {
		return printJSON(map[string]interface{}{
			"procedure":  rtiProcedure,
			"calls":      calls,
			"enrichment": enrich,
		})
	}

	// Print details
	fmt.Printf("Procedure: %s\n", rtiProcedure)
	fmt.Printf("Calls in log: %d\n", len(calls))
	if enrich != nil && enrich.Found {
		fmt.Printf("Source: %s:%d-%d\n", enrich.SourceFile, enrich.LineStart, enrich.LineEnd)
		if len(enrich.Params) > 0 {
			fmt.Println("Parameters:")
			for _, p := range enrich.Params {
				fmt.Printf("  %s %s (%s)\n", p.Direction, p.Name, p.Type)
			}
		}
	} else {
		fmt.Println("Source: (not found)")
	}
	fmt.Println()
	for i, c := range calls {
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
		if retCodeMap != nil && c.RetVal != nil && *c.RetVal != 0 {
			if lookup, ok := retCodeMap[int64(*c.RetVal)]; ok && lookup != nil {
				fmt.Printf("    Error: %s (proc: %s)\n", lookup.Message, lookup.ProcName)
			}
		}
		if len(c.Params) > 0 {
			fmt.Printf("    Params: %d\n", len(c.Params))
		}
	}
	return nil
}

func runRTIBlog(cmd *cobra.Command, args []string) error {
	if rtiProcedure == "" {
		return fmt.Errorf("--proc is required for blog command")
	}
	result, err := loadRTICalls(args)
	if err != nil {
		return err
	}

	var calls []*rti.RTICall
	for _, c := range result.Calls {
		if c.Procedure == rtiProcedure {
			calls = append(calls, c)
		}
	}
	if len(calls) == 0 {
		return fmt.Errorf("procedure %q not found in RTI log", rtiProcedure)
	}

	if rtiOutputJSON {
		type callBLog struct {
			EnterLine   int                 `json:"enter_line"`
			ElapsedMs   int                 `json:"elapsed_ms,omitempty"`
			BLogBlocks  []rti.RTIBLogBlock  `json:"blog_blocks,omitempty"`
			Checkpoints []rti.RTICheckpoint `json:"checkpoints,omitempty"`
			BLogTables  []rti.RTIBLogTable  `json:"blog_tables,omitempty"`
		}
		var items []callBLog
		for _, c := range calls {
			items = append(items, callBLog{
				EnterLine:   c.EnterLine,
				ElapsedMs:   c.ElapsedMs,
				BLogBlocks:  c.BLogBlocks,
				Checkpoints: c.Checkpoints,
				BLogTables:  c.BLogTables,
			})
		}
		return printJSON(map[string]interface{}{
			"procedure": rtiProcedure,
			"count":     len(calls),
			"calls":     items,
		})
	}

	fmt.Printf("Business log: %s  (%d call(s))\n\n", rtiProcedure, len(calls))
	for i, c := range calls {
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
	cfg := config.Get()
	if cfg == nil {
		return fmt.Errorf("no config available")
	}
	db, err := store.NewDB(cfg.DB)
	if err != nil {
		return fmt.Errorf("failed to connect to DB: %w", err)
	}
	defer db.Close()

	sessions, err := rti.ListSessions(db, rtiListLimit)
	if err != nil {
		return fmt.Errorf("failed to list sessions: %w", err)
	}
	if len(sessions) == 0 {
		fmt.Println("No saved sessions.")
		return nil
	}
	if rtiOutputJSON {
		return printJSON(sessions)
	}
	fmt.Printf("%d session(s):\n\n", len(sessions))
	for _, s := range sessions {
		fmt.Printf("  %d  %s  calls=%d  errors=%d  size=%d  parsed=%s\n",
			s.ID, s.FilePath, s.TotalCalls, s.ErrorsCount, s.FileSize,
			s.ParsedAt.Format("2006-01-02 15:04:05"))
	}
	return nil
}

func runRTIDelete(cmd *cobra.Command, args []string) error {
	if rtiSessionID <= 0 {
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

	session, err := rti.GetSession(db, rtiSessionID)
	if err != nil {
		return fmt.Errorf("session %d not found: %w", rtiSessionID, err)
	}

	if err := rti.DeleteSession(db, rtiSessionID); err != nil {
		return fmt.Errorf("failed to delete session: %w", err)
	}

	if rtiOutputJSON {
		return printJSON(map[string]interface{}{
			"deleted":    true,
			"session_id": rtiSessionID,
			"file_path":  session.FilePath,
		})
	}
	fmt.Printf("Deleted session %d (file: %s, %d calls)\n",
		rtiSessionID, session.FilePath, session.TotalCalls)
	return nil
}

func runRTIPrune(cmd *cobra.Command, args []string) error {
	if rtiKeepLast <= 0 {
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

	deleted, err := rti.PruneSessions(db, rtiKeepLast)
	if err != nil {
		return fmt.Errorf("failed to prune sessions: %w", err)
	}

	if rtiOutputJSON {
		return printJSON(map[string]interface{}{
			"deleted_count": deleted,
			"kept_last":     rtiKeepLast,
		})
	}
	fmt.Printf("Deleted %d session(s), kept last %d\n", deleted, rtiKeepLast)
	return nil
}

func runRTISlow(cmd *cobra.Command, args []string) error {
	result, err := loadRTICalls(args)
	if err != nil {
		return err
	}
	threshold := rtiSlowThreshold
	if threshold <= 0 {
		threshold = 100
	}
	var slow []*rti.RTICall
	for _, c := range result.Calls {
		if c.ElapsedMs >= threshold {
			slow = append(slow, c)
		}
	}
	sort.Slice(slow, func(i, j int) bool {
		return slow[i].ElapsedMs > slow[j].ElapsedMs
	})
	if rtiOutputJSON {
		return printJSON(slow)
	}
	if len(slow) == 0 {
		fmt.Printf("No calls slower than %dms found.\n", threshold)
		return nil
	}
	fmt.Printf("Found %d slow call(s) (>= %dms):\n\n", len(slow), threshold)
	for _, c := range slow {
		fmt.Printf("  %s [%dms] Line %d ← %s (%d) NestLevel=%d\n",
			c.Procedure, c.ElapsedMs, c.EnterLine,
			c.ModuleName, c.ModuleID, c.NestLevel)
		if c.RetVal != nil {
			fmt.Printf("    RetVal=%d %s\n", *c.RetVal, c.RetValContext)
		}
	}
	return nil
}

func printSummary(result *rti.RTIParseResult) {
	s := result.Summary
	fmt.Printf("RTI Log: %s\n", s.FilePath)
	fmt.Printf("File size: %d bytes\n", s.FileSize)
	fmt.Printf("Total calls: %d\n", s.TotalCalls)
	fmt.Printf("Errors: %d\n", s.ErrorsCount)
	fmt.Printf("Max nest level: %d\n", s.MaxNestLevel)
	fmt.Printf("Unparsed lines: %d\n", s.UnparsedLines)
	if len(s.TopSlow) > 0 {
		fmt.Println("\nTop 10 slowest calls:")
		for i, c := range s.TopSlow {
			fmt.Printf("  %d. %s [%dms] ← %s (%d)\n", i+1, c.Procedure, c.ElapsedMs, c.ModuleName, c.ModuleID)
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
	rtiTreeCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "load from saved session ID instead of file")
	rtiErrorsCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "load from saved session ID instead of file")
	rtiSlowCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "load from saved session ID instead of file")
	rtiDetailsCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "load from saved session ID instead of file")
	rtiDeleteCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "session ID to delete")
	rtiPruneCmd.Flags().IntVar(&rtiKeepLast, "keep-last", 0, "keep only last N sessions")
	rtiListCmd.Flags().IntVar(&rtiListLimit, "limit", 20, "max sessions to list")

	rtiCmd.AddCommand(rtiParseCmd)
	rtiCmd.AddCommand(rtiSummaryCmd)
	rtiCmd.AddCommand(rtiTreeCmd)
	rtiCmd.AddCommand(rtiErrorsCmd)
	rtiCmd.AddCommand(rtiSlowCmd)
	rtiBlogCmd.Flags().StringVar(&rtiProcedure, "proc", "", "procedure name (required)")
	rtiBlogCmd.Flags().Int64Var(&rtiSessionID, "session", 0, "load from saved session ID instead of file")
	rtiCmd.AddCommand(rtiBlogCmd)
	rtiCmd.AddCommand(rtiDetailsCmd)
	rtiCmd.AddCommand(rtiListCmd)
	rtiCmd.AddCommand(rtiDeleteCmd)
	rtiCmd.AddCommand(rtiPruneCmd)
	rootCmd.AddCommand(rtiCmd)
}
