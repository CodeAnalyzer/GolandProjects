package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/indexer"
	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
	"github.com/spf13/cobra"
)

var (
	rootPath  string
	parallel  int
	noConfirm bool
)

func printPipelineTimings(stats *model.ScanStats) {
	fmt.Printf("\nStage timings:\n")
	fmt.Printf("  Walk+save files: %d ms (wall-clock)\n", stats.WalkSaveMs)
	if stats.SaveMs > 0 || stats.ParseMs > 0 {
		fmt.Printf("  Save (Σ workers):  %d ms\n", stats.SaveMs)
		fmt.Printf("  Parse (Σ workers): %d ms\n", stats.ParseMs)
		if stats.WalkSaveMs > 0 {
			ratio := float64(stats.SaveMs+stats.ParseMs) / float64(stats.WalkSaveMs)
			fmt.Printf("  Parallel efficiency: %.1fx (Σ / wall-clock)\n", ratio)
		}
	}
	if stats.ProcessMs > 0 {
		fmt.Printf("  Parse+insert:    %d ms (wall-clock, post-walk)\n", stats.ProcessMs)
	}
	fmt.Printf("  Post-processing: %d ms\n", stats.PostProcessMs)
	if stats.CleanupMs > 0 {
		fmt.Printf("  Cleanup:         %d ms\n", stats.CleanupMs)
	}
}

var initCmd = &cobra.Command{
	Use:   "init [root_path]",
	Short: "Full scan and index building",
	Long: `Performs a full scan of the source code directory (Diasoft 5NT),
parses files (SQL, H, PAS, INC, JS, SMF, DFM) and builds an index in PostgreSQL.

Arguments:
  root_path - project root directory (defaults to config)`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Путь можно передать либо аргументом CLI, либо через --path,
		// либо взять из конфигурации как fallback.
		if len(args) > 0 {
			rootPath = args[0]
		}

		if rootPath == "" {
			cfg := config.Get()
			if cfg == nil || cfg.RootPath == "" {
				return fmt.Errorf("root path not specified. Use 'codebase init <path>' or set root_path in config")
			}
			rootPath = cfg.RootPath
		}

		// Ранняя валидация пути позволяет завершиться до подключения к БД,
		// если пользователь передал некорректный каталог исходников.
		if _, err := os.Stat(rootPath); os.IsNotExist(err) {
			return fmt.Errorf("path does not exist: %s", rootPath)
		}

		cfg := config.Get()
		if cfg == nil {
			return fmt.Errorf("config not loaded")
		}

		effectiveParallel := parallel
		if !cmd.Flags().Changed("parallel") && cfg.Indexer.Parallel > 0 {
			effectiveParallel = cfg.Indexer.Parallel
		}

		// init всегда работает через новый DB handle, чтобы команда была
		// полностью самодостаточной и не зависела от внешнего состояния процесса.
		db, err := store.NewDB(cfg.DB)
		if err != nil {
			return fmt.Errorf("failed to connect to database: %w", err)
		}
		defer db.Close()

		if err := db.InitSchema(); err != nil {
			return fmt.Errorf("failed to init schema: %w", err)
		}

		hasCompletedInit, err := db.HasCompletedInit()
		if err != nil {
			return fmt.Errorf("failed to check init state: %w", err)
		}
		if hasCompletedInit {
			return fmt.Errorf("primary initialization has already been completed. Use 'codebase update' for further updates. To rebuild from scratch, manually delete the database and run 'codebase init' again")
		}

		// Indexer инкапсулирует весь pipeline (конвейер): walk -> parse -> persist.
		idx := indexer.New(db, cfg)
		startedAt := time.Now()
		fmt.Printf("\nIndexing started: %s\n", startedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("rootPath=%s parallel=%d\n", rootPath, effectiveParallel)
		stats, err := idx.Init(rootPath, effectiveParallel)
		if err != nil {
			return fmt.Errorf("indexing failed: %w", err)
		}

		finishedAt := time.Now()
		fmt.Printf("\nIndexing completed: %s\n", finishedAt.Format("2006-01-02 15:04:05"))
		printPipelineTimings(stats)
		fmt.Printf("\nStats:\n")
		fmt.Printf("  Files scanned:  %d\n", stats.FilesScanned)
		fmt.Printf("  Files indexed:  %d\n", stats.FilesIndexed)
		fmt.Printf("  SQL files:      %d\n", stats.SQLFiles)
		fmt.Printf("  PAS files:      %d\n", stats.PASFiles)
		fmt.Printf("  JS files:       %d\n", stats.JSFiles)
		fmt.Printf("  H files:        %d\n", stats.HFiles)
		fmt.Printf("  XML files:      %d\n", stats.XMLFiles)
		fmt.Printf("  DFM files:      %d\n", stats.DFMFiles)
		fmt.Printf("  SMF files:      %d\n", stats.SMFFiles)
		fmt.Printf("  TPR files:      %d\n", stats.TPRFiles)
		fmt.Printf("  RPT files:      %d\n", stats.RPTFiles)
		fmt.Printf("\n")
		fmt.Printf("SQL Entities:\n")
		fmt.Printf("  Procedures:     %d\n", stats.Procedures)
		fmt.Printf("  Tables:         %d\n", stats.Tables)
		fmt.Printf("  Columns:        %d\n", stats.Columns)
		fmt.Printf("  Defines:        %d\n", stats.Defines)
		fmt.Printf("\n")
		fmt.Printf("Pascal Entities:\n")
		fmt.Printf("  Units:          %d\n", stats.Units)
		fmt.Printf("  Classes:        %d\n", stats.Classes)
		fmt.Printf("  Methods:        %d\n", stats.Methods)
		fmt.Printf("  Fields:         %d\n", stats.PASFields)
		fmt.Printf("\n")
		fmt.Printf("Embedded SQL:\n")
		fmt.Printf("  Query fragments:%d\n", stats.QueryFragments)
		fmt.Printf("\n")
		fmt.Printf("Report Entities:\n")
		fmt.Printf("  Forms:          %d\n", stats.Forms)
		fmt.Printf("  Report fields:  %d\n", stats.ReportFields)
		fmt.Printf("  Report params:  %d\n", stats.ReportParams)
		fmt.Printf("  VB functions:   %d\n", stats.VBFunctions)
		fmt.Printf("\n")
		fmt.Printf("API XML Entities:\n")
		fmt.Printf("  Contracts:      %d\n", stats.APIContracts)
		fmt.Printf("  Params:         %d\n", stats.APIParams)
		fmt.Printf("  Tables:         %d\n", stats.APITables)
		fmt.Printf("  Table fields:   %d\n", stats.APITableFields)
		fmt.Printf("  Table indexes:  %d\n", stats.APITableIndexes)
		fmt.Printf("\n")
		fmt.Printf("Errors:           %d\n", stats.Errors)

		return nil
	},
}

func init() {
	initCmd.Flags().StringVarP(&rootPath, "path", "p", "", "root path to scan")
	initCmd.Flags().IntVarP(&parallel, "parallel", "j", 4, "number of parallel workers")
	initCmd.Flags().BoolVar(&noConfirm, "yes", false, "skip confirmation prompts")
	rootCmd.AddCommand(initCmd)
}
