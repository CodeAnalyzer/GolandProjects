package cmd

import (
	"fmt"
	"time"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/indexer"
	"github.com/codebase/internal/store"
	"github.com/spf13/cobra"
)

var (
	onlyModified bool
)

var updateCmd = &cobra.Command{
	Use:   "update",
	Short: "Incremental index update",
	Long: `Scans files and updates only changed entries in the index.
Uses file hashes to detect changes.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// update опирается на уже сохранённый root_path в config,
		// потому что должен обновлять тот же индекс, который был создан init-ом.
		cfg := config.Get()
		if cfg == nil || cfg.RootPath == "" {
			return fmt.Errorf("root path not configured")
		}

		effectiveParallel := parallel
		if !cmd.Flags().Changed("parallel") && cfg.Indexer.Parallel > 0 {
			effectiveParallel = cfg.Indexer.Parallel
		}

		db, err := store.NewDB(cfg.DB)
		if err != nil {
			return fmt.Errorf("failed to connect to database: %w", err)
		}
		defer db.Close()

		// onlyModified позволяет переключаться между настоящим incremental update
		// и принудительным повторным проходом по всем файлам без полной re-init операции.
		idx := indexer.New(db, cfg)
		startedAt := time.Now()
		fmt.Printf("\nUpdating started: %s\n", startedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("rootPath=%s parallel=%d\n", cfg.RootPath, effectiveParallel)
		stats, err := idx.Update(cfg.RootPath, onlyModified, effectiveParallel)
		if err != nil {
			return fmt.Errorf("update failed: %w", err)
		}

		finishedAt := time.Now()
		fmt.Printf("\nUpdating completed: %s\n", finishedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("  Files scanned:  %d\n", stats.FilesScanned)
		fmt.Printf("  Files indexed:  %d\n", stats.FilesIndexed)
		fmt.Printf("  Files updated:  %d\n", stats.FilesUpdated)
		fmt.Printf("  Files added:    %d\n", stats.FilesAdded)
		fmt.Printf("  Files deleted:  %d\n", stats.FilesDeleted)
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
	updateCmd.Flags().BoolVar(&onlyModified, "modified", true, "scan only modified files")
	updateCmd.Flags().IntVarP(&parallel, "parallel", "j", 4, "number of parallel workers")
	rootCmd.AddCommand(updateCmd)
}
