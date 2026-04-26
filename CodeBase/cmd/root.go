package cmd

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/codebase/internal/config"
	"github.com/spf13/cobra"
)

var (
	appName        = "CodeBase"
	version        = "0.6.4"
	buildNumber    = "608"
	copyright      = "Copyright (c) 2026"
	cfgFile        string
	commandLogger  *log.Logger
	commandLogFile *os.File
	// rootCmd описывает только общую оболочку CLI.
	// Реальная работа выполняется в дочерних командах init/update/query/stats.
	rootCmd = &cobra.Command{
		Use:   "codebase",
		Short: "Local indexer for Diasoft 5NT source code",
		Long: `CodeBase - tool for indexing and semantic navigation
of Diasoft 5NT codebase (SQL, H, PAS, DFM, SMF, JS, TPR, RPT, XML files).

Supported modes:
  init   - full scan and index building
  update - incremental update by modified files
  query  - point-in-time index queries for symbols, tables, DFM forms/components/captions, SQL fragments, reports, JS, SMF and relations
  stats  - index summary`,
		Version: version,
	}
)

// Execute executes the root command.
func Execute() (err error) {
	args := os.Args[1:]
	startedAt := time.Now()
	initCommandLogger(startedAt)
	defer closeCommandLogger()
	defer func() {
		logCommandExecution(startedAt, args, err)
	}()
	if isMachineReadableMode(args) {
		rootCmd.SilenceErrors = true
		rootCmd.SilenceUsage = true
	}
	if shouldPrintBanner(args) {
		fmt.Printf("%s %s build %s\n%s\n", appName, version, buildNumber, copyright)
	}
	rootCmd.Version = version
	// Cobra сам разбирает args/flags и вызывает подходящую подкоманду.
	err = rootCmd.Execute()
	if err != nil {
		if isQueryJSONMode(args) {
			commandName := detectQueryCommandName(args)
			if writeErr := writeQueryErrorResponse(commandName, err); writeErr != nil {
				return writeErr
			}
			return nil
		}
		if isStatsJSONMode(args) {
			if writeErr := writeStatsErrorResponse(err); writeErr != nil {
				return writeErr
			}
			return nil
		}
		if isHealthJSONMode(args) {
			if writeErr := writeHealthErrorResponse(err); writeErr != nil {
				return writeErr
			}
			return nil
		}
	}
	return err
}

func initCommandLogger(startedAt time.Time) {
	if !isCommandLoggingEnabled() {
		commandLogger = nil
		commandLogFile = nil
		return
	}
	exePath, err := os.Executable()
	if err != nil {
		commandLogger = nil
		commandLogFile = nil
		return
	}
	exeDir := filepath.Dir(exePath)
	logName := filepath.Join(exeDir, fmt.Sprintf("codebase_%s.log", startedAt.Format("20060102")))
	file, err := os.OpenFile(logName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		commandLogger = nil
		commandLogFile = nil
		return
	}
	commandLogFile = file
	commandLogger = log.New(file, "", 0)
}

func closeCommandLogger() {
	if commandLogFile != nil {
		_ = commandLogFile.Close()
		commandLogFile = nil
	}
	commandLogger = nil
}

func logCommandExecution(startedAt time.Time, args []string, execErr error) {
	if commandLogger == nil {
		return
	}
	status := "success"
	errorText := ""
	if execErr != nil {
		status = "error"
		errorText = singleLineError(execErr)
	}
	commandText := strings.Join(append([]string{"codebase"}, args...), " ")
	commandLogger.Printf(
		"started_at=%s command=%q duration=%s duration_ms=%d status=%s error=%q",
		startedAt.Format("2006-01-02 15:04:05"),
		commandText,
		time.Since(startedAt).Round(time.Millisecond),
		time.Since(startedAt).Milliseconds(),
		status,
		errorText,
	)
}

func singleLineError(err error) string {
	if err == nil {
		return ""
	}
	return strings.Join(strings.Fields(err.Error()), " ")
}

func isCommandLoggingEnabled() bool {
	cfg := config.Get()
	if cfg == nil || cfg.Logging.CommandEnabled == nil {
		return true
	}
	return *cfg.Logging.CommandEnabled
}

func shouldPrintBanner(args []string) bool {
	if isMachineReadableMode(args) {
		return false
	}
	for _, arg := range args {
		if arg == "--json" {
			return false
		}
	}
	return true
}

func isQueryJSONMode(args []string) bool {
	if len(args) == 0 || args[0] != "query" {
		return false
	}
	for _, arg := range args[1:] {
		if arg == "--json" || arg == "--ndjson" || arg == "--summary" {
			return true
		}
	}
	return false
}

func isStatsJSONMode(args []string) bool {
	if len(args) == 0 || args[0] != "stats" {
		return false
	}
	for _, arg := range args[1:] {
		if arg == "--json" {
			return true
		}
	}
	return false
}

func isHealthJSONMode(args []string) bool {
	if len(args) == 0 || args[0] != "health" {
		return false
	}
	for _, arg := range args[1:] {
		if arg == "--json" {
			return true
		}
	}
	return false
}

func isMachineReadableMode(args []string) bool {
	return isQueryJSONMode(args) || isStatsJSONMode(args) || isHealthJSONMode(args)
}

func detectQueryCommandName(args []string) string {
	if len(args) < 2 || args[0] != "query" {
		return "query"
	}
	if args[1] == "--json" || args[1] == "--limit" || args[1] == "-h" || args[1] == "--help" {
		return "query"
	}
	if len(args[1]) > 0 && args[1][0] == '-' {
		return "query"
	}
	return "query " + args[1]
}

func init() {
	// Конфигурация поднимается один раз до выполнения любой команды,
	// чтобы все подкоманды работали с единым in-memory состоянием config.
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default: codebase.toml)")
}

func initConfig() {
	// Явно переданный путь имеет приоритет над автопоиском codebase.toml.
	if cfgFile != "" {
		config.SetConfigFile(cfgFile)
	}

	if err := config.Load(); err != nil {
		if !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
			os.Exit(1)
		}
		// Отсутствие файла конфигурации не является ошибкой для старта CLI:
		// команда init может создать его позже с дефолтными значениями.
	}
}
