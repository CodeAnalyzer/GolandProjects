package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/indexer"
	"github.com/codebase/internal/mcp"
	"github.com/codebase/internal/rti"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/trc"
	"github.com/codebase/internal/util"
	"github.com/spf13/cobra"
)

var (
	appName        = "CodeBase"
	version        = "0.9.4"
	buildNumber    = "1657"
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
of Diasoft 5NT codebase (SQL, H, PAS, DFM, SMF, JS, TPR, RPT, XML, MD, YAML files).

Supported modes:
  init   - full scan and index building
  update - incremental update by modified files
  review - SQL file checks before deployment
  query  - point-in-time index queries for symbols, tables, DFM forms/components/captions, SQL fragments, reports, JS, SMF and relations
  stats  - index summary
  rti    - RTI trace log analyzer (parse, summary, tree, errors, slow)
  trc    - SQL Server Profiler trace analyzer (parse, summary, events, procedures, tree, slow, errors)
  dsinfo - DiasoftSystemInfo profile manager (parse, list, show, delete, use)
  mcp    - MCP JSON-RPC server over stdio`,
		Version: version,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return initConfigE()
		},
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
	// Root context with signal handling — Ctrl+C / SIGTERM cancel all operations.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	rootCmd.SetContext(ctx)
	// Cobra сам разбирает args/flags и вызывает подходящую подкоманду.
	err = rootCmd.ExecuteContext(ctx)
	if err != nil {
		// Machine-readable режимы: ошибка уходит в структурированный envelope,
		// но возвращается как есть — deferred-логгер фиксирует status=error,
		// main.go завершает процесс с кодом 1 (readonly-schema-policy). Дубля
		// текста в stderr нет: SilenceErrors включён для machine-readable режимов.
		if isQueryJSONMode(args) {
			commandName := detectQueryCommandName(args)
			if writeErr := writeQueryErrorResponse(commandName, err); writeErr != nil {
				return writeErr
			}
			return err
		}
		if isStatsJSONMode(args) {
			if writeErr := writeStatsErrorResponse(err); writeErr != nil {
				return writeErr
			}
			return err
		}
		if isHealthJSONMode(args) {
			if writeErr := writeHealthErrorResponse(err); writeErr != nil {
				return writeErr
			}
			return err
		}
		if isReviewJSONMode(args) {
			if writeErr := writeReviewErrorResponse(err); writeErr != nil {
				return writeErr
			}
			return err
		}
		if isDsinfoJSONMode(args) {
			// Ошибки уровня Cobra (неизвестный флаг/команда, неверные аргументы)
			// не доходят до RunE — конверт пишется здесь, чтобы dsinfo --json
			// не завершался молча (SilenceErrors включён machine-readable режимом).
			if writeErr := writeDsinfoErrorResponse(detectDsinfoCommandName(args), err); writeErr != nil {
				return writeErr
			}
			return err
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
	return isQueryJSONMode(args) || isStatsJSONMode(args) || isHealthJSONMode(args) || isReviewJSONMode(args) || isMCPMode(args) || isDsinfoJSONMode(args)
}

func isDsinfoJSONMode(args []string) bool {
	if len(args) == 0 || args[0] != "dsinfo" {
		return false
	}
	for _, arg := range args[1:] {
		if arg == "--json" {
			return true
		}
	}
	return false
}

func detectDsinfoCommandName(args []string) string {
	if len(args) < 2 || strings.HasPrefix(args[1], "-") {
		return "dsinfo"
	}
	return "dsinfo " + args[1]
}

// writeDsinfoErrorResponse печатает machine-readable конверт ошибки dsinfo
// и возвращает исходную ошибку наверх (exit code 1, лог status=error).
func writeDsinfoErrorResponse(command string, err error) error {
	response := dsinfoErrorResponse{
		Success:       false,
		FormatVersion: "1.0",
		Command:       command,
		Error:         dsinfoErrorBody{Code: classifyDsinfoError(err), Message: err.Error()},
	}
	if writeErr := writeJSON(os.Stdout, response); writeErr != nil {
		return writeErr
	}
	return err
}

func isMCPMode(args []string) bool {
	return len(args) > 0 && args[0] == "mcp"
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
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default: codebase.toml)")
}

func initConfigE() error {
	// Явно переданный путь имеет приоритет над автопоиском codebase.toml.
	if cfgFile != "" {
		config.SetConfigFile(cfgFile)
	}

	if err := config.Load(); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("loading config: %w", err)
		}
		// Отсутствие файла конфигурации не является ошибкой для старта CLI:
		// команда init может создать его позже с дефолтными значениями.
	}

	// Применяем параметры из конфига к пакетам, не имеющим прямого доступа к config.
	applyConfigToPackages()
	return nil
}

func applyConfigToPackages() {
	cfg := config.Get()
	if cfg == nil {
		return
	}
	trc.SetBatchSize(cfg.Indexer.BatchInsertSize)
	rti.SetBatchSize(cfg.Indexer.BatchInsertSize)
	mcp.SetQueryLimits(cfg.Query.DefaultLimit, cfg.Query.MaxLimit)
	rti.SetSlowThresholdMs(cfg.RTI.SlowThresholdMs)
	rti.SetTopSlowCount(cfg.RTI.TopSlowCount)
	trc.SetSlowThresholdMs(cfg.TRC.SlowThresholdMs)
	trc.SetEnrichWorkers(cfg.TRC.MaxEnrichWorkers, cfg.TRC.MinProcsForParallelEnrich)
	indexer.SetProgressInterval(cfg.Indexer.ProgressIntervalMs)
	if ttl, err := time.ParseDuration(cfg.MCP.PaginationTTL); err == nil && ttl > 0 {
		mcp.SetPaginationTTL(ttl)
	}
	if cfg.MCP.RegexpCacheMaxEntries > 0 {
		util.SetRegexpCacheMaxEntries(cfg.MCP.RegexpCacheMaxEntries)
	}
}

// applyQueryLimit applies default/max limit from config to a raw limit value.
func applyQueryLimit(limit int) int {
	cfg := config.Get()
	if cfg != nil {
		if limit <= 0 {
			limit = cfg.Query.DefaultLimit
		}
		if limit > cfg.Query.MaxLimit {
			limit = cfg.Query.MaxLimit
		}
	} else {
		if limit <= 0 {
			limit = 100
		}
		if limit > 1000 {
			limit = 1000
		}
	}
	return limit
}

// closeDB safely closes a DB connection. No-op if db is nil.
func closeDB(db *store.DB) {
	if db != nil {
		db.Close()
	}
}

// openDBReady открывает БД и выполняет read-only проверку готовности схемы
// (readonly-schema-policy: команды, кроме init/update, не выполняют DDL).
// Ошибки типизированы — конфиг/подключение/схема; команда пробрасывает их в ответ.
func openDBReady(ctx context.Context) (*store.DB, error) {
	if ctx == nil {
		// Прямые вызовы (тесты) могут передать nil — RunE в Cobra всегда
		// получает контекст от ExecuteContext.
		ctx = context.Background()
	}
	cfg := config.Get()
	if cfg == nil {
		return nil, errs.ErrConfigNotLoaded
	}
	db, err := store.NewDB(cfg.DB)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errs.ErrDBConnect, err)
	}
	if err := db.CheckSchemaVersion(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// openDBOptional — вариант для команд rti/trc с файловым режимом: БД
// желательна, но необязательна. Схема не готова при доступной БД —
// типизированная ошибка; БД/конфиг недоступны — (nil, nil): прежний
// offline-режим (сервисный слой разбирает файл без сохранения).
func openDBOptional(ctx context.Context) (*store.DB, error) {
	db, err := openDBReady(ctx)
	if err == nil {
		return db, nil
	}
	switch {
	case errors.Is(err, store.ErrSchemaNotInitialized), errors.Is(err, store.ErrSchemaUpdateRequired):
		// Схема не готова при доступной БД — жёсткая различимая ошибка.
		return nil, err
	case errors.Is(err, errs.ErrDBConnect), errors.Is(err, errs.ErrConfigNotLoaded):
		// БД/конфиг недоступны — прежний offline-режим: rtisvc/trcsvc
		// разбирают файл без сохранения (warning «database unavailable»).
		return nil, nil
	default:
		// Прочие сбои проверки схемы (таймаут, конфликт, отмена) — наружу,
		// а не молчаливый offline-разбор.
		return nil, err
	}
}
