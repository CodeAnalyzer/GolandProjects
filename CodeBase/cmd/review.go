package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/review"
	"github.com/codebase/internal/reviewsvc"
	"github.com/spf13/cobra"
)

type reviewResponseMeta struct {
	Rules       []string `json:"rules,omitempty"`
	MinSeverity int      `json:"min_severity,omitempty"`
}

type reviewSuccessResponse struct {
	Success       bool               `json:"success"`
	FormatVersion string             `json:"format_version"`
	Command       string             `json:"command"`
	Count         int                `json:"count"`
	Items         []review.Finding   `json:"items"`
	Summary       review.Summary     `json:"summary"`
	Meta          reviewResponseMeta `json:"meta"`
}

type reviewErrorResponse struct {
	Success       bool           `json:"success"`
	FormatVersion string         `json:"format_version"`
	Command       string         `json:"command"`
	Error         queryErrorBody `json:"error"`
}

type reviewBatchFiles struct {
	Total     int                      `json:"total"`
	Succeeded int                      `json:"succeeded"`
	Failed    int                      `json:"failed"`
	Results   []review.BatchFileResult `json:"results"`
}

type reviewBatchSuccessResponse struct {
	Success       bool               `json:"success"`
	FormatVersion string             `json:"format_version"`
	Command       string             `json:"command"`
	Mode          string             `json:"mode"`
	Count         int                `json:"count"`
	Items         []review.Finding   `json:"items"`
	Summary       review.Summary     `json:"summary"`
	Files         reviewBatchFiles   `json:"files"`
	Meta          reviewResponseMeta `json:"meta"`
}

var (
	reviewOutputJSON  bool
	reviewRulesRaw    string
	reviewMinSeverity int
	reviewFilesRaw    string
)

var reviewCmd = &cobra.Command{
	Use:     "review <sql-file-path>",
	Short:   "Run SQL review checks for one file (.sql or .h) or a file list",
	Example: "  CodeBase.exe review path/to/file.sql\n  CodeBase.exe review --files list.txt --json",
	Args:    cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		opts, rawRules, err := buildReviewOptions(reviewRulesRaw, reviewMinSeverity, analysisDsInfoID)
		if err != nil {
			return err
		}

		if reviewFilesRaw != "" {
			if len(args) > 0 {
				return fmt.Errorf("%w: positional file path cannot be used with --files", errs.ErrInvalidArguments)
			}
			paths, err := readReviewFileList(reviewFilesRaw)
			if err != nil {
				return fmt.Errorf("%w: %v", errs.ErrInvalidArguments, err)
			}
			if len(paths) == 0 {
				return fmt.Errorf("%w: file list is empty: %s", errs.ErrInvalidArguments, reviewFilesRaw)
			}
			return executeReviewBatch(ctx, paths, opts, rawRules)
		}

		if len(args) == 0 {
			return fmt.Errorf("%w: sql file path is required (or use --files)", errs.ErrInvalidArguments)
		}
		return executeReviewSingle(ctx, strings.TrimSpace(args[0]), opts, rawRules)
	},
}

func executeReviewSingle(ctx context.Context, filePath string, opts review.Options, rawRules []string) error {
	var progress *reviewProgress
	if !reviewOutputJSON {
		progress = newReviewProgress(filePath)
		progress.start()
	}

	var onProgress func(completed, total int)
	if progress != nil {
		onProgress = progress.update
	}
	result, err := reviewsvc.ExecuteCtx(ctx, filePath, opts, onProgress)
	if progress != nil {
		progress.stop()
	}
	if err != nil {
		return err
	}

	if reviewOutputJSON {
		response := reviewSuccessResponse{
			Success:       true,
			FormatVersion: "1.0",
			Command:       "review",
			Count:         len(result.Findings),
			Items:         result.Findings,
			Summary:       result.Summary,
			Meta: reviewResponseMeta{
				Rules:       rawRules,
				MinSeverity: opts.MinSeverity,
			},
		}
		return writeJSON(os.Stdout, response)
	}

	printReviewFile(filePath, result)
	return nil
}

func executeReviewBatch(ctx context.Context, paths []string, opts review.Options, rawRules []string) error {
	hooks := review.BatchHooks{}
	var progress *reviewBatchProgress
	if !reviewOutputJSON {
		progress = newReviewBatchProgress()
		progress.start()
		defer progress.stop()
		hooks.OnFileStart = progress.onFileStart
		hooks.OnRuleProgress = progress.onRuleProgress
		hooks.OnFileDone = func(file review.BatchFileResult, result *review.Result) {
			printReviewBatchFile(file, result)
		}
	}

	result, err := reviewsvc.ExecuteBatchCtx(ctx, paths, opts, hooks)
	if progress != nil {
		progress.stop()
	}
	if err != nil {
		return err
	}

	if reviewOutputJSON {
		response := newReviewBatchResponse(result, rawRules, opts.MinSeverity)
		if err := writeJSON(os.Stdout, response); err != nil {
			return err
		}
	} else {
		fmt.Printf("Files: %d, succeeded: %d, failed: %d, findings: %d\n",
			result.Total, result.Succeeded, result.Failed, len(result.Findings))
	}

	if result.Failed > 0 {
		return fmt.Errorf("%w: %d file(s) failed", errs.ErrAlreadyReported, result.Failed)
	}
	return nil
}

func newReviewBatchResponse(result *review.BatchResult, rawRules []string, minSeverity int) reviewBatchSuccessResponse {
	items := result.Findings
	if items == nil {
		items = []review.Finding{}
	}
	results := result.Files
	if results == nil {
		results = []review.BatchFileResult{}
	}
	return reviewBatchSuccessResponse{
		Success:       result.Failed == 0,
		FormatVersion: "1.0",
		Command:       "review",
		Mode:          "batch",
		Count:         len(items),
		Items:         items,
		Summary:       result.Summary,
		Files: reviewBatchFiles{
			Total:     result.Total,
			Succeeded: result.Succeeded,
			Failed:    result.Failed,
			Results:   results,
		},
		Meta: reviewResponseMeta{
			Rules:       rawRules,
			MinSeverity: minSeverity,
		},
	}
}

func printReviewFile(filePath string, result *review.Result) {
	fmt.Printf("Review file: %s\n", filePath)
	fmt.Printf("Findings: %d\n\n", len(result.Findings))
	for _, finding := range result.Findings {
		fmt.Printf("- [%d] %s line=%d object=%s\n  %s\n", finding.Severity, finding.Rule, finding.Line, finding.Object, finding.Message)
	}
}

func printReviewBatchFile(file review.BatchFileResult, result *review.Result) {
	if file.Status == "error" {
		message := ""
		if file.Error != nil {
			message = file.Error.Message
		}
		fmt.Printf("Review file: %s\nError: %s\n\n", file.File, message)
		return
	}
	fmt.Printf("Review file: %s\n", file.File)
	fmt.Printf("Findings: %d\n", file.Count)
	for _, finding := range result.Findings {
		fmt.Printf("- [%d] %s line=%d object=%s\n  %s\n", finding.Severity, finding.Rule, finding.Line, finding.Object, finding.Message)
	}
	fmt.Println()
}

func buildReviewOptions(rawRules string, minSeverity int, dsinfoID int64) (review.Options, []string, error) {
	rules, raw, err := parseReviewRules(rawRules)
	if err != nil {
		return review.Options{}, nil, err
	}
	if minSeverity <= 0 {
		minSeverity = review.SeverityFineCode
	}
	return review.Options{Rules: rules, MinSeverity: minSeverity, DsinfoID: dsinfoID}, raw, nil
}

func parseReviewRules(raw string) ([]review.RuleID, []string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil, nil
	}
	parts := strings.Split(trimmed, ",")
	rawParts := make([]string, 0, len(parts))
	for _, part := range parts {
		s := strings.TrimSpace(part)
		if s != "" {
			rawParts = append(rawParts, s)
		}
	}
	rules, err := review.ValidateRuleIDs(rawParts)
	if err != nil {
		return nil, nil, err
	}
	return rules, rawParts, nil
}

func writeReviewErrorResponse(err error) error {
	response := reviewErrorResponse{
		Success:       false,
		FormatVersion: "1.0",
		Command:       "review",
		Error: queryErrorBody{
			Code:    reviewsvc.ClassifyError(err),
			Message: err.Error(),
		},
	}
	return writeJSON(os.Stdout, response)
}

// readReviewFileList читает список путей (UTF-8, один путь на строку).
// Пустые строки и #-комментарии игнорируются, парные кавычки снимаются,
// дубликаты схлопываются с сохранением порядка первого вхождения.
func readReviewFileList(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read file list %s: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	seen := make(map[string]struct{})
	paths := make([]string, 0)
	first := true
	for scanner.Scan() {
		line := scanner.Text()
		if first {
			line = strings.TrimPrefix(line, "\ufeff")
			first = false
		}
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = trimPairedQuotes(line)
		if line == "" {
			continue
		}
		// Дедуп согласован с индексным lookup'ом (LOWER(path)): регистр и
		// сепараторы Windows/Linux не должны давать две проверки одного файла.
		key := strings.ToLower(filepath.ToSlash(line))
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		paths = append(paths, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read file list %s: %w", path, err)
	}
	return paths, nil
}

func trimPairedQuotes(value string) string {
	if len(value) < 2 {
		return value
	}
	if value[0] == value[len(value)-1] && (value[0] == '"' || value[0] == '\'') {
		return strings.TrimSpace(value[1 : len(value)-1])
	}
	return value
}

func isReviewJSONMode(args []string) bool {
	if len(args) == 0 || args[0] != "review" {
		return false
	}
	for _, arg := range args[1:] {
		if arg == "--json" {
			return true
		}
	}
	return false
}

var spinnerFrames = []string{"|", "/", "-", "\\"}

type reviewProgress struct {
	filePath  string
	mu        sync.Mutex
	completed int
	total     int
	stopCh    chan struct{}
	done      chan struct{}
	stopped   bool
}

func newReviewProgress(filePath string) *reviewProgress {
	return &reviewProgress{
		filePath: filePath,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func (p *reviewProgress) update(completed, total int) {
	p.mu.Lock()
	p.completed = completed
	p.total = total
	p.mu.Unlock()
}

func (p *reviewProgress) start() {
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-p.stopCh:
				fmt.Fprintf(os.Stderr, "\r\033[K")
				return
			case <-ticker.C:
				p.mu.Lock()
				completed, total := p.completed, p.total
				p.mu.Unlock()
				frame := spinnerFrames[i%len(spinnerFrames)]
				displayName := filepath.Base(p.filePath)
				if total > 0 {
					fmt.Fprintf(os.Stderr, "\r\033[Kreview %s %s checked=%d/%d", frame, displayName, completed, total)
				} else {
					fmt.Fprintf(os.Stderr, "\r\033[Kreview %s %s", frame, displayName)
				}
				i++
			}
		}
	}()
}

func (p *reviewProgress) stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	p.mu.Unlock()
	close(p.stopCh)
	<-p.done
}

type reviewBatchProgress struct {
	mu            sync.Mutex
	completed     int
	total         int
	current       string
	ruleCompleted int
	ruleTotal     int
	stopCh        chan struct{}
	done          chan struct{}
	stopped       bool
}

func newReviewBatchProgress() *reviewBatchProgress {
	return &reviewBatchProgress{
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (p *reviewBatchProgress) onFileStart(completed, total int, current string) {
	p.mu.Lock()
	p.completed = completed
	p.total = total
	p.current = current
	p.ruleCompleted = 0
	p.ruleTotal = 0
	p.mu.Unlock()
}

func (p *reviewBatchProgress) onRuleProgress(completed, total int) {
	p.mu.Lock()
	p.ruleCompleted = completed
	p.ruleTotal = total
	p.mu.Unlock()
}

func (p *reviewBatchProgress) start() {
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-p.stopCh:
				fmt.Fprintf(os.Stderr, "\r\033[K")
				return
			case <-ticker.C:
				p.mu.Lock()
				completed, total := p.completed, p.total
				current := p.current
				ruleCompleted, ruleTotal := p.ruleCompleted, p.ruleTotal
				p.mu.Unlock()
				frame := spinnerFrames[i%len(spinnerFrames)]
				displayName := filepath.Base(current)
				fmt.Fprintf(os.Stderr, "\r\033[Kreview %s [%d/%d] %s", frame, completed+1, total, displayName)
				if ruleTotal > 0 {
					fmt.Fprintf(os.Stderr, " rules %d/%d", ruleCompleted, ruleTotal)
				}
				i++
			}
		}
	}()
}

func (p *reviewBatchProgress) stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	p.mu.Unlock()
	close(p.stopCh)
	<-p.done
}

func init() {
	reviewCmd.Flags().BoolVar(&reviewOutputJSON, "json", false, "output as JSON")
	reviewCmd.Flags().StringVar(&reviewRulesRaw, "rules", "", "comma-separated rules ("+review.RuleListString()+")")
	reviewCmd.Flags().IntVar(&reviewMinSeverity, "min-severity", review.SeverityFineCode, "minimum severity to output")
	reviewCmd.Flags().StringVar(&reviewFilesRaw, "files", "", "path to a file with SQL file paths (one per line) for batch review")
	reviewCmd.PersistentFlags().Int64Var(&analysisDsInfoID, "dsinfo", 0, "dsinfo profile id for foreign product branch resolution (0 = env/active chain)")
	rootCmd.AddCommand(reviewCmd)
}
