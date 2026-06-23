package cmd

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

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

var (
	reviewOutputJSON  bool
	reviewRulesRaw    string
	reviewMinSeverity int
)

var reviewCmd = &cobra.Command{
	Use:   "review <sql-file-path>",
	Short: "Run SQL review checks for one file",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		filePath := strings.TrimSpace(args[0])
		opts, rawRules, err := buildReviewOptions(reviewRulesRaw, reviewMinSeverity)
		if err != nil {
			return handleReviewError(err)
		}
		var progress *reviewProgress
		if !reviewOutputJSON {
			progress = newReviewProgress(filePath)
			progress.start()
		}

		var onProgress func(completed, total int)
		if progress != nil {
			onProgress = progress.update
		}
		result, err := reviewsvc.Execute(filePath, opts, onProgress)
		if progress != nil {
			progress.stop()
		}
		if err != nil {
			return handleReviewError(err)
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

		fmt.Printf("Review file: %s\n", filePath)
		fmt.Printf("Findings: %d\n\n", len(result.Findings))
		for _, finding := range result.Findings {
			fmt.Printf("- [%d] %s line=%d object=%s\n  %s\n", finding.Severity, finding.Rule, finding.Line, finding.Object, finding.Message)
		}
		return nil
	},
}

func buildReviewOptions(rawRules string, minSeverity int) (review.Options, []string, error) {
	rules, raw, err := parseReviewRules(rawRules)
	if err != nil {
		return review.Options{}, nil, err
	}
	if minSeverity <= 0 {
		minSeverity = review.SeverityFineCode
	}
	return review.Options{Rules: rules, MinSeverity: minSeverity}, raw, nil
}

func parseReviewRules(raw string) ([]review.RuleID, []string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil, nil
	}
	parts := strings.Split(trimmed, ",")
	result := make([]review.RuleID, 0, len(parts))
	resultRaw := make([]string, 0, len(parts))
	seen := make(map[review.RuleID]struct{})
	for _, part := range parts {
		rule := review.RuleID(strings.TrimSpace(part))
		if rule == "" {
			continue
		}
		switch rule {
		case review.RuleForeignTablesUsing, review.RuleForeignPTablesUsing, review.RuleForeignProcedureUsing, review.RuleExecNotExistsProc, review.RuleProcDuplicate, review.RuleProcParamDefValue, review.RuleProcElseCase, review.RuleUseSelectAll, review.RuleTruncTbl, review.RuleDatatype, review.RuleAnsiInJoin, review.RuleInsertRowLock, review.RuleUseEqColumn, review.RuleTableFullScan, review.RuleTableHintExists, review.RuleTableHintIsRight, review.RuleIndexExistsInDB, review.RuleIndexWrong, review.RuleUpdateOnlyVar, review.RulePTableSpid, review.RuleForceOrder2Tbl, review.RuleSaveTran, review.RuleUseDrop, review.RuleMathOperations, review.RuleExistsWithAndInIf, review.RuleNullComparison, review.RuleShouldBeCP866, review.RuleTooManyJoins, review.RuleMaxProcParam, review.RuleModifyOutProc, review.RuleEmptyReturn, review.RuleRawTransactionControl, review.RuleDeferredUpdate, review.RuleInSubQuery, review.RuleVarcharSize, review.RuleColumnInsert, review.RulePostgreLabelGotoLevel, review.RuleDateIntoString, review.RuleEmptyStringDate, review.RuleVarUseAfterCursor, review.RuleExcessProcParams, review.RuleDuplicateOutputVariable, review.RuleUseOnlyDeclaredCursors, review.RuleCursorFetchArguments, review.RuleUsageVarInSameSelect, review.RuleVarAssignInUpdate, review.RuleStatementsWithJoinsRequireAliases, review.RuleUseFuncInIndCol, review.RuleIsNullSameTypes, review.RuleDiffTypesComparison, review.RuleFloatToStringConvert, review.RuleSelectAfterSetRowcount, review.RuleAliasWhenUsingUnion:
		default:
			return nil, nil, fmt.Errorf("unknown review rule: %s", rule)
		}
		if _, exists := seen[rule]; exists {
			continue
		}
		seen[rule] = struct{}{}
		result = append(result, rule)
		resultRaw = append(resultRaw, string(rule))
	}
	return result, resultRaw, nil
}

func handleReviewError(err error) error {
	if !reviewOutputJSON {
		return err
	}
	return writeReviewErrorResponse(err)
}

func writeReviewErrorResponse(err error) error {
	response := reviewErrorResponse{
		Success:       false,
		FormatVersion: "1.0",
		Command:       "review",
		Error: queryErrorBody{
			Code:    classifyReviewError(err),
			Message: err.Error(),
		},
	}
	return writeJSON(os.Stdout, response)
}

func classifyReviewError(err error) string {
	message := err.Error()
	switch {
	case message == "config not loaded":
		return "config_error"
	case containsAny(message, "required flag", "accepts", "unknown review rule"):
		return "invalid_arguments"
	case containsAny(message, "failed to connect to database", "connection refused", "dial tcp"):
		return "database_unavailable"
	case containsAny(message, "failed to init schema"):
		return "schema_init_failed"
	case containsAny(message, "review failed"):
		return "review_failed"
	default:
		return "internal_error"
	}
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
	filePath string
	mu       sync.Mutex
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
				if total > 0 {
					fmt.Fprintf(os.Stderr, "\rreview %s %s checked=%d/%d", frame, p.filePath, completed, total)
				} else {
					fmt.Fprintf(os.Stderr, "\rreview %s %s %s", frame, p.filePath, spinnerFrames[0])
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

func init() {
	reviewCmd.Flags().BoolVar(&reviewOutputJSON, "json", false, "output as JSON")
	reviewCmd.Flags().StringVar(&reviewRulesRaw, "rules", "", "comma-separated rules (foreignTablesUsing,foreignPTablesUsing,foreignProcedureUsing,execNotExistsProc,procDuplicate,procParamDefValue,procElseCase,useSelectAll,truncTbl,datatype,ansiInJoin,insertRowLock,useEqColumn,tableFullScan,tableHintExists,tableHintIsRight,indexExistsInDB,indexWrong,updateOnlyVar,pTableSpid,forceOrder2Tbl,saveTran,useDrop,mathOperations,existsWithAndInIf,nullComparison,shouldBeCP866,tooManyJoins,maxProcParam,modifyOutProc,emptyReturn,rawTransactionControl,deferredUpdate,inSubQuery,varcharSize,columnInsert,postgreLabelGotoLevel,dateIntoString,emptyStringDate,cursorFetchArguments,usageVarInSameSelect,varAssignInUpdate,statementsWithJoinsRequireAliases,useFuncInIndCol,isNullSameTypes,diffTypesComparison,floatToStringConvert,selectAfterSetRowcount,aliasWhenUsingUnion)")
	reviewCmd.Flags().IntVar(&reviewMinSeverity, "min-severity", review.SeverityFineCode, "minimum severity to output")
	rootCmd.AddCommand(reviewCmd)
}
