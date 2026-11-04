package cmd

import (
	"fmt"
	"os"

	"github.com/codebase/internal/query"
)

// printQueryFragmentResults — печать выдачи sql-fragment: JSON-конверт
// содержит meta.truncated/hint (потолок кандидатов sql_fragment_match_cap,
// change optimize-query-search-bottlenecks, B5); пустой результат — [] без
// ошибки.
func printQueryFragmentResults(commandName string, filters map[string]string, output *query.QueryFragmentSearchOutput) error {
	if output == nil {
		output = &query.QueryFragmentSearchOutput{Items: []query.QueryFragmentResult{}}
	}
	items := output.Items
	if items == nil {
		items = []query.QueryFragmentResult{}
	}

	warnings := query.CollectBranchWarnings(items)
	if outputNDJSON {
		err := writeNDJSON(os.Stdout, items)
		printQueryWarnings(warnings, os.Stderr)
		// Диагностика усечения — в stderr, чтобы не ломать построчный
		// контракт stdout (по образцу warnings).
		if output.Truncated {
			fmt.Fprintln(os.Stderr, "("+query.QueryFragmentTruncatedHint+")")
		}
		return err
	}

	if outputJSON {
		meta := queryResponseMeta{
			Limit:     limit,
			Filters:   filterEmptyValues(filters),
			Output:    detectQueryOutputMode(),
			Truncated: output.Truncated,
		}
		if output.Truncated {
			meta.Hint = query.QueryFragmentTruncatedHint
		}
		response := querySuccessResponse{
			Success:       true,
			FormatVersion: "1.0",
			Command:       commandName,
			Count:         len(items),
			Items:         items,
			Meta:          meta,
			Warnings:      warnings,
		}
		if outputSummary {
			response.Summary = buildQuerySummary(items)
		}
		return writeJSON(os.Stdout, response)
	}

	if outputSummary {
		err := writeJSON(os.Stdout, buildQuerySummary(items))
		printQueryWarnings(warnings, os.Stderr)
		return err
	}

	if output.Truncated {
		fmt.Fprintln(os.Stdout, "("+query.QueryFragmentTruncatedHint+")")
	}
	fmt.Printf("%+v\n", items)
	printQueryWarnings(warnings, os.Stderr)
	return nil
}
