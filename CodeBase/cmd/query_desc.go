package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/codebase/internal/query"
	"github.com/codebase/internal/querysvc"
	"github.com/spf13/cobra"
)

var (
	descSearchText  string
	descSearchKinds []string
)

var queryDescSearchCmd = &cobra.Command{
	Use:   "desc-search --text <query> [--kind <kind>]... [--limit N]",
	Short: "Full-text search over procedure and API contract descriptions",
	Long: `Search over human-readable descriptions: SQL procedure header comments and
API contract ShortDescription/FullDescription. Name matches rank above
description matches (weights A/B). A procedure implementing a contract with a
non-empty description is represented by the contract (kind=service).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		var hasMore bool
		results, err := querysvc.Execute(func(q *query.Query) (interface{}, error) {
			items, hm, err := q.SearchDescriptions(ctx, descSearchText, descSearchKinds, limit)
			if err != nil {
				return nil, err
			}
			hasMore = hm
			return items, nil
		})
		if err != nil {
			return handleQueryError("query desc-search", err)
		}
		return printDescriptionResults("query desc-search", map[string]string{
			"text":  descSearchText,
			"kinds": strings.Join(descSearchKinds, ","),
		}, results, hasMore)
	},
}

func init() {
	queryDescSearchCmd.Flags().StringVar(&descSearchText, "text", "", "search text (procedure or contract descriptions)")
	queryDescSearchCmd.Flags().StringSliceVar(&descSearchKinds, "kind", nil, "filter by kind: procedure, service, event, callback_event, used_service (repeatable)")
	cobra.CheckErr(queryDescSearchCmd.MarkFlagRequired("text"))
	queryCmd.AddCommand(queryDescSearchCmd)
}

// printDescriptionResults — печать выдачи desc-search: JSON-конверт содержит
// meta.has_more (признак усечения); пустой результат — [] без ошибки.
func printDescriptionResults(commandName string, filters map[string]string, results interface{}, hasMore bool) error {
	if outputNDJSON {
		return writeNDJSON(os.Stdout, normalizeNilResults(results))
	}

	if outputJSON {
		response := querySuccessResponse{
			Success:       true,
			FormatVersion: "1.0",
			Command:       commandName,
			Count:         resultCount(results),
			Items:         normalizeNilResults(results),
			Meta: queryResponseMeta{
				Limit:   limit,
				Filters: filterEmptyValues(filters),
				Output:  detectQueryOutputMode(),
				HasMore: hasMore,
			},
		}
		if outputSummary {
			response.Summary = buildQuerySummary(results)
		}
		return writeJSON(os.Stdout, response)
	}

	if outputSummary {
		return writeJSON(os.Stdout, buildQuerySummary(results))
	}

	items := normalizeNilResults(results)
	if hasMore {
		fmt.Fprintf(os.Stdout, "(truncated at --limit %d, use --limit to get more)\n", limit)
	}
	fmt.Printf("%+v\n", items)
	return nil
}
