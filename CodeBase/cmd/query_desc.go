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
		results, err := querysvc.Execute(func(q *query.Query) (interface{}, error) {
			return q.SearchDescriptions(ctx, descSearchText, descSearchKinds, limit)
		})
		if err != nil {
			return handleQueryError("query desc-search", err)
		}
		output, ok := results.(*query.DescriptionSearchOutput)
		if !ok {
			return fmt.Errorf("unexpected desc-search result type %T", results)
		}
		return printDescriptionResults("query desc-search", map[string]string{
			"text":  descSearchText,
			"kinds": strings.Join(descSearchKinds, ","),
		}, output)
	},
}

func init() {
	queryDescSearchCmd.Flags().StringVar(&descSearchText, "text", "", "search text (procedure or contract descriptions)")
	queryDescSearchCmd.Flags().StringSliceVar(&descSearchKinds, "kind", nil, "filter by kind: procedure, service, event, callback_event, used_service (repeatable)")
	cobra.CheckErr(queryDescSearchCmd.MarkFlagRequired("text"))
	queryCmd.AddCommand(queryDescSearchCmd)
}

// printDescriptionResults — печать выдачи desc-search: JSON-конверт содержит
// meta.has_more (признак усечения) и meta.semantic_meta (диагностика semantic-
// слоя); пустой результат — [] без ошибки.
func printDescriptionResults(commandName string, filters map[string]string, output *query.DescriptionSearchOutput) error {
	if output == nil {
		output = &query.DescriptionSearchOutput{Items: []query.DescriptionSearchResult{}}
	}
	items := output.Items
	if items == nil {
		items = []query.DescriptionSearchResult{}
	}

	if outputNDJSON {
		return writeNDJSON(os.Stdout, items)
	}

	if outputJSON {
		response := querySuccessResponse{
			Success:       true,
			FormatVersion: "1.0",
			Command:       commandName,
			Count:         len(items),
			Items:         items,
			Meta: queryResponseMeta{
				Limit:        limit,
				Filters:      filterEmptyValues(filters),
				Output:       detectQueryOutputMode(),
				HasMore:      output.HasMore,
				SemanticMeta: output.SemanticMeta,
			},
		}
		if outputSummary {
			response.Summary = buildQuerySummary(items)
		}
		return writeJSON(os.Stdout, response)
	}

	if outputSummary {
		return writeJSON(os.Stdout, buildQuerySummary(items))
	}

	if output.HasMore {
		fmt.Fprintf(os.Stdout, "(truncated at --limit %d, use --limit to get more)\n", limit)
	}
	fmt.Printf("%+v\n", items)
	return nil
}
