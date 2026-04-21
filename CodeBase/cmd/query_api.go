package cmd

import (
	"github.com/codebase/internal/query"
	"github.com/spf13/cobra"
)

var (
	apiContractName string
	apiTableName    string
	apiTableIndexName string
	apiParamName    string
	apiEventName    string
)

var queryAPIContractCmd = &cobra.Command{
	Use:   "api-contract --name <name>",
	Short: "Search DSArchitect API contracts",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query api-contract",
			filters: map[string]string{"name": apiContractName},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchAPIContract(apiContractName, limit)
			},
		})
	},
}

var queryAPITableCmd = &cobra.Command{
	Use:   "api-table --name <name>",
	Short: "Search DSArchitect API tables",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query api-table",
			filters: map[string]string{"name": apiTableName},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchAPITable(apiTableName, limit)
			},
		})
	},
}

var queryAPITableIndexCmd = &cobra.Command{
	Use:   "api-table-index --name <name>",
	Short: "Search indexes of standalone DSArchitect API tables",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query api-table-index",
			filters: map[string]string{"name": apiTableIndexName, "like": boolFilterValue(apiTableIndexLikeSearch)},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchAPITableIndex(apiTableIndexName, apiTableIndexLikeSearch, limit)
			},
		})
	},
}

var queryAPIParamCmd = &cobra.Command{
	Use:   "api-param --name <name>",
	Short: "Search DSArchitect API params",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query api-param",
			filters: map[string]string{"name": apiParamName},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchAPIParam(apiParamName, limit)
			},
		})
	},
}

var queryAPIImplCmd = &cobra.Command{
	Use:   "api-impl --name <name>",
	Short: "Show SQL implementations of API contracts",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query api-impl",
			filters: map[string]string{"name": apiContractName},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchAPIImplementations(apiContractName, limit)
			},
		})
	},
}

var queryAPIPublishersCmd = &cobra.Command{
	Use:   "api-publishers --event <name>",
	Short: "Show procedures publishing DSArchitect events",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query api-publishers",
			filters: map[string]string{"event": apiEventName},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchAPIPublishers(apiEventName, limit)
			},
		})
	},
}

var queryAPIConsumersCmd = &cobra.Command{
	Use:   "api-consumers --name <name>",
	Short: "Show procedures consuming API contracts",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runQueryCommand(queryCommandSpec{
			commandName: "query api-consumers",
			filters: map[string]string{"name": apiContractName},
			run: func(q *query.Query) (interface{}, error) {
				return q.SearchAPIConsumers(apiContractName, limit)
			},
		})
	},
}

func init() {
	queryAPIContractCmd.Flags().StringVar(&apiContractName, "name", "", "API contract name to search")
	cobra.CheckErr(queryAPIContractCmd.MarkFlagRequired("name"))
	queryAPITableCmd.Flags().StringVar(&apiTableName, "name", "", "API table name to search")
	cobra.CheckErr(queryAPITableCmd.MarkFlagRequired("name"))
	queryAPITableIndexCmd.Flags().StringVar(&apiTableIndexName, "name", "", "API table index or table name to search")
	queryAPITableIndexCmd.Flags().BoolVar(&apiTableIndexLikeSearch, "like", false, "use partial match search for API table index or table name")
	cobra.CheckErr(queryAPITableIndexCmd.MarkFlagRequired("name"))
	queryAPIParamCmd.Flags().StringVar(&apiParamName, "name", "", "API param name to search")
	cobra.CheckErr(queryAPIParamCmd.MarkFlagRequired("name"))
	queryAPIImplCmd.Flags().StringVar(&apiContractName, "name", "", "API contract name to search")
	cobra.CheckErr(queryAPIImplCmd.MarkFlagRequired("name"))
	queryAPIPublishersCmd.Flags().StringVar(&apiEventName, "event", "", "Event contract name to search")
	cobra.CheckErr(queryAPIPublishersCmd.MarkFlagRequired("event"))
	queryAPIConsumersCmd.Flags().StringVar(&apiContractName, "name", "", "API contract name to search")
	cobra.CheckErr(queryAPIConsumersCmd.MarkFlagRequired("name"))

	queryCmd.AddCommand(queryAPIContractCmd)
	queryCmd.AddCommand(queryAPITableCmd)
	queryCmd.AddCommand(queryAPITableIndexCmd)
	queryCmd.AddCommand(queryAPIParamCmd)
	queryCmd.AddCommand(queryAPIImplCmd)
	queryCmd.AddCommand(queryAPIPublishersCmd)
	queryCmd.AddCommand(queryAPIConsumersCmd)
}
