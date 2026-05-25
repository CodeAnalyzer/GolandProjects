package cmd

import (
	"github.com/codebase/internal/mcp"
	"github.com/spf13/cobra"
)

var mcpCmd = &cobra.Command{
	Use:   "mcp",
	Short: "Run MCP server over stdio",
	Long:  `Starts CodeBase as MCP JSON-RPC server over stdin/stdout transport.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return mcp.RunStdio(version, commandLogger)
	},
}

func init() {
	rootCmd.AddCommand(mcpCmd)
}
