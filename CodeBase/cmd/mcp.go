package cmd

import (
	"github.com/codebase/internal/mcp"
	"github.com/spf13/cobra"
)

var mcpProfile string

var mcpCmd = &cobra.Command{
	Use:   "mcp",
	Short: "Run MCP server over stdio",
	Long: `Starts CodeBase as MCP JSON-RPC server over stdin/stdout transport.

Use --profile to register only a subset of tools (reduces tools/list response size):
  --profile=query   - base + query tools (~30)
  --profile=rti     - base + RTI tools (~17)
  --profile=trc     - base + TRC tools (~14)
  --profile=review  - base + review tools (~5)
Without --profile, all tools are registered (default behavior).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return mcp.RunStdio(version, mcpProfile, commandLogger)
	},
}

func init() {
	mcpCmd.Flags().StringVar(&mcpProfile, "profile", "", "tool profile: query, rti, trc, review (empty = all tools)")
	rootCmd.AddCommand(mcpCmd)
}
