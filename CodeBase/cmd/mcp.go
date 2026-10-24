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
  --profile=query   - base + query tools (~45)
  --profile=rti     - base + RTI tools (~21)
  --profile=trc     - base + TRC tools (~21)
  --profile=review  - base + review tools (~10)
Without --profile, all tools are registered (default behavior).
Base tools (all profiles): ping, health, stats, read_more, dsinfo_* (parse, list, show, delete, use).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return mcp.RunStdio(version, mcpProfile, commandLogger)
	},
}

func init() {
	mcpCmd.Flags().StringVar(&mcpProfile, "profile", "", "tool profile: query, rti, trc, review (empty = all tools)")
	rootCmd.AddCommand(mcpCmd)
}
