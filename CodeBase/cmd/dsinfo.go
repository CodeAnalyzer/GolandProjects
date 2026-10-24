package cmd

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codebase/internal/dsinfosvc"
	"github.com/codebase/internal/store"
	"github.com/spf13/cobra"
)

var (
	dsinfoOutputJSON bool
	dsinfoName       string
	dsinfoModule     string
	dsinfoPacket     string
	dsinfoSearch     string
	dsinfoLimit      int
	dsinfoListLimit  int
)

// dsinfoSuccessResponse — machine-readable конверт (общий контракт проекта).
type dsinfoSuccessResponse struct {
	Success       bool                `json:"success"`
	FormatVersion string              `json:"format_version"`
	Command       string              `json:"command"`
	Count         int                 `json:"count"`
	Items         interface{}         `json:"items"`
	Meta          *dsinfoResponseMeta `json:"meta,omitempty"`
}

type dsinfoResponseMeta struct {
	Limit   int               `json:"limit,omitempty"`
	Filters map[string]string `json:"filters,omitempty"`
}

type dsinfoErrorResponse struct {
	Success       bool            `json:"success"`
	FormatVersion string          `json:"format_version"`
	Command       string          `json:"command"`
	Error         dsinfoErrorBody `json:"error"`
}

type dsinfoErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// printDsInfoSuccess печатает конверт успеха (items — единичный объект или
// срез; count передаётся явно).
func printDsInfoSuccess(command string, count int, items interface{}, meta *dsinfoResponseMeta) error {
	return printJSON(dsinfoSuccessResponse{
		Success:       true,
		FormatVersion: "1.0",
		Command:       command,
		Count:         count,
		Items:         items,
		Meta:          meta,
	})
}

// handleDsInfoError в --json-режиме печатает structured-ошибку вместо
// текстового вывода cobra.
func handleDsInfoError(command string, err error) error {
	if !dsinfoOutputJSON {
		return err
	}
	code := "internal_error"
	switch {
	case strings.Contains(err.Error(), "not found"):
		code = "not_found"
	case strings.Contains(err.Error(), "required"), strings.Contains(err.Error(), "invalid"):
		code = "invalid_arguments"
	}
	return printJSON(dsinfoErrorResponse{
		Success:       false,
		FormatVersion: "1.0",
		Command:       command,
		Error:         dsinfoErrorBody{Code: code, Message: err.Error()},
	})
}

var dsinfoCmd = &cobra.Command{
	Use:   "dsinfo",
	Short: "DiasoftSystemInfo profile manager",
	Long: `Parse and inspect DiasoftSystemInfo CSV files.

The file name is not canonical: content is sniffed (sections
"Продукты Digital Q" and "Модули" with the FileName; header). Parsing is
idempotent: re-parsing the same content returns the existing profile.

Subcommands:
  parse   - parse DiasoftSystemInfo file and save profile
  list    - list saved profiles
  show    - show profile details (products, modules, filtered procedures)
  delete  - delete a profile by ID
  use     - set or show the active profile`,
}

var dsinfoParseCmd = &cobra.Command{
	Use:   "parse <file.csv>",
	Short: "Parse DiasoftSystemInfo file and save profile",
	Args:  cobra.ExactArgs(1),
	RunE:  runDsInfoParse,
}

var dsinfoListCmd = &cobra.Command{
	Use:   "list",
	Short: "List saved dsinfo profiles",
	Args:  cobra.NoArgs,
	RunE:  runDsInfoList,
}

var dsinfoShowCmd = &cobra.Command{
	Use:   "show <id>",
	Short: "Show dsinfo profile details",
	Args:  cobra.ExactArgs(1),
	RunE:  runDsInfoShow,
}

var dsinfoDeleteCmd = &cobra.Command{
	Use:   "delete <id>",
	Short: "Delete a dsinfo profile (final)",
	Args:  cobra.ExactArgs(1),
	RunE:  runDsInfoDelete,
}

var dsinfoUseCmd = &cobra.Command{
	Use:   "use [<id>]",
	Short: "Set the active dsinfo profile (or show the current one)",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runDsInfoUse,
}

func runDsInfoParse(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := dsinfosvc.ExecuteParse(ctx, db, dsinfosvc.ParseParams{
		FilePath: args[0],
		Name:     dsinfoName,
	})
	if err != nil {
		return handleDsInfoError("dsinfo parse", err)
	}
	if dsinfoOutputJSON {
		return printDsInfoSuccess("dsinfo parse", 1, result, nil)
	}
	state := "new"
	if result.Reused {
		state = "reused"
	}
	fmt.Printf("DsInfo profile: %d (%s)\n", result.ProfileID, state)
	fmt.Printf("  File: %s\n", result.FilePath)
	fmt.Printf("  Name: %s\n", result.SessionName)
	fmt.Printf("  Server: %s  DB: %s  Alias: %s\n", result.DBServer, result.DBName, result.ClientAlias)
	fmt.Printf("  Products: %d  Modules: %d  Procedures: %d\n",
		result.ProductsCount, result.ModulesCount, result.ProceduresCount)
	return nil
}

func runDsInfoList(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := dsinfosvc.ExecuteList(ctx, db, dsinfoListLimit)
	if err != nil {
		return handleDsInfoError("dsinfo list", err)
	}
	if dsinfoOutputJSON {
		items := result.Profiles
		if items == nil {
			items = []store.DsInfoProfileSummary{}
		}
		return printDsInfoSuccess("dsinfo list", len(items), items,
			&dsinfoResponseMeta{Limit: dsinfoListLimit})
	}
	if len(result.Profiles) == 0 {
		fmt.Println("No dsinfo profiles.")
		return nil
	}
	for _, p := range result.Profiles {
		active := " "
		if p.IsActive {
			active = "*"
		}
		fmt.Printf("%s %3d  %-30s  %s  p=%d m=%d proc=%d  %s/%s/%s\n",
			active, p.ID, p.SessionName, p.ParsedAt.Local().Format(time.DateTime),
			p.ProductsCount, p.ModulesCount, p.ProceduresCount,
			p.DBServer, p.DBName, p.ClientAlias)
	}
	return nil
}

func runDsInfoShow(cmd *cobra.Command, args []string) error {
	id, err := parseDsInfoID(args[0])
	if err != nil {
		return handleDsInfoError("dsinfo show", err)
	}
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := dsinfosvc.ExecuteShow(ctx, db, dsinfosvc.ShowParams{
		ProfileID: id,
		Module:    dsinfoModule,
		Packet:    dsinfoPacket,
		Search:    dsinfoSearch,
		Limit:     dsinfoLimit,
	})
	if err != nil {
		return handleDsInfoError("dsinfo show", err)
	}
	if dsinfoOutputJSON {
		meta := &dsinfoResponseMeta{Limit: result.Limit}
		for name, value := range map[string]string{
			"module": dsinfoModule, "packet": dsinfoPacket, "search": dsinfoSearch,
		} {
			if value != "" {
				if meta.Filters == nil {
					meta.Filters = map[string]string{}
				}
				meta.Filters[name] = value
			}
		}
		return printDsInfoSuccess("dsinfo show", 1, result, meta)
	}

	p := result.Profile
	active := ""
	if p.IsActive {
		active = " (* active)"
	}
	fmt.Printf("DsInfo profile %d: %s%s\n", p.ID, p.SessionName, active)
	fmt.Printf("  File: %s\n", p.FilePath)
	fmt.Printf("  Parsed: %s\n", p.ParsedAt.Local().Format(time.DateTime))
	fmt.Printf("  Server: %s  DB: %s  Alias: %s\n", p.DBServer, p.DBName, p.ClientAlias)
	fmt.Printf("  Products: %d  Modules: %d  Procedures: %d\n",
		p.ProductsCount, p.ModulesCount, p.ProceduresCount)

	fmt.Printf("\nProducts (%d):\n", len(result.Products))
	for _, prod := range result.Products {
		fmt.Printf("  %4d  %s\n", prod.LineNo, prod.RawName)
	}
	if result.ProductsTruncated {
		fmt.Printf("  ... truncated (limit %d of %d, use --limit)\n", result.Limit, p.ProductsCount)
	}

	fmt.Printf("\nModules (%d):\n", len(result.Modules))
	for _, m := range result.Modules {
		fmt.Printf("  %-28s module=%-14s packet=%s\n", m.FileName, m.ModuleName, m.PacketName)
		if m.HotfixName != "" {
			fmt.Printf("    hotfix: %s\n", m.HotfixName)
		}
	}
	if result.ModulesTruncated {
		fmt.Printf("  ... truncated (limit %d of %d, use --limit)\n", result.Limit, p.ModulesCount)
	}

	fmt.Println()
	if !result.ProceduresFiltered {
		fmt.Printf("Procedures: %d total (use --module/--packet/--search to list)\n", result.ProceduresCount)
		return nil
	}
	fmt.Printf("Procedures (%d of %d by filter module=%q packet=%q search=%q):\n",
		len(result.Procedures), result.ProceduresCount, dsinfoModule, dsinfoPacket, dsinfoSearch)
	for _, proc := range result.Procedures {
		packet := proc.PacketName
		if packet == "" {
			packet = "-"
		}
		fmt.Printf("  %-44s module=%-14s packet=%s  %s\n",
			proc.ObjectName, proc.ModuleName, packet, proc.Status)
	}
	if len(result.Procedures) < result.ProceduresCount {
		fmt.Printf("  ... truncated (limit %d of %d, use --limit)\n", result.Limit, result.ProceduresCount)
	}
	return nil
}

func runDsInfoDelete(cmd *cobra.Command, args []string) error {
	id, err := parseDsInfoID(args[0])
	if err != nil {
		return handleDsInfoError("dsinfo delete", err)
	}
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := dsinfosvc.ExecuteDelete(ctx, db, dsinfosvc.DeleteParams{ProfileID: id})
	if err != nil {
		return handleDsInfoError("dsinfo delete", err)
	}
	if dsinfoOutputJSON {
		return printDsInfoSuccess("dsinfo delete", 1, result, nil)
	}
	fmt.Printf("Deleted dsinfo profile %d (%s)\n", result.ProfileID, result.SessionName)
	if result.ActiveReset {
		fmt.Println("Active profile reference reset (profile was active).")
	}
	return nil
}

func runDsInfoUse(cmd *cobra.Command, args []string) error {
	var id int64
	if len(args) > 0 {
		var err error
		id, err = parseDsInfoID(args[0])
		if err != nil {
			return handleDsInfoError("dsinfo use", err)
		}
	}
	ctx := cmd.Context()
	db := openDB()
	defer closeDB(db)
	result, err := dsinfosvc.ExecuteUse(ctx, db, dsinfosvc.UseParams{ProfileID: id})
	if err != nil {
		return handleDsInfoError("dsinfo use", err)
	}
	if dsinfoOutputJSON {
		return printDsInfoSuccess("dsinfo use", 1, result, nil)
	}
	if result.Active == nil {
		fmt.Println("No active dsinfo profile.")
		return nil
	}
	fmt.Printf("Active dsinfo profile: %d (%s)\n", result.Active.ID, result.Active.SessionName)
	fmt.Printf("  Server: %s  DB: %s  Alias: %s  (p=%d m=%d proc=%d)\n",
		result.Active.DBServer, result.Active.DBName, result.Active.ClientAlias,
		result.Active.ProductsCount, result.Active.ModulesCount, result.Active.ProceduresCount)
	return nil
}

// parseDsInfoID разбирает позиционный аргумент id.
func parseDsInfoID(arg string) (int64, error) {
	id, err := strconv.ParseInt(arg, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid profile id %q", arg)
	}
	return id, nil
}

func init() {
	dsinfoCmd.PersistentFlags().BoolVar(&dsinfoOutputJSON, "json", false, "output as JSON")
	dsinfoParseCmd.Flags().StringVar(&dsinfoName, "name", "", "profile name (default: file name)")
	dsinfoListCmd.Flags().IntVar(&dsinfoListLimit, "limit", 20, "max profiles to list")
	dsinfoShowCmd.Flags().StringVar(&dsinfoModule, "module", "", "filter procedures by exact module name")
	dsinfoShowCmd.Flags().StringVar(&dsinfoPacket, "packet", "", "filter procedures by packet name substring (case-insensitive)")
	dsinfoShowCmd.Flags().StringVar(&dsinfoSearch, "search", "", "filter procedures by object name substring (case-insensitive)")
	dsinfoShowCmd.Flags().IntVar(&dsinfoLimit, "limit", 100, "max rows per section (max 1000)")

	dsinfoCmd.AddCommand(dsinfoParseCmd)
	dsinfoCmd.AddCommand(dsinfoListCmd)
	dsinfoCmd.AddCommand(dsinfoShowCmd)
	dsinfoCmd.AddCommand(dsinfoDeleteCmd)
	dsinfoCmd.AddCommand(dsinfoUseCmd)
	rootCmd.AddCommand(dsinfoCmd)
}
