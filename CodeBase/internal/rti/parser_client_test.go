package rti

import (
	"testing"
)

func TestParseContent_ClientBPLList(t *testing.T) {
	content := "05.06.2026 11:45:54.835\tINFO\tDebug.d5ntsys\tTDBLFileHandler\tWriteAllBPL2Log\t15860\t15\t\t0\t1343\n" +
		"\n" +
		"Текущий PID: 8676\n" +
		"Список загруженных BPL\n" +
		"======================================================================\n" +
		"Файл            Версии       Название             Комментарий         \n" +
		"======================================================================\n" +
		"d5ntcard.bpl    1.0.0.0                                               \n" +
		"d5ntACln.bpl    3.5.1.4      Счета                26.10.2001          \n" +
		"\n" +
		"05.06.2026 11:45:54.845\tINFO\tSQL\tDSConnectorADO\tDoConnect\t15860\t19\t\t0\t203\n" +
		"--- New Connection  : SPID =    67\n" +
		"--- Server          : kdoms15.       \n" +
		"--- Database        : IFRS09_demo.       \n" +
		"--- User            : diasoft.       \n" +
		"--- Application Name: TDBLSHandler.       \n"

	result, err := parseContent(content, "test.rti", int64(len(content)))
	if err != nil {
		t.Fatalf("parseContent failed: %v", err)
	}
	if len(result.ClientEvents) != 2 {
		t.Fatalf("expected 2 client events, got %d", len(result.ClientEvents))
	}

	bpl := result.ClientEvents[0]
	if bpl.Kind != "bpl_list" {
		t.Fatalf("expected kind bpl_list, got %s", bpl.Kind)
	}
	if len(bpl.BPL) != 2 {
		t.Fatalf("expected 2 BPL modules, got %d: %+v", len(bpl.BPL), bpl.BPL)
	}
	if bpl.BPL[0].File != "d5ntcard.bpl" || bpl.BPL[0].Version != "1.0.0.0" {
		t.Fatalf("unexpected first BPL module: %+v", bpl.BPL[0])
	}
	if bpl.BPL[1].File != "d5ntACln.bpl" || bpl.BPL[1].Version != "3.5.1.4" {
		t.Fatalf("unexpected second BPL module: %+v", bpl.BPL[1])
	}

	conn := result.ClientEvents[1]
	if conn.Kind != "connection" {
		t.Fatalf("expected kind connection, got %s", conn.Kind)
	}
	if conn.Connection == nil {
		t.Fatalf("expected connection info to be set")
	}
	if conn.Connection.SPID != 67 {
		t.Fatalf("expected SPID 67, got %d", conn.Connection.SPID)
	}
	if conn.Connection.Server != "kdoms15" {
		t.Fatalf("expected server 'kdoms15', got %q", conn.Connection.Server)
	}
	if conn.Connection.Database != "IFRS09_demo" {
		t.Fatalf("expected database 'IFRS09_demo', got %q", conn.Connection.Database)
	}
}

func TestParseContent_ClientBareEnterExit(t *testing.T) {
	content := "05.06.2026 11:45:54.831\tFINE\tDebug.d5ntsys\tDsADORecordset\tOpen\t15860\t14\t\t0\t7\n" +
		"Enter\n" +
		"05.06.2026 11:45:54.839\tFINE\tDebug.d5ntsys\tDsADORecordset\tOpen\t15860\t16\t\t0\t6\n" +
		"Exit\n"

	result, err := parseContent(content, "test.rti", int64(len(content)))
	if err != nil {
		t.Fatalf("parseContent failed: %v", err)
	}
	if len(result.ClientEvents) != 2 {
		t.Fatalf("expected 2 client events, got %d", len(result.ClientEvents))
	}
	if result.ClientEvents[0].Kind != "recordset_open" || result.ClientEvents[0].RawBody != "Enter" {
		t.Fatalf("unexpected first event: %+v", result.ClientEvents[0])
	}
	if result.ClientEvents[1].Kind != "recordset_open" || result.ClientEvents[1].RawBody != "Exit" {
		t.Fatalf("unexpected second event: %+v", result.ClientEvents[1])
	}
}

func TestParseContent_ClientSQLExecBlock(t *testing.T) {
	content := "05.06.2026 11:45:54.822\tINFO\tSQL\tGreen_guy\tAdministrator.\t15860\t13\t\t57\t1225\n" +
		"SPID = 64\n" +
		"SERVER = 'kdoms15'\n" +
		"DATABASE = 'IFRS09_demo'\n" +
		"PREPARED\n" +
		"\n" +
		"exec FCD_10_Log_SaveOption\n" +
		"        @UserID                = 0,\n" +
		"        @HostName              = @HostName,\n" +
		"        @ActivateClientFlag    = 1\n" +
		"\n" +
		"select 0\n" +
		"\n" +
		"\n" +
		"05.06.2026 11:45:54.882\tFINE\tDebug.d5ntsys\tDsADORecordset\tOpen\t15860\t31\t\t0\t7\n" +
		"Enter\n"

	result, err := parseContent(content, "test.rti", int64(len(content)))
	if err != nil {
		t.Fatalf("parseContent failed: %v", err)
	}
	if len(result.ClientEvents) != 2 {
		t.Fatalf("expected 2 client events, got %d", len(result.ClientEvents))
	}

	sqlEv := result.ClientEvents[0]
	if sqlEv.Kind != "sql_block" {
		t.Fatalf("expected kind sql_block, got %s", sqlEv.Kind)
	}
	if sqlEv.SQL == nil {
		t.Fatalf("expected SQL block to be set")
	}
	if sqlEv.SQL.SPID != 64 {
		t.Fatalf("expected SPID 64, got %d", sqlEv.SQL.SPID)
	}
	if sqlEv.SQL.ExecProcedure != "FCD_10_Log_SaveOption" {
		t.Fatalf("expected exec procedure FCD_10_Log_SaveOption, got %q", sqlEv.SQL.ExecProcedure)
	}
	if len(sqlEv.SQL.ExecParams) != 3 {
		t.Fatalf("expected 3 exec params, got %d: %+v", len(sqlEv.SQL.ExecParams), sqlEv.SQL.ExecParams)
	}
	paramNames := map[string]string{}
	for _, p := range sqlEv.SQL.ExecParams {
		paramNames[p.Name] = p.Value
	}
	if paramNames["UserID"] != "0" {
		t.Fatalf("expected UserID=0, got %q", paramNames["UserID"])
	}
	if paramNames["ActivateClientFlag"] != "1" {
		t.Fatalf("expected ActivateClientFlag=1, got %q", paramNames["ActivateClientFlag"])
	}
}

func TestParseContent_ClientSQLDurationBlock(t *testing.T) {
	content := "05.06.2026 11:45:59.386\tINFO\tSQL\tDsQuery\tAdministrator.InclusiveTerm\t15860\t140\t\t20\t9\n" +
		"Duration = '0.022'\n" +
		"STARTED\n" +
		"05.06.2026 11:45:59.396\tFINE\tSQL_TranCount\tDsQuery\tWriteStartedToRTI\t15860\t141\t\t0\t15\n" +
		"trancount = 0\n"

	result, err := parseContent(content, "test.rti", int64(len(content)))
	if err != nil {
		t.Fatalf("parseContent failed: %v", err)
	}
	if len(result.ClientEvents) != 2 {
		t.Fatalf("expected 2 client events, got %d", len(result.ClientEvents))
	}
	durEv := result.ClientEvents[0]
	if durEv.Kind != "sql_block" || durEv.SQL == nil {
		t.Fatalf("unexpected duration event: %+v", durEv)
	}
	if durEv.SQL.DurationSec != 0.022 {
		t.Fatalf("expected duration 0.022, got %v", durEv.SQL.DurationSec)
	}
	if durEv.SQL.State != "STARTED" {
		t.Fatalf("expected state STARTED, got %q", durEv.SQL.State)
	}

	tcEv := result.ClientEvents[1]
	if tcEv.Kind != "trancount" || tcEv.TranCount == nil || *tcEv.TranCount != 0 {
		t.Fatalf("unexpected trancount event: %+v", tcEv)
	}
}

func TestParseContent_ClientMemoryUsage(t *testing.T) {
	content := "05.06.2026 11:45:59.398\tINFO\tDebug.d5ntsys\tWriteCurrentProcessMemoryUsage\t\t15860\t142\t\t0\t127\n" +
		"Пам-ть (Delphi manager): 22536кб\n" +
		"Пам-ть (WINAPI manager): 171336кб\n" +
		"Дескрипторы: 1311; Объекты User: 555; Объекты GDI: 653\n" +
		"\n" +
		"05.06.2026 11:45:59.402\tINFO\tSQL\tGreen_guy\tAdministrator.\t15860\t143\t\t57\t205\n" +
		"SPID = 64\n" +
		"SERVER = 'kdoms15'\n" +
		"DATABASE = 'IFRS09_demo'\n" +
		"PREPARED\n"

	result, err := parseContent(content, "test.rti", int64(len(content)))
	if err != nil {
		t.Fatalf("parseContent failed: %v", err)
	}
	if len(result.ClientEvents) < 1 {
		t.Fatalf("expected at least 1 client event, got %d", len(result.ClientEvents))
	}
	mem := result.ClientEvents[0]
	if mem.Kind != "memory_usage" || mem.Memory == nil {
		t.Fatalf("unexpected memory event: %+v", mem)
	}
	if mem.Memory.DelphiKB != 22536 {
		t.Fatalf("expected DelphiKB 22536, got %d", mem.Memory.DelphiKB)
	}
	if mem.Memory.WinAPIKB != 171336 {
		t.Fatalf("expected WinAPIKB 171336, got %d", mem.Memory.WinAPIKB)
	}
	if mem.Memory.Descriptors != 1311 || mem.Memory.ObjectsUser != 555 || mem.Memory.ObjectsGDI != 653 {
		t.Fatalf("unexpected memory descriptors: %+v", mem.Memory)
	}
}

func TestParseContent_ClientSevereError(t *testing.T) {
	content := "05.06.2026 11:45:57.908\tSEVERE\tError.d5ntServ\tTCodeProtection\tReportViolation\t15860\t72\t\t0\t138\n" +
		"Some violation message text\n" +
		"05.06.2026 11:45:57.910\tINFO\tSQL\tGreen_guy\tAdministrator.\t15860\t73\t\t57\t151\n" +
		"SPID = 64\n"

	result, err := parseContent(content, "test.rti", int64(len(content)))
	if err != nil {
		t.Fatalf("parseContent failed: %v", err)
	}
	if len(result.ClientEvents) < 1 {
		t.Fatalf("expected at least 1 client event, got %d", len(result.ClientEvents))
	}
	errEv := result.ClientEvents[0]
	if errEv.Kind != "error" {
		t.Fatalf("expected kind error, got %s", errEv.Kind)
	}
	if errEv.ErrorText != "Some violation message text" {
		t.Fatalf("unexpected error text: %q", errEv.ErrorText)
	}
	if result.Summary.ClientErrorsCount != 1 {
		t.Fatalf("expected ClientErrorsCount=1, got %d", result.Summary.ClientErrorsCount)
	}
}

// TestParseContent_MixedClientAndServer проверяет, что клиентские и серверные
// записи в рамках одного файла разбираются независимо и не мешают друг другу.
func TestParseContent_MixedClientAndServer(t *testing.T) {
	content := "10.06.2026 15:21:05.520\tINFO\tTrace.Server.Proc\t\tCons_Get_ProtocolNumber\t58\t692544\t\t0\t244\n" +
		"Enter Cons_Get_ProtocolNumber @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 39\n" +
		"Elapsed, ms: 0\n" +
		"\n" +
		"@InterfaceObjectID             : DSIDENTIFIER                   = 442\n" +
		"\n" +
		"10.06.2026 15:21:05.537\tINFO\tTrace.Server.Proc\t\tCons_Get_ProtocolNumber\t58\t692546\t\t0\t196\n" +
		"Exit Cons_Get_ProtocolNumber @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 39\n" +
		"Elapsed, ms: 16\n" +
		"Return 0\n" +
		"\n" +
		"05.06.2026 11:45:54.835\tINFO\tDebug.d5ntsys\tTDBLFileHandler\tWriteAllBPL2Log\t15860\t15\t\t0\t1343\n" +
		"\n" +
		"Текущий PID: 8676\n" +
		"======================================================================\n" +
		"Файл            Версии       Название             Комментарий         \n" +
		"======================================================================\n" +
		"d5ntcard.bpl    1.0.0.0                                               \n" +
		"\n"

	result, err := parseContent(content, "test.rti", int64(len(content)))
	if err != nil {
		t.Fatalf("parseContent failed: %v", err)
	}
	if len(result.Calls) != 1 {
		t.Fatalf("expected 1 server call, got %d", len(result.Calls))
	}
	if result.Calls[0].Procedure != "Cons_Get_ProtocolNumber" {
		t.Fatalf("unexpected procedure: %s", result.Calls[0].Procedure)
	}
	if len(result.ClientEvents) != 1 {
		t.Fatalf("expected 1 client event, got %d", len(result.ClientEvents))
	}
	if result.ClientEvents[0].Kind != "bpl_list" {
		t.Fatalf("expected bpl_list, got %s", result.ClientEvents[0].Kind)
	}
	if len(result.ClientEvents[0].BPL) != 1 {
		t.Fatalf("expected 1 BPL module, got %d", len(result.ClientEvents[0].BPL))
	}
}

func TestFillClientSummary_FillsAllFields(t *testing.T) {
	events := []*RTIClientEvent{
		{Kind: "error", ErrorText: "something went wrong"},
		{Kind: "sql_block", SQL: &RTISQLBlock{DurationSec: 0.5}},
		{Kind: "sql_block", SQL: &RTISQLBlock{DurationSec: 0.05}},
		{Kind: "bpl_list"},
	}

	var s RTISummary
	FillClientSummary(&s, events)

	if s.ClientEventsCount != 4 {
		t.Fatalf("expected ClientEventsCount=4, got %d", s.ClientEventsCount)
	}
	if s.ClientErrorsCount != 1 {
		t.Fatalf("expected ClientErrorsCount=1, got %d", s.ClientErrorsCount)
	}
	if s.ClientSlowSQLCount != 1 {
		t.Fatalf("expected ClientSlowSQLCount=1, got %d", s.ClientSlowSQLCount)
	}
	if len(s.TopSlowClientSQL) != 2 {
		t.Fatalf("expected 2 top slow client SQL (all with duration>0), got %d", len(s.TopSlowClientSQL))
	}
	if s.TopSlowClientSQL[0].SQL.DurationSec != 0.5 {
		t.Fatalf("expected top slow duration 0.5, got %v", s.TopSlowClientSQL[0].SQL.DurationSec)
	}
	if s.TopSlowClientSQL[1].SQL.DurationSec != 0.05 {
		t.Fatalf("expected second slow duration 0.05, got %v", s.TopSlowClientSQL[1].SQL.DurationSec)
	}
}

func TestFillClientSummary_EmptyEvents(t *testing.T) {
	var s RTISummary
	FillClientSummary(&s, nil)

	if s.ClientEventsCount != 0 {
		t.Fatalf("expected ClientEventsCount=0, got %d", s.ClientEventsCount)
	}
	if s.ClientErrorsCount != 0 {
		t.Fatalf("expected ClientErrorsCount=0, got %d", s.ClientErrorsCount)
	}
	if s.ClientSlowSQLCount != 0 {
		t.Fatalf("expected ClientSlowSQLCount=0, got %d", s.ClientSlowSQLCount)
	}
	if len(s.TopSlowClientSQL) != 0 {
		t.Fatalf("expected 0 top slow client SQL, got %d", len(s.TopSlowClientSQL))
	}
}
