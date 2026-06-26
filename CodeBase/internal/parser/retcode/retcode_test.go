package retcode

import (
	"testing"
)

func TestParse_ReturnCodeInsert_WithConstant(t *testing.T) {
	content := `exec ReturnCode_Insert 20100, LOC_RETCODE_20100, 'OperDeal_Generate'`
	entries := Parse(content)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.RetCode != 20100 {
		t.Errorf("RetCode = %d, want 20100", e.RetCode)
	}
	if e.Message != "LOC_RETCODE_20100" {
		t.Errorf("Message = %q, want %q", e.Message, "LOC_RETCODE_20100")
	}
	if !e.IsConstant {
		t.Errorf("IsConstant = false, want true")
	}
	if e.ProcName != "OperDeal_Generate" {
		t.Errorf("ProcName = %q, want %q", e.ProcName, "OperDeal_Generate")
	}
	if e.ModuleID != 2 {
		t.Errorf("ModuleID = %d, want 2", e.ModuleID)
	}
}

func TestParse_ReturnCodeInsert_WithLiteral(t *testing.T) {
	content := `exec ReturnCode_Insert 20100, 'Ошибка операции', 'OperDeal_Generate'`
	entries := Parse(content)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Message != "Ошибка операции" {
		t.Errorf("Message = %q, want %q", e.Message, "Ошибка операции")
	}
	if e.IsConstant {
		t.Errorf("IsConstant = true, want false")
	}
}

func TestParse_AddRetCodeMacro(t *testing.T) {
	content := `_ADD_RETCODE_(390001, 'Невозможно добавить вид задолженности.', 'UsrDbtType_Insert')`
	entries := Parse(content)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.RetCode != 390001 {
		t.Errorf("RetCode = %d, want 390001", e.RetCode)
	}
	if e.Message != "Невозможно добавить вид задолженности." {
		t.Errorf("Message = %q", e.Message)
	}
	if e.ProcName != "UsrDbtType_Insert" {
		t.Errorf("ProcName = %q, want %q", e.ProcName, "UsrDbtType_Insert")
	}
	if e.ModuleID != 39 {
		t.Errorf("ModuleID = %d, want 39", e.ModuleID)
	}
	if e.IsConstant {
		t.Errorf("IsConstant = true, want false")
	}
}

func TestParse_NotificationSave_4args(t *testing.T) {
	content := `__Notification_Save(760001, 'По переданному идентификатору процесс не найден!', '', DSMODULE_CONSUMER_EXT_ID)`
	entries := Parse(content)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.RetCode != 760001 {
		t.Errorf("RetCode = %d, want 760001", e.RetCode)
	}
	if e.Message != "По переданному идентификатору процесс не найден!" {
		t.Errorf("Message = %q", e.Message)
	}
	if e.ProcName != "" {
		t.Errorf("ProcName = %q, want empty", e.ProcName)
	}
	if e.ModuleID != 76 {
		t.Errorf("ModuleID = %d, want 76 (DSMODULE_CONSUMER_EXT_ID)", e.ModuleID)
	}
}

func TestParse_NotificationSave_WithProcName(t *testing.T) {
	content := `__Notification_Save(760004, 'Не найдено ни одного запроса', 'API_LnExt_FndLstStatusEDocByID', DSMODULE_CONSUMER_EXT_ID)`
	entries := Parse(content)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.ProcName != "API_LnExt_FndLstStatusEDocByID" {
		t.Errorf("ProcName = %q, want %q", e.ProcName, "API_LnExt_FndLstStatusEDocByID")
	}
}

func TestHasReturnCodes_Positive(t *testing.T) {
	cases := []string{
		`exec ReturnCode_Insert 20100, LOC_RETCODE_20100, 'Proc'`,
		`_ADD_RETCODE_(390001, 'msg', 'proc')`,
		`__Notification_Save(760001, 'msg', 'proc', DSMODULE_CONSUMER_EXT_ID)`,
		`exec FCD_CON_Notification_Save @NotificationID = 1`,
		`M_CRD_RETCODE_INSERT(290000, 'msg', 'proc')`,
	}
	for _, c := range cases {
		if !HasReturnCodes(c) {
			t.Errorf("HasReturnCodes(%q) = false, want true", c)
		}
	}
}

func TestHasReturnCodes_Negative(t *testing.T) {
	cases := []string{
		`SELECT * FROM t WHERE x = 1`,
		`create proc MyProc as begin end`,
		``,
		`-- just a comment`,
	}
	for _, c := range cases {
		if HasReturnCodes(c) {
			t.Errorf("HasReturnCodes(%q) = true, want false", c)
		}
	}
}

func TestParse_MultipleEntries(t *testing.T) {
	content := `
  __Notification_Save(760001, 'msg1', 'proc1', DSMODULE_CONSUMER_EXT_ID)
  __Notification_Save(760002, 'msg2', 'proc2', DSMODULE_CONSUMER_EXT_ID)
  __Notification_Save(760003, 'msg3', '', DSMODULE_CONSUMER_EXT_ID)
`
	entries := Parse(content)
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	if entries[0].RetCode != 760001 || entries[1].RetCode != 760002 || entries[2].RetCode != 760003 {
		t.Errorf("unexpected RetCodes: %d, %d, %d", entries[0].RetCode, entries[1].RetCode, entries[2].RetCode)
	}
}

func TestParse_NoDuplicates_3and4arg(t *testing.T) {
	// If a 4-arg match exists for a code, the 3-arg regex should not duplicate it
	content := `
  __Notification_Save(760001, 'msg', 'proc', DSMODULE_CONSUMER_EXT_ID)
  SomeOtherMacro(760001, 'msg', 'proc')
`
	entries := Parse(content)
	// The 4-arg match should suppress the 3-arg match for the same code
	count := 0
	for _, e := range entries {
		if e.RetCode == 760001 {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected 1 entry for code 760001, got %d", count)
	}
}
