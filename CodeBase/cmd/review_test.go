package cmd

import (
	"testing"

	"github.com/codebase/internal/review"
)

func TestParseReviewRules_AcceptsExecNotExistsProc(t *testing.T) {
	rules, raw, err := parseReviewRules("execNotExistsProc,datatype")
	if err != nil {
		t.Fatalf("parseReviewRules returned error: %v", err)
	}
	if len(rules) != 2 || rules[0] != review.RuleExecNotExistsProc || rules[1] != review.RuleDatatype {
		t.Fatalf("unexpected rules: %#v", rules)
	}
	if len(raw) != 2 || raw[0] != "execNotExistsProc" || raw[1] != "datatype" {
		t.Fatalf("unexpected raw rules: %#v", raw)
	}
}

func TestParseReviewRules_ProcDuplicate(t *testing.T) {
	rules, raw, err := parseReviewRules("procDuplicate")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleProcDuplicate {
		t.Fatalf("expected procDuplicate, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "procDuplicate" {
		t.Fatalf("expected raw procDuplicate, got %v", raw)
	}
}

func TestParseReviewRules_ProcParamDefValue(t *testing.T) {
	rules, raw, err := parseReviewRules("procParamDefValue")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleProcParamDefValue {
		t.Fatalf("expected procParamDefValue, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "procParamDefValue" {
		t.Fatalf("expected raw procParamDefValue, got %v", raw)
	}
}

func TestParseReviewRules_ProcElseCase(t *testing.T) {
	rules, raw, err := parseReviewRules("procElseCase")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleProcElseCase {
		t.Fatalf("expected procElseCase, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "procElseCase" {
		t.Fatalf("expected raw procElseCase, got %v", raw)
	}
}

func TestParseReviewRules_UseSelectAll(t *testing.T) {
	rules, raw, err := parseReviewRules("useSelectAll")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleUseSelectAll {
		t.Fatalf("expected useSelectAll, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "useSelectAll" {
		t.Fatalf("expected raw useSelectAll, got %v", raw)
	}
}

func TestParseReviewRules_TruncTbl(t *testing.T) {
	rules, raw, err := parseReviewRules("truncTbl")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleTruncTbl {
		t.Fatalf("expected truncTbl, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "truncTbl" {
		t.Fatalf("expected raw truncTbl, got %v", raw)
	}
}

func TestParseReviewRules_AnsiInJoin(t *testing.T) {
	rules, raw, err := parseReviewRules("ansiInJoin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleAnsiInJoin {
		t.Fatalf("expected ansiInJoin, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "ansiInJoin" {
		t.Fatalf("expected raw ansiInJoin, got %v", raw)
	}
}

func TestParseReviewRules_InsertRowLock(t *testing.T) {
	rules, raw, err := parseReviewRules("insertRowLock")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleInsertRowLock {
		t.Fatalf("expected insertRowLock, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "insertRowLock" {
		t.Fatalf("expected raw insertRowLock, got %v", raw)
	}
}

func TestParseReviewRules_UseEqColumn(t *testing.T) {
	rules, raw, err := parseReviewRules("useEqColumn")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleUseEqColumn {
		t.Fatalf("expected useEqColumn, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "useEqColumn" {
		t.Fatalf("expected raw useEqColumn, got %v", raw)
	}
}

func TestParseReviewRules_TableFullScan(t *testing.T) {
	rules, raw, err := parseReviewRules("tableFullScan")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleTableFullScan {
		t.Fatalf("expected tableFullScan, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "tableFullScan" {
		t.Fatalf("expected raw tableFullScan, got %v", raw)
	}
}

func TestParseReviewRules_TableHintExists(t *testing.T) {
	rules, raw, err := parseReviewRules("tableHintExists")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleTableHintExists {
		t.Fatalf("expected tableHintExists, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "tableHintExists" {
		t.Fatalf("expected raw tableHintExists, got %v", raw)
	}
}

func TestParseReviewRules_TableHintIsRight(t *testing.T) {
	rules, raw, err := parseReviewRules("tableHintIsRight")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleTableHintIsRight {
		t.Fatalf("expected tableHintIsRight, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "tableHintIsRight" {
		t.Fatalf("expected raw tableHintIsRight, got %v", raw)
	}
}

func TestParseReviewRules_IndexExistsInDB(t *testing.T) {
	rules, raw, err := parseReviewRules("indexExistsInDB")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleIndexExistsInDB {
		t.Fatalf("expected indexExistsInDB, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "indexExistsInDB" {
		t.Fatalf("expected raw indexExistsInDB, got %v", raw)
	}
}

func TestParseReviewRules_IndexWrong(t *testing.T) {
	rules, raw, err := parseReviewRules("indexWrong")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleIndexWrong {
		t.Fatalf("expected indexWrong, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "indexWrong" {
		t.Fatalf("expected raw indexWrong, got %v", raw)
	}
}

func TestParseReviewRules_UpdateOnlyVar(t *testing.T) {
	rules, raw, err := parseReviewRules("updateOnlyVar")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleUpdateOnlyVar {
		t.Fatalf("expected updateOnlyVar, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "updateOnlyVar" {
		t.Fatalf("expected raw updateOnlyVar, got %v", raw)
	}
}

func TestParseReviewRules_PTableSpid(t *testing.T) {
	rules, raw, err := parseReviewRules("pTableSpid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RulePTableSpid {
		t.Fatalf("expected pTableSpid, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "pTableSpid" {
		t.Fatalf("expected raw pTableSpid, got %v", raw)
	}
}

func TestParseReviewRules_ForceOrder2Tbl(t *testing.T) {
	rules, raw, err := parseReviewRules("forceOrder2Tbl")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleForceOrder2Tbl {
		t.Fatalf("expected forceOrder2Tbl, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "forceOrder2Tbl" {
		t.Fatalf("expected raw forceOrder2Tbl, got %v", raw)
	}
}

func TestParseReviewRules_SaveTran(t *testing.T) {
	rules, raw, err := parseReviewRules("saveTran")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleSaveTran {
		t.Fatalf("expected saveTran, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "saveTran" {
		t.Fatalf("expected raw saveTran, got %v", raw)
	}
}

func TestParseReviewRules_UseDrop(t *testing.T) {
	rules, raw, err := parseReviewRules("useDrop")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleUseDrop {
		t.Fatalf("expected useDrop, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "useDrop" {
		t.Fatalf("expected raw useDrop, got %v", raw)
	}
}

func TestParseReviewRules_MathOperations(t *testing.T) {
	rules, raw, err := parseReviewRules("mathOperations")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleMathOperations {
		t.Fatalf("expected mathOperations, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "mathOperations" {
		t.Fatalf("expected raw mathOperations, got %v", raw)
	}
}

func TestParseReviewRules_ExistsWithAndInIf(t *testing.T) {
	rules, raw, err := parseReviewRules("existsWithAndInIf")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleExistsWithAndInIf {
		t.Fatalf("expected existsWithAndInIf, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "existsWithAndInIf" {
		t.Fatalf("expected raw existsWithAndInIf, got %v", raw)
	}
}

func TestParseReviewRules_EmptyReturn(t *testing.T) {
	rules, raw, err := parseReviewRules("emptyReturn")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleEmptyReturn {
		t.Fatalf("expected emptyReturn, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "emptyReturn" {
		t.Fatalf("expected raw emptyReturn, got %v", raw)
	}
}

func TestParseReviewRules_RawTransactionControl(t *testing.T) {
	rules, raw, err := parseReviewRules("rawTransactionControl")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleRawTransactionControl {
		t.Fatalf("expected rawTransactionControl, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "rawTransactionControl" {
		t.Fatalf("expected raw rawTransactionControl, got %v", raw)
	}
}

func TestParseReviewRules_DeferredUpdate(t *testing.T) {
	rules, raw, err := parseReviewRules("deferredUpdate")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleDeferredUpdate {
		t.Fatalf("expected deferredUpdate, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "deferredUpdate" {
		t.Fatalf("expected raw deferredUpdate, got %v", raw)
	}
}

func TestParseReviewRules_InSubQuery(t *testing.T) {
	rules, raw, err := parseReviewRules("inSubQuery")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleInSubQuery {
		t.Fatalf("expected inSubQuery, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "inSubQuery" {
		t.Fatalf("expected raw inSubQuery, got %v", raw)
	}
}

func TestParseReviewRules_VarcharSize(t *testing.T) {
	rules, raw, err := parseReviewRules("varcharSize")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleVarcharSize {
		t.Fatalf("expected varcharSize, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "varcharSize" {
		t.Fatalf("expected raw varcharSize, got %v", raw)
	}
}

func TestParseReviewRules_ColumnInsert(t *testing.T) {
	rules, raw, err := parseReviewRules("columnInsert")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleColumnInsert {
		t.Fatalf("expected columnInsert, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "columnInsert" {
		t.Fatalf("expected raw columnInsert, got %v", raw)
	}
}

func TestParseReviewRules_PostgreLabelGotoLevel(t *testing.T) {
	rules, raw, err := parseReviewRules("postgreLabelGotoLevel")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RulePostgreLabelGotoLevel {
		t.Fatalf("expected postgreLabelGotoLevel, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "postgreLabelGotoLevel" {
		t.Fatalf("expected raw postgreLabelGotoLevel, got %v", raw)
	}
}

func TestParseReviewRules_DateIntoString(t *testing.T) {
	rules, raw, err := parseReviewRules("dateIntoString")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleDateIntoString {
		t.Fatalf("expected dateIntoString, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "dateIntoString" {
		t.Fatalf("expected raw dateIntoString, got %v", raw)
	}
}

func TestParseReviewRules_EmptyStringDate(t *testing.T) {
	rules, raw, err := parseReviewRules("emptyStringDate")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != review.RuleEmptyStringDate {
		t.Fatalf("expected emptyStringDate, got %v", rules)
	}
	if len(raw) != 1 || raw[0] != "emptyStringDate" {
		t.Fatalf("expected raw emptyStringDate, got %v", raw)
	}
}

func TestParseReviewRules_UnknownRule(t *testing.T) {
	_, _, err := parseReviewRules("unknownRule")
	if err == nil {
		t.Fatalf("expected error for unknown rule")
	}
}
