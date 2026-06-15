package review

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunRuleTasks_SuccessAggregatesFindings(t *testing.T) {
	tasks := []ruleTask{
		{rule: RuleUseDrop, run: func(ctx context.Context) ([]Finding, error) {
			return []Finding{{Rule: RuleUseDrop, Line: 10}}, nil
		}},
		{rule: RuleMathOperations, run: func(ctx context.Context) ([]Finding, error) {
			return []Finding{{Rule: RuleMathOperations, Line: 20}}, nil
		}},
	}

	findings, err := runRuleTasks(tasks, 2)
	if err != nil {
		t.Fatalf("runRuleTasks(...) unexpected error: %v", err)
	}
	if len(findings) != 2 {
		t.Fatalf("findings count = %d, want 2", len(findings))
	}
}

func TestRunRuleTasks_FirstErrorCancels(t *testing.T) {
	wantErr := errors.New("boom")

	tasks := []ruleTask{
		{rule: RuleUseDrop, run: func(ctx context.Context) ([]Finding, error) {
			select {
			case <-ctx.Done():
				return nil, nil
			case <-time.After(2 * time.Second):
				return nil, errors.New("context was not cancelled in time")
			}
		}},
		{rule: RuleMathOperations, run: func(ctx context.Context) ([]Finding, error) {
			return nil, wantErr
		}},
	}

	_, err := runRuleTasks(tasks, 2)
	if !errors.Is(err, wantErr) {
		t.Fatalf("runRuleTasks(...) error = %v, want %v", err, wantErr)
	}
}

func TestEnabledRuleSet_CustomSubset(t *testing.T) {
	rules := []RuleID{RuleForeignTablesUsing, RuleDatatype}
	set := enabledRuleSet(rules)

	if !set[RuleForeignTablesUsing] {
		t.Fatalf("RuleForeignTablesUsing should be enabled")
	}
	if !set[RuleDatatype] {
		t.Fatalf("RuleDatatype should be enabled")
	}
	if set[RuleForeignPTablesUsing] {
		t.Fatalf("RuleForeignPTablesUsing should be disabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
	if set[RuleExecNotExistsProc] {
		t.Fatalf("RuleExecNotExistsProc should be disabled")
	}
}

func TestEnabledRuleSet_ExecNotExistsProc(t *testing.T) {
	rules := []RuleID{RuleExecNotExistsProc}
	set := enabledRuleSet(rules)

	if !set[RuleExecNotExistsProc] {
		t.Fatalf("RuleExecNotExistsProc should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_IndexWrong(t *testing.T) {
	rules := []RuleID{RuleIndexWrong}
	set := enabledRuleSet(rules)

	if !set[RuleIndexWrong] {
		t.Fatalf("RuleIndexWrong should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_UpdateOnlyVar(t *testing.T) {
	rules := []RuleID{RuleUpdateOnlyVar}
	set := enabledRuleSet(rules)

	if !set[RuleUpdateOnlyVar] {
		t.Fatalf("RuleUpdateOnlyVar should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_PTableSpid(t *testing.T) {
	rules := []RuleID{RulePTableSpid}
	set := enabledRuleSet(rules)

	if !set[RulePTableSpid] {
		t.Fatalf("RulePTableSpid should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_ForceOrder2Tbl(t *testing.T) {
	rules := []RuleID{RuleForceOrder2Tbl}
	set := enabledRuleSet(rules)

	if !set[RuleForceOrder2Tbl] {
		t.Fatalf("RuleForceOrder2Tbl should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_SaveTran(t *testing.T) {
	rules := []RuleID{RuleSaveTran}
	set := enabledRuleSet(rules)

	if !set[RuleSaveTran] {
		t.Fatalf("RuleSaveTran should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_UseDrop(t *testing.T) {
	rules := []RuleID{RuleUseDrop}
	set := enabledRuleSet(rules)

	if !set[RuleUseDrop] {
		t.Fatalf("RuleUseDrop should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_MathOperations(t *testing.T) {
	rules := []RuleID{RuleMathOperations}
	set := enabledRuleSet(rules)

	if !set[RuleMathOperations] {
		t.Fatalf("RuleMathOperations should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_ExistsWithAndInIf(t *testing.T) {
	rules := []RuleID{RuleExistsWithAndInIf}
	set := enabledRuleSet(rules)

	if !set[RuleExistsWithAndInIf] {
		t.Fatalf("RuleExistsWithAndInIf should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_IndexExistsInDB(t *testing.T) {
	rules := []RuleID{RuleIndexExistsInDB}
	set := enabledRuleSet(rules)

	if !set[RuleIndexExistsInDB] {
		t.Fatalf("RuleIndexExistsInDB should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_ProcDuplicate(t *testing.T) {
	rules := []RuleID{RuleProcDuplicate}
	set := enabledRuleSet(rules)

	if !set[RuleProcDuplicate] {
		t.Fatalf("RuleProcDuplicate should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_ProcParamDefValue(t *testing.T) {
	rules := []RuleID{RuleProcParamDefValue}
	set := enabledRuleSet(rules)

	if !set[RuleProcParamDefValue] {
		t.Fatalf("RuleProcParamDefValue should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_ProcElseCase(t *testing.T) {
	rules := []RuleID{RuleProcElseCase}
	set := enabledRuleSet(rules)

	if !set[RuleProcElseCase] {
		t.Fatalf("RuleProcElseCase should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_UseSelectAll(t *testing.T) {
	rules := []RuleID{RuleUseSelectAll}
	set := enabledRuleSet(rules)

	if !set[RuleUseSelectAll] {
		t.Fatalf("RuleUseSelectAll should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_TruncTbl(t *testing.T) {
	rules := []RuleID{RuleTruncTbl}
	set := enabledRuleSet(rules)

	if !set[RuleTruncTbl] {
		t.Fatalf("RuleTruncTbl should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_AnsiInJoin(t *testing.T) {
	rules := []RuleID{RuleAnsiInJoin}
	set := enabledRuleSet(rules)

	if !set[RuleAnsiInJoin] {
		t.Fatalf("RuleAnsiInJoin should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_UseEqColumn(t *testing.T) {
	rules := []RuleID{RuleUseEqColumn}
	set := enabledRuleSet(rules)

	if !set[RuleUseEqColumn] {
		t.Fatalf("RuleUseEqColumn should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_TableFullScan(t *testing.T) {
	rules := []RuleID{RuleTableFullScan}
	set := enabledRuleSet(rules)

	if !set[RuleTableFullScan] {
		t.Fatalf("RuleTableFullScan should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_TableHintExists(t *testing.T) {
	rules := []RuleID{RuleTableHintExists}
	set := enabledRuleSet(rules)

	if !set[RuleTableHintExists] {
		t.Fatalf("RuleTableHintExists should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_TableHintIsRight(t *testing.T) {
	rules := []RuleID{RuleTableHintIsRight}
	set := enabledRuleSet(rules)

	if !set[RuleTableHintIsRight] {
		t.Fatalf("RuleTableHintIsRight should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_EmptyReturn(t *testing.T) {
	rules := []RuleID{RuleEmptyReturn}
	set := enabledRuleSet(rules)

	if !set[RuleEmptyReturn] {
		t.Fatalf("RuleEmptyReturn should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_RawTransactionControl(t *testing.T) {
	rules := []RuleID{RuleRawTransactionControl}
	set := enabledRuleSet(rules)

	if !set[RuleRawTransactionControl] {
		t.Fatalf("RuleRawTransactionControl should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_InsertRowLock(t *testing.T) {
	rules := []RuleID{RuleInsertRowLock}
	set := enabledRuleSet(rules)

	if !set[RuleInsertRowLock] {
		t.Fatalf("RuleInsertRowLock should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_DeferredUpdate(t *testing.T) {
	rules := []RuleID{RuleDeferredUpdate}
	set := enabledRuleSet(rules)

	if !set[RuleDeferredUpdate] {
		t.Fatalf("RuleDeferredUpdate should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_InSubQuery(t *testing.T) {
	rules := []RuleID{RuleInSubQuery}
	set := enabledRuleSet(rules)
	if !set[RuleInSubQuery] {
		t.Fatalf("RuleInSubQuery should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_VarcharSize(t *testing.T) {
	rules := []RuleID{RuleVarcharSize}
	set := enabledRuleSet(rules)
	if !set[RuleVarcharSize] {
		t.Fatalf("RuleVarcharSize should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_ColumnInsert(t *testing.T) {
	rules := []RuleID{RuleColumnInsert}
	set := enabledRuleSet(rules)
	if !set[RuleColumnInsert] {
		t.Fatalf("RuleColumnInsert should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_PostgreLabelGotoLevel(t *testing.T) {
	rules := []RuleID{RulePostgreLabelGotoLevel}
	set := enabledRuleSet(rules)
	if !set[RulePostgreLabelGotoLevel] {
		t.Fatalf("RulePostgreLabelGotoLevel should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}

func TestEnabledRuleSet_UseOnlyDeclaredCursors(t *testing.T) {
	rules := []RuleID{RuleUseOnlyDeclaredCursors}
	set := enabledRuleSet(rules)
	if !set[RuleUseOnlyDeclaredCursors] {
		t.Fatalf("RuleUseOnlyDeclaredCursors should be enabled")
	}
	if set[RuleForeignProcedureUsing] {
		t.Fatalf("RuleForeignProcedureUsing should be disabled")
	}
}
