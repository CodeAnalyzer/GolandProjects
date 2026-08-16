package review

import (
	"context"
	"fmt"
	"sort"
	"strings"

	sqlparser "github.com/codebase/internal/parser/sql"
)

type ruleMeta struct {
	Severity    int
	Description string
	Build       func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error)
}

var ruleCatalog = map[RuleID]ruleMeta{
	RuleForeignTablesUsing: {
		Severity:    SeverityFineCode,
		Description: "Использование таблицы чужого продукта",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkForeignTables(ctx, parsed, file, "t")
		},
	},
	RuleForeignPTablesUsing: {
		Severity:    SeverityFineCode,
		Description: "Использование P-таблицы чужого продукта",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkForeignPTables(ctx, parsed, file)
		},
	},
	RuleForeignProcedureUsing: {
		Severity:    SeverityFineCode,
		Description: "Вызов процедуры чужого продукта",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkForeignProcedures(ctx, parsed, file)
		},
	},
	RuleExecNotExistsProc: {
		Severity:    SeverityDeployStopper,
		Description: "Вызов несуществующей процедуры",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkExecNotExistsProcedures(ctx, parsed, file)
		},
	},
	RuleProcDuplicate: {
		Severity:    SeverityDeployStopper,
		Description: "Дублирование процедур",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkProcDuplicate(ctx, parsed, file)
		},
	},
	RuleProcParamDefValue: {
		Severity:    SeverityDeployStopper,
		Description: "Параметр процедуры без значения по умолчанию",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkProcParamDefValue(ctx, parsed, file)
		},
	},
	RuleProcElseCase: {
		Severity:    SeverityDeployStopper,
		Description: "ELSE в CASE вместо отдельной ветки",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkProcElseCase(ctx, file)
		},
	},
	RuleUseSelectAll: {
		Severity:    SeverityDeployStopper,
		Description: "SELECT * вместо явного списка столбцов",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkUseSelectAll(ctx, file)
		},
	},
	RuleTruncTbl: {
		Severity:    SeverityDeployStopper,
		Description: "TRUNCATE TABLE в процедуре",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkTruncTbl(ctx, file)
		},
	},
	RuleDatatype: {
		Severity:    SeverityFineCode,
		Description: "Несовпадение типов данных",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkDatatype(ctx, parsed, file)
		},
	},
	RuleAnsiInJoin: {
		Severity:    SeverityDeployStopper,
		Description: "ANSI JOIN без индексного хинта",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkAnsiInJoin(ctx, file)
		},
	},
	RuleInsertRowLock: {
		Severity:    SeverityDeployStopper,
		Description: "INSERT без ROWLOCK",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkInsertRowLock(ctx, file)
		},
	},
	RuleUseEqColumn: {
		Severity:    SeverityDeployStopper,
		Description: "Сравнение по столбцу без индекса",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkUseEqColumn(ctx, file)
		},
	},
	RuleTableFullScan: {
		Severity:    SeverityDeployStopper,
		Description: "Полное сканирование таблицы",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkTableFullScan(ctx, file)
		},
	},
	RuleTableHintExists: {
		Severity:    SeverityDeployStopper,
		Description: "Отсутствие хинта индекса у таблицы",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkTableHintExists(ctx, file)
		},
	},
	RuleTableHintIsRight: {
		Severity:    SeverityDeployStopper,
		Description: "Неправильный хинт индекса для типа операции",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkTableHintIsRight(ctx, file)
		},
	},
	RuleIndexExistsInDB: {
		Severity:    SeverityDeployStopper,
		Description: "Индекс не существует в БД",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkIndexExistsInDB(ctx, file)
		},
	},
	RuleIndexWrong: {
		Severity:    SeverityDeployStopper,
		Description: "Неправильный индекс",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkIndexWrong(ctx, file)
		},
	},
	RuleUpdateOnlyVar: {
		Severity:    SeverityDeployStopper,
		Description: "UPDATE только по переменной",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkUpdateOnlyVar(ctx, file)
		},
	},
	RulePTableSpid: {
		Severity:    SeverityDeployStopper,
		Description: "P-таблица без фильтра SPID",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkPTableSpid(ctx, file)
		},
	},
	RuleForceOrder2Tbl: {
		Severity:    SeverityDeployStopper,
		Description: "FORCEORDER для двух таблиц",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkForceOrder2Tbl(ctx, file)
		},
	},
	RuleSaveTran: {
		Severity:    SeverityDeployStopper,
		Description: "SAVE TRAN в процедуре",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkSaveTran(ctx, file)
		},
	},
	RuleUseDrop: {
		Severity:    SeverityDeployStopper,
		Description: "DROP в процедуре",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkUseDrop(ctx, file)
		},
	},
	RuleMathOperations: {
		Severity:    SeverityDeployStopper,
		Description: "Математические операции над типами datetime/numeric",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkMathOperations(ctx, file)
		},
	},
	RuleExistsWithAndInIf: {
		Severity:    SeverityDeployStopper,
		Description: "EXISTS с AND в IF-условии",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkExistsWithAndInIf(ctx, file)
		},
	},
	RuleNullComparison: {
		Severity:    SeverityPostgreReq,
		Description: "Сравнение с NULL через =/<> вместо IS NULL",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkNullComparison(ctx, file)
		},
	},
	RuleShouldBeCP866: {
		Severity:    SeverityPostgreReq,
		Description: "Строковые литералы должны быть в CP866",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkShouldBeCP866(ctx, file)
		},
	},
	RuleTooManyJoins: {
		Severity:    SeverityPostgreReq,
		Description: "Слишком много JOIN в запросе",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkTooManyJoins(ctx, file)
		},
	},
	RuleMaxProcParam: {
		Severity:    SeverityPostgreReq,
		Description: "Превышение максимума параметров процедуры",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkMaxProcParam(ctx, parsed, file)
		},
	},
	RuleModifyOutProc: {
		Severity:    SeverityPostgreReq,
		Description: "Модификация данных в out-процедуре",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkModifyOutProc(ctx, parsed, file)
		},
	},
	RuleEmptyReturn: {
		Severity:    SeverityPostgreReq,
		Description: "Пустой RETURN в процедуре",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkEmptyReturn(ctx, file)
		},
	},
	RuleRawTransactionControl: {
		Severity:    SeverityPostgreReq,
		Description: "Прямое управление транзакциями (BEGIN/COMMIT/ROLLBACK)",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkRawTransactionControl(ctx, file)
		},
	},
	RuleDeferredUpdate: {
		Severity:    SeverityDeployStopper,
		Description: "Отложенное обновление (deferred update)",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkDeferredUpdate(ctx, file)
		},
	},
	RuleInSubQuery: {
		Severity:    SeverityDeployStopper,
		Description: "IN с подзапросом вместо JOIN/EXISTS",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkInSubQuery(ctx, file)
		},
	},
	RuleVarcharSize: {
		Severity:    SeverityDeployStopper,
		Description: "VARCHAR без указания размера",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkVarcharSize(ctx, parsed, file)
		},
	},
	RuleColumnInsert: {
		Severity:    SeverityDeployStopper,
		Description: "INSERT без явного списка столбцов",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkColumnInsert(ctx, file)
		},
	},
	RulePostgreLabelGotoLevel: {
		Severity:    SeverityPostgreReq,
		Description: "GOTO на уровень метки (не поддерживается в PostgreSQL)",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkPostgreLabelGotoLevel(ctx, file)
		},
	},
	RuleDateIntoString: {
		Severity:    SeverityPostgreReq,
		Description: "Преобразование date/datetime в строку",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkDateIntoString(ctx, parsed, file)
		},
	},
	RuleEmptyStringDate: {
		Severity:    SeverityPostgreReq,
		Description: "Пустая строка вместо NULL для date/datetime",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkEmptyStringDate(ctx, parsed, file)
		},
	},
	RuleVarUseAfterCursor: {
		Severity:    SeverityPostgreReq,
		Description: "Использование переменной после закрытия курсора",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkVarUseAfterCursor(ctx, file)
		},
	},
	RuleExcessProcParams: {
		Severity:    SeverityPostgreReq,
		Description: "Избыточные параметры процедуры",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkExcessProcParams(ctx, parsed, file)
		},
	},
	RuleDuplicateOutputVariable: {
		Severity:    SeverityPostgreReq,
		Description: "Дублирование OUTPUT-переменной",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkDuplicateOutputVariable(ctx, parsed, file)
		},
	},
	RuleUseOnlyDeclaredCursors: {
		Severity:    SeverityPostgreReq,
		Description: "Использование курсора без DECLARE",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkUseOnlyDeclaredCursors(ctx, parsed, file)
		},
	},
	RuleCursorFetchArguments: {
		Severity:    SeverityPostgreReq,
		Description: "Несовпадение аргументов FETCH с DECLARE CURSOR",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkCursorFetchArguments(ctx, parsed, file)
		},
	},
	RuleUsageVarInSameSelect: {
		Severity:    SeverityPostgreReq,
		Description: "Использование переменной в том же SELECT",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkUsageVarInSameSelect(ctx, parsed, file)
		},
	},
	RuleVarAssignInUpdate: {
		Severity:    SeverityPostgreReq,
		Description: "Присвоение переменной в UPDATE",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkVarAssignInUpdate(ctx, parsed, file)
		},
	},
	RuleStatementsWithJoinsRequireAliases: {
		Severity:    SeverityPostgreReq,
		Description: "JOIN без алиасов таблиц",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkStatementsWithJoinsRequireAliases(ctx, parsed, file)
		},
	},
	RuleUseFuncInIndCol: {
		Severity:    SeverityPostgreReq,
		Description: "Функция в индексированном столбце",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkUseFuncInIndCol(ctx, file)
		},
	},
	RuleIsNullSameTypes: {
		Severity:    SeverityPostgreReq,
		Description: "IS NULL с разными типами",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkIsNullSameTypes(ctx, parsed, file)
		},
	},
	RuleDiffTypesComparison: {
		Severity:    SeverityPostgreReq,
		Description: "Сравнение значений разных типов",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkDiffTypesComparison(ctx, parsed, file)
		},
	},
	RuleFloatToStringConvert: {
		Severity:    SeverityPostgreReq,
		Description: "Преобразование float в строку",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkFloatToStringConvert(ctx, parsed, file)
		},
	},
	RuleSelectAfterSetRowcount: {
		Severity:    SeverityPostgreReq,
		Description: "SELECT после SET ROWCOUNT",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkSelectAfterSetRowcount(ctx, parsed, file)
		},
	},
	RuleAliasWhenUsingUnion: {
		Severity:    SeverityPostgreReq,
		Description: "Алиасы столбцов при использовании UNION",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkAliasWhenUsingUnion(ctx, parsed, file)
		},
	},
}

func AllRuleIDs() []RuleID {
	ids := make([]RuleID, 0, len(ruleCatalog))
	for id := range ruleCatalog {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return string(ids[i]) < string(ids[j]) })
	return ids
}

func RuleListString() string {
	ids := AllRuleIDs()
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = string(id)
	}
	return strings.Join(parts, ",")
}

func ValidateRuleIDs(raw []string) ([]RuleID, error) {
	seen := make(map[RuleID]struct{})
	result := make([]RuleID, 0, len(raw))
	for _, s := range raw {
		id := RuleID(strings.TrimSpace(s))
		if id == "" {
			continue
		}
		if _, exists := ruleCatalog[id]; !exists {
			return nil, fmt.Errorf("unknown review rule: %s", id)
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result, nil
}

func DefaultEnabledRules() map[RuleID]bool {
	result := make(map[RuleID]bool, len(ruleCatalog))
	for id := range ruleCatalog {
		result[id] = true
	}
	return result
}
