package review

type RuleID string

const (
	RuleForeignTablesUsing    RuleID = "foreignTablesUsing"
	RuleForeignPTablesUsing   RuleID = "foreignPTablesUsing"
	RuleForeignProcedureUsing RuleID = "foreignProcedureUsing"
	RuleExecNotExistsProc     RuleID = "execNotExistsProc"
	RuleProcDuplicate         RuleID = "procDuplicate"
	RuleProcParamDefValue     RuleID = "procParamDefValue"
	RuleProcElseCase          RuleID = "procElseCase"
	RuleUseSelectAll          RuleID = "useSelectAll"
	RuleTruncTbl              RuleID = "truncTbl"
	RuleDatatype              RuleID = "datatype"
	RuleAnsiInJoin            RuleID = "ansiInJoin"
	RuleInsertRowLock         RuleID = "insertRowLock"
	RuleUseEqColumn           RuleID = "useEqColumn"
	RuleTableFullScan         RuleID = "tableFullScan"
	RuleTableHintExists       RuleID = "tableHintExists"
	RuleTableHintIsRight      RuleID = "tableHintIsRight"
	RuleIndexExistsInDB       RuleID = "indexExistsInDB"
	RuleIndexWrong            RuleID = "indexWrong"
	RuleUpdateOnlyVar         RuleID = "updateOnlyVar"
	RulePTableSpid            RuleID = "pTableSpid"
	RuleForceOrder2Tbl        RuleID = "forceOrder2Tbl"
	RuleSaveTran              RuleID = "saveTran"
	RuleUseDrop               RuleID = "useDrop"
	RuleMathOperations        RuleID = "mathOperations"
	RuleExistsWithAndInIf     RuleID = "existsWithAndInIf"
)

const (
	SeverityDeployStopper = 1
	SeverityFineCode      = 3
)

type Finding struct {
	Rule             RuleID `json:"rule"`
	Severity         int    `json:"severity"`
	Message          string `json:"message"`
	File             string `json:"file"`
	Line             int    `json:"line,omitempty"`
	Object           string `json:"object,omitempty"`
	CurrentProductID int64  `json:"current_product_id,omitempty"`
	TargetProductID  int64  `json:"target_product_id,omitempty"`
}

type Summary struct {
	Total      int            `json:"total"`
	ByRule     map[RuleID]int `json:"by_rule,omitempty"`
	BySeverity map[int]int    `json:"by_severity,omitempty"`
}

type Result struct {
	AnalyzedFile string    `json:"analyzed_file"`
	Summary      Summary   `json:"summary"`
	Findings     []Finding `json:"findings"`
}

type Options struct {
	Rules       []RuleID
	MinSeverity int
}

type indexedFile struct {
	ID          int64
	Path        string
	RelPath     string
	DsProductID int64
}

type tableIndexCandidate struct {
	Name   string
	Fields []string
}

type tableFromClause struct {
	TableName string
	Alias     string
	Hint      string // Извлеченный хинт индекса
	IndexName string // Имя индекса из M_*_INDEX(...)
}

type whereAnalysisResult struct {
	Aliases                  []string
	HasUnqualifiedConditions bool
}

type columnRef struct {
	Table  string
	Column string
}

type tableRef struct {
	Name string
	Line int
}

type procedureRef struct {
	Name string
	Line int
}

type updateAssignment struct {
	Target     string
	Expression string
}

type updateSetStatement struct {
	TargetTable string
	TargetAlias string
	Assignments []updateAssignment
	FromClause  string
}

type insertSelectStatement struct {
	TargetTable       string
	TargetColumns     []string
	SelectExpressions []string
	FromClause        string
}

type selectAssignment struct {
	TargetVariable string
	Expression     string
}

type selectAssignStatement struct {
	Assignments []selectAssignment
	FromClause  string
}
