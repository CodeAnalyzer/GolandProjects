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
