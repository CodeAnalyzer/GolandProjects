package apimacro

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	cbencoding "github.com/codebase/internal/encoding"
	"github.com/codebase/internal/model"
)

type Parser struct {
	createProcRe *regexp.Regexp
	initEventRe  *regexp.Regexp
	execAPIRe    *regexp.Regexp
	execProcRe   *regexp.Regexp
	dispatchRe   *regexp.Regexp
	procNameRe   *regexp.Regexp
}

type ParseResult struct {
	ProcedureName string
	Invocations   []*model.APIMacroInvocation
}

func NewParser() *Parser {
	return &Parser{
		createProcRe: regexp.MustCompile(`(?i)API_CREATE_PROC\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)`),
		initEventRe:  regexp.MustCompile(`(?i)API_INIT_EVENT\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)`),
		execAPIRe:    regexp.MustCompile(`(?i)API_EXEC\s*\((?:\s*[A-Za-z_][A-Za-z0-9_]*\s*,\s*)?\s*([A-Za-z_][A-Za-z0-9_]*)`),
		execProcRe:   regexp.MustCompile(`(?i)exec\s+(?:@RetVal\s*=\s*)?([A-Za-z_][A-Za-z0-9_]*)`),
		dispatchRe:   regexp.MustCompile(`(?i)exec\s+(?:@RetVal\s*=\s*)?([A-Za-z_][A-Za-z0-9_]*)[\s\S]*@ProcessID\s*=\s*@GlobalProcessID`),
		procNameRe:   regexp.MustCompile(`(?i)(?:__BEGIN_PROCEDURE__\s*\(|create\s+(?:procedure|proc)\s+)([A-Za-z_][A-Za-z0-9_]*)`),
	}
}

func (p *Parser) ParseFile(path string, language string) (*ParseResult, error) {
	enc := cbencoding.CP866
	switch strings.ToUpper(strings.TrimSpace(language)) {
	case "T01":
		enc = cbencoding.CP866
	case "SQL":
		enc = cbencoding.CP866
	}
	content, err := cbencoding.ReadFile(path, enc)
	if err != nil {
		return nil, err
	}
	return p.ParseContent(path, content)
}

func (p *Parser) ParseContent(path string, content string) (*ParseResult, error) {
	result := &ParseResult{Invocations: make([]*model.APIMacroInvocation, 0)}
	lines := strings.Split(content, "\n")
	result.ProcedureName = detectProcedureName(p.procNameRe, lines, filepath.Base(path))
	for i, line := range lines {
		if m := p.createProcRe.FindStringSubmatch(line); len(m) > 1 {
			result.Invocations = append(result.Invocations, &model.APIMacroInvocation{ProcedureName: result.ProcedureName, MacroType: "create_proc", TargetName: strings.TrimSpace(m[1]), TargetKind: "contract", LineNumber: i + 1, RawText: strings.TrimSpace(line)})
		}
		if m := p.initEventRe.FindStringSubmatch(line); len(m) > 1 {
			result.Invocations = append(result.Invocations, &model.APIMacroInvocation{ProcedureName: result.ProcedureName, MacroType: "init_event", TargetName: strings.TrimSpace(m[1]), TargetKind: "contract", LineNumber: i + 1, RawText: strings.TrimSpace(line)})
		}
		if m := p.execAPIRe.FindStringSubmatch(line); len(m) > 1 {
			result.Invocations = append(result.Invocations, &model.APIMacroInvocation{ProcedureName: result.ProcedureName, MacroType: "exec_contract", TargetName: strings.TrimSpace(m[1]), TargetKind: "contract", LineNumber: i + 1, RawText: strings.TrimSpace(line)})
		}
		if strings.HasSuffix(strings.ToLower(path), ".t01") {
			if strings.Contains(strings.ToLower(line), "@processid") {
				m := p.execProcRe.FindStringSubmatch(line)
				if len(m) > 1 {
					proc := strings.TrimSpace(m[1])
					if !strings.EqualFold(proc, "GetAPIProcessID") {
						result.Invocations = append(result.Invocations, &model.APIMacroInvocation{ProcedureName: result.ProcedureName, MacroType: "dispatches_to", TargetName: proc, TargetKind: "procedure", LineNumber: i + 1, RawText: strings.TrimSpace(line)})
					}
				}
			}
		}
	}
	return result, nil
}

func detectProcedureName(procNameRe *regexp.Regexp, lines []string, fallback string) string {
	for _, line := range lines {
		if m := procNameRe.FindStringSubmatch(line); len(m) > 1 {
			return strings.TrimSpace(m[1])
		}
	}
	base := strings.TrimSuffix(filepath.Base(fallback), filepath.Ext(fallback))
	if strings.TrimSpace(base) == "" {
		return ""
	}
	return base
}

func (p *Parser) MustParseContent(path string, content string) *ParseResult {
	result, err := p.ParseContent(path, content)
	if err != nil {
		panic(fmt.Sprintf("failed to parse macro content: %v", err))
	}
	return result
}
