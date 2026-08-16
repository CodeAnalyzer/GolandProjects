package review

import (
	"strings"
	"testing"

	"github.com/codebase/internal/model"
	sqlparser "github.com/codebase/internal/parser/sql"
)

func TestIsEmptyStringLiteral(t *testing.T) {
	cases := []struct {
		expr string
		want bool
	}{
		{"''", true},
		{"N''", true},
		{"' '", true},
		{"'   '", true},
		{"'hello'", false},
		{"0", false},
		{"null", false},
		{"", false},
		{"@var", false},
	}
	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			got := isEmptyStringLiteral(tc.expr)
			if got != tc.want {
				t.Fatalf("isEmptyStringLiteral(%q) = %v, want %v", tc.expr, got, tc.want)
			}
		})
	}
}

func TestCheckEmptyStringDate(t *testing.T) {
	cases := []struct {
		name       string
		content    string
		procs      []*model.SQLProcedure
		fragments  []*model.QueryFragment
		wantCount  int
		wantLine   int
		wantObject string
	}{
		{
			name:    "SELECT empty string into datetime var",
			content: "create proc TestProc\nas\ndeclare @DateVar datetime\n\nselect @DateVar = ''\n",
			procs:   []*model.SQLProcedure{{ProcName: "TestProc", LineStart: 1}},
			fragments: []*model.QueryFragment{
				{QueryText: "select @DateVar = ''", LineNumber: 5},
			},
			wantCount:  1,
			wantLine:   5,
			wantObject: "@DateVar",
		},
		{
			name:    "SET empty string into datetime var",
			content: "create proc TestProc\nas\ndeclare @DateVar datetime\n\nset @DateVar = ''\n",
			procs:   []*model.SQLProcedure{{ProcName: "TestProc", LineStart: 1}},
			wantCount:  1,
			wantLine:   5,
			wantObject: "@datevar",
		},
		{
			name:    "DECLARE datetime with empty string init",
			content: "create proc TestProc\nas\ndeclare @DateVar datetime = ''\n",
			procs:   []*model.SQLProcedure{{ProcName: "TestProc", LineStart: 1}},
			wantCount:  1,
			wantLine:   3,
			wantObject: "@datevar",
		},
		{
			name:    "convert datetime with empty string",
			content: "create proc TestProc\nas\nselect convert(datetime, '')\n",
			procs:   []*model.SQLProcedure{{ProcName: "TestProc", LineStart: 1}},
			wantCount:  1,
			wantLine:   3,
			wantObject: "convert(datetime, '')",
		},
		{
			name:    "cast empty string as datetime",
			content: "create proc TestProc\nas\nselect cast('' as datetime)\n",
			procs:   []*model.SQLProcedure{{ProcName: "TestProc", LineStart: 1}},
			wantCount:  1,
			wantLine:   3,
			wantObject: "cast('' as datetime)",
		},
		{
			name:    "datetime param with empty string default",
			content: "create proc TestProc\n\t@DateParam datetime = ''\nas\nselect 1\n",
			procs: []*model.SQLProcedure{
				{ProcName: "TestProc", LineStart: 1, Params: []model.SQLParam{
					{Name: "@DateParam", Type: "datetime", DefaultValue: "''"},
				}},
			},
			wantCount:  1,
			wantLine:   1,
			wantObject: "@DateParam",
		},
		{
			name:    "int var should skip",
			content: "create proc TestProc\nas\ndeclare @IntVar int\n\nset @IntVar = ''\n",
			procs:   []*model.SQLProcedure{{ProcName: "TestProc", LineStart: 1}},
			wantCount: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &Runner{}
			path := normalizePath("test.sql")
			r.exec = &reviewExecContext{filePath: path, content: []byte(tc.content), lines: strings.Split(tc.content, "\n"), macroResult: replaceMacros(tc.content)}
			parsed := &sqlparser.ParseResult{
				Procedures: tc.procs,
				Fragments:  tc.fragments,
			}
			findings, err := r.checkEmptyStringDate(parsed, &indexedFile{Path: path, DsProductID: 1})
			r.exec = nil
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(findings) != tc.wantCount {
				t.Fatalf("findings count = %d, want %d", len(findings), tc.wantCount)
			}
			if tc.wantCount > 0 && len(findings) > 0 {
				if findings[0].Line != tc.wantLine {
					t.Fatalf("findings[0].Line = %d, want %d", findings[0].Line, tc.wantLine)
				}
				if findings[0].Object != tc.wantObject {
					t.Fatalf("findings[0].Object = %q, want %q", findings[0].Object, tc.wantObject)
				}
			}
		})
	}
}
