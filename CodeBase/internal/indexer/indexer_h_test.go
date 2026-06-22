package indexer

import "testing"

func TestIsLineInsideMacroDefinition(t *testing.T) {
	tests := []struct {
		name    string
		lines   []string
		lineNum int
		want    bool
	}{
		{
			name: "single line macro definition",
			lines: []string{
				"#define DCL_PROC_BEGIN(NAME) create procedure NAME as",
			},
			lineNum: 1,
			want:    true,
		},
		{
			name: "multi line macro continuation",
			lines: []string{
				"#define ARC_PROC_BEGIN(proc_name) \\",
				"DCL_PROC_BEGIN(proc_name)             \\",
				"__BEGIN_PROCEDURE__(proc_name)        \\",
				"__END_PROCEDURE__(proc_name)",
			},
			lineNum: 4,
			want:    true,
		},
		{
			name: "real procedure not inside macro",
			lines: []string{
				"#define DCL_PROC_BEGIN(NAME) create procedure NAME as",
				"",
				"DCL_PROC_BEGIN(Ins_Check_ExistsLinkObject)",
				"as",
				"  __BEGIN_PROCEDURE__(Ins_Check_ExistsLinkObject)",
				"  select 1",
				"__END_PROCEDURE__(Ins_Check_ExistsLinkObject)",
			},
			lineNum: 3,
			want:    false,
		},
		{
			name: "line after macro not continued",
			lines: []string{
				"#define FOO 1",
				"DCL_PROC_BEGIN(RealProc)",
			},
			lineNum: 2,
			want:    false,
		},
		{
			name: "indented macro",
			lines: []string{
				"  #define DCL_PROC_BEGIN(NAME) create procedure NAME as",
			},
			lineNum: 1,
			want:    true,
		},
		{
			name: "comment with #define inside not counted",
			lines: []string{
				"/* #define DCL_PROC_BEGIN(NAME) */",
				"DCL_PROC_BEGIN(RealProc)",
			},
			lineNum: 2,
			want:    false,
		},
		{
			name: "line number out of range",
			lines: []string{
				"#define FOO 1",
			},
			lineNum: 99,
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isLineInsideMacroDefinition(tt.lines, tt.lineNum)
			if got != tt.want {
				t.Fatalf("isLineInsideMacroDefinition(...) = %v, want %v", got, tt.want)
			}
		})
	}
}
