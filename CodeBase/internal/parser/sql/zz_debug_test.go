package sql

import (
	"fmt"
	"testing"
)

func TestDebugTrace(t *testing.T) {
	content := "create proc First_Proc\nas\n/* desc */\nbegin\n  select 1\nend\nX_ANYMODE(First_Proc)\n\ncreate proc Second_Proc\nas\nbegin\n  select 2\nend\nX_ANYMODE(Second_Proc)"
	result, _ := NewParser().ParseContent(content)
	fmt.Println("procs:", len(result.Procedures))
}
