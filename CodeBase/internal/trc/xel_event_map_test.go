package trc

import (
	"testing"
)

// TestXELEventMapGenerated_NoCollisions проверяет, что сгенерированная карта
// xeEventNameToClass не содержит очевидных дублирующих ошибок — т.е. что
// семантически разные XE-события не отображаются на один и тот же TRC EventClass
// (если это не задокументированное исключение), и что известные парные
// соответствия (starting/completed, sp_statement/sql_statement/module) не
// перепутаны.
func TestXELEventMapGenerated_NoCollisions(t *testing.T) {
	// 1. Известные парные соответствия — starting vs completed не должны
	//    совпадать внутри пары и не должны путаться между уровнями
	//    (SP:Stmt vs SQL:Stmt vs SP/module).
	type pair struct {
		starting    string
		completed   string
		startClass  int
		compClass   int
		startName   string
		compName    string
	}
	pairs := []pair{
		{"sp_statement_starting", "sp_statement_completed", 44, 45, "SP:StmtStarting", "SP:StmtCompleted"},
		{"sql_statement_starting", "sql_statement_completed", 40, 41, "SQL:StmtStarting", "SQL:StmtCompleted"},
		{"sql_batch_starting", "sql_batch_completed", 13, 12, "SQL:BatchStarting", "SQL:BatchCompleted"},
		{"rpc_starting", "rpc_completed", 11, 10, "RPC:Starting", "RPC:Completed"},
		{"module_start", "module_end", 42, 43, "SP:Starting", "SP:Completed"},
	}
	for _, p := range pairs {
		gotStart, okS := xeEventNameToClass[p.starting]
		if !okS {
			t.Errorf("xeEventNameToClass: %q отсутствует в карте", p.starting)
			continue
		}
		gotComp, okC := xeEventNameToClass[p.completed]
		if !okC {
			t.Errorf("xeEventNameToClass: %q отсутствует в карте", p.completed)
			continue
		}
		if gotStart != p.startClass {
			t.Errorf("xeEventNameToClass[%q] = %d, ожидается %d (%s)",
				p.starting, gotStart, p.startClass, p.startName)
		}
		if gotComp != p.compClass {
			t.Errorf("xeEventNameToClass[%q] = %d, ожидается %d (%s)",
				p.completed, gotComp, p.compClass, p.compName)
		}
		if gotStart == gotComp {
			t.Errorf("пара %q/%q: оба события отображаются на EventClass %d — коллизия",
				p.starting, p.completed, gotStart)
		}
	}

	// 2. Ключевая проверка из плана: module_start != sql_statement_starting.
	//    module_start = SP:Starting (42), sql_statement_starting = SQL:StmtStarting (40).
	//    Это разные уровни (SP vs SQL statement) и они НЕ должны совпадать.
	if xeEventNameToClass["module_start"] == xeEventNameToClass["sql_statement_starting"] {
		t.Errorf("module_start и sql_statement_starting отображаются на один EventClass %d — коллизия",
			xeEventNameToClass["module_start"])
	}
	if xeEventNameToClass["module_end"] == xeEventNameToClass["sql_statement_completed"] {
		t.Errorf("module_end и sql_statement_completed отображаются на один EventClass %d — коллизия",
			xeEventNameToClass["module_end"])
	}

	// 3. Проверка: все значения в xeEventNameToClass должны быть положительными
	//    (EventClass 0 зарезервирован для XE-only событий без TRC-эквивалента).
	for name, class := range xeEventNameToClass {
		if class <= 0 {
			t.Errorf("xeEventNameToClass[%q] = %d — недопустимый EventClass (должен быть > 0)", name, class)
		}
	}

	// 4. Проверка: все значения в xeActionNameToColumn должны быть положительными.
	for name, colID := range xeActionNameToColumn {
		if colID <= 0 {
			t.Errorf("xeActionNameToColumn[%q] = %d — недопустимый ColumnID (должен быть > 0)", name, colID)
		}
	}

	// 5. Известные конфликты action->ColumnID из генератора (документируем
	//    принятые решения, чтобы изменение генератора сломало тест).
	knownActionConflicts := map[string]int{
		// tsql_frame: в CSV встречается с ColumnID [5, 61, 63];
		// генератор берёт первое встреченное = 5 (LineNumber).
		"tsql_frame": 5,
		// nt_username: в CSV встречается с ColumnID [6, 7];
		// генератор берёт первое встреченное = 7 (NTDomainName).
		"nt_username": 7,
	}
	for action, expectedCol := range knownActionConflicts {
		got, ok := xeActionNameToColumn[action]
		if !ok {
			t.Errorf("xeActionNameToColumn: %q отсутствует в карте", action)
			continue
		}
		if got != expectedCol {
			t.Errorf("xeActionNameToColumn[%q] = %d, ожидается %d (задокументированное решение конфликта)",
				action, got, expectedCol)
		}
	}
}
