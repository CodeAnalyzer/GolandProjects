package config

import (
	"strings"
	"testing"
)

// timeoutsCase связывает имя параметра с полем конфигурации для табличных проверок.
type timeoutsCase struct {
	name  string
	field *int
}

// TestTimeouts_ExplicitZeroPreservedAfterLoad — явный 0 в TOML доезжает до
// конфига как указатель на 0, а не заменяется дефолтом (сценарий «Явный ноль
// отключает parse-таймаут» / «Явный ноль отключает query-таймаут»).
func TestTimeouts_ExplicitZeroPreservedAfterLoad(t *testing.T) {
	section := "\n[rti]\nparse_timeout_sec = 0\n\n[trc]\nparse_timeout_sec = 0\n\n[mcp]\nquery_timeout_sec = 0\nreview_timeout_sec = 0\n"
	if err := writeTestConfig(t, section); err != nil {
		t.Fatal(err)
	}
	got := Get()
	cases := []timeoutsCase{
		{"rti.parse_timeout_sec", got.RTI.ParseTimeoutSec},
		{"trc.parse_timeout_sec", got.TRC.ParseTimeoutSec},
		{"mcp.query_timeout_sec", got.MCP.QueryTimeoutSec},
		{"mcp.review_timeout_sec", got.MCP.ReviewTimeoutSec},
	}
	for _, c := range cases {
		if c.field == nil {
			t.Errorf("%s: поле nil, ожидался явный 0", c.name)
			continue
		}
		if *c.field != 0 {
			t.Errorf("%s: значение %d, ожидался 0 (без замены дефолтом)", c.name, *c.field)
		}
	}
}

// TestTimeouts_ExplicitPositivePreservedAfterLoad — положительное значение
// сохраняется как есть (без нормализации).
func TestTimeouts_ExplicitPositivePreservedAfterLoad(t *testing.T) {
	section := "\n[rti]\nparse_timeout_sec = 600\n\n[trc]\nparse_timeout_sec = 120\n\n[mcp]\nquery_timeout_sec = 45\nreview_timeout_sec = 90\n"
	if err := writeTestConfig(t, section); err != nil {
		t.Fatal(err)
	}
	got := Get()
	cases := []timeoutsCase{
		{"rti.parse_timeout_sec", got.RTI.ParseTimeoutSec},
		{"trc.parse_timeout_sec", got.TRC.ParseTimeoutSec},
		{"mcp.query_timeout_sec", got.MCP.QueryTimeoutSec},
		{"mcp.review_timeout_sec", got.MCP.ReviewTimeoutSec},
	}
	wants := []int{600, 120, 45, 90}
	for i, c := range cases {
		if c.field == nil {
			t.Errorf("%s: поле nil, ожидалось %d", c.name, wants[i])
			continue
		}
		if *c.field != wants[i] {
			t.Errorf("%s: значение %d, ожидалось %d", c.name, *c.field, wants[i])
		}
	}
}

// TestTimeouts_AbsentLeavesNilAfterLoad — отсутствие параметра оставляет nil
// (дефолт применяет потребитель, не Load).
func TestTimeouts_AbsentLeavesNilAfterLoad(t *testing.T) {
	if err := writeTestConfig(t, ""); err != nil {
		t.Fatal(err)
	}
	got := Get()
	cases := []timeoutsCase{
		{"rti.parse_timeout_sec", got.RTI.ParseTimeoutSec},
		{"trc.parse_timeout_sec", got.TRC.ParseTimeoutSec},
		{"mcp.query_timeout_sec", got.MCP.QueryTimeoutSec},
		{"mcp.review_timeout_sec", got.MCP.ReviewTimeoutSec},
	}
	for _, c := range cases {
		if c.field != nil {
			t.Errorf("%s: поле не nil (%d), ожидался nil (параметр отсутствует)", c.name, *c.field)
		}
	}
}

// TestTimeouts_NegativeRejected — явные отрицательные значения отклоняются
// ошибкой Load с именем параметра (сценарий «Отрицательный parse-таймаут
// отклоняется» / «Отрицательный query-таймаут отклоняется»).
func TestTimeouts_NegativeRejected(t *testing.T) {
	cases := []struct {
		section string
		param   string
	}{
		{"\n[rti]\nparse_timeout_sec = -5\n", "rti.parse_timeout_sec"},
		{"\n[trc]\nparse_timeout_sec = -1\n", "trc.parse_timeout_sec"},
		{"\n[mcp]\nquery_timeout_sec = -10\n", "mcp.query_timeout_sec"},
		{"\n[mcp]\nreview_timeout_sec = -2\n", "mcp.review_timeout_sec"},
	}
	for _, c := range cases {
		err := writeTestConfig(t, c.section)
		if err == nil || !strings.Contains(err.Error(), c.param) {
			t.Errorf("%s: ожидалась ошибка Load с упоминанием %q, получено: %v", c.param, c.param, err)
		}
	}
}

// TestTimeouts_CreateDefaultExplicitDefaults — CreateDefault инициализирует
// поля явными дефолтами (не nil).
func TestTimeouts_CreateDefaultExplicitDefaults(t *testing.T) {
	got := CreateDefault("D:/root")
	cases := []timeoutsCase{
		{"rti.parse_timeout_sec", got.RTI.ParseTimeoutSec},
		{"trc.parse_timeout_sec", got.TRC.ParseTimeoutSec},
		{"mcp.query_timeout_sec", got.MCP.QueryTimeoutSec},
		{"mcp.review_timeout_sec", got.MCP.ReviewTimeoutSec},
	}
	wants := []int{300, 300, 30, 120}
	for i, c := range cases {
		if c.field == nil {
			t.Errorf("%s: поле nil, ожидался явный дефолт %d", c.name, wants[i])
			continue
		}
		if *c.field != wants[i] {
			t.Errorf("%s: значение %d, ожидался дефолт %d", c.name, *c.field, wants[i])
		}
	}
}
