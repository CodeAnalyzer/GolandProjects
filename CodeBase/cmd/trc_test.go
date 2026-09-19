package cmd

import (
	"strings"
	"testing"

	"github.com/codebase/internal/trcsvc"
	"github.com/spf13/cobra"
)

func intPtr(v int) *int { return &v }

func TestFormatTRCEventsHeader(t *testing.T) {
	got := formatTRCEventsHeader(&trcsvc.EventsResult{ReturnedCount: 1, FilteredCount: 2, TotalCount: intPtr(3), Limit: 4})
	want := "1 event(s) returned (2 matched, 3 total, limit 4):\n\n"
	if got != want {
		t.Fatalf("header = %q, want %q", got, want)
	}
}

func TestFormatTRCEventsHeader_NextPage(t *testing.T) {
	got := formatTRCEventsHeader(&trcsvc.EventsResult{ReturnedCount: 1, FilteredCount: 2, TotalCount: intPtr(3), Limit: 4, NextAfterID: 77})
	if !strings.Contains(got, "--after-id 77") {
		t.Fatalf("header = %q, want next page hint with after-id 77", got)
	}
}

func TestFormatTRCEventsHeader_NoTotalCount(t *testing.T) {
	// страница продолжения: total_count отсутствует, выводится 0 total
	got := formatTRCEventsHeader(&trcsvc.EventsResult{ReturnedCount: 1, FilteredCount: 2, Limit: 4})
	want := "1 event(s) returned (2 matched, 0 total, limit 4):\n\n"
	if got != want {
		t.Fatalf("header = %q, want %q", got, want)
	}
}

func TestParseSPIDCSV(t *testing.T) {
	got, err := parseSPIDCSV("728, 700", "--spids")
	if err != nil || len(got) != 2 || got[0] != 728 || got[1] != 700 {
		t.Fatalf("got %v, %v", got, err)
	}
	if v, err := parseSPIDCSV("", "--spids"); err != nil || v != nil {
		t.Fatalf("empty: got %v, %v", v, err)
	}
	for raw, wantSub := range map[string]string{
		"728,abc": "--spids[1]",
		"0":       "--spids[0]",
		"-5":      "--spids[0]",
		"728,":    "--spids[1]",
	} {
		_, err := parseSPIDCSV(raw, "--spids")
		if err == nil || !strings.Contains(err.Error(), wantSub) {
			t.Errorf("parseSPIDCSV(%q) err = %v, want %q", raw, err, wantSub)
		}
	}
}

func TestParseStringCSV(t *testing.T) {
	got, err := parseStringCSV("SP:Completed,RPC:Completed", "--event-names")
	if err != nil || len(got) != 2 || got[0] != "SP:Completed" || got[1] != "RPC:Completed" {
		t.Fatalf("got %v, %v", got, err)
	}
	_, err = parseStringCSV("SP:Completed,,RPC:Completed", "--event-names")
	if err == nil || !strings.Contains(err.Error(), "--event-names[1]") {
		t.Fatalf("err = %v, want --event-names[1]", err)
	}
}

func TestParseRFC3339Flag(t *testing.T) {
	v, err := parseRFC3339Flag("2026-09-14T13:56:50Z", "--time-from")
	if err != nil || v == nil {
		t.Fatalf("got %v, %v", v, err)
	}
	if v, err := parseRFC3339Flag("", "--time-from"); err != nil || v != nil {
		t.Fatalf("empty: got %v, %v", v, err)
	}
	_, err = parseRFC3339Flag("2026-09-14 13:56:50", "--time-from")
	if err == nil || !strings.Contains(err.Error(), "--time-from") {
		t.Fatalf("err = %v, want --time-from RFC3339 error", err)
	}
}

func TestTRCCompareAndSpidsCommandsFlags(t *testing.T) {
	for _, tc := range []struct {
		cmd  string
		want []string
	}{
		{"compare-procedures", []string{"focus-spid", "compare-spids", "event-names", "top", "sort", "session"}},
		{"spids", []string{"spids", "time-from", "time-to", "sort", "limit", "session"}},
	} {
		var cmd *cobra.Command
		for _, c := range trcCmd.Commands() {
			if c.Name() == tc.cmd {
				cmd = c
				break
			}
		}
		if cmd == nil {
			t.Fatalf("subcommand %s not registered", tc.cmd)
		}
		for _, flag := range tc.want {
			if cmd.Flags().Lookup(flag) == nil {
				t.Errorf("%s missing flag --%s", tc.cmd, flag)
			}
		}
	}
}

func TestDerefFloat(t *testing.T) {
	if derefFloat(nil) != 0 {
		t.Fatal("nil must be 0")
	}
	v := 1.5
	if derefFloat(&v) != 1.5 {
		t.Fatal("value must pass through")
	}
}
