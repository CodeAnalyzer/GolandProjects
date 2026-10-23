package cmd

import (
	"strings"
	"testing"
)

func TestSessionFlagsRegistered(t *testing.T) {
	rtiFlags := map[string][]string{
		"parse":  {"name"},
		"delete": {"session", "name"},
		"prune":  {"keep-last", "keep-hours"},
	}
	for sub, flags := range rtiFlags {
		var found bool
		for _, c := range rtiCmd.Commands() {
			if c.Name() != sub {
				continue
			}
			found = true
			for _, flag := range flags {
				if c.Flags().Lookup(flag) == nil {
					t.Errorf("rti %s: flag --%s not registered", sub, flag)
				}
			}
		}
		if !found {
			t.Errorf("rti subcommand %s not found", sub)
		}
	}

	trcFlags := map[string][]string{
		"parse":  {"name"},
		"delete": {"session", "name"},
		"prune":  {"keep-last", "keep-hours"},
	}
	for sub, flags := range trcFlags {
		var found bool
		for _, c := range trcCmd.Commands() {
			if c.Name() != sub {
				continue
			}
			found = true
			for _, flag := range flags {
				if c.Flags().Lookup(flag) == nil {
					t.Errorf("trc %s: flag --%s not registered", sub, flag)
				}
			}
		}
		if !found {
			t.Errorf("trc subcommand %s not found", sub)
		}
	}
}

func TestDeleteRejectsConflictingSelectors(t *testing.T) {
	oldID, oldName := rtiSessionID, rtiSessionName
	rtiSessionID, rtiSessionName = 1, "trace.rti"
	err := runRTIDelete(rtiDeleteCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("rti delete conflict err = %v", err)
	}
	rtiSessionID, rtiSessionName = oldID, oldName

	oldID, oldName = trcSessionID, trcSessionName
	trcSessionID, trcSessionName = 1, "trace.trc"
	err = runTRCDelete(trcDeleteCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("trc delete conflict err = %v", err)
	}
	trcSessionID, trcSessionName = oldID, oldName
}

func TestDeleteRequiresSelector(t *testing.T) {
	oldID, oldName := rtiSessionID, rtiSessionName
	rtiSessionID, rtiSessionName = 0, ""
	err := runRTIDelete(rtiDeleteCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("rti delete missing selector err = %v", err)
	}
	rtiSessionID, rtiSessionName = oldID, oldName

	oldID, oldName = trcSessionID, trcSessionName
	trcSessionID, trcSessionName = 0, ""
	err = runTRCDelete(trcDeleteCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("trc delete missing selector err = %v", err)
	}
	trcSessionID, trcSessionName = oldID, oldName
}
