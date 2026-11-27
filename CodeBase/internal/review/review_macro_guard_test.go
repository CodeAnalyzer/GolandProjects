package review

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// --- fix-cyrillic-macro-names: guard роста раскрытия макросов ---

// macroGuardRunawayContent — саморекурсивный многострочный макрос:
// собственный вызов в КАЖДОЙ строке тела → число строк удваивается каждый
// проход (2^32). Вызов только в одной строке тела давал бы линейный рост и
// висягу не воспроизводил бы.
const macroGuardRunawayContent = "#define _A_(_x_) _A_(_x_) + 1\\\n_A_(_x_) + 2\nselect _A_(1) from t\n"

// TestMacroGuardRunawayExpansion: runaway-раскрытие MUST завершаться за
// конечное время. Red-фаза — через вспомогательный процесс с дедлайном:
// goroutine прервать нельзя, а 2^32 строк OOM-ит тест-бинарник; глобальный
// -timeout валил бы весь пакет.
func TestMacroGuardRunawayExpansion(t *testing.T) {
	if os.Getenv("CODEBASE_MACRO_GUARD_CHILD") == "1" {
		macroGuardRunawayChild(t)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestMacroGuardRunawayExpansion$", "-test.v")
	cmd.Env = append(os.Environ(), "CODEBASE_MACRO_GUARD_CHILD=1")
	// C2: вывод потомка не теряется — причина падения видна в отчёте родителя.
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start child process: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("child exited with error: %v\nchild output:\n%s%s", err, out.String(), errOut.String())
		}
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("runaway macro expansion did not complete within 10s deadline")
	}
}

// macroGuardRunawayChild — режим потомка: выполняет runaway-раскрытие и
// проверяет маркеры бюджета (fix-cyrillic-macro-names, tasks 3.1/3.2).
func macroGuardRunawayChild(t *testing.T) {
	result := replaceMacros(macroGuardRunawayContent)

	if !result.GuardTripped {
		t.Fatalf("guard must trip on runaway expansion")
	}
	if result.GuardTrippedLines < 1 {
		t.Fatalf("GuardTrippedLines = %d, want >= 1", result.GuardTrippedLines)
	}
	if result.PassesExhausted {
		t.Fatalf("exponential runaway must be stopped by budget, not by pass cap: PassesExhausted = true")
	}
	t.Logf("child completed: %d lines, GuardTrippedLines=%d", strings.Count(result.Content, "\n")+1, result.GuardTrippedLines)
}

// TestMacroGuardPassesExhaustedLinearRunaway (C1): линейный runaway — вызов
// только в одной строке тела: +1 строка за проход, бюджет не срабатывает,
// 32 прохода исчерпываются с незавершёнными изменениями → PassesExhausted.
func TestMacroGuardPassesExhaustedLinearRunaway(t *testing.T) {
	content := "#define _A_(_x_) select _A_(_x_)\\\nfrom t\nselect _A_(1) from t\n"
	result := replaceMacros(content)

	if !result.PassesExhausted {
		t.Fatalf("PassesExhausted = false, want true (linear runaway exhausts pass cap)")
	}
	if result.PassesExhaustedLines != 1 {
		t.Fatalf("PassesExhaustedLines = %d, want 1", result.PassesExhaustedLines)
	}
	if result.GuardTripped {
		t.Fatalf("GuardTripped = true, want false (line budget not exceeded by linear growth)")
	}
}

// TestMacroGuardNormalFileZeroCounters: файл без патологий — счётчики нулевые.
func TestMacroGuardNormalFileZeroCounters(t *testing.T) {
	content := "#define ADD(a, b) a + b\nselect ADD(1, 2) from t\n"
	result := replaceMacros(content)

	if result.GuardTripped || result.GuardTrippedLines != 0 {
		t.Fatalf("guard must not trip on a normal file: GuardTripped=%v, GuardTrippedLines=%d", result.GuardTripped, result.GuardTrippedLines)
	}
	if result.PassesExhausted || result.PassesExhaustedLines != 0 {
		t.Fatalf("passes must converge on a normal file: PassesExhausted=%v, PassesExhaustedLines=%d", result.PassesExhausted, result.PassesExhaustedLines)
	}
}
