package systemsvc

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLSAStateGeneration_Fallback — активное поколение LSA для метрик stats:
// валидный state отдаёт generation, отсутствующий/повреждённый — "" (фолбэк
// на полный счёт без фильтра по поколению).
func TestLSAStateGeneration_Fallback(t *testing.T) {
	dir := t.TempDir()

	// Отсутствующий файл state — не ошибка, фолбэк
	if got := lsaStateGeneration(filepath.Join(dir, "absent_state.json")); got != "" {
		t.Errorf("missing state: generation = %q, want empty", got)
	}

	// Повреждённый state — фолбэк, не паника
	corrupt := filepath.Join(dir, "corrupt_state.json")
	if err := os.WriteFile(corrupt, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := lsaStateGeneration(corrupt); got != "" {
		t.Errorf("corrupted state: generation = %q, want empty", got)
	}

	// Валидный state — его generation
	valid := filepath.Join(dir, "valid_state.json")
	if err := os.WriteFile(valid, []byte(`{"fingerprint":"fp","generation":"gen-active"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := lsaStateGeneration(valid); got != "gen-active" {
		t.Errorf("valid state: generation = %q, want %q", got, "gen-active")
	}

	// Валидный state без generation — legacy по умолчанию не считается активным
	// фильтром: LoadLSAState подставляет "legacy", он и возвращается.
	noGen := filepath.Join(dir, "nogen_state.json")
	if err := os.WriteFile(noGen, []byte(`{"fingerprint":"fp"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := lsaStateGeneration(noGen); got != "legacy" {
		t.Errorf("state without generation: generation = %q, want %q", got, "legacy")
	}
}
