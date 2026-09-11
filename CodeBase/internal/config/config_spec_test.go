package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTestConfig пишет временный конфиг и загружает его.
func writeTestConfig(t *testing.T, specSection string) error {
	t.Helper()
	oldCfg := cfg
	oldConfigFile := configFile
	t.Cleanup(func() {
		cfg = oldCfg
		configFile = oldConfigFile
	})

	path := filepath.Join(t.TempDir(), "codebase.toml")
	content := "root_path = \"D:/repo\"\n\n[database]\npassword = \"secret\"\n" + specSection
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	SetConfigFile(path)
	return Load()
}

func TestSpecDefaults_NilThresholds(t *testing.T) {
	// Отсутствующие настройки → дефолты через геттеры
	if err := writeTestConfig(t, ""); err != nil {
		t.Fatal(err)
	}
	got := Get()
	if got.Spec.RetrainThreshold() != 100 {
		t.Errorf("RetrainThreshold() = %d, want 100 (default, амортизация SVD ~97 c)", got.Spec.RetrainThreshold())
	}
	if got.Spec.MinCosine() != 0.15 {
		t.Errorf("MinCosine() = %v, want 0.15", got.Spec.MinCosine())
	}
	if got.Spec.RelativeCutoff() != 0.5 {
		t.Errorf("RelativeCutoff() = %v, want 0.5", got.Spec.RelativeCutoff())
	}
}

func TestSpecDefaults_ExplicitZeroThresholdDisablesAmortization(t *testing.T) {
	// Явный 0 = «переобучать при любом изменении fingerprint»
	if err := writeTestConfig(t, "\n[spec]\nlsa_retrain_threshold = 0\n"); err != nil {
		t.Fatal(err)
	}
	if got := Get().Spec.RetrainThreshold(); got != 0 {
		t.Errorf("explicit 0 must disable amortization, got %d", got)
	}
}

func TestSpecDefaults_ExplicitAmortizationThreshold(t *testing.T) {
	if err := writeTestConfig(t, "\n[spec]\nlsa_retrain_threshold = 25\n"); err != nil {
		t.Fatal(err)
	}
	if got := Get().Spec.RetrainThreshold(); got != 25 {
		t.Errorf("explicit 25 must be preserved, got %d", got)
	}
}

func TestSpecDefaults_NegativeThresholdRejected(t *testing.T) {
	err := writeTestConfig(t, "\n[spec]\nlsa_retrain_threshold = -5\n")
	if err == nil || !strings.Contains(err.Error(), "lsa_retrain_threshold") {
		t.Errorf("expected error for negative threshold, got %v", err)
	}
}

func TestSpecThresholds_ExplicitValues(t *testing.T) {
	if err := writeTestConfig(t, "\n[spec]\nlsa_min_cosine = 0.3\nlsa_relative_cutoff = 0.7\n"); err != nil {
		t.Fatal(err)
	}
	got := Get().Spec
	if got.MinCosine() != 0.3 {
		t.Errorf("MinCosine() = %v, want 0.3", got.MinCosine())
	}
	if got.RelativeCutoff() != 0.7 {
		t.Errorf("RelativeCutoff() = %v, want 0.7", got.RelativeCutoff())
	}
}

func TestSpecThresholds_ZeroDisablesFilters(t *testing.T) {
	// Явный 0 = отключить фильтр (nil → дефолт, 0 → осознанное отключение)
	if err := writeTestConfig(t, "\n[spec]\nlsa_min_cosine = 0.0\nlsa_relative_cutoff = 0.0\n"); err != nil {
		t.Fatal(err)
	}
	got := Get().Spec
	if got.MinCosine() != 0 {
		t.Errorf("MinCosine() = %v, want 0", got.MinCosine())
	}
	if got.RelativeCutoff() != 0 {
		t.Errorf("RelativeCutoff() = %v, want 0", got.RelativeCutoff())
	}
}

func TestSpecThresholds_OutOfRangeRejected(t *testing.T) {
	cases := []string{
		"\n[spec]\nlsa_min_cosine = 1.5\n",
		"\n[spec]\nlsa_min_cosine = -0.1\n",
		"\n[spec]\nlsa_relative_cutoff = 1.2\n",
		"\n[spec]\nlsa_relative_cutoff = -0.5\n",
	}
	for _, specSection := range cases {
		err := writeTestConfig(t, specSection)
		if err == nil {
			t.Errorf("expected range error for %q", specSection)
		}
	}
}
