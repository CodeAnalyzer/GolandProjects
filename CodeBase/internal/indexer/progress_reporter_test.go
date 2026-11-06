package indexer

import (
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codebase/internal/model"
)

// captureStdout подменяет os.Stdout на пайп и возвращает функцию чтения
// накопленного вывода (репортер пишет через fmt.Printf → os.Stdout).
func captureStdout(t *testing.T) func() string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	old := os.Stdout
	os.Stdout = w

	var buf strings.Builder
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(&buf, r)
	}()

	return func() string {
		os.Stdout = old
		_ = w.Close()
		_ = r.Close()
		wg.Wait()
		return buf.String()
	}
}

// TestProgressReporterStageRendering — рендер подготовительных стадий
// (change update-startup-visibility): все prep-стадии имеют общий префикс
// «preparing» и живут на ОДНОЙ строке прогресса — смена детали (счётчик
// детекции, registry, safety-net, load state) перезаписывает её на месте;
// перевод строки — только при переходе к строке счётчиков walk. Сброс Stage
// возвращает строку счётчиков.
func TestProgressReporterStageRendering(t *testing.T) {
	SetProgressInterval(20)
	defer SetProgressInterval(250)

	read := captureStdout(t)
	collector := &statsCollector{}
	stop := startProgressReporter("update", collector.Snapshot)

	setStage := func(stage string) {
		collector.Add(func(stats *model.ScanStats) { stats.Stage = stage })
	}

	// Счётчик детекции растёт в деталях одной prep-строки.
	setStage("preparing: git detection 1/2")
	time.Sleep(80 * time.Millisecond)
	setStage("preparing: git detection 2/2")
	time.Sleep(80 * time.Millisecond)
	// Пост-детекционная работа — деталь той же строки, без счётчика.
	setStage("preparing: source registry upsert")
	time.Sleep(80 * time.Millisecond)
	setStage("preparing: safety-net duplicates")
	time.Sleep(80 * time.Millisecond)
	setStage("preparing: load state files")
	time.Sleep(80 * time.Millisecond)
	// Переход к счётчикам walk — фиксация prep-строки и новая строка.
	setStage("")
	time.Sleep(80 * time.Millisecond)

	stop()
	out := read()

	for _, want := range []string{"preparing", "git detection", "1/2", "2/2", "source registry", "upsert", "safety-net", "duplicates", "load state", "files", "scanned="} {
		if !strings.Contains(out, want) {
			t.Errorf("reporter output missing %q:\n%q", want, out)
		}
	}
	// Prep-фазы — одна строка: единственный перевод строки фиксирует её
	// при переходе к строке счётчиков walk (финальное затирание при stop
	// переводов строки не печатает).
	if got := strings.Count(out, "\n"); got != 1 {
		t.Errorf("phase-fixing newlines = %d, want exactly 1 (prep-строка → счётчики):\n%q", got, out)
	}
	// Стадия без «: » отрисовалась бы пустым префиксом («\r | деталь») —
	// все стадии обязательны в форме «фаза: деталь».
	if strings.Contains(out, "\r | ") {
		t.Errorf("stage rendered with empty phase prefix:\n%q", out)
	}
}
