//go:build integration

package indexer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store/testutil"
)

// gateDetector — подставной детектор для проверки конкурентности пула:
// каждая детекция ждёт, пока concurrent достигнет pool (гейт), либо
// timeout — если пул не параллелит, тест увидит maxConcurrent < pool.
// Дополнительно фиксирует стадию репортера на момент входа в Detect.
type gateDetector struct {
	pool    int
	timeout time.Duration
	failDir string

	mu            sync.Mutex
	concurrent    int
	maxConcurrent int
}

func (d *gateDetector) Detect(ctx context.Context, dir string) (*GitSource, error) {
	d.mu.Lock()
	d.concurrent++
	if d.concurrent > d.maxConcurrent {
		d.maxConcurrent = d.concurrent
	}
	d.mu.Unlock()

	// Гейт: ждём набора pool одновременных детекций.
	deadline := time.Now().Add(d.timeout)
	for {
		d.mu.Lock()
		cur := d.concurrent
		d.mu.Unlock()
		if cur >= d.pool {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(5 * time.Millisecond)

	d.mu.Lock()
	d.concurrent--
	d.mu.Unlock()

	if d.failDir != "" && dir == d.failDir {
		return nil, errors.New("git unavailable (stub)")
	}
	return &GitSource{
		Product:    filepath.Base(dir),
		BranchName: "develop",
		BranchKey:  "develop",
		CommitSHA:  "deadbeef",
	}, nil
}

// stageSnapshotAtDetect removed: стадии читает stageReaderDetector.

// stubDirsWithGit создаёт n каталогов с пустым .git (кандидаты discovery).
func stubDirsWithGit(t *testing.T, root string, n int) []string {
	t.Helper()
	dirs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		dir := filepath.Join(root, fmt.Sprintf("fa-product-%02d", i))
		if err := os.MkdirAll(filepath.Join(dir, ".git"), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		dirs = append(dirs, dir)
	}
	return dirs
}

func newParallelDiscoveryIndexer(t *testing.T) *Indexer {
	t.Helper()
	return &Indexer{
		db: testutil.Open(t),
		config: &config.Config{Indexer: config.IndexerConfig{
			Parallel:  1,
			BatchSize: 100,
		}},
		errorLogger: log.New(io.Discard, "", 0),
		shared:      newIndexerSharedState(),
	}
}

// TestDiscovery_ParallelDetection_ReachesPoolSize — детекция выполняется
// конкурентно: максимум одновременных детекций = размер пула и достигает
// его (наблюдаемое свойство, без тайминговых ассертов).
func TestDiscovery_ParallelDetection_ReachesPoolSize(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	stubDirsWithGit(t, root, 8)

	idx := newParallelDiscoveryIndexer(t)
	detector := &gateDetector{pool: 4, timeout: 500 * time.Millisecond}
	collector := &statsCollector{}

	idx.discoverGitSources(ctx, root, detector, 4, collector)

	if detector.maxConcurrent != 4 {
		t.Fatalf("max concurrent detections = %d, want exactly pool size 4", detector.maxConcurrent)
	}

	// Стадия discovery сброшена — строка счётчиков walk свободна.
	if stage := collector.Snapshot().Stage; stage != "" {
		t.Fatalf("stage after discovery = %q, want empty", stage)
	}

	// Все каталоги задетектированы и привязаны: 8 продуктов × 1 develop-линия.
	var bound int
	if err := idx.db.QueryRow(`
		SELECT count(*) FROM ds_products_branch b JOIN ds_products p ON p.id = b.ds_product_id
		WHERE b.branch_key = 'develop' AND p.product_name LIKE 'fa-product-%'
	`).Scan(&bound); err != nil {
		t.Fatalf("count branches: %v", err)
	}
	if bound != 8 {
		t.Fatalf("bound develop lines = %d, want 8", bound)
	}
}

// TestDiscovery_ParallelDetection_SingleWorker — пул из одного воркера:
// детекция строго последовательна (максимум одновременных = 1).
func TestDiscovery_ParallelDetection_SingleWorker(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	stubDirsWithGit(t, root, 5)

	idx := newParallelDiscoveryIndexer(t)
	detector := &gateDetector{pool: 1, timeout: 10 * time.Millisecond}
	collector := &statsCollector{}

	idx.discoverGitSources(ctx, root, detector, 1, collector)

	if detector.maxConcurrent != 1 {
		t.Fatalf("max concurrent detections = %d, want 1", detector.maxConcurrent)
	}
}

// TestDiscovery_ParallelDetection_ResultEqualsSequential — результат
// параллельной детекции (продукты/линии/пути/commits) идентичен
// последовательной на той же структуре каталогов.
func TestDiscovery_ParallelDetection_ResultEqualsSequential(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	stubDirsWithGit(t, root, 6)

	idx := newParallelDiscoveryIndexer(t)
	collector := &statsCollector{}

	idx.discoverGitSources(ctx, root, &gateDetector{pool: 6, timeout: 300 * time.Millisecond}, 4, collector)
	parallel := branchRowsSnapshot(t, idx)

	// Второй прогон по «новому» корню с теми же именами каталогов:
	// та же структура, последовательный пул.
	root2 := t.TempDir()
	stubDirsWithGit(t, root2, 6)
	idx2 := newParallelDiscoveryIndexer(t)
	idx2.discoverGitSources(ctx, root2, &gateDetector{pool: 6, timeout: 300 * time.Millisecond}, 1, &statsCollector{})
	sequential := branchRowsSnapshot(t, idx2)

	if len(parallel) != len(sequential) {
		t.Fatalf("branch rows: parallel = %d, sequential = %d", len(parallel), len(sequential))
	}
	for i := range parallel {
		if parallel[i] != sequential[i] {
			t.Fatalf("branch rows diverge at %d: parallel %q != sequential %q", i, parallel[i], sequential[i])
		}
	}
}

// TestDiscovery_ParallelDetection_WorkerErrorIsolated — ошибка exec в
// одном воркере (git недоступен для каталога) не валит детекцию остальных.
func TestDiscovery_ParallelDetection_WorkerErrorIsolated(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dirs := stubDirsWithGit(t, root, 4)

	idx := newParallelDiscoveryIndexer(t)
	detector := &gateDetector{pool: 4, timeout: 300 * time.Millisecond, failDir: dirs[2]}
	collector := &statsCollector{}

	idx.discoverGitSources(ctx, root, detector, 4, collector)

	var bound int
	if err := idx.db.QueryRow(`
		SELECT count(*) FROM ds_products_branch b JOIN ds_products p ON p.id = b.ds_product_id
		WHERE b.branch_key = 'develop' AND p.product_name LIKE 'fa-product-%'
	`).Scan(&bound); err != nil {
		t.Fatalf("count branches: %v", err)
	}
	if bound != 3 {
		t.Fatalf("bound lines = %d, want 3 (один каталог пропущен по ошибке exec)", bound)
	}
}

// TestDiscovery_StageSequence — детали prep-строки discovery: счётчик
// «preparing: git detection N/M» инкрементируется только по завершении
// детекции каталога (детектор видит на входе i уже завершённых), формат
// всегда «фаза: деталь» с общим prep-префиксом.
func TestDiscovery_StageSequence(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	stubDirsWithGit(t, root, 4)

	idx := newParallelDiscoveryIndexer(t)
	detector := &gateDetector{pool: 1, timeout: 5 * time.Millisecond}
	collector := &statsCollector{}

	// Детектор читает текущую стадию collector'а на входе: при
	// последовательной детекции каталог i видит счётчик i.
	detHook := &stageReaderDetector{inner: detector, collector: collector}
	idx.discoverGitSources(ctx, root, detHook, 1, collector)

	stages := detHook.observed
	if len(stages) != 4 {
		t.Fatalf("observed stages = %d (%v), want 4", len(stages), stages)
	}
	re := regexp.MustCompile(`^preparing: git detection (\d+)/4$`)
	for i, stage := range stages {
		m := re.FindStringSubmatch(stage)
		if m == nil {
			t.Fatalf("stage %d = %q, want format 'preparing: git detection N/4'", i, stage)
		}
		if m[1] != fmt.Sprint(i) {
			t.Fatalf("stage %d counter = %s, want %d (инкремент только по завершении детекции)", i, m[1], i)
		}
	}
}

// stageReaderDetector оборачивает детектор: на входе в Detect фиксирует
// текущую стадию репортера из collector.
type stageReaderDetector struct {
	inner     *gateDetector
	collector *statsCollector

	mu       sync.Mutex
	observed []string
}

func (d *stageReaderDetector) Detect(ctx context.Context, dir string) (*GitSource, error) {
	d.mu.Lock()
	d.observed = append(d.observed, d.collector.Snapshot().Stage)
	d.mu.Unlock()
	return d.inner.Detect(ctx, dir)
}

// branchRowsSnapshot — детерминированный снимок строк линий для сравнения.
func branchRowsSnapshot(t *testing.T, idx *Indexer) []string {
	t.Helper()
	rows, err := idx.db.Query(`
		SELECT p.product_name, b.branch_key, b.branch_name, b.commit_sha, b.is_default
		FROM ds_products_branch b JOIN ds_products p ON p.id = b.ds_product_id
		ORDER BY p.product_name
	`)
	if err != nil {
		t.Fatalf("query branches: %v", err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var product, key, name, sha string
		var isDefault bool
		if err := rows.Scan(&product, &key, &name, &sha, &isDefault); err != nil {
			t.Fatalf("scan branch: %v", err)
		}
		result = append(result, strings.Join([]string{product, key, name, sha, fmt.Sprint(isDefault)}, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("branches rows: %v", err)
	}
	sort.Strings(result)
	return result
}
