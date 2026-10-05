//go:build integration

package indexer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/specfts"
)

// TestPostProcessDescLSA_TrainPublishIdempotent: обучение desc-модели,
// публикация поколения, идемпотентный повторный прогон; spec-модель не тронута.
func TestPostProcessDescLSA_TrainPublishIdempotent(t *testing.T) {
	idx, root := newTestIndexer(t)
	ctx := context.Background()

	var scanID int64
	if err := idx.db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'done') RETURNING id`).Scan(&scanID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	var fileID int64
	if err := idx.db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, 'repo/proc.sql', 'proc.sql', '.sql', 'h1', NOW()) RETURNING id`, scanID).Scan(&fileID); err != nil {
		t.Fatalf("insert file: %v", err)
	}
	var xmlFileID int64
	if err := idx.db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, 'repo/api.xml', 'api.xml', '.xml', 'h2', NOW()) RETURNING id`, scanID).Scan(&xmlFileID); err != nil {
		t.Fatalf("insert xml file: %v", err)
	}

	procedures := []string{
		"ReturnCashFund_Insert|возврат сумм из Банка-партнера на счет клиента",
		"BindClassifierProc|привязка кредитных договоров к классификатору",
		"ClientMacros_Insert|вставка клиентских макросов в договор",
	}
	for _, p := range procedures {
		name, desc, _ := strings.Cut(p, "|")
		if _, err := idx.db.Exec(`
			INSERT INTO sql_procedures (file_id, proc_name, line_start, line_end, description)
			VALUES ($1, $2, 1, 2, $3)`, fileID, name, desc); err != nil {
			t.Fatalf("insert proc %s: %v", name, err)
		}
	}
	contracts := [][2]string{
		{"API_CCred_BindClassifier", "Метод осуществляет привязку кредитных договоров к классификатору."},
		{"API_Client_MassOperation", "Массовые операции над клиентами и их договорами."},
	}
	for _, c := range contracts {
		if _, err := idx.db.Exec(`
			INSERT INTO api_contracts (file_id, contract_name, contract_kind, short_description, full_description, line_start, line_end)
			VALUES ($1, $2, 'service', $3, $3, 1, 1)`, xmlFileID, c[0], c[1]); err != nil {
			t.Fatalf("insert contract %s: %v", c[0], err)
		}
	}

	// Конфиг: desc-LSA с минимальным корпусом, sidecar во временном каталоге
	oldConfigFile := config.GetConfigFile()
	oldCfg := config.Get()
	var oldCfgCopy config.Config
	if oldCfg != nil {
		oldCfgCopy = *oldCfg
	}
	config.CreateDefault(root)
	cfg := config.Get()
	descEnabled := true
	cfg.DescLSA.LSAEnabled = &descEnabled
	cfg.DescLSA.LSAMinCorpus = 1
	cfg.DescLSA.LSAMinDF = 1
	cfg.DescLSA.LSAMaxDF = 1
	cfg.DescLSA.LSAK = 2
	threshold := 0
	cfg.DescLSA.LSARetrainThreshold = &threshold
	config.SetConfigFile(filepath.Join(root, "codebase.toml"))
	t.Cleanup(func() {
		config.SetConfigFile(oldConfigFile)
		if current := config.Get(); current != nil {
			if oldCfg != nil {
				*current = oldCfgCopy
			}
		}
	})

	collector := &statsCollector{}
	idx.postProcessDescLSA(ctx, collector)

	// Модель обучена: generation опубликован для обоих типов сущностей
	modelPath := config.DescLSAModelPath()
	statePath := config.DescLSAStatePath()
	state, err := specfts.LoadLSAState(statePath)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state == nil || state.Generation == "" {
		t.Fatal("desc LSA state не создан после обучения")
	}
	if _, err := os.Stat(modelPath); err != nil {
		t.Fatalf("model file missing: %v", err)
	}
	var procEmb, contractEmb int
	if err := idx.db.QueryRow(`SELECT count(*) FROM desc_embeddings WHERE generation = $1 AND entity_type = 'procedure'`, state.Generation).Scan(&procEmb); err != nil {
		t.Fatalf("count proc embeddings: %v", err)
	}
	if err := idx.db.QueryRow(`SELECT count(*) FROM desc_embeddings WHERE generation = $1 AND entity_type = 'contract'`, state.Generation).Scan(&contractEmb); err != nil {
		t.Fatalf("count contract embeddings: %v", err)
	}
	if procEmb != 3 || contractEmb != 2 {
		t.Fatalf("embeddings proc=%d contract=%d, want 3/2", procEmb, contractEmb)
	}

	// Идемпотентность: повторный прогон не переобучает (TrainedAt не меняется)
	time.Sleep(10 * time.Millisecond)
	collector2 := &statsCollector{}
	idx.postProcessDescLSA(ctx, collector2)
	state2, err := specfts.LoadLSAState(statePath)
	if err != nil {
		t.Fatalf("load state 2: %v", err)
	}
	if state2 == nil || !state2.TrainedAt.Equal(state.TrainedAt) {
		t.Fatalf("повторный прогон переобучил модель: %v vs %v", state2.TrainedAt, state.TrainedAt)
	}

	// Spec-модель не затронута
	var specEmbeddings int
	if err := idx.db.QueryRow(`SELECT count(*) FROM spec_embeddings`).Scan(&specEmbeddings); err != nil {
		t.Fatalf("count spec embeddings: %v", err)
	}
	if specEmbeddings != 0 {
		t.Fatalf("spec_embeddings = %d, want 0 (desc-обучение не должно трогать спеки)", specEmbeddings)
	}
	if _, err := os.Stat(config.SpecLSAModelPath()); err == nil {
		t.Fatal("spec LSA model file не должен создаваться desc-обучением")
	}
}
