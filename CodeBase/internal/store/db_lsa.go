package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

// LoadAllSpecCapabilitiesForLSA загружает все capability с текстом для LSA.
// Возвращает слайс документов (id + текст: title + purpose + notes).
func (db *DB) LoadAllSpecCapabilitiesForLSA(ctx context.Context) ([]model.SpecCapability, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, title, purpose, notes
		FROM spec_capabilities
		ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("LoadAllSpecCapabilitiesForLSA: %w", err)
	}
	defer rows.Close()

	var result []model.SpecCapability
	for rows.Next() {
		var c model.SpecCapability
		var purpose, notes sql.NullString
		if err := rows.Scan(&c.ID, &c.Title, &purpose, &notes); err != nil {
			return nil, fmt.Errorf("LoadAllSpecCapabilitiesForLSA scan: %w", err)
		}
		c.Purpose = purpose.String
		c.Notes = notes.String
		result = append(result, c)
	}
	return result, rows.Err()
}

// LoadAllSpecCapabilitiesWithReqsForLSA загружает все capability с агрегированным текстом
// требований и сценариев (LSAText) для обучения LSA. Агрегация детерминирована:
// string_agg с ORDER BY, NULL-поля пропускаются через concat_ws, текст требования
// предшествует сценариям этого же требования (interleave по req_order).
func (db *DB) LoadAllSpecCapabilitiesWithReqsForLSA(ctx context.Context) ([]model.SpecCapability, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT c.id, c.title, c.purpose, c.notes,
		       COALESCE((
		           SELECT string_agg(t.part, ' ' ORDER BY t.ord1, t.ord2, t.ord3, t.id)
		           FROM (
		               SELECT req.req_order AS ord1, 0 AS ord2, 0 AS ord3, req.id AS id,
		                      concat_ws(' ', req.requirement_name, req.body_text) AS part
		               FROM spec_requirements req
		               WHERE req.capability_id = c.id
		               UNION ALL
		               SELECT req.req_order, 1, s.scn_order, s.id,
		                      concat_ws(' ', s.scenario_name, s.given_text, s.when_text, s.then_text)
		               FROM spec_scenarios s
		               JOIN spec_requirements req ON req.id = s.requirement_id
		               WHERE req.capability_id = c.id
		           ) t
		       ), '')
		FROM spec_capabilities c
		ORDER BY c.id`)
	if err != nil {
		return nil, fmt.Errorf("LoadAllSpecCapabilitiesWithReqsForLSA: %w", err)
	}
	defer rows.Close()

	var result []model.SpecCapability
	for rows.Next() {
		var c model.SpecCapability
		var purpose, notes sql.NullString
		if err := rows.Scan(&c.ID, &c.Title, &purpose, &notes, &c.LSAText); err != nil {
			return nil, fmt.Errorf("LoadAllSpecCapabilitiesWithReqsForLSA scan: %w", err)
		}
		c.Purpose = purpose.String
		c.Notes = notes.String
		result = append(result, c)
	}
	return result, rows.Err()
}

// CountSpecCapabilities возвращает количество capability в БД.
func (db *DB) CountSpecCapabilities(ctx context.Context) (int, error) {
	var count int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM spec_capabilities`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("CountSpecCapabilities: %w", err)
	}
	return count, nil
}

func (db *DB) PublishSpecLSAGeneration(ctx context.Context, generation string, terms []model.SpecVocabTerm, embeddings []model.SpecEmbedding) error {
	generation = strings.TrimSpace(generation)
	if generation == "" {
		return fmt.Errorf("PublishSpecLSAGeneration: generation must not be empty")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("PublishSpecLSAGeneration begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM spec_vocab WHERE generation = $1`, generation); err != nil {
		return fmt.Errorf("PublishSpecLSAGeneration delete vocab: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM spec_embeddings WHERE generation = $1`, generation); err != nil {
		return fmt.Errorf("PublishSpecLSAGeneration delete embeddings: %w", err)
	}

	vocabStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO spec_vocab (generation, term, doc_freq, idf)
		VALUES ($1, $2, $3, $4)`)
	if err != nil {
		return fmt.Errorf("PublishSpecLSAGeneration prepare vocab: %w", err)
	}
	for _, term := range terms {
		if _, err := vocabStmt.ExecContext(ctx, generation, term.Term, term.DocFreq, term.IDF); err != nil {
			_ = vocabStmt.Close()
			return fmt.Errorf("PublishSpecLSAGeneration insert vocab: %w", err)
		}
	}
	if err := vocabStmt.Close(); err != nil {
		return fmt.Errorf("PublishSpecLSAGeneration close vocab: %w", err)
	}

	embeddingStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO spec_embeddings (generation, spec_id, embed_level, embed_text, embedding, embed_method, embed_dim)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`)
	if err != nil {
		return fmt.Errorf("PublishSpecLSAGeneration prepare embeddings: %w", err)
	}
	for _, embedding := range embeddings {
		if _, err := embeddingStmt.ExecContext(ctx, generation, embedding.SpecID, embedding.EmbedLevel,
			embedding.EmbedText, pq.Array(embedding.Embedding), embedding.EmbedMethod, embedding.EmbedDim); err != nil {
			_ = embeddingStmt.Close()
			return fmt.Errorf("PublishSpecLSAGeneration insert embeddings: %w", err)
		}
	}
	if err := embeddingStmt.Close(); err != nil {
		return fmt.Errorf("PublishSpecLSAGeneration close embeddings: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("PublishSpecLSAGeneration commit: %w", err)
	}
	return nil
}

func (db *DB) HasSpecLSAGeneration(ctx context.Context, generation string) (bool, error) {
	generation = strings.TrimSpace(generation)
	if generation == "" {
		return false, fmt.Errorf("HasSpecLSAGeneration: generation must not be empty")
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM spec_embeddings WHERE generation = $1)`, generation).Scan(&exists); err != nil {
		return false, fmt.Errorf("HasSpecLSAGeneration: %w", err)
	}
	return exists, nil
}

func (db *DB) DeleteSpecLSAGenerationsExcept(ctx context.Context, current, previous string) error {
	current = strings.TrimSpace(current)
	previous = strings.TrimSpace(previous)
	if current == "" {
		return fmt.Errorf("DeleteSpecLSAGenerationsExcept: current generation must not be empty")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("DeleteSpecLSAGenerationsExcept begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	keepPrevious := previous != ""
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM spec_vocab
		WHERE generation <> $1 AND ($2 = FALSE OR generation <> $3)`, current, keepPrevious, previous); err != nil {
		return fmt.Errorf("DeleteSpecLSAGenerationsExcept delete vocab: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM spec_embeddings
		WHERE generation <> $1 AND ($2 = FALSE OR generation <> $3)`, current, keepPrevious, previous); err != nil {
		return fmt.Errorf("DeleteSpecLSAGenerationsExcept delete embeddings: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("DeleteSpecLSAGenerationsExcept commit: %w", err)
	}
	return nil
}
