package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/codebase/internal/model"
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

// CountSpecCapabilities возвращает количество capability в БД.
func (db *DB) CountSpecCapabilities(ctx context.Context) (int, error) {
	var count int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM spec_capabilities`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("CountSpecCapabilities: %w", err)
	}
	return count, nil
}

// ClearSpecVocab удаляет все записи из spec_vocab.
func (db *DB) ClearSpecVocab(ctx context.Context) error {
	_, err := db.ExecContext(ctx, `DELETE FROM spec_vocab`)
	if err != nil {
		return fmt.Errorf("ClearSpecVocab: %w", err)
	}
	return nil
}

// ClearSpecEmbeddings удаляет все записи из spec_embeddings.
func (db *DB) ClearSpecEmbeddings(ctx context.Context) error {
	_, err := db.ExecContext(ctx, `DELETE FROM spec_embeddings`)
	if err != nil {
		return fmt.Errorf("ClearSpecEmbeddings: %w", err)
	}
	return nil
}

// InsertSpecVocabBatch вставляет слайс vocab-терминов пакетами.
func (db *DB) InsertSpecVocabBatch(ctx context.Context, terms []model.SpecVocabTerm) error {
	if len(terms) == 0 {
		return nil
	}
	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("InsertSpecVocabBatch begin: %w", err)
	}
	defer txn.Rollback()

	stmt, err := txn.Prepare(`
		INSERT INTO spec_vocab (term, doc_freq, idf)
		VALUES ($1, $2, $3)
		ON CONFLICT (term) DO UPDATE SET doc_freq = EXCLUDED.doc_freq, idf = EXCLUDED.idf`)
	if err != nil {
		return fmt.Errorf("InsertSpecVocabBatch prepare: %w", err)
	}
	defer stmt.Close()

	for _, t := range terms {
		if _, err := stmt.Exec(t.Term, t.DocFreq, t.IDF); err != nil {
			return fmt.Errorf("InsertSpecVocabBatch exec: %w", err)
		}
	}

	return txn.Commit()
}

// InsertSpecEmbeddingsBatch вставляет LSA-эмбеддинги пакетами.
func (db *DB) InsertSpecEmbeddingsBatch(ctx context.Context, embeddings []model.SpecEmbedding) error {
	if len(embeddings) == 0 {
		return nil
	}
	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("InsertSpecEmbeddingsBatch begin: %w", err)
	}
	defer txn.Rollback()

	stmt, err := txn.Prepare(`
		INSERT INTO spec_embeddings (spec_id, embed_level, embed_text, embedding, embed_method, embed_dim)
		VALUES ($1, $2, $3, $4, $5, $6)`)
	if err != nil {
		return fmt.Errorf("InsertSpecEmbeddingsBatch prepare: %w", err)
	}
	defer stmt.Close()

	for _, e := range embeddings {
		// PostgreSQL array literal: {1.0,2.0,3.0}
		arrStr := floatSliceToPGArray(e.Embedding)
		if _, err := stmt.Exec(e.SpecID, e.EmbedLevel, e.EmbedText, arrStr, e.EmbedMethod, e.EmbedDim); err != nil {
			return fmt.Errorf("InsertSpecEmbeddingsBatch exec: %w", err)
		}
	}

	return txn.Commit()
}

// floatSliceToPGArray преобразует слайс float64 в PostgreSQL array literal.
func floatSliceToPGArray(vals []float64) string {
	if len(vals) == 0 {
		return "{}"
	}
	result := "{"
	for i, v := range vals {
		if i > 0 {
			result += ","
		}
		result += fmt.Sprintf("%g", v)
	}
	result += "}"
	return result
}
