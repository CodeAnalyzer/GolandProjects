package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/codebase/internal/model"
	"github.com/lib/pq"
)

// copyInBatches разбивает [0,total) на куски по batchSize и вызывает insertRange.
func copyInBatches(total int, batchSize int, insertRange func(from, to int) error) error {
	if total == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = 1
	}
	for from := 0; from < total; from += batchSize {
		to := from + batchSize
		if to > total {
			to = total
		}
		if err := insertRange(from, to); err != nil {
			return err
		}
	}
	return nil
}

// BatchInsertSpecConfigs пакетная вставка профилей продуктов.
func (db *DB) BatchInsertSpecConfigs(ctx context.Context, configs []*model.SpecConfig, batchSize int) error {
	insertBatch := func(items []*model.SpecConfig) error {
		return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
			stmt, err := tx.Prepare(pq.CopyIn("spec_configs",
				"file_id", "ds_product_id", "root_dir", "schema_name", "product_name",
				"usecase_layout", "id_style", "cross_ref_style", "normative_lang", "traceability",
				"has_changes", "has_audit", "has_adr", "coverage_metrics", "context_text"))
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, c := range items {
				if _, err := stmt.Exec(
					c.FileID, NullableInt64(c.DsProductID), NullableString(c.RootDir), NullableString(c.SchemaName), sanitizeUTF8String(c.ProductName),
					sanitizeUTF8String(c.UsecaseLayout), sanitizeUTF8String(c.IDStyle), sanitizeUTF8String(c.CrossRefStyle),
					sanitizeUTF8String(c.NormativeLang), sanitizeUTF8String(c.Traceability),
					c.HasChanges, c.HasAudit, c.HasADR, c.CoverageMetrics, NullableString(c.ContextText),
				); err != nil {
					return err
				}
			}
			_, err = stmt.Exec()
			return err
		})
	}
	return copyInBatches(len(configs), batchSize, func(from, to int) error {
		return insertBatch(configs[from:to])
	})
}

// BatchInsertSpecCapabilities пакетная вставка capability.
func (db *DB) BatchInsertSpecCapabilities(ctx context.Context, caps []*model.SpecCapability, batchSize int) error {
	insertBatch := func(items []*model.SpecCapability) error {
		return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
			stmt, err := tx.Prepare(pq.CopyIn("spec_capabilities",
				"file_id", "spec_config_id", "ds_product_id", "parent_id",
				"capability_name", "title", "purpose", "notes", "related_code",
				"line_start", "line_end", "api_total", "api_covered", "code_total", "code_listed"))
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, c := range items {
				if _, err := stmt.Exec(
					c.FileID, c.SpecConfigID, NullableInt64(c.DsProductID), NullableInt64(c.ParentID),
					sanitizeUTF8String(c.CapabilityName), sanitizeUTF8String(c.Title),
					NullableString(c.Purpose), NullableString(c.Notes), NullableString(c.RelatedCode),
					c.LineStart, c.LineEnd,
					nullableIntPtr(c.ApiTotal), nullableIntPtr(c.ApiCovered),
					nullableIntPtr(c.CodeTotal), nullableIntPtr(c.CodeListed),
				); err != nil {
					return err
				}
			}
			_, err = stmt.Exec()
			return err
		})
	}
	return copyInBatches(len(caps), batchSize, func(from, to int) error {
		return insertBatch(caps[from:to])
	})
}

// BatchInsertSpecRequirements пакетная вставка требований.
func (db *DB) BatchInsertSpecRequirements(ctx context.Context, reqs []*model.SpecRequirement, batchSize int) error {
	insertBatch := func(items []*model.SpecRequirement) error {
		return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
			stmt, err := tx.Prepare(pq.CopyIn("spec_requirements",
				"file_id", "capability_id", "requirement_name", "body_text",
				"line_start", "line_end", "req_order"))
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, r := range items {
				if _, err := stmt.Exec(
					r.FileID, r.CapabilityID, sanitizeUTF8String(r.RequirementName),
					sanitizeUTF8String(r.BodyText), r.LineStart, r.LineEnd, r.ReqOrder,
				); err != nil {
					return err
				}
			}
			_, err = stmt.Exec()
			return err
		})
	}
	return copyInBatches(len(reqs), batchSize, func(from, to int) error {
		return insertBatch(reqs[from:to])
	})
}

// BatchInsertSpecScenarios пакетная вставка сценариев.
func (db *DB) BatchInsertSpecScenarios(ctx context.Context, scenarios []*model.SpecScenario, batchSize int) error {
	insertBatch := func(items []*model.SpecScenario) error {
		return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
			stmt, err := tx.Prepare(pq.CopyIn("spec_scenarios",
				"file_id", "requirement_id", "scenario_name",
				"given_text", "when_text", "then_text",
				"line_start", "line_end", "scn_order"))
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, s := range items {
				if _, err := stmt.Exec(
					s.FileID, s.RequirementID, sanitizeUTF8String(s.ScenarioName),
					NullableString(s.GivenText), NullableString(s.WhenText), NullableString(s.ThenText),
					s.LineStart, s.LineEnd, s.ScnOrder,
				); err != nil {
					return err
				}
			}
			_, err = stmt.Exec()
			return err
		})
	}
	return copyInBatches(len(scenarios), batchSize, func(from, to int) error {
		return insertBatch(scenarios[from:to])
	})
}

func (db *DB) fillSpecUsecaseStepFileIDs(ctx context.Context, steps []*model.SpecUsecaseStep) error {
	fileIDs := make(map[int64]int64)
	for _, step := range steps {
		if step.FileID > 0 {
			continue
		}
		fileID, ok := fileIDs[step.UsecaseID]
		if !ok {
			if err := db.QueryRowContext(ctx, `SELECT file_id FROM spec_usecases WHERE id = $1`, step.UsecaseID).Scan(&fileID); err != nil {
				return fmt.Errorf("resolve file_id for spec_usecase_step: %w", err)
			}
			fileIDs[step.UsecaseID] = fileID
		}
		step.FileID = fileID
	}
	return nil
}

// BatchInsertSpecUsecases пакетная вставка usecase вместе со шагами.
func (db *DB) BatchInsertSpecUsecases(ctx context.Context, usecases []*model.SpecUsecase, steps []*model.SpecUsecaseStep, batchSize int) error {
	insertUsecases := func(items []*model.SpecUsecase) error {
		return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
			stmt, err := tx.Prepare(pq.CopyIn("spec_usecases",
				"file_id", "spec_config_id", "usecase_name", "title",
				"description", "actors", "preconditions", "postconditions",
				"business_value", "architecture", "data_schema",
				"source_dir", "usecase_kind", "page_id", "line_start", "line_end"))
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, u := range items {
				if _, err := stmt.Exec(
					u.FileID, u.SpecConfigID, sanitizeUTF8String(u.UsecaseName), sanitizeUTF8String(u.Title),
					NullableString(u.Description), NullableString(u.Actors),
					NullableString(u.Preconditions), NullableString(u.Postconditions),
					NullableString(u.BusinessValue), NullableString(u.Architecture), NullableString(u.DataSchema),
					sanitizeUTF8String(u.SourceDir), sanitizeUTF8String(u.UsecaseKind),
					NullableInt64(u.PageID), u.LineStart, u.LineEnd,
				); err != nil {
					return err
				}
			}
			_, err = stmt.Exec()
			return err
		})
	}
	if err := copyInBatches(len(usecases), batchSize, func(from, to int) error {
		return insertUsecases(usecases[from:to])
	}); err != nil {
		return err
	}
	if err := db.fillSpecUsecaseStepFileIDs(ctx, steps); err != nil {
		return err
	}

	insertSteps := func(items []*model.SpecUsecaseStep) error {
		return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
			stmt, err := tx.Prepare(pq.CopyIn("spec_usecase_steps",
				"file_id", "usecase_id", "flow_kind", "step_order", "step_text", "line_number"))
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, s := range items {
				if _, err := stmt.Exec(
					s.FileID, s.UsecaseID, sanitizeUTF8String(s.FlowKind), s.StepOrder,
					sanitizeUTF8String(s.StepText), s.LineNumber,
				); err != nil {
					return err
				}
			}
			_, err = stmt.Exec()
			return err
		})
	}
	return copyInBatches(len(steps), batchSize, func(from, to int) error {
		return insertSteps(steps[from:to])
	})
}

// BatchInsertSpecUsecaseSteps пакетная вставка шагов usecase.
func (db *DB) BatchInsertSpecUsecaseSteps(ctx context.Context, steps []*model.SpecUsecaseStep, batchSize int) error {
	if err := db.fillSpecUsecaseStepFileIDs(ctx, steps); err != nil {
		return err
	}
	insertSteps := func(items []*model.SpecUsecaseStep) error {
		return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
			stmt, err := tx.Prepare(pq.CopyIn("spec_usecase_steps",
				"file_id", "usecase_id", "flow_kind", "step_order", "step_text", "line_number"))
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, s := range items {
				if _, err := stmt.Exec(
					s.FileID, s.UsecaseID, sanitizeUTF8String(s.FlowKind), s.StepOrder,
					sanitizeUTF8String(s.StepText), s.LineNumber,
				); err != nil {
					return err
				}
			}
			_, err = stmt.Exec()
			return err
		})
	}
	return copyInBatches(len(steps), batchSize, func(from, to int) error {
		return insertSteps(steps[from:to])
	})
}

// BatchInsertSpecChanges пакетная вставка changes вместе с delta-требованиями.
func (db *DB) BatchInsertSpecChanges(ctx context.Context, changes []*model.SpecChange, deltas []*model.SpecChangeDelta, batchSize int) error {
	insertChanges := func(items []*model.SpecChange) error {
		return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
			stmt, err := tx.Prepare(pq.CopyIn("spec_changes",
				"file_id", "spec_config_id", "change_name", "status", "dir_path"))
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, c := range items {
				if _, err := stmt.Exec(
					c.FileID, c.SpecConfigID, sanitizeUTF8String(c.ChangeName),
					sanitizeUTF8String(c.Status), sanitizeUTF8String(c.DirPath),
				); err != nil {
					return err
				}
			}
			_, err = stmt.Exec()
			return err
		})
	}
	if err := copyInBatches(len(changes), batchSize, func(from, to int) error {
		return insertChanges(changes[from:to])
	}); err != nil {
		return err
	}

	insertDeltas := func(items []*model.SpecChangeDelta) error {
		return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
			stmt, err := tx.Prepare(pq.CopyIn("spec_change_delta",
				"file_id", "change_id", "section", "capability_slug",
				"requirement_name", "body_text", "line_start", "line_end"))
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, d := range items {
				if _, err := stmt.Exec(
					d.FileID, d.ChangeID, sanitizeUTF8String(d.Section), sanitizeUTF8String(d.CapabilitySlug),
					sanitizeUTF8String(d.RequirementName), sanitizeUTF8String(d.BodyText),
					d.LineStart, d.LineEnd,
				); err != nil {
					return err
				}
			}
			_, err = stmt.Exec()
			return err
		})
	}
	return copyInBatches(len(deltas), batchSize, func(from, to int) error {
		return insertDeltas(deltas[from:to])
	})
}

// BatchInsertSpecCodeMentions пакетная вставка сырых упоминаний кода (staging).
func (db *DB) BatchInsertSpecCodeMentions(ctx context.Context, mentions []*model.SpecCodeMention, batchSize int) error {
	insertBatch := func(items []*model.SpecCodeMention) error {
		return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
			stmt, err := tx.Prepare(pq.CopyIn("spec_code_mentions",
				"file_id", "source_type", "source_id", "mention_name", "mention_kind", "line_number"))
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, m := range items {
				if _, err := stmt.Exec(
					m.FileID, sanitizeUTF8String(m.SourceType), m.SourceID,
					sanitizeUTF8String(m.MentionName), sanitizeUTF8String(m.MentionKind), m.LineNumber,
				); err != nil {
					return err
				}
			}
			_, err = stmt.Exec()
			return err
		})
	}
	return copyInBatches(len(mentions), batchSize, func(from, to int) error {
		return insertBatch(mentions[from:to])
	})
}

// BatchInsertSpecVocab пакетная вставка словаря LSA (уникальность по term —
// перед вставкой словарь пересоздаётся в ReplaceSpecVocab).
func (db *DB) BatchInsertSpecVocab(ctx context.Context, terms []*model.SpecVocabTerm, batchSize int) error {
	insertBatch := func(items []*model.SpecVocabTerm) error {
		return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
			stmt, err := tx.Prepare(pq.CopyIn("spec_vocab", "term", "doc_freq", "idf"))
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, term := range items {
				if _, err := stmt.Exec(sanitizeUTF8String(term.Term), term.DocFreq, term.IDF); err != nil {
					return err
				}
			}
			_, err = stmt.Exec()
			return err
		})
	}
	return copyInBatches(len(terms), batchSize, func(from, to int) error {
		return insertBatch(terms[from:to])
	})
}

// ReplaceSpecVocab пересоздаёт словарь LSA целиком.
func (db *DB) ReplaceSpecVocab(ctx context.Context, terms []*model.SpecVocabTerm, batchSize int) error {
	if _, err := db.ExecContext(ctx, `DELETE FROM spec_vocab`); err != nil {
		return fmt.Errorf("clear spec_vocab: %w", err)
	}
	return db.BatchInsertSpecVocab(ctx, terms, batchSize)
}

// ReplaceSpecEmbeddings заменяет LSA-векторы для capability-уровня.
func (db *DB) ReplaceSpecEmbeddings(ctx context.Context, embeddings []*model.SpecEmbedding, batchSize int) error {
	if _, err := db.ExecContext(ctx, `DELETE FROM spec_embeddings WHERE embed_level = 'spec'`); err != nil {
		return fmt.Errorf("clear spec_embeddings: %w", err)
	}
	insertBatch := func(items []*model.SpecEmbedding) error {
		return db.withCopyInTxCtx(ctx, func(tx *sql.Tx) error {
			stmt, err := tx.Prepare(pq.CopyIn("spec_embeddings",
				"spec_id", "embed_level", "embed_text", "embedding", "embed_method", "embed_dim"))
			if err != nil {
				return err
			}
			defer stmt.Close()
			for _, e := range items {
				if _, err := stmt.Exec(
					e.SpecID, sanitizeUTF8String(e.EmbedLevel), sanitizeUTF8String(e.EmbedText),
					pq.Array(e.Embedding), sanitizeUTF8String(e.EmbedMethod), e.EmbedDim,
				); err != nil {
					return err
				}
			}
			_, err = stmt.Exec()
			return err
		})
	}
	return copyInBatches(len(embeddings), batchSize, func(from, to int) error {
		return insertBatch(embeddings[from:to])
	})
}

// EnsureSpecSearchVectors заполняет search_vector (tsvector 'russian') для spec-сущностей
// файла, вставленных без полнотекстового вектора. Вызывается после batch insert файла.
func (db *DB) EnsureSpecSearchVectors(ctx context.Context, fileID int64) error {
	statements := []string{
		`UPDATE spec_configs SET search_vector =
			to_tsvector('russian', coalesce(product_name,'') || ' ' || coalesce(context_text,''))
			WHERE file_id = $1 AND search_vector IS NULL`,
		`UPDATE spec_capabilities SET search_vector =
			to_tsvector('russian', coalesce(capability_name,'') || ' ' || coalesce(title,'') || ' ' || coalesce(purpose,'') || ' ' || coalesce(notes,''))
			WHERE file_id = $1 AND search_vector IS NULL`,
		`UPDATE spec_requirements SET search_vector =
			to_tsvector('russian', coalesce(requirement_name,'') || ' ' || coalesce(body_text,''))
			WHERE file_id = $1 AND search_vector IS NULL`,
		`UPDATE spec_scenarios SET search_vector =
			to_tsvector('russian', coalesce(scenario_name,'') || ' ' || coalesce(given_text,'') || ' ' || coalesce(when_text,'') || ' ' || coalesce(then_text,''))
			WHERE file_id = $1 AND search_vector IS NULL`,
		`UPDATE spec_usecases SET search_vector =
			to_tsvector('russian', coalesce(usecase_name,'') || ' ' || coalesce(title,'') || ' ' || coalesce(description,''))
			WHERE file_id = $1 AND search_vector IS NULL`,
		`UPDATE spec_changes SET search_vector =
			to_tsvector('russian', coalesce(change_name,'') || ' ' || coalesce(status,''))
			WHERE file_id = $1 AND search_vector IS NULL`,
	}
	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt, fileID); err != nil {
			return fmt.Errorf("ensure search_vector: %w", err)
		}
	}
	return nil
}

// GetOrCreateSpecConfig возвращает id spec_config по продукту и openspec-корню, создавая
// минимальную запись при отсутствии. Решает проблему порядка индексации:
// spec.md может быть обработан раньше config.yaml — тогда создаётся заглушка,
// которая обновляется при обработке config.yaml через UpdateSpecConfig.
func (db *DB) GetOrCreateSpecConfig(ctx context.Context, fileID int64, dsProductID int64, productName, rootDir string) (int64, error) {
	// Verify file_id exists before creating config
	var fileExists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM files WHERE id = $1)`, fileID).Scan(&fileExists); err != nil {
		return 0, fmt.Errorf("verify file_id %d exists: %w", fileID, err)
	}
	if !fileExists {
		return 0, fmt.Errorf("file_id %d does not exist in files (product=%q) — cannot create spec_config", fileID, productName)
	}
	if dsProductID > 0 && rootDir != "" {
		// Serialize lookup and insert for the unique product/root key. Without
		// this, concurrent first files can wait on the unique index and deadlock
		// with transactions writing the same OpenSpec root.
		if _, err := db.ExecContext(ctx, `
			SELECT pg_advisory_xact_lock(hashtextextended($1, 0))
		`, fmt.Sprintf("spec-config-key/%d/%s", dsProductID, rootDir)); err != nil {
			return 0, fmt.Errorf("lock spec config key: %w", err)
		}
	}
	if dsProductID > 0 && rootDir != "" {
		var configID int64
		err := db.QueryRowContext(ctx, `
			SELECT id FROM spec_configs
			WHERE ds_product_id = $1 AND root_dir = $2
			ORDER BY id DESC LIMIT 1
		`, dsProductID, rootDir).Scan(&configID)
		if err == nil {
			return configID, nil
		}
		if err != sql.ErrNoRows {
			return 0, fmt.Errorf("query spec_config by product: %w", err)
		}
	}
	var configID int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO spec_configs (file_id, ds_product_id, root_dir, product_name)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (ds_product_id, root_dir) WHERE root_dir IS NOT NULL
		DO UPDATE SET id = spec_configs.id
		RETURNING id
	`, fileID, NullableInt64(dsProductID), NullableString(rootDir), sanitizeUTF8String(productName)).Scan(&configID)
	if err != nil {
		return 0, fmt.Errorf("create spec_config (fileID=%d, dsProductID=%d, product=%q): %w", fileID, dsProductID, productName, err)
	}
	return configID, nil
}

// UpdateSpecConfig обновляет профиль продукта при обработке config.yaml.
func (db *DB) UpdateSpecConfig(ctx context.Context, configID int64, fileID int64,
	schemaName, productName, contextText string) error {
	if _, err := db.ExecContext(ctx, `
		SELECT pg_advisory_xact_lock(hashtextextended($1, 0))
	`, fmt.Sprintf("spec-config/%d", configID)); err != nil {
		return fmt.Errorf("lock spec config for update: %w", err)
	}
	_, err := db.ExecContext(ctx, `
		UPDATE spec_configs
		SET file_id = $2, schema_name = $3, product_name = $4, context_text = $5
		WHERE id = $1
	`, configID, fileID, NullableString(schemaName), sanitizeUTF8String(productName), NullableString(contextText))
	if err != nil {
		return fmt.Errorf("update spec_config: %w", err)
	}
	return nil
}

// nullableIntPtr — *int → NULL-совместимое значение для SQL.
func nullableIntPtr(v *int) interface{} {
	if v == nil {
		return nil
	}
	return *v
}
