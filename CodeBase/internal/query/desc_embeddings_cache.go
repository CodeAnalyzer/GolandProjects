package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/specfts"
	"github.com/lib/pq"
)

// descEmbeddingRow — эмбеддинг документа корпуса описаний с метаданными для
// выдачи semantic-слоя.
type descEmbeddingRow struct {
	EntityType  string    `json:"t"` // procedure | contract
	EntityID    int64     `json:"i"` // id процедуры/контракта
	Kind        string    `json:"k"` // procedure | service | event | callback_event | used_service
	Name        string    `json:"n"` // имя процедуры/контракта
	File        string    `json:"f"` // rel_path
	LineStart   int       `json:"l"` // строка начала
	SnippetLine string    `json:"s"` // первая строка описания (сниппет semantic-хита)
	Dim         int       `json:"d"` // размерность эмбеддинга
	FloatsLE    []float32 `json:"-"` // embedding в float32 (LE) — сериализуется отдельным блоком
}

// Интерфейс строки кэша specfts.EmbeddingRow (generic-хелпер кэша).
func (r *descEmbeddingRow) EmbeddingDim() int          { return r.Dim }
func (r *descEmbeddingRow) EmbeddingFloats() []float32 { return r.FloatsLE }
func (r *descEmbeddingRow) SetEmbedding(dim int, floats []float32) {
	r.Dim = dim
	r.FloatsLE = floats
}

// descEmbeddingsCachePath — путь к бинарному кэшу эмбеддингов.
// Файл входит в семейство sidecar-артефактов desc-LSA: desc_lsa_model.bin /
// desc_lsa_state.json / desc_lsa_embeddings.bin. Инвалидация — по поколению
// в заголовке файла (не по имени): смена поколения → промах при чтении →
// пересборка из БД и перезапись.
// Кэш исключает главный тормоз semantic-слоя: lib/pq парсит float8[] как текст
// (54k × 512 значений ≈ 6 c на каждый вызов), бинарный кэш читается за ~0.2 c.
func descEmbeddingsCachePath() string {
	return filepath.Join(filepath.Dir(config.DescLSAStatePath()), "desc_lsa_embeddings.bin")
}

// descEmbeddingsCacheMagic — маркер формата/владельца кэша desc-корпуса.
const descEmbeddingsCacheMagic = "DSEMBC1"

// loadDescEmbeddingsCached возвращает эмбеддинги поколения: из бинарного кэша
// (быстрый путь, поколение сверяется по заголовку) или из БД с последующей
// перезаписью кэша.
func (q *Query) loadDescEmbeddingsCached(ctx context.Context, generation string) ([]*descEmbeddingRow, error) {
	cachePath := descEmbeddingsCachePath()
	if rows, err := specfts.ReadEmbeddingsCache[*descEmbeddingRow](cachePath, descEmbeddingsCacheMagic, generation); err == nil {
		return rows, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		// повреждённый/устаревший кэш не фатален — пересоберём из БД
		_ = os.Remove(cachePath)
	}

	rows, err := q.loadDescEmbeddingsFromDB(ctx, generation)
	if err != nil {
		return nil, err
	}
	if err := specfts.WriteEmbeddingsCache(cachePath, descEmbeddingsCacheMagic, generation, rows); err != nil {
		// ошибка записи кэша не ломает поиск
		_ = os.Remove(cachePath)
	}
	// Уборка legacy-кэшей старого формата (desc_embeddings_<generation>.f32)
	if matches, gErr := filepath.Glob(filepath.Join(filepath.Dir(cachePath), "desc_embeddings_*.f32")); gErr == nil {
		for _, m := range matches {
			_ = os.Remove(m)
		}
	}
	return rows, nil
}

// loadDescEmbeddingsFromDB читает эмбеддинги поколения из desc_embeddings
// вместе с метаданными сущностей.
func (q *Query) loadDescEmbeddingsFromDB(ctx context.Context, generation string) ([]*descEmbeddingRow, error) {
	rows, err := q.db.QueryContext(ctx, `
		SELECT de.entity_type, de.entity_id,
		       CASE WHEN de.entity_type = 'contract' THEN lower(c.contract_kind) ELSE 'procedure' END AS kind,
		       CASE WHEN de.entity_type = 'contract' THEN c.contract_name ELSE p.proc_name END AS name,
		       f.rel_path,
		       CASE WHEN de.entity_type = 'contract' THEN c.line_start ELSE p.line_start END AS line_start,
		       CASE WHEN de.entity_type = 'contract'
		            THEN coalesce(c.short_description, '') || ' ' || coalesce(c.full_description, '')
		            ELSE coalesce(p.description, '') END AS description,
		       de.embedding
		FROM desc_embeddings de
		LEFT JOIN api_contracts c ON de.entity_type = 'contract' AND c.id = de.entity_id
		LEFT JOIN sql_procedures p ON de.entity_type = 'procedure' AND p.id = de.entity_id
		JOIN files f ON f.id = CASE WHEN de.entity_type = 'contract' THEN c.file_id ELSE p.file_id END
		WHERE de.generation = $1`, generation)
	if err != nil {
		return nil, fmt.Errorf("desc semantic load embeddings: %w", err)
	}
	defer rows.Close()

	out := make([]*descEmbeddingRow, 0, 4096)
	for rows.Next() {
		row := &descEmbeddingRow{}
		var lineStart sql.NullInt64
		var emb pq.Float64Array
		var description sql.NullString
		if err := rows.Scan(&row.EntityType, &row.EntityID, &row.Kind, &row.Name,
			&row.File, &lineStart, &description, &emb); err != nil {
			return nil, fmt.Errorf("desc semantic scan embeddings: %w", err)
		}
		if lineStart.Valid {
			row.LineStart = int(lineStart.Int64)
		}
		row.SnippetLine = firstDescriptionLine(description.String)
		row.FloatsLE = make([]float32, len(emb))
		for i, v := range emb {
			row.FloatsLE[i] = float32(v)
		}
		row.Dim = len(row.FloatsLE)
		out = append(out, row)
	}
	return out, rows.Err()
}
