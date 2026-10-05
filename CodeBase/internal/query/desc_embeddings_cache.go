package query

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/codebase/internal/config"
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

// loadDescEmbeddingsCached возвращает эмбеддинги поколения: из бинарного кэша
// (быстрый путь, поколение сверяется по заголовку) или из БД с последующей
// перезаписью кэша.
func (q *Query) loadDescEmbeddingsCached(ctx context.Context, generation string) ([]descEmbeddingRow, error) {
	cachePath := descEmbeddingsCachePath()
	if rows, err := readDescEmbeddingsCache(cachePath, generation); err == nil {
		return rows, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		// повреждённый/устаревший кэш не фатален — пересоберём из БД
		_ = os.Remove(cachePath)
	}

	rows, err := q.loadDescEmbeddingsFromDB(ctx, generation)
	if err != nil {
		return nil, err
	}
	if err := writeDescEmbeddingsCache(cachePath, generation, rows); err != nil {
		// ошибка записи кэша не ломает поиск
		_ = os.Remove(cachePath)
	}
	return rows, nil
}

// loadDescEmbeddingsFromDB читает эмбеддинги поколения из desc_embeddings
// вместе с метаданными сущностей.
func (q *Query) loadDescEmbeddingsFromDB(ctx context.Context, generation string) ([]descEmbeddingRow, error) {
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

	out := make([]descEmbeddingRow, 0, 4096)
	for rows.Next() {
		var row descEmbeddingRow
		var entityID int64
		var lineStart sql.NullInt64
		var emb pq.Float64Array
		var description sql.NullString
		if err := rows.Scan(&row.EntityType, &entityID, &row.Kind, &row.Name,
			&row.File, &lineStart, &description, &emb); err != nil {
			return nil, fmt.Errorf("desc semantic scan embeddings: %w", err)
		}
		row.EntityID = entityID
		if lineStart.Valid {
			row.LineStart = int(lineStart.Int64)
		}
		desc := description.String
		row.SnippetLine = firstDescriptionLine(desc)
		row.FloatsLE = make([]float32, len(emb))
		for i, v := range emb {
			row.FloatsLE[i] = float32(v)
		}
		row.Dim = len(row.FloatsLE)
		out = append(out, row)
	}
	return out, rows.Err()
}

const descEmbeddingsCacheMagic = "DSEMBC1"

// writeDescEmbeddingsCache сериализует кэш: magic + generation + JSON-мета +
// плоский блок float32 (LE). Запись через temp + rename (атомарность).
func writeDescEmbeddingsCache(path, generation string, rows []descEmbeddingRow) error {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	tmpPath := f.Name()
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmpPath)
	}()
	// Буфер обязателен: 27M+ одиночных f.Write занимают ~90 c
	bw := bufio.NewWriterSize(f, 1<<20)
	writeUint32 := func(v uint32) error {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], v)
		_, err := bw.Write(b[:])
		return err
	}
	writeString := func(s string) error {
		if err := writeUint32(uint32(len(s))); err != nil {
			return err
		}
		_, err := bw.Write([]byte(s))
		return err
	}

	if err := writeString(descEmbeddingsCacheMagic); err != nil {
		return err
	}
	if err := writeString(generation); err != nil {
		return err
	}
	meta, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	if err := writeString(string(meta)); err != nil {
		return err
	}
	dim := 0
	if len(rows) > 0 {
		dim = rows[0].Dim
	}
	if err := writeUint32(uint32(len(rows))); err != nil {
		return err
	}
	if err := writeUint32(uint32(dim)); err != nil {
		return err
	}
	buf := make([]byte, 4)
	for i := range rows {
		for _, v := range rows[i].FloatsLE {
			binary.LittleEndian.PutUint32(buf, math.Float32bits(v))
			if _, err := bw.Write(buf); err != nil {
				return err
			}
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	// Уборка legacy-кэшей старого формата (desc_embeddings_<generation>.f32)
	if matches, gErr := filepath.Glob(filepath.Join(filepath.Dir(path), "desc_embeddings_*.f32")); gErr == nil {
		for _, m := range matches {
			_ = os.Remove(m)
		}
	}
	return nil
}

// readDescEmbeddingsCache читает кэш; os.ErrNotExist — кэша нет.
func readDescEmbeddingsCache(path, generation string) ([]descEmbeddingRow, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	off := 0
	readString := func() (string, error) {
		if len(data)-off < 4 {
			return "", fmt.Errorf("desc embeddings cache: truncated string header")
		}
		n := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		if n > len(data)-off {
			return "", fmt.Errorf("desc embeddings cache: truncated string")
		}
		s := string(data[off : off+n])
		off += n
		return s, nil
	}

	magic, err := readString()
	if err != nil {
		return nil, err
	}
	if magic != descEmbeddingsCacheMagic {
		return nil, fmt.Errorf("desc embeddings cache: bad magic")
	}
	storedGeneration, err := readString()
	if err != nil {
		return nil, err
	}
	if storedGeneration != generation {
		return nil, fmt.Errorf("desc embeddings cache: generation mismatch")
	}
	metaJSON, err := readString()
	if err != nil {
		return nil, err
	}
	var rows []descEmbeddingRow
	if err := json.Unmarshal([]byte(metaJSON), &rows); err != nil {
		return nil, fmt.Errorf("desc embeddings cache: meta: %w", err)
	}
	if len(data)-off < 8 {
		return nil, fmt.Errorf("desc embeddings cache: truncated dims")
	}
	count := int(binary.LittleEndian.Uint32(data[off:]))
	dim := int(binary.LittleEndian.Uint32(data[off+4:]))
	off += 8
	if len(rows) != count || len(data)-off < count*dim*4 {
		return nil, fmt.Errorf("desc embeddings cache: truncated floats")
	}
	for i := range rows {
		rows[i].Dim = dim
		rows[i].FloatsLE = make([]float32, dim)
		for j := 0; j < dim; j++ {
			rows[i].FloatsLE[j] = math.Float32frombits(binary.LittleEndian.Uint32(data[off:]))
			off += 4
		}
	}
	return rows, nil
}
