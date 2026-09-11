package specfts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// CompositionVersion — версия состава LSA-документа capability.
// Изменение состава документа требует bump значения (fingerprint меняется, модель переобучается).
//
//	1: title + purpose + notes
//	2: title×2 + purpose + notes + имена/тексты требований + имена/тексты сценариев
const CompositionVersion = 2

// TokenizerVersion — версия токенизации/стемминга.
//
//	1: исходный токенизатор
//	2: нормализация ё→е
const TokenizerVersion = 2

// AlgorithmVersion — версия алгоритма обучения (SVD-обрезка/проекция).
// Входит в fingerprint: смена алгоритма обязана переобучать модель
// даже при неизменном корпусе и параметрах.
//
//	1: SVD с багом обрезки VT до min(nDocs, nTerms) колонок — при nTerms > nDocs
//	   термины с алфавитным индексом ≥ nDocs выпадали из проекции запроса
//	2: VT строится на все nTerms; bump принудительно переобучает существующие модели
const AlgorithmVersion = 2

// LSAParams — параметры обучения, входящие в fingerprint модели.
type LSAParams struct {
	MinDF int     `json:"min_df"`
	MaxDF float64 `json:"max_df"`
	K     int     `json:"k"`
}

// LSADocText собирает текст LSA-документа capability:
// title с удвоенным весом + purpose + notes + агрегированный текст требований и сценариев.
func LSADocText(title, purpose, notes, lsaText string) string {
	return strings.Join([]string{title, title, purpose, notes, lsaText}, " ")
}

// CorpusFingerprint вычисляет детерминированный SHA-256 хеш корпуса и параметров модели.
// Поля разделяются NUL-байтом, чтобы конкатенации не коллизировали.
func CorpusFingerprint(docs []Document, params LSAParams) string {
	h := sha256.New()
	writeField := func(s string) {
		_, _ = h.Write([]byte(s))
		_, _ = h.Write([]byte{0})
	}
	writeField(fmt.Sprintf("composition=%d", CompositionVersion))
	writeField(fmt.Sprintf("tokenizer=%d", TokenizerVersion))
	writeField(fmt.Sprintf("algorithm=%d", AlgorithmVersion))
	writeField(fmt.Sprintf("params=%d/%.6f/%d", params.MinDF, params.MaxDF, params.K))
	writeField(fmt.Sprintf("ndocs=%d", len(docs)))
	for _, d := range docs {
		writeField(fmt.Sprintf("id=%d", d.ID))
		writeField(d.Text)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// LSAState — состояние LSA-обучения (sidecar-файл рядом с файлом модели).
type LSAState struct {
	Fingerprint string    `json:"fingerprint"` // fingerprint корпуса, на котором обучена модель
	Pending     int       `json:"pending"`     // накопленные изменения с прошлого обучения
	Params      LSAParams `json:"params"`      // параметры обученной модели
	Algorithm   int       `json:"algorithm"`   // версия алгоритма обучения (AlgorithmVersion)
	NumDocs     int       `json:"num_docs"`    // размер корпуса при обучении
	TrainedAt   time.Time `json:"trained_at"`
}

// LoadLSAState загружает состояние из файла. Отсутствие файла — не ошибка: (nil, nil).
func LoadLSAState(path string) (*LSAState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("LoadLSAState: %w", err)
	}
	var st LSAState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("LoadLSAState parse %s: %w", path, err)
	}
	return &st, nil
}

// SaveLSAState атомарно сохраняет состояние (tmp + rename).
func SaveLSAState(path string, st *LSAState) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("SaveLSAState marshal: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("SaveLSAState tmp %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("SaveLSAState rename %s: %w", path, err)
	}
	return nil
}
