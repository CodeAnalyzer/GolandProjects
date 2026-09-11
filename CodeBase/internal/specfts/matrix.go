package specfts

import (
	"encoding/gob"
	"math"
	"os"

	"gonum.org/v1/gonum/mat"
)

// DenseMatrix — обёртка над gonum mat.Dense для удобства сериализации.
type DenseMatrix struct {
	Data *mat.Dense
}

// NewDenseMatrix создаёт матрицу rows×cols, заполненную нулями.
func NewDenseMatrix(rows, cols int) *DenseMatrix {
	return &DenseMatrix{Data: mat.NewDense(rows, cols, nil)}
}

// Rows возвращает число строк.
func (m *DenseMatrix) Rows() int {
	if m.Data == nil {
		return 0
	}
	r, _ := m.Data.Dims()
	return r
}

// Cols возвращает число столбцов.
func (m *DenseMatrix) Cols() int {
	if m.Data == nil {
		return 0
	}
	_, c := m.Data.Dims()
	return c
}

// At возвращает элемент.
func (m *DenseMatrix) At(i, j int) float64 {
	return m.Data.At(i, j)
}

// Set устанавливает элемент.
func (m *DenseMatrix) Set(i, j int, v float64) {
	m.Data.Set(i, j, v)
}

// RawMatrix возвращает gonum mat.Dense для SVD.
func (m *DenseMatrix) RawMatrix() *mat.Dense {
	return m.Data
}

// SVD выполняет сингулярное разложение матрицы.
// Возвращает U (nDocs×k), S (k сингулярных значений), Vᵀ (k×nTerms).
func SVD(m *DenseMatrix, k int) (u *DenseMatrix, s []float64, vt *DenseMatrix, err error) {
	rows, cols := m.Rows(), m.Cols()
	if rows == 0 || cols == 0 {
		return nil, nil, nil, nil
	}

	// k не может превышать min(rows, cols)
	maxK := rows
	if cols < maxK {
		maxK = cols
	}
	if k > maxK {
		k = maxK
	}
	if k < 1 {
		k = 1
	}

	var svd mat.SVD
	if !svd.Factorize(m.Data, mat.SVDThin) {
		return nil, nil, nil, nil
	}

	var uDense mat.Dense
	svd.UTo(&uDense)
	var vtDense mat.Dense
	svd.VTo(&vtDense)
	singulars := svd.Values(nil)

	// Обрезаем до k
	actualK := k
	if len(singulars) < actualK {
		actualK = len(singulars)
	}

	s = make([]float64, actualK)
	copy(s, singulars[:actualK])

	// Фактические размеры U и V после SVDThin
	uRows, uCols := uDense.Dims()
	vtRows, _ := vtDense.Dims()

	// U: rows×actualK (но не больше uCols)
	uColsEff := actualK
	if uColsEff > uCols {
		uColsEff = uCols
	}
	u = NewDenseMatrix(uRows, uColsEff)
	for i := 0; i < uRows; i++ {
		for j := 0; j < uColsEff; j++ {
			u.Set(i, j, uDense.At(i, j))
		}
	}

	// Vᵀ: actualK×cols (nTerms) — ВСЕ термины.
	// svd.VTo возвращает V размером nTerms×r, где r = min(nDocs, nTerms).
	// Vᵀ[j][t] = V[t][j]: индекс термина t ограничен строками V (= nTerms),
	// индекс сингулярного направления j — колонками V (= r), у нас j < actualK ≤ r.
	// Ошибочно было клампить число колонок VT до r: при nTerms > nDocs термины
	// с индексом ≥ r выпадали из проекции запроса (см. AlgorithmVersion 2).
	vtRowsEff := actualK
	if vtRowsEff > vtRows {
		vtRowsEff = vtRows
	}
	vt = NewDenseMatrix(vtRowsEff, cols)
	for i := 0; i < vtRowsEff; i++ {
		for j := 0; j < cols; j++ {
			vt.Set(i, j, vtDense.At(j, i))
		}
	}

	return u, s, vt, nil
}

// LSAModel — сохраняемая LSA-модель.
type LSAModel struct {
	Vocab      *Vocab
	VT         *DenseMatrix // k×nTerms
	Singulars  []float64    // k сингулярных значений
	K          int
	NumDocs    int
	NumTerms   int
}

// SaveLSAModel сохраняет модель в файл.
func SaveLSAModel(model *LSAModel, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	if err := enc.Encode(model); err != nil {
		return err
	}
	return nil
}

// LoadLSAModel загружает модель из файла.
func LoadLSAModel(path string) (*LSAModel, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var model LSAModel
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&model); err != nil {
		return nil, err
	}
	return &model, nil
}

// CosineSimilarity вычисляет косинусную близость двух векторов.
func CosineSimilarity(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// DocEmbedding возвращает LSA-вектор документа: U[i] · diag(S)
func DocEmbedding(u *DenseMatrix, s []float64, docIdx int) []float64 {
	k := len(s)
	if k == 0 {
		return nil
	}
	result := make([]float64, k)
	for j := 0; j < k; j++ {
		result[j] = u.At(docIdx, j) * s[j]
	}
	return result
}

func init() {
	// Регистрация типов для gob
	gob.Register(&Vocab{})
	gob.Register(&DenseMatrix{})
	gob.Register(&LSAModel{})
}
