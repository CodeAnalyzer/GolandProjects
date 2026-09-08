package specfts

import (
	"math"
	"sort"
)

// Document — документ для LSA (capability-уровень)
type Document struct {
	ID   int64  // spec_capabilities.id
	Text string // текст: title + purpose + notes + body_text требований
}

// Vocab — словарь терминов с TF-IDF весами
type Vocab struct {
	Terms   []string       // упорядоченный список терминов
	Index   map[string]int // term → индекс в векторе
	DocFreq []int          // число документов, содержащих термин
	IDF     []float64      // inverse document frequency
	MinDF   int            // минимальная document frequency
	MaxDF   float64        // максимальная доля документов (0.0–1.0)
}

// BuildVocab строит словарь из документов с фильтрацией minDF/maxDF.
func BuildVocab(docs []Document, minDF int, maxDF float64) *Vocab {
	nDocs := len(docs)
	if nDocs == 0 {
		return &Vocab{Index: map[string]int{}}
	}

	// Подсчёт document frequency
	df := map[string]int{}
	for _, doc := range docs {
		stems := TokenizeToStems(doc.Text)
		seen := map[string]bool{}
		for _, s := range stems {
			if !seen[s] {
				seen[s] = true
				df[s]++
			}
		}
	}

	// Фильтрация по minDF и maxDF
	maxDFCount := int(maxDF * float64(nDocs))
	var terms []string
	for term, freq := range df {
		if freq < minDF {
			continue
		}
		if maxDFCount > 0 && freq > maxDFCount {
			continue
		}
		terms = append(terms, term)
	}
	sort.Strings(terms)

	vocab := &Vocab{
		Terms:   terms,
		Index:   make(map[string]int, len(terms)),
		DocFreq: make([]int, len(terms)),
		IDF:     make([]float64, len(terms)),
		MinDF:   minDF,
		MaxDF:   maxDF,
	}
	for i, term := range terms {
		vocab.Index[term] = i
		vocab.DocFreq[i] = df[term]
		// smooth IDF: ln((1 + N) / (1 + df)) + 1
		vocab.IDF[i] = math.Log(float64(1+nDocs)/float64(1+df[term])) + 1.0
	}
	return vocab
}

// TFIDFMatrix строит матрицу TF-IDF (nDocs × nTerms).
// TF = 1 + log(count), IDF = smooth, L2-нормализация по строкам.
func (v *Vocab) TFIDFMatrix(docs []Document) *DenseMatrix {
	nDocs := len(docs)
	nTerms := len(v.Terms)
	m := NewDenseMatrix(nDocs, nTerms)

	for i, doc := range docs {
		stems := TokenizeToStems(doc.Text)
		tf := make(map[int]float64)
		for _, s := range stems {
			if idx, ok := v.Index[s]; ok {
				tf[idx]++
			}
		}
		// TF = 1 + log(count), weight = TF * IDF
		for j, count := range tf {
			tfidf := (1.0 + math.Log(count)) * v.IDF[j]
			m.Set(i, j, tfidf)
		}
		// L2-нормализация строки
		var norm float64
		for j := 0; j < nTerms; j++ {
			val := m.At(i, j)
			norm += val * val
		}
		if norm > 0 {
			norm = math.Sqrt(norm)
			for j := 0; j < nTerms; j++ {
				m.Set(i, j, m.At(i, j)/norm)
			}
		}
	}
	return m
}

// ProjectQuery строит TF-IDF вектор запроса и проецирует его через Vᵀ.
// Возвращает вектор в LSA-пространстве (k-мерный).
func (v *Vocab) ProjectQuery(query string, vt *DenseMatrix) []float64 {
	nTerms := len(v.Terms)
	q := make([]float64, nTerms)
	stems := TokenizeToStems(query)
	tf := make(map[int]float64)
	for _, s := range stems {
		if idx, ok := v.Index[s]; ok {
			tf[idx]++
		}
	}
	for j, count := range tf {
		q[j] = (1.0 + math.Log(count)) * v.IDF[j]
	}
	// L2-нормализация
	var norm float64
	for _, val := range q {
		norm += val * val
	}
	if norm > 0 {
		norm = math.Sqrt(norm)
		for j := range q {
			q[j] /= norm
		}
	}
	// Проекция: q_lsa = q · Vᵀ → но Vᵀ имеет размер k×nTerms
	// q_lsa[j] = sum_i q[i] * Vᵀ[j][i]
	k := vt.Rows()
	vtCols := vt.Cols()
	result := make([]float64, k)
	for j := 0; j < k; j++ {
		var sum float64
		for i := 0; i < vtCols; i++ {
			sum += q[i] * vt.At(j, i)
		}
		result[j] = sum
	}
	return result
}
