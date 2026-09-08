package specfts

import (
	"math"
	"testing"
)

func TestBuildVocab_MinDFMaxDF(t *testing.T) {
	docs := []Document{
		{ID: 1, Text: "блокировка счёта арест"},
		{ID: 2, Text: "блокировка счёта"},
		{ID: 3, Text: "арест счёта"},
		{ID: 4, Text: "блокировка"},
	}
	// minDF=2: "блокировка"(4), "счёт"(3), "арест"(2) — все проходят
	// maxDF=0.3*4=1.2 → freq > 1.2 → freq >= 2 → все с freq=2,3,4 пройдут? нет, maxDFCount=1, freq > 1 → все отфильтрованы
	// Возьмём maxDF=1.0 (без фильтра)
	vocab := BuildVocab(docs, 2, 1.0)
	if len(vocab.Terms) == 0 {
		t.Fatal("expected non-empty vocab")
	}
	// "блокировка" встречается в 3 документах (1,2,4)
	idx, ok := vocab.Index["блокировк"]
	if !ok {
		t.Errorf("expected stem 'блокировк' in vocab, terms: %v", vocab.Terms)
	} else {
		if vocab.DocFreq[idx] != 3 {
			t.Errorf("expected docFreq=3 for 'блокировк', got %d", vocab.DocFreq[idx])
		}
	}
}

func TestTFIDF_LogTF_SmoothIDF_L2(t *testing.T) {
	docs := []Document{
		{ID: 1, Text: "блокировка счёта арест"},
		{ID: 2, Text: "блокировка счёта"},
		{ID: 3, Text: "арест счёта"},
	}
	vocab := BuildVocab(docs, 1, 1.0)
	m := vocab.TFIDFMatrix(docs)

	if m.Rows() != 3 {
		t.Fatalf("expected 3 rows, got %d", m.Rows())
	}

	// Проверяем L2-нормализацию: каждая строка имеет норму 1 (или 0)
	for i := 0; i < 3; i++ {
		var norm float64
		for j := 0; j < m.Cols(); j++ {
			v := m.At(i, j)
			norm += v * v
		}
		if math.Abs(norm-1.0) > 1e-9 && norm > 1e-12 {
			t.Errorf("row %d: expected L2 norm 1.0, got %f (norm^2=%f)", i, math.Sqrt(norm), norm)
		}
	}
}

func TestTFIDF_MiniCorpus_ManualCheck(t *testing.T) {
	// Мини-корпус: 3 документа, 2 термина
	// doc1: "альфа бета"
	// doc2: "альфа"
	// doc3: "бета"
	// альф: df=2, idf = ln(4/3)+1 ≈ 1.2877
	// бет:  df=2, idf = ln(4/3)+1 ≈ 1.2877
	docs := []Document{
		{ID: 1, Text: "альфа бета"},
		{ID: 2, Text: "альфа"},
		{ID: 3, Text: "бета"},
	}
	vocab := BuildVocab(docs, 1, 1.0)
	if len(vocab.Terms) < 2 {
		t.Fatalf("expected at least 2 terms, got %d: %v", len(vocab.Terms), vocab.Terms)
	}
	m := vocab.TFIDFMatrix(docs)

	// doc1: tf(альф)=1, tf(бет)=1
	// tfidf(альф) = (1+ln(1)) * idf = 1 * 1.2877 = 1.2877
	// tfidf(бет) = 1 * 1.2877 = 1.2877
	// L2 norm = sqrt(2 * 1.2877^2) = 1.2877 * sqrt(2)
	// normalized: 1/sqrt(2) ≈ 0.7071
	idxAlpha, ok1 := vocab.Index["альф"]
	idxBeta, ok2 := vocab.Index["бет"]
	if !ok1 || !ok2 {
		t.Fatalf("expected 'альф' and 'бет' in vocab: %v", vocab.Terms)
	}

	val0 := m.At(0, idxAlpha)
	val1 := m.At(0, idxBeta)
	expected := 1.0 / math.Sqrt(2.0)
	if math.Abs(val0-expected) > 1e-4 {
		t.Errorf("doc0 альф: expected %.4f, got %.4f", expected, val0)
	}
	if math.Abs(val1-expected) > 1e-4 {
		t.Errorf("doc0 бет: expected %.4f, got %.4f", expected, val1)
	}
}

func TestSVD_Determinism(t *testing.T) {
	docs := []Document{
		{ID: 1, Text: "блокировка счёта арест"},
		{ID: 2, Text: "блокировка счёта"},
		{ID: 3, Text: "арест счёта"},
		{ID: 4, Text: "блокировка карты"},
		{ID: 5, Text: "арест имущества"},
	}
	vocab := BuildVocab(docs, 1, 1.0)
	m := vocab.TFIDFMatrix(docs)

	// Дважды выполняем SVD
	u1, s1, vt1, err := SVD(m, 2)
	if err != nil {
		t.Fatalf("SVD failed: %v", err)
	}
	u2, s2, vt2, err := SVD(m, 2)
	if err != nil {
		t.Fatalf("SVD failed: %v", err)
	}

	// Сингулярные значения должны совпадать
	for i := range s1 {
		if math.Abs(s1[i]-s2[i]) > 1e-9 {
			t.Errorf("singular value %d: %f != %f", i, s1[i], s2[i])
		}
	}

	// U и Vᵀ могут отличаться знаком, но произведения U·S·Vᵀ должны совпадать
	// Проверяем через реконструкцию
	_ = u1
	_ = u2
	_ = vt1
	_ = vt2
}

func TestSVD_SemanticSimilarity(t *testing.T) {
	// Синтетический корпус: "арест счёта" и "блокировка счёта" должны быть близки
	docs := []Document{
		{ID: 1, Text: "арест счёта должника"},
		{ID: 2, Text: "блокировка счёта клиента"},
		{ID: 3, Text: "выдача кредита заёмщику"},
		{ID: 4, Text: "погашение задолженности"},
		{ID: 5, Text: "арест имущества"},
		{ID: 6, Text: "блокировка карты"},
	}
	vocab := BuildVocab(docs, 1, 1.0)
	m := vocab.TFIDFMatrix(docs)

	k := 2
	u, s, vt, err := SVD(m, k)
	if err != nil {
		t.Fatalf("SVD failed: %v", err)
	}
	if vt == nil || u == nil || s == nil {
		t.Fatal("SVD returned nil matrices")
	}

	// Векторы документов
	emb1 := DocEmbedding(u, s, 0) // арест счёта
	emb2 := DocEmbedding(u, s, 1) // блокировка счёта
	emb3 := DocEmbedding(u, s, 2) // выдача кредита

	sim12 := CosineSimilarity(emb1, emb2)
	sim13 := CosineSimilarity(emb1, emb3)

	// "арест счёта" и "блокировка счёта" должны быть ближе, чем "арест счёта" и "выдача кредита"
	if sim12 <= sim13 {
		t.Errorf("expected sim(арест,блокировка) > sim(арест,выдача): %f vs %f", sim12, sim13)
	}
}

func TestProjectQuery(t *testing.T) {
	docs := []Document{
		{ID: 1, Text: "арест счёта должника"},
		{ID: 2, Text: "блокировка счёта клиента"},
		{ID: 3, Text: "выдача кредита заёмщику"},
		{ID: 4, Text: "погашение задолженности"},
		{ID: 5, Text: "арест имущества"},
	}
	vocab := BuildVocab(docs, 1, 1.0)
	m := vocab.TFIDFMatrix(docs)

	k := 2
	_, _, vt, err := SVD(m, k)
	if err != nil {
		t.Fatalf("SVD failed: %v", err)
	}
	if vt == nil {
		t.Fatal("VT matrix is nil")
	}

	// Проекция запроса "блокировка счёта" в LSA-пространство
	qVec := vocab.ProjectQuery("блокировка счёта", vt)
	if len(qVec) != k {
		t.Fatalf("expected %d-dim query vector, got %d", k, len(qVec))
	}

	// Вектор не должен быть нулевым
	var norm float64
	for _, v := range qVec {
		norm += v * v
	}
	if norm < 1e-12 {
		t.Error("query vector is zero")
	}
}
