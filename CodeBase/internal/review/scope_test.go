package review

import (
	"reflect"
	"testing"

	"github.com/codebase/internal/store"
)

func branchInfo(productID, branchID int64, key string, isDefault bool) store.ProductBranchInfo {
	return store.ProductBranchInfo{ProductID: productID, BranchID: branchID, BranchKey: key, IsDefault: isDefault}
}

func assertArea(t *testing.T, got map[int64][]int64, productID int64, want []int64) {
	t.Helper()
	if !reflect.DeepEqual(got[productID], want) {
		t.Fatalf("область продукта %d = %v, want %v", productID, got[productID], want)
	}
}

// TestBuildProductBranchAreas_OwnBranchVariants: свой продукт — ветка файла
// авторитетна, профиль не переопределяет (спека review-scope, «Область
// видимости review по продукту и ветке» + «Область видимости EXT-продуктов»).
func TestBuildProductBranchAreas_OwnBranchVariants(t *testing.T) {
	const (
		own       = int64(1)
		devBranch = int64(10)
		spBranch  = int64(11)
		otherSP   = int64(12)
	)
	branches := []store.ProductBranchInfo{
		branchInfo(own, devBranch, "develop", true),
		branchInfo(own, spBranch, "fa-contracts-11-01-7", false),
		branchInfo(own, otherSP, "fa-contracts-11-01-6", false),
	}

	// develop-файл → область {develop}
	got := buildProductBranchAreas(own, devBranch, []int64{own}, branches, nil)
	assertArea(t, got, own, []int64{devBranch})

	// релизный файл → область {fa-contracts-11-01-7}
	got = buildProductBranchAreas(own, spBranch, []int64{own}, branches, nil)
	assertArea(t, got, own, []int64{spBranch})

	// файл без ветки → product-scope {0}
	got = buildProductBranchAreas(own, 0, []int64{own}, branches, nil)
	assertArea(t, got, own, []int64{0})

	// профиль с другой линией своего продукта не переопределяет ветку файла
	got = buildProductBranchAreas(own, spBranch, []int64{own}, branches, map[int64]int64{own: otherSP})
	assertArea(t, got, own, []int64{spBranch})
}

// TestBuildProductBranchAreas_OwnEXT: EXT-продукт — объединение поставки
// банка: prj-файл → {prj, develop}; develop-файл → {develop} ∪ prj.
func TestBuildProductBranchAreas_OwnEXT(t *testing.T) {
	const (
		ext       = int64(1)
		devBranch = int64(10)
		prjA      = int64(20)
		prjB      = int64(21)
	)
	branches := []store.ProductBranchInfo{
		branchInfo(ext, devBranch, "develop", true),
		branchInfo(ext, prjA, "prj/novikom-persform", false),
		branchInfo(ext, prjB, "prj/bank-b", false),
	}

	got := buildProductBranchAreas(ext, prjA, []int64{ext}, branches, nil)
	assertArea(t, got, ext, []int64{prjA, devBranch})

	got = buildProductBranchAreas(ext, devBranch, []int64{ext}, branches, nil)
	assertArea(t, got, ext, []int64{devBranch, prjA, prjB})

	// prj-ветка другого банка не входит в область prj-файла
	got = buildProductBranchAreas(ext, prjB, []int64{ext}, branches, nil)
	assertArea(t, got, ext, []int64{prjB, devBranch})
}

// TestBuildProductBranchAreas_ForeignBranchVariants: чужой продукт — линия
// профиля, при отсутствии — default-ветка; чужой EXT — default ∪ prj либо
// default ∪ профильная prj (спека «Разрешение ветки чужого продукта»).
func TestBuildProductBranchAreas_ForeignBranchVariants(t *testing.T) {
	const (
		own       = int64(1)
		ownBranch = int64(10)
		foreign   = int64(2)
		devBranch = int64(20)
		spBranch  = int64(21)
	)
	branches := []store.ProductBranchInfo{
		branchInfo(own, ownBranch, "develop", true),
		branchInfo(foreign, devBranch, "develop", true),
		branchInfo(foreign, spBranch, "sup/11-01-7", false),
	}
	products := []int64{own, foreign}

	// без профиля — default-ветка чужого
	got := buildProductBranchAreas(own, ownBranch, products, branches, nil)
	assertArea(t, got, foreign, []int64{devBranch})

	// профиль с проиндексированной линией — она
	got = buildProductBranchAreas(own, ownBranch, products, branches, map[int64]int64{foreign: spBranch})
	assertArea(t, got, foreign, []int64{spBranch})

	// профиль с непроиндексированной линией (0 в карте не бывает,
	// fallback выполняет ResolveAnalysisContext) — default
	got = buildProductBranchAreas(own, ownBranch, products, branches, map[int64]int64{})
	assertArea(t, got, foreign, []int64{devBranch})
}

// TestBuildProductBranchAreas_ForeignEXT: чужой EXT — default ∪ все prj;
// профильная prj-линия сужает до {prj, default}.
func TestBuildProductBranchAreas_ForeignEXT(t *testing.T) {
	const (
		own       = int64(1)
		ownBranch = int64(10)
		ext       = int64(2)
		devBranch = int64(20)
		prjA      = int64(21)
		prjB      = int64(22)
	)
	branches := []store.ProductBranchInfo{
		branchInfo(own, ownBranch, "develop", true),
		branchInfo(ext, devBranch, "develop", true),
		branchInfo(ext, prjA, "prj/novikom-persform", false),
		branchInfo(ext, prjB, "prj/bank-b", false),
	}
	products := []int64{own, ext}

	// без профиля — default ∪ все prj банка
	got := buildProductBranchAreas(own, ownBranch, products, branches, nil)
	assertArea(t, got, ext, []int64{devBranch, prjA, prjB})

	// профильная prj-линия — {prj, default}
	got = buildProductBranchAreas(own, ownBranch, products, branches, map[int64]int64{ext: prjA})
	assertArea(t, got, ext, []int64{prjA, devBranch})
}

// TestBuildProductBranchAreas_BranchlessProducts: продукт без единой линии
// (свой и чужой) получает product-scope {0}, а не пустую область — иначе
// area-фильтр отсекал бы его файлы и порождал ложные findings.
func TestBuildProductBranchAreas_BranchlessProducts(t *testing.T) {
	const (
		own        = int64(1)
		ownBranch  = int64(10)
		branchless = int64(2)
	)
	branches := []store.ProductBranchInfo{
		branchInfo(own, ownBranch, "develop", true),
	}
	products := []int64{own, branchless}

	got := buildProductBranchAreas(own, ownBranch, products, branches, nil)
	assertArea(t, got, branchless, []int64{0})

	// свой продукт без линий — тоже product-scope
	got = buildProductBranchAreas(own, 0, []int64{own}, nil, nil)
	assertArea(t, got, own, []int64{0})
}

// TestBuildProductBranchAreas_OwnBranchNotInBranchTable: ветка файла есть,
// а строки ds_products_branch для неё нет (ветка удалена после индексации) —
// область {ветка файла}, не пустая.
func TestBuildProductBranchAreas_OwnBranchNotInBranchTable(t *testing.T) {
	const (
		own       = int64(1)
		ownBranch = int64(77)
	)
	got := buildProductBranchAreas(own, ownBranch, []int64{own}, nil, nil)
	assertArea(t, got, own, []int64{ownBranch})
}

// TestIsPrjBranchKey: детект EXT-линий по ключу prj/*.
func TestIsPrjBranchKey(t *testing.T) {
	cases := []struct {
		key  string
		want bool
	}{
		{"prj/novikom-persform", true},
		{"PRJ/Bank-A", true},
		{" prj/x ", true},
		{"develop", false},
		{"sup/11-01-7", false},
		{"prjbank", false},
		{"", false},
	}
	for _, c := range cases {
		if got := isPrjBranchKey(c.key); got != c.want {
			t.Fatalf("isPrjBranchKey(%q) = %v, want %v", c.key, got, c.want)
		}
	}
}

// TestReviewScopePairs: карта областей разворачивается в плоские пары
// unnest; пустая карта — пары пустые (глобальное поведение).
func TestReviewScopePairs(t *testing.T) {
	scope := ReviewScope{
		FileID:    7,
		ProductID: 1,
		BranchID:  10,
		ProductBranch: map[int64][]int64{
			1: {10},
			2: {0},
			3: {30, 31},
		},
	}
	pairs := scope.Pairs()
	if pairs.IsEmpty() {
		t.Fatal("pairs не должны быть пустыми")
	}
	want := store.ProductBranchPairs{
		Products: []int64{1, 2, 3, 3},
		Branches: []int64{10, 0, 30, 31},
	}
	if !reflect.DeepEqual(pairs, want) {
		t.Fatalf("pairs = %+v, want %+v", pairs, want)
	}
	if ls := scope.LookupScope(); ls.ProductID != 1 || ls.BranchID != 10 {
		t.Fatalf("LookupScope = %+v", ls)
	}

	empty := ReviewScope{}
	if !empty.Pairs().IsEmpty() {
		t.Fatal("пустая карта областей должна давать пустые пары")
	}
}
