//go:build integration

package dsinfosvc

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/codebase/internal/store/testutil"
)

// TestResolveAnalysisContext_Chain — цепочка приоритетов
// явный id > CODEBASE_DSINFO_ID > активный профиль > отсутствие профиля,
// а также ошибки на некорректный env и несуществующие id.
func TestResolveAnalysisContext_Chain(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	adminProductID, _ := insertProductBranch(t, db, "fa-administrator", "develop", "develop", "/repo/fa-administrator/develop", true)
	insertProductBranch(t, db, "fa-administrator", "FA-Administrator-11-01-7", "fa-administrator-11-01-7", "/repo/fa-administrator/11017", false)
	insertProductBranch(t, db, "fa-contracts-ext", "develop", "develop", "/repo/fa-contracts-ext/develop", true)
	insertProductBranch(t, db, "fa-contracts-ext", "FA-Contracts-Ext-9-01-553", "fa-contracts-ext-9-01-553", "/repo/fa-contracts-ext/901553", false)
	insertProductBranch(t, db, "fa-admintoolkit", "develop", "develop", "/repo/fa-admintoolkit/develop", true)
	insertProductBranch(t, db, "fa-custody", "develop", "develop", "/repo/fa-custody/develop", true)
	insertProductBranch(t, db, "fa-custody", "sup/11-01-7", "sup/11-01-7", "/repo/fa-custody/sup", false)

	first, err := ExecuteParse(ctx, db, ParseParams{FilePath: writeDsInfoFile(t, "Dsinfo1.csv", matchFixture)})
	if err != nil {
		t.Fatalf("parse first: %v", err)
	}
	second, err := ExecuteParse(ctx, db, ParseParams{
		FilePath: writeDsInfoFile(t, "Dsinfo2.csv", strings.Replace(matchFixture, "sql2019_testv", "sql2019_other", 1)),
	})
	if err != nil {
		t.Fatalf("parse second: %v", err)
	}
	if first.ProfileID == second.ProfileID {
		t.Fatal("profiles must differ")
	}

	// 1. Ничего не задано, активного нет.
	got, err := ResolveAnalysisContext(ctx, db, 0)
	if err != nil {
		t.Fatalf("resolve none: %v", err)
	}
	if got.Source != AnalysisSourceNone || got.ProfileID != 0 {
		t.Fatalf("none = %+v", got)
	}

	// 2. Активный профиль.
	if _, err := db.SetDsInfoActiveProfile(ctx, first.ProfileID); err != nil {
		t.Fatalf("set active: %v", err)
	}
	got, err = ResolveAnalysisContext(ctx, db, 0)
	if err != nil {
		t.Fatalf("resolve active: %v", err)
	}
	if got.Source != AnalysisSourceActive || got.ProfileID != first.ProfileID {
		t.Fatalf("active = %+v", got)
	}
	if got.Branches[adminProductID] == 0 {
		t.Fatalf("active branches = %v, want admin %d", got.Branches, adminProductID)
	}
	if len(got.Requested[adminProductID]) != 1 || got.Requested[adminProductID][0] != "fa-administrator-11-01-7" {
		t.Fatalf("active requested = %v", got.Requested)
	}

	// 3. Env перекрывает активный профиль.
	t.Setenv(EnvDsinfoID, strconv.FormatInt(second.ProfileID, 10))
	got, err = ResolveAnalysisContext(ctx, db, 0)
	if err != nil {
		t.Fatalf("resolve env: %v", err)
	}
	if got.Source != AnalysisSourceEnv || got.ProfileID != second.ProfileID {
		t.Fatalf("env = %+v", got)
	}

	// 4. Явный id перекрывает env.
	got, err = ResolveAnalysisContext(ctx, db, first.ProfileID)
	if err != nil {
		t.Fatalf("resolve explicit: %v", err)
	}
	if got.Source != AnalysisSourceExplicit || got.ProfileID != first.ProfileID {
		t.Fatalf("explicit = %+v", got)
	}

	// 5. Некорректный env — ошибка.
	t.Setenv(EnvDsinfoID, "abc")
	if _, err := ResolveAnalysisContext(ctx, db, 0); err == nil {
		t.Fatal("invalid env must fail")
	}

	// 6. Явный id игнорирует некорректный env.
	if _, err := ResolveAnalysisContext(ctx, db, first.ProfileID); err != nil {
		t.Fatalf("explicit with invalid env: %v", err)
	}

	// 7. Несуществующие id — ошибки.
	if _, err := ResolveAnalysisContext(ctx, db, 999999); err == nil {
		t.Fatal("unknown explicit id must fail")
	}
	t.Setenv(EnvDsinfoID, "999999")
	if _, err := ResolveAnalysisContext(ctx, db, 0); err == nil {
		t.Fatal("unknown env id must fail")
	}
}
