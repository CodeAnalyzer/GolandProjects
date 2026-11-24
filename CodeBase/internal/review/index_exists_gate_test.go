package review

import (
	"context"
	"testing"
)

// TestIndexExistsInDB_CreatorGate: гейт проверяемости по создающему источнику
// (change table-existence-evidence, design D4). Негативная запись кэша
// создателей подавляет finding по хинту и по M_DELETE_PTABLE_INDEX;
// положительная — проверка выполняется; пустой кэш без БД — гейт инертен
// (существующие unit-тесты правила без изменений).
func TestIndexExistsInDB_CreatorGate(t *testing.T) {
	const hintContent = "select * from pT t M_NOLOCK_INDEX(XPK_missing) where t.SPID = 1\n"
	const macroContent = "M_DELETE_PTABLE_INDEX(pT, XPK_missing)\n"
	cases := []struct {
		name     string
		content  string
		creators map[string]bool
		want     int
	}{
		{name: "no creator + hint: suppressed", content: hintContent, creators: map[string]bool{"pt": false}, want: 0},
		{name: "no creator + M_DELETE_PTABLE_INDEX: suppressed", content: macroContent, creators: map[string]bool{"pt": false}, want: 0},
		{name: "creator + missing index: finding", content: hintContent, creators: map[string]bool{"pt": true}, want: 1},
		{name: "creator + missing index via macro: finding", content: macroContent, creators: map[string]bool{"pt": true}, want: 1},
		{name: "empty cache, db == nil: gate inert", content: hintContent, creators: map[string]bool{}, want: 1},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			file := newRuleTestFile(t, tc.content)
			r := &Runner{tableCreatorCache: tc.creators}
			r.exec = newRuleTestExec(file.Path, tc.content)
			r.indexCandCache = map[string][]tableIndexCandidate{"pt": {{Name: "XPKpT", Fields: []string{"spid"}}}}

			findings, err := r.checkIndexExistsInDB(context.Background(), file)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(findings) != tc.want {
				t.Fatalf("findings count = %d, want %d: %+v", len(findings), tc.want, findings)
			}
			for _, f := range findings {
				if f.Rule != RuleIndexExistsInDB {
					t.Fatalf("finding rule = %q, want %q", f.Rule, RuleIndexExistsInDB)
				}
			}
		})
	}
}

// TestIndexCheckVerifiable: порядок разрешения гейта — запись кэша (включая
// false) сильнее отсутствия БД; промах по непустому кэшу без БД инертен.
func TestIndexCheckVerifiable(t *testing.T) {
	r := &Runner{tableCreatorCache: map[string]bool{"tcreated": true, "tnocreator": false}}
	if !r.indexCheckVerifiable(context.Background(), "TCreated") {
		t.Fatal("tcreated: want true (создатель есть)")
	}
	if r.indexCheckVerifiable(context.Background(), "tNoCreator") {
		t.Fatal("tnocreator: want false (негативная запись кэша)")
	}
	if !r.indexCheckVerifiable(context.Background(), "tNotInCache") {
		t.Fatal("tnotincache: want true (промах без БД — гейт инертен)")
	}
	if !r.indexCheckVerifiable(context.Background(), "") {
		t.Fatal("empty name: want true (гейт неприменим)")
	}
}
