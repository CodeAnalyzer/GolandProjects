package dsinfo

import "testing"

func TestNormalizeProduct(t *testing.T) {
	for _, tc := range []struct {
		name        string
		rawName     string
		packetRef   string
		wantProduct string
		wantBranch  string
		wantHotfix  string
	}{
		{
			name:        "hotfix ref",
			rawName:     "FA-Contracts-11-01-7",
			packetRef:   "FA-Contracts-11-01-7-HF008-25110701",
			wantProduct: "fa-contracts",
			wantBranch:  "fa-contracts-11-01-7",
			wantHotfix:  "FA-Contracts-11-01-7-HF008-25110701",
		},
		{
			name:        "pkg suffix collapsed",
			rawName:     "FA-Contracts-Ext-9-01-553-PKG-IFRS9_TOYOTA",
			packetRef:   "FA-Contracts-Ext-9-01-553-PKG-IFRS9_TOYOTA-901553",
			wantProduct: "fa-contracts-ext",
			wantBranch:  "fa-contracts-ext-9-01-553",
			wantHotfix:  "",
		},
		{
			name:        "composite version collapses to branch",
			rawName:     "FA-Contracts-Ext-7-02-2-7-02-2",
			packetRef:   "",
			wantProduct: "fa-contracts-ext",
			wantBranch:  "fa-contracts-ext-7-02-2",
			wantHotfix:  "",
		},
		{
			name:        "sp6 suffix",
			rawName:     "FA-UserProc-7-02-06-SP6",
			packetRef:   "",
			wantProduct: "fa-userproc",
			wantBranch:  "fa-userproc-7-02-06",
			wantHotfix:  "",
		},
		{
			name:        "sp suffix",
			rawName:     "FA-FinancialAsset-Ext-8-03-33-SP",
			packetRef:   "FA-FinancialAsset-Ext-8-03-33-SP-80333",
			wantProduct: "fa-financialasset-ext",
			wantBranch:  "fa-financialasset-ext-8-03-33",
			wantHotfix:  "",
		},
		{
			name:        "plain product without ref",
			rawName:     "FA-Administrator-11-01-7",
			packetRef:   "",
			wantProduct: "fa-administrator",
			wantBranch:  "fa-administrator-11-01-7",
			wantHotfix:  "",
		},
		{
			name:        "no numeric tokens",
			rawName:     "FM-Utils",
			packetRef:   "",
			wantProduct: "fm-utils",
			wantBranch:  "fm-utils",
			wantHotfix:  "",
		},
		{
			name:        "leading zeros preserved",
			rawName:     "FA-Administrator-Ext-8-01-6-PKG-toyota-repltask-term",
			packetRef:   "FA-Administrator-Ext-8-01-6-PKG-toyota-repltask-term-1-26013002",
			wantProduct: "fa-administrator-ext",
			wantBranch:  "fa-administrator-ext-8-01-6",
			wantHotfix:  "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotProduct, gotBranch, gotHotfix := NormalizeProduct(tc.rawName, tc.packetRef)
			if gotProduct != tc.wantProduct || gotBranch != tc.wantBranch || gotHotfix != tc.wantHotfix {
				t.Fatalf("NormalizeProduct(%q, %q) = (%q, %q, %q), want (%q, %q, %q)",
					tc.rawName, tc.packetRef, gotProduct, gotBranch, gotHotfix,
					tc.wantProduct, tc.wantBranch, tc.wantHotfix)
			}
		})
	}
}
