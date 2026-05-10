package store

import "testing"

func TestAPILookupKeyBuilders(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "business object table",
			got:  BuildAPIBusinessObjectTableLookupKey(" CreditBO ", " TAccount "),
			want: "creditbo|taccount",
		},
		{
			name: "business object table index",
			got:  BuildAPIBusinessObjectTableIndexLookupKey(" CreditBO ", " TAccount ", " XAK1 "),
			want: "creditbo|taccount|xak1",
		},
		{
			name: "api contract",
			got:  BuildAPIContractLookupKey(" GetCredit ", " SERVICE "),
			want: "getcredit|service",
		},
		{
			name: "api contract table",
			got:  BuildAPIContractTableLookupKey(" INPUT ", " TAccount "),
			want: "input|taccount",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("lookup key = %q, want %q", tt.got, tt.want)
			}
		})
	}
}
