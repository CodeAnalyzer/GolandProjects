package indexer

import (
	"testing"

	"github.com/codebase/internal/store"
)

func TestSourceRegistryMatch(t *testing.T) {
	registry := newSourceRegistry()
	registry.set([]sourceEntry{
		{FileSourceBinding: store.FileSourceBinding{Path: `/r/fa-contracts`, ProductID: 1, BranchID: 11}, ProductName: "fa-contracts"},
		{FileSourceBinding: store.FileSourceBinding{Path: `/r/fa-contracts-11017`, ProductID: 1, BranchID: 17}, ProductName: "fa-contracts"},
		{FileSourceBinding: store.FileSourceBinding{Path: `/r/other`, ProductID: 2, BranchID: 22}, ProductName: "other"},
	})

	cases := []struct {
		path     string
		wantPath string // путь ожидаемой привязки; "" → вне источников
	}{
		{`/r/fa-contracts-11017/SOURCE/A.pas`, `/r/fa-contracts-11017`}, // самый длинный префикс
		{`/r/fa-contracts/SOURCE/A.pas`, `/r/fa-contracts`},
		{`/r/fa-contracts/UPLOAD/X.t01`, `/r/fa-contracts`},
		{`/r/fa-contracts-11017extra/A.pas`, ``}, // без разделителя — не префикс
		{`/r/other/lib.js`, `/r/other`},
		{`/r/plain/file.sql`, ``},
		{`/r/fa-contracts-1101`, ``},    // укороченный сегмент не матчится
		{`/r/fa-contracts-11017`, `/r/fa-contracts-11017`}, // сам каталог
	}
	for _, tc := range cases {
		got := registry.match(tc.path)
		if tc.wantPath == "" {
			if got != nil {
				t.Errorf("match(%q) = %q, want nil", tc.path, got.Path)
			}
			continue
		}
		if got == nil {
			t.Errorf("match(%q) = nil, want %q", tc.path, tc.wantPath)
			continue
		}
		if got.Path != tc.wantPath {
			t.Errorf("match(%q) = %q, want %q", tc.path, got.Path, tc.wantPath)
		}
	}

	// Windows-разделители: префикс и путь в одинаковой нотации.
	winRegistry := newSourceRegistry()
	winRegistry.set([]sourceEntry{{FileSourceBinding: store.FileSourceBinding{Path: `D:\FA\fa-contracts-11017`, ProductID: 1, BranchID: 17}, ProductName: "fa-contracts"}})
	if got := winRegistry.match(`D:\FA\fa-contracts-11017\SOURCE\A.pas`); got == nil || got.BranchID != 17 {
		t.Errorf("windows-separator match failed: %+v", got)
	}
	if got := winRegistry.match(`D:\FA\fa-contracts\SOURCE\A.pas`); got != nil {
		t.Errorf("windows non-matching prefix returned binding: %+v", got)
	}
}
