package util

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileHash_DeterministicAndHex64(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sample.bin")
	content := "CodeBase file hash test\nвторой рядок unicode"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write sample: %v", err)
	}

	h1, err := FileHash(path)
	if err != nil {
		t.Fatalf("FileHash #1: %v", err)
	}
	h2, err := FileHash(path)
	if err != nil {
		t.Fatalf("FileHash #2: %v", err)
	}
	if h1 != h2 {
		t.Fatalf("hash is not deterministic: %s != %s", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("hash length = %d, want 64", len(h1))
	}
	lower := strings.ToLower(h1)
	if lower != h1 {
		t.Fatalf("hash is not lowercase hex: %s", h1)
	}
}

func TestFileHash_DifferentContentDifferentHash(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.bin")
	b := filepath.Join(dir, "b.bin")
	if err := os.WriteFile(a, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}
	ha, err := FileHash(a)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := FileHash(b)
	if err != nil {
		t.Fatal(err)
	}
	if ha == hb {
		t.Fatalf("different content produced identical hash: %s", ha)
	}
}

func TestFileHash_MissingFile(t *testing.T) {
	_, err := FileHash(filepath.Join(t.TempDir(), "no-such-file.bin"))
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}
