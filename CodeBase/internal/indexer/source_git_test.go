package indexer

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func gitAvailable() bool {
	_, err := exec.LookPath("git")
	return err == nil
}

// initGitRepo создаёт временный git-репозиторий с одним коммитом на ветке
// branch и remote origin указанного URL.
func initGitRepo(t *testing.T, branch, remoteURL string) string {
	t.Helper()
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		args = append([]string{"-c", "user.email=test@example.com", "-c", "user.name=test", "-c", "init.defaultBranch=" + branch}, args...)
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		var stderr strings.Builder
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, stderr.String())
		}
	}
	run("init")
	if remoteURL != "" {
		run("remote", "add", "origin", remoteURL)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("# repo\n"), 0o644); err != nil {
		t.Fatalf("write readme: %v", err)
	}
	run("add", ".")
	run("commit", "-m", "init")
	return dir
}

func TestProductFromRemoteURL(t *testing.T) {
	cases := []struct{ url, want string }{
		{"git@github.com:Diasoft/fa-contracts.git", "fa-contracts"},
		{"https://github.com/Diasoft/fa-contracts.git", "fa-contracts"},
		{"https://github.com/Diasoft/fa-contracts", "fa-contracts"},
		{"ssh://git@host:22/srv/fa-contracts.git", "fa-contracts"},
		{"D:/repos/fa-contracts.git", "fa-contracts"},
		{"D:\\repos\\fa-contracts.git", "fa-contracts"},
		{"git@host:fa-contracts-ext.git/", "fa-contracts-ext"},
		{"git@host:", ""},
		{"", ""},
	}
	for _, tc := range cases {
		if got := productFromRemoteURL(tc.url); got != tc.want {
			t.Errorf("productFromRemoteURL(%q) = %q, want %q", tc.url, got, tc.want)
		}
	}
}

func TestDetectSource_PlainRepo(t *testing.T) {
	if !gitAvailable() {
		t.Skip("git is not in PATH")
	}
	detector := newGitDetector()
	dir := initGitRepo(t, "FA-Contracts-11-01-7-SP", "git@github.com:Diasoft/fa-contracts.git")

	src, err := detector.Detect(context.Background(), dir)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if src == nil {
		t.Fatal("Detect returned nil source for plain git repo")
	}
	if src.Product != "fa-contracts" {
		t.Fatalf("Product = %q, want fa-contracts", src.Product)
	}
	if src.BranchName != "FA-Contracts-11-01-7-SP" {
		t.Fatalf("BranchName = %q, want FA-Contracts-11-01-7-SP", src.BranchName)
	}
	if src.BranchKey != "fa-contracts-11-01-7" {
		t.Fatalf("BranchKey = %q, want fa-contracts-11-01-7", src.BranchKey)
	}
	if len(src.CommitSHA) < 7 {
		t.Fatalf("CommitSHA = %q, want non-empty sha", src.CommitSHA)
	}

	// Кэш: повторная детекция возвращает тот же результат без вызова git.
	again, err := detector.Detect(context.Background(), dir)
	if err != nil {
		t.Fatalf("Detect cached: %v", err)
	}
	if again != src {
		t.Fatalf("cached Detect returned different result: %v vs %v", again, src)
	}
}

func TestDetectSource_Worktree(t *testing.T) {
	if !gitAvailable() {
		t.Skip("git is not in PATH")
	}
	main := initGitRepo(t, "develop", "https://github.com/Diasoft/fa-contracts.git")
	root := t.TempDir()
	wtDir := filepath.Join(root, "fa-contracts-11016")

	cmd := exec.Command("git", "-C", main, "worktree", "add", "-b", "FA-Contracts-11-01-6-SP", wtDir)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("git worktree add: %v: %s", err, stderr.String())
	}

	src, err := newGitDetector().Detect(context.Background(), wtDir)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if src == nil {
		t.Fatal("Detect returned nil source for worktree (.git file)")
	}
	if src.Product != "fa-contracts" {
		t.Fatalf("Product = %q, want fa-contracts", src.Product)
	}
	if src.BranchName != "FA-Contracts-11-01-6-SP" {
		t.Fatalf("BranchName = %q, want FA-Contracts-11-01-6-SP", src.BranchName)
	}
	if src.BranchKey != "fa-contracts-11-01-6" {
		t.Fatalf("BranchKey = %q, want fa-contracts-11-01-6", src.BranchKey)
	}
}

func TestDetectSource_DetachedHead(t *testing.T) {
	if !gitAvailable() {
		t.Skip("git is not in PATH")
	}
	dir := initGitRepo(t, "develop", "git@github.com:Diasoft/fa-payments.git")
	cmd := exec.Command("git", "-C", dir, "checkout", "--detach")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("git checkout --detach: %v: %s", err, stderr.String())
	}

	src, err := newGitDetector().Detect(context.Background(), dir)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if src == nil {
		t.Fatal("Detect returned nil source for detached HEAD repo")
	}
	if src.Product != "fa-payments" {
		t.Fatalf("Product = %q, want fa-payments", src.Product)
	}
	// describe --always без тегов даёт короткий SHA; главное — не пусто и не "HEAD".
	if src.BranchName == "" || src.BranchName == "HEAD" {
		t.Fatalf("BranchName = %q, want describe/short sha", src.BranchName)
	}
}

func TestDetectSource_NoRemote(t *testing.T) {
	if !gitAvailable() {
		t.Skip("git is not in PATH")
	}
	dir := initGitRepo(t, "develop", "")

	src, err := newGitDetector().Detect(context.Background(), dir)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if src != nil {
		t.Fatalf("Detect returned source for repo without remote: %+v", src)
	}
}

func TestDetectSource_NotGitDir(t *testing.T) {
	dir := t.TempDir()
	src, err := newGitDetector().Detect(context.Background(), dir)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if src != nil {
		t.Fatalf("Detect returned source for non-git dir: %+v", src)
	}
}

func TestDetectSource_GitNotAvailable(t *testing.T) {
	dir := initGitRepo(t, "develop", "git@github.com:Diasoft/fa-contracts.git")
	detector := newGitDetector()
	detector.binary = "definitely-missing-git-binary-xyz"

	if _, err := detector.Detect(context.Background(), dir); err == nil {
		t.Fatal("Detect with missing git binary must return explicit error")
	}
}
