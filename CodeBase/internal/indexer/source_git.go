package indexer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/codebase/internal/branchkey"
)

// GitSource — результат git-детекции каталога-источника индексации.
type GitSource struct {
	// Product — каноническое имя продукта: basename URL remote origin
	// (без .git), lowercase. Пример: .../fa-contracts.git → fa-contracts.
	Product string
	// BranchName — сырое имя git-ветки HEAD; для detached HEAD —
	// describe --tags --always (или короткий SHA).
	BranchName string
	// BranchKey — нормализованный ключ линии (общая нормализация с
	// манифестом dsinfo: internal/branchkey).
	BranchKey string
	// CommitSHA — полный SHA HEAD на момент детекции.
	CommitSHA string
}

// gitDetector — детекция git-источников с кэшем на время скана: один набор
// вызовов git CLI на каталог.
type gitDetector struct {
	binary string
	mu     sync.Mutex
	cache  map[string]*GitSource
}

func newGitDetector() *gitDetector {
	return &gitDetector{
		binary: "git",
		cache:  make(map[string]*GitSource),
	}
}

// Detect определяет источник по git-состоянию каталога. Возвращает
// (nil, nil), если каталог — не git-источник (нет .git или нет remote
// origin): вызывающая сторона обрабатывает его прежней эвристикой по пути.
// Ошибка означает недоступность git CLI — источник пропускается с warning.
func (d *gitDetector) Detect(ctx context.Context, dir string) (*GitSource, error) {
	d.mu.Lock()
	if src, ok := d.cache[dir]; ok {
		d.mu.Unlock()
		return src, nil
	}
	d.mu.Unlock()

	src, err := d.detect(ctx, dir)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	d.cache[dir] = src
	d.mu.Unlock()
	return src, nil
}

func (d *gitDetector) detect(ctx context.Context, dir string) (*GitSource, error) {
	// .git может быть каталогом (обычный clone) или файлом (worktree) —
	// обе формы делают каталог git-источником; чтение .git/HEAD напрямую
	// в worktree недостаточно, поэтому дальше только git -C.
	gitPath := filepath.Join(dir, ".git")
	if _, err := os.Stat(gitPath); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("git source stat %s: %w", gitPath, err)
	}

	remote, err := d.output(ctx, dir, "remote", "get-url", "origin")
	if err != nil {
		// Недоступный git CLI — явная ошибка (требуется warning вызывающего);
		// ненулевой exit самого git при существующем .git — нет remote:
		// прежняя эвристика по пути.
		var execErr *exec.Error
		if errors.As(err, &execErr) {
			return nil, err
		}
		return nil, nil
	}
	product := productFromRemoteURL(remote)
	if product == "" {
		return nil, nil
	}

	branch, err := d.output(ctx, dir, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		// unborn HEAD (репозиторий без коммитов) или повреждённый .git —
		// не источник: файлов с привязкой к линии быть не может.
		return nil, nil
	}
	branchName := strings.TrimSpace(branch)
	if branchName == "" || branchName == "HEAD" {
		// detached HEAD: describe даёт тег или уникальный префикс SHA,
		// при неудаче (нет тегов/коммитов) — короткий SHA.
		described, descErr := d.output(ctx, dir, "describe", "--tags", "--always")
		if descErr == nil && strings.TrimSpace(described) != "" {
			branchName = strings.TrimSpace(described)
		} else {
			short, shortErr := d.output(ctx, dir, "rev-parse", "--short", "HEAD")
			if shortErr != nil {
				return nil, nil
			}
			branchName = strings.TrimSpace(short)
		}
	}

	commit := ""
	if sha, err := d.output(ctx, dir, "rev-parse", "HEAD"); err == nil {
		commit = strings.TrimSpace(sha)
	}

	return &GitSource{
		Product:    product,
		BranchName: branchName,
		BranchKey:  branchkey.NormalizeBranchKey(branchName),
		CommitSHA:  commit,
	}, nil
}

// output выполняет git -C dir и возвращает stdout без хвостового перевода
// строки. Ошибка — как от exec (включая недоступный бинарник), так и от
// самого git (ненулевой exit).
func (d *gitDetector) output(ctx context.Context, dir string, args ...string) (string, error) {
	full := append([]string{"-C", dir}, args...)
	cmd := exec.CommandContext(ctx, d.binary, full...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s in %s: %w: %s", strings.Join(args, " "), dir, err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

// productFromRemoteURL извлекает каноническое имя продукта из URL remote:
// последний сегмент пути без суффикса .git, lowercase. Поддерживает
// scp-подобную форму (git@host:repo.git) и обычные URL/локальные пути.
func productFromRemoteURL(url string) string {
	name := strings.TrimSpace(url)
	name = strings.TrimSuffix(name, "/")
	name = strings.TrimSuffix(name, ".git")
	if idx := strings.LastIndexAny(name, `/\:`); idx >= 0 && idx < len(name)-1 {
		name = name[idx+1:]
	}
	name = strings.ToLower(strings.TrimSpace(name))
	// Мусорные/неполные URL (git@host:, https://) дают «имя» с хвостом
	// схемы/учётки — такие источники не определяются.
	if name == "" || strings.ContainsAny(name, " \t@:/\\") {
		return ""
	}
	return name
}
