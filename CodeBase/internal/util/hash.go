package util

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// FileHash вычисляет SHA-256 содержимого файла и возвращает его в hex (64 символа).
// Читает файл одним последовательным проходом, не загружая его в память целиком.
func FileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("util: open %s: %w", path, err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("util: read %s: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
