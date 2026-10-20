package store

import (
	"errors"

	"github.com/lib/pq"
)

// IsUniqueViolation сообщает, является ли ошибка (включая обёрнутые) нарушением
// unique-ограничения (SQLSTATE 23505). Используется для разрешения гонки
// параллельных parse одного файла при простановке file_hash сессии.
func IsUniqueViolation(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505"
}
