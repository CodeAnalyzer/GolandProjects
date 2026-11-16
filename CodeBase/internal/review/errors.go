package review

import "errors"

var (
	// ErrFileNotIndexed — путь отсутствует в индексе (files).
	ErrFileNotIndexed = errors.New("file is not indexed")
	// ErrProductNotSet — у проиндексированного файла не заполнен ds_product_id.
	ErrProductNotSet = errors.New("ds_product_id is not set for file")
)

// BatchErrorCode маппит ошибку файла в стабильный код пакетного результата.
func BatchErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrFileNotIndexed):
		return "not_indexed"
	case errors.Is(err, ErrProductNotSet):
		return "missing_product"
	default:
		return "review_failed"
	}
}
