package dsinfo

import "github.com/codebase/internal/branchkey"

// NormalizeProduct делегирует общему пакету branchkey — единому источнику
// нормализации имён продуктов и линий (используется и индексатором git-веток).
func NormalizeProduct(rawName, packetRef string) (productName, branchKey, hotfixName string) {
	return branchkey.NormalizeProduct(rawName, packetRef)
}
