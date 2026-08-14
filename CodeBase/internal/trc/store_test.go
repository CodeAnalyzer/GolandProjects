package trc

import (
	"testing"
)

func TestBatchDeleteSizeConstant(t *testing.T) {
	if batchDeleteSize != 50000 {
		t.Errorf("batchDeleteSize = %d, expected 50000", batchDeleteSize)
	}
}
