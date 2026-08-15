package trc

import (
	"testing"
)

func TestBatchSizeDefault(t *testing.T) {
	if trcBatchSize != 50000 {
		t.Errorf("trcBatchSize = %d, expected 50000", trcBatchSize)
	}
}

func TestSetBatchSize(t *testing.T) {
	original := trcBatchSize
	defer func() { trcBatchSize = original }()

	SetBatchSize(10000)
	if trcBatchSize != 10000 {
		t.Errorf("after SetBatchSize(10000): trcBatchSize = %d, expected 10000", trcBatchSize)
	}

	SetBatchSize(0)
	if trcBatchSize != 10000 {
		t.Errorf("SetBatchSize(0) should not change value: trcBatchSize = %d, expected 10000", trcBatchSize)
	}
}
