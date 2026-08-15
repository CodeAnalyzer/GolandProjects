package rti

import (
	"testing"
)

func TestBatchSizeDefault(t *testing.T) {
	if rtiBatchSize != 50000 {
		t.Errorf("rtiBatchSize = %d, expected 50000", rtiBatchSize)
	}
}

func TestSetBatchSize(t *testing.T) {
	original := rtiBatchSize
	defer func() { rtiBatchSize = original }()

	SetBatchSize(10000)
	if rtiBatchSize != 10000 {
		t.Errorf("after SetBatchSize(10000): rtiBatchSize = %d, expected 10000", rtiBatchSize)
	}

	SetBatchSize(0)
	if rtiBatchSize != 10000 {
		t.Errorf("SetBatchSize(0) should not change value: rtiBatchSize = %d, expected 10000", rtiBatchSize)
	}
}
