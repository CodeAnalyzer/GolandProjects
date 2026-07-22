package rti

import (
	"testing"
)

func TestBatchDeleteSizeConstant(t *testing.T) {
	if batchDeleteSize != 50000 {
		t.Errorf("batchDeleteSize = %d, expected 50000", batchDeleteSize)
	}
}

func TestPruneSessions_FunctionExists(t *testing.T) {
	_ = PruneSessions // function exists
}

func TestDeleteSession_FunctionExists(t *testing.T) {
	_ = DeleteSession // function exists
}
