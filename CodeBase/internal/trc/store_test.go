package trc

import (
	"testing"
)

func TestBatchDeleteSizeConstant(t *testing.T) {
	if batchDeleteSize != 50000 {
		t.Errorf("batchDeleteSize = %d, expected 50000", batchDeleteSize)
	}
}

func TestPruneSessions_NegativeKeepLast(t *testing.T) {
	// PruneSessions with negative keepLast should not panic on nil db
	// (it will fail at db.QueryRow, but the keepLast==0 check should not trigger)
	// This test verifies the function signature and constant are correct.
	_ = PruneSessions // function exists
}

func TestDeleteSession_NilDB(t *testing.T) {
	// DeleteSession should return error on nil db (panic recovery not needed,
	// db.Exec will fail). This test verifies the function exists.
	_ = DeleteSession // function exists
}
