package reviewsvc

import (
	"fmt"
	"testing"

	"github.com/codebase/internal/errs"
)

func TestClassifyError_InvalidArguments(t *testing.T) {
	err := fmt.Errorf("%w: file list is empty: list.txt", errs.ErrInvalidArguments)
	if got := ClassifyError(err); got != "invalid_arguments" {
		t.Fatalf("ClassifyError = %q, want invalid_arguments", got)
	}
}

func TestClassifyError_ReviewFailed(t *testing.T) {
	if got := ClassifyError(fmt.Errorf("%w: parse failed", errs.ErrReviewFailed)); got != "review_failed" {
		t.Fatalf("ClassifyError = %q, want review_failed", got)
	}
}
