package telemetry

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Classify reduces an error to a bounded AttrOutcome value. Every metric that
// reports success or failure goes through this rather than deriving an
// attribute from the error itself, which is what keeps the attribute bounded
// as the error types underneath it change.
//
// A canceled context is deliberately not an error here: background loops shut
// down by cancellation, and a shutdown that increments an error count teaches
// operators to ignore the error count.
func Classify(err error) string {
	switch {
	case err == nil:
		return OutcomeOK
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return OutcomeCanceled
	default:
		return OutcomeError
	}
}

// EndSpan closes span with the status err warrants. A canceled context leaves
// the span Unset rather than Error for the same reason Classify does: it is
// the caller leaving, not a fault, and marking it Error makes every shutdown
// look like an incident in a trace backend's error rate.
func EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		if Classify(err) == OutcomeError {
			span.SetStatus(codes.Error, err.Error())
		}
	}
	span.End()
}
