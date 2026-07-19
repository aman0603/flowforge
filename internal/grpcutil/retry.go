package grpcutil

import (
	"context"
	"time"

	"github.com/aman0603/flowforge/internal/proto/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryableCodes are gRPC status codes that warrant a client retry because they
// indicate a transient, recoverable condition (e.g. the upstream service is
// temporarily unavailable or overloaded).
var RetryableCodes = map[codes.Code]bool{
	codes.Unavailable:       true,
	codes.ResourceExhausted: true,
	codes.DeadlineExceeded:  true,
	codes.Internal:          true,
	codes.Aborted:           true,
}

// CallOptions configures the resilience behavior of Call.
type CallOptions struct {
	// MaxAttempts is the total number of tries (including the first). 0 or 1
	// disables retries.
	MaxAttempts int
	// BaseDelay is the initial backoff; it doubles each attempt, capped.
	BaseDelay time.Duration
	// RequestTimeout bounds a single RPC attempt. Zero means no per-attempt
	// deadline (the caller's context still applies).
	RequestTimeout time.Duration
}

// DefaultCallOptions returns sensible defaults aligned with the service config.
func DefaultCallOptions() CallOptions {
	return CallOptions{
		MaxAttempts:    3,
		BaseDelay:      50 * time.Millisecond,
		RequestTimeout: 5 * time.Second,
	}
}

// Call runs fn with resilience: a per-attempt deadline derived from
// RequestTimeout, and retry with exponential backoff for retryable gRPC status
// codes. The final error (if any) is returned as-is so callers can map it.
func Call(ctx context.Context, opts CallOptions, fn func(ctx context.Context) error) error {
	if opts.MaxAttempts <= 1 {
		return runOnce(ctx, opts.RequestTimeout, fn)
	}

	var lastErr error
	delay := opts.BaseDelay
	if delay <= 0 {
		delay = 50 * time.Millisecond
	}

	for attempt := 1; attempt <= opts.MaxAttempts; attempt++ {
		err := runOnce(ctx, opts.RequestTimeout, fn)
		if err == nil {
			return nil
		}
		lastErr = err

		if !isRetryable(err) {
			return err
		}
		if attempt == opts.MaxAttempts {
			return err
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay *= 2
		if delay > 2*time.Second {
			delay = 2 * time.Second
		}
	}
	return lastErr
}

func runOnce(parent context.Context, timeout time.Duration, fn func(ctx context.Context) error) error {
	if timeout <= 0 {
		return fn(parent)
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return fn(ctx)
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if st, ok := status.FromError(err); ok {
		return RetryableCodes[st.Code()]
	}
	return false
}

// ErrorCodeFromStatus maps a gRPC status error to the protobuf ErrorCode used
// by internal service contracts, so servers and clients share a consistent
// retryability/severity classification.
func ErrorCodeFromStatus(err error) common.ErrorCode {
	if err == nil {
		return common.ErrorCode_ERROR_CODE_UNSPECIFIED
	}
	st, ok := status.FromError(err)
	if !ok {
		return common.ErrorCode_ERROR_CODE_INTERNAL
	}
	switch st.Code() {
	case codes.InvalidArgument:
		return common.ErrorCode_ERROR_CODE_VALIDATION
	case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded, codes.Aborted:
		return common.ErrorCode_ERROR_CODE_RETRYABLE
	case codes.Internal:
		return common.ErrorCode_ERROR_CODE_INTERNAL
	case codes.NotFound, codes.AlreadyExists, codes.PermissionDenied, codes.FailedPrecondition:
		return common.ErrorCode_ERROR_CODE_PERMANENT
	default:
		return common.ErrorCode_ERROR_CODE_INTERNAL
	}
}

// WithGRPCDefaults returns dial options that include default message size
// limits, preventing a single large payload from exhausting memory.
func WithGRPCDefaults() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(16<<20),
			grpc.MaxCallSendMsgSize(16<<20),
		),
	}
}
