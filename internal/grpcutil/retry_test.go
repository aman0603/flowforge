package grpcutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCall_SuccessFirstTry(t *testing.T) {
	opts := CallOptions{MaxAttempts: 3, BaseDelay: time.Millisecond, RequestTimeout: time.Second}
	calls := 0
	err := Call(context.Background(), opts, func(ctx context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestCall_RetriesOnUnavailableThenSucceeds(t *testing.T) {
	opts := CallOptions{MaxAttempts: 3, BaseDelay: time.Millisecond, RequestTimeout: time.Second}
	calls := 0
	err := Call(context.Background(), opts, func(ctx context.Context) error {
		calls++
		if calls < 3 {
			return status.Error(codes.Unavailable, "transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestCall_NonRetryableReturnsImmediately(t *testing.T) {
	opts := CallOptions{MaxAttempts: 5, BaseDelay: time.Millisecond, RequestTimeout: time.Second}
	calls := 0
	permanent := status.Error(codes.NotFound, "missing")
	err := Call(context.Background(), opts, func(ctx context.Context) error {
		calls++
		return permanent
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("expected 1 call for non-retryable, got %d", calls)
	}
}

func TestCall_ExhaustsAttempts(t *testing.T) {
	opts := CallOptions{MaxAttempts: 2, BaseDelay: time.Millisecond, RequestTimeout: time.Second}
	calls := 0
	err := Call(context.Background(), opts, func(ctx context.Context) error {
		calls++
		return status.Error(codes.Unavailable, "down")
	})
	if err == nil {
		t.Fatal("expected error after exhaustion")
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
}

func TestCall_RequestTimeoutExceeded(t *testing.T) {
	opts := CallOptions{MaxAttempts: 1, RequestTimeout: 10 * time.Millisecond}
	err := Call(context.Background(), opts, func(ctx context.Context) error {
		select {
		case <-time.After(100 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestErrorCodeFromStatus(t *testing.T) {
	cases := []struct {
		err  error
		want int32
	}{
		{status.Error(codes.InvalidArgument, "x"), int32(3)}, // ERROR_CODE_VALIDATION
		{status.Error(codes.Unavailable, "x"), int32(1)},     // ERROR_CODE_RETRYABLE
		{status.Error(codes.NotFound, "x"), int32(2)},        // ERROR_CODE_PERMANENT
		{status.Error(codes.Internal, "x"), int32(4)},        // ERROR_CODE_INTERNAL
		{nil, int32(0)},                 // UNSPECIFIED
		{errors.New("plain"), int32(4)}, // ERROR_CODE_INTERNAL
	}
	for _, c := range cases {
		if got := int32(ErrorCodeFromStatus(c.err)); got != c.want {
			t.Errorf("ErrorCodeFromStatus(%v) = %d, want %d", c.err, got, c.want)
		}
	}
}
