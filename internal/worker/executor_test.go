package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aman0603/flowforge/internal/model"
)

func TestSleepExecutor(t *testing.T) {
	executor := NewSleepExecutor()

	tests := []struct {
		name        string
		task        *model.ClaimedTask
		setupCtx    func() (context.Context, context.CancelFunc)
		wantErr     bool
		expectedErr string
	}{
		{
			name: "Valid duration",
			task: &model.ClaimedTask{
				TaskType: "SLEEP",
				Config:   json.RawMessage(`{"duration_ms": 10}`),
			},
			setupCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 100*time.Millisecond)
			},
			wantErr: false,
		},
		{
			name: "Missing duration_ms",
			task: &model.ClaimedTask{
				TaskType: "SLEEP",
				Config:   json.RawMessage(`{}`),
			},
			setupCtx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErr:     true,
			expectedErr: "duration_ms must be positive and non-zero, got 0",
		},
		{
			name: "Zero duration",
			task: &model.ClaimedTask{
				TaskType: "SLEEP",
				Config:   json.RawMessage(`{"duration_ms": 0}`),
			},
			setupCtx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErr:     true,
			expectedErr: "duration_ms must be positive and non-zero, got 0",
		},
		{
			name: "Negative duration",
			task: &model.ClaimedTask{
				TaskType: "SLEEP",
				Config:   json.RawMessage(`{"duration_ms": -50}`),
			},
			setupCtx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErr:     true,
			expectedErr: "duration_ms must be positive and non-zero, got -50",
		},
		{
			name: "Malformed JSON",
			task: &model.ClaimedTask{
				TaskType: "SLEEP",
				Config:   json.RawMessage(`{"duration_ms": invalid}`),
			},
			setupCtx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErr:     true,
			expectedErr: "malformed JSON config",
		},
		{
			name: "Context already cancelled before execution",
			task: &model.ClaimedTask{
				TaskType: "SLEEP",
				Config:   json.RawMessage(`{"duration_ms": 100}`),
			},
			setupCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // Cancel immediately
				return ctx, func() {}
			},
			wantErr:     true,
			expectedErr: context.Canceled.Error(),
		},
		{
			name: "Context cancelled during execution",
			task: &model.ClaimedTask{
				TaskType: "SLEEP",
				Config:   json.RawMessage(`{"duration_ms": 2000}`), // Long sleep
			},
			setupCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond) // Cancel quickly via timeout
				return ctx, cancel
			},
			wantErr:     true,
			expectedErr: context.DeadlineExceeded.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := tt.setupCtx()
			defer cancel()

			output, err := executor.Execute(ctx, tt.task)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.expectedErr != "" {
					// Check for substring match to be robust to wrapped errors
					contains := false
					// check direct match or wrapped check
					if err.Error() == tt.expectedErr {
						contains = true
					} else {
						// check context error directly or general substring
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							contains = err.Error() == tt.expectedErr
						} else {
							// check substring for general errors
							contains = len(err.Error()) >= len(tt.expectedErr) && err.Error()[:len(tt.expectedErr)] == tt.expectedErr || (err.Error() != "" && tt.expectedErr != "" && containsSubstring(err.Error(), tt.expectedErr))
						}
					}
					if !contains {
						t.Errorf("expected error containing %q, got %q", tt.expectedErr, err.Error())
					}
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				// Verify success output
				var outMap map[string]string
				if err := json.Unmarshal(output, &outMap); err != nil {
					t.Fatalf("failed to parse output JSON: %v", err)
				}
				if outMap["status"] != "completed" {
					t.Errorf("expected status 'completed', got %q", outMap["status"])
				}
			}
		})
	}
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
