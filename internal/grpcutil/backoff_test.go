package grpcutil

import (
	"testing"
	"time"
)

func TestNextBackoffDoublesAndCaps(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want time.Duration
	}{
		{50 * time.Millisecond, 100 * time.Millisecond},
		{100 * time.Millisecond, 200 * time.Millisecond},
		{1 * time.Second, 2 * time.Second},
		{2 * time.Second, 2 * time.Second}, // capped
		{5 * time.Second, 2 * time.Second}, // over cap stays at cap
	}
	for _, tc := range tests {
		if got := nextBackoff(tc.in); got != tc.want {
			t.Errorf("nextBackoff(%s) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

func BenchmarkNextBackoff(b *testing.B) {
	d := 50 * time.Millisecond
	for i := 0; i < b.N; i++ {
		d = nextBackoff(d)
		if d >= maxBackoff {
			d = 50 * time.Millisecond
		}
	}
}
