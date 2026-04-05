package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"enrichgo/pkg/store"
)

type mockTimeoutError struct{}

func (mockTimeoutError) Error() string   { return "mock timeout" }
func (mockTimeoutError) Timeout() bool   { return true }
func (mockTimeoutError) Temporary() bool { return true }

func TestIsRetryableIDMapError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: true},
		{name: "net timeout", err: mockTimeoutError{}, want: true},
		{name: "client timeout marker", err: errors.New("Client.Timeout while awaiting headers"), want: true},
		{name: "io timeout marker", err: errors.New("read tcp: i/o timeout"), want: true},
		{name: "unexpected eof marker", err: errors.New("unexpected EOF"), want: true},
		{name: "non retryable", err: errors.New("unsupported species for extended idmaps"), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isRetryableIDMapError(tc.err)
			if got != tc.want {
				t.Fatalf("isRetryableIDMapError(%v)=%v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestWriteIDMapsToSQLiteWithRetryValidation(t *testing.T) {
	dummyStore := &store.SQLiteStore{}

	if err := writeIDMapsToSQLiteWithRetry(nil, "hsa", "basic", time.Minute, 1, time.Second, nil); err == nil || !strings.Contains(err.Error(), "store is nil") {
		t.Fatalf("expected nil-store validation error, got %v", err)
	}
	if err := writeIDMapsToSQLiteWithRetry(dummyStore, "hsa", "basic", 0, 1, time.Second, nil); err == nil || !strings.Contains(err.Error(), "invalid idmaps timeout") {
		t.Fatalf("expected timeout validation error, got %v", err)
	}
	if err := writeIDMapsToSQLiteWithRetry(dummyStore, "hsa", "basic", time.Minute, -1, time.Second, nil); err == nil || !strings.Contains(err.Error(), "invalid idmaps retries") {
		t.Fatalf("expected retries validation error, got %v", err)
	}
	if err := writeIDMapsToSQLiteWithRetry(dummyStore, "hsa", "basic", time.Minute, 1, -1*time.Second, nil); err == nil || !strings.Contains(err.Error(), "invalid idmaps retry backoff") {
		t.Fatalf("expected backoff validation error, got %v", err)
	}
	if err := writeIDMapsToSQLiteWithRetry(dummyStore, "hsa", "invalid", time.Minute, 1, time.Second, nil); err == nil || !strings.Contains(err.Error(), "unknown --idmaps-level") {
		t.Fatalf("expected level validation error, got %v", err)
	}
}

func TestWriteKEGGFallbackIDMapsBestEffortRetryableExhausted(t *testing.T) {
	attempts := 0
	err := writeKEGGFallbackIDMapsBestEffort(context.Background(), "hsa", 1, 0, func(context.Context, string) error {
		attempts++
		return context.DeadlineExceeded
	})
	if err != nil {
		t.Fatalf("expected nil for retryable exhausted error, got %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}

func TestWriteKEGGFallbackIDMapsBestEffortNonRetryable(t *testing.T) {
	wantErr := errors.New("sqlite constraint failed")
	err := writeKEGGFallbackIDMapsBestEffort(context.Background(), "hsa", 2, 0, func(context.Context, string) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected non-retryable error %v, got %v", wantErr, err)
	}
}

func TestWriteKEGGFallbackIDMapsBestEffortSucceedsOnRetry(t *testing.T) {
	attempts := 0
	err := writeKEGGFallbackIDMapsBestEffort(context.Background(), "hsa", 2, 0, func(context.Context, string) error {
		attempts++
		if attempts == 1 {
			return mockTimeoutError{}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success on retry, got %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}

func TestWriteKEGGFallbackIDMapsBestEffortContextDoneSkips(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	err := writeKEGGFallbackIDMapsBestEffort(ctx, "hsa", 2, 0, func(context.Context, string) error {
		attempts++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil when context is done, got %v", err)
	}
	if attempts != 0 {
		t.Fatalf("expected 0 attempts when context is done, got %d", attempts)
	}
}
