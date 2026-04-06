package main

import (
	"context"
	"errors"
	"os"
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

func TestParseKEGGIDMapTSV(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/kegg_hsa_idmap.tsv"
	content := strings.Join([]string{
		"# comment",
		"hsa:1\tTP53",
		"ncbi-geneid:2\tEgfr",
		"3\t",
		"invalid",
		"hsa:1\tTP53_DUP",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	s2e, e2s, seen, dropped, err := parseKEGGIDMapTSV(path, "hsa")
	if err != nil {
		t.Fatalf("parseKEGGIDMapTSV: %v", err)
	}
	if seen != 5 {
		t.Fatalf("seen=%d want 5", seen)
	}
	if dropped != 2 {
		t.Fatalf("dropped=%d want 2", dropped)
	}
	if got := s2e["TP53"]; got != "1" {
		t.Fatalf("TP53 -> %q want 1", got)
	}
	if got := s2e["EGFR"]; got != "2" {
		t.Fatalf("EGFR -> %q want 2", got)
	}
	if got := e2s["1"]; got != "TP53" {
		t.Fatalf("1 -> %q want TP53", got)
	}
}

func TestWriteBasicIDMapsFromLocalTSV(t *testing.T) {
	dir := t.TempDir()
	tsvPath := dir + "/kegg_hsa_idmap.tsv"
	if err := os.WriteFile(tsvPath, []byte("1\tTP53\n2\tEGFR\n"), 0644); err != nil {
		t.Fatalf("write tsv: %v", err)
	}

	dbPath := dir + "/test.db"
	st, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := writeBasicIDMapsFromLocalTSV(ctx, st, "hsa", dir); err != nil {
		t.Fatalf("writeBasicIDMapsFromLocalTSV: %v", err)
	}

	forward, err := st.LookupIDMap(ctx, "hsa", "SYMBOL", "ENTREZID", []string{"TP53", "EGFR"})
	if err != nil {
		t.Fatalf("LookupIDMap forward: %v", err)
	}
	if len(forward["TP53"]) != 1 || forward["TP53"][0] != "1" {
		t.Fatalf("TP53 -> %v want [1]", forward["TP53"])
	}
	if len(forward["EGFR"]) != 1 || forward["EGFR"][0] != "2" {
		t.Fatalf("EGFR -> %v want [2]", forward["EGFR"])
	}
}

func TestRunExtendedIDMapSourceResumeSkip(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/test.db"
	st, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := st.ReplaceIDMap(ctx, "hsa", "resume_src", "SYMBOL", "ENTREZID", []store.IDMapRow{{From: "A", To: "1"}}); err != nil {
		t.Fatalf("seed ReplaceIDMap: %v", err)
	}

	called := false
	err = runExtendedIDMapSource(ctx, st, "hsa", "resume_src", "SYMBOL", "ENTREZID", true, func(emit store.IDMapEmit) error {
		called = true
		return emit("B", "2")
	})
	if err != nil {
		t.Fatalf("runExtendedIDMapSource: %v", err)
	}
	if called {
		t.Fatalf("producer should not run when resume skip is active")
	}

	got, err := st.ScanIDMap(ctx, "hsa", "SYMBOL", "ENTREZID")
	if err != nil {
		t.Fatalf("ScanIDMap: %v", err)
	}
	if len(got) != 1 || len(got["A"]) != 1 || got["A"][0] != "1" {
		t.Fatalf("unexpected mappings after resume skip: %v", got)
	}
}

func TestWriteIDMapsToSQLiteWithRetryConfigValidation(t *testing.T) {
	dummyStore := &store.SQLiteStore{}

	if err := writeIDMapsToSQLiteWithRetryConfig(nil, "hsa", "basic", time.Minute, 1, time.Second, nil, true, "data"); err == nil || !strings.Contains(err.Error(), "store is nil") {
		t.Fatalf("expected nil-store validation error, got %v", err)
	}
	if err := writeIDMapsToSQLiteWithRetryConfig(dummyStore, "hsa", "basic", 0, 1, time.Second, nil, true, "data"); err == nil || !strings.Contains(err.Error(), "invalid idmaps timeout") {
		t.Fatalf("expected timeout validation error, got %v", err)
	}
	if err := writeIDMapsToSQLiteWithRetryConfig(dummyStore, "hsa", "basic", time.Minute, -1, time.Second, nil, true, "data"); err == nil || !strings.Contains(err.Error(), "invalid idmaps retries") {
		t.Fatalf("expected retries validation error, got %v", err)
	}
	if err := writeIDMapsToSQLiteWithRetryConfig(dummyStore, "hsa", "basic", time.Minute, 1, -1*time.Second, nil, true, "data"); err == nil || !strings.Contains(err.Error(), "invalid idmaps retry backoff") {
		t.Fatalf("expected backoff validation error, got %v", err)
	}
	if err := writeIDMapsToSQLiteWithRetryConfig(dummyStore, "hsa", "invalid", time.Minute, 1, time.Second, nil, true, "data"); err == nil || !strings.Contains(err.Error(), "unknown --idmaps-level") {
		t.Fatalf("expected level validation error, got %v", err)
	}
}

func TestEffectiveIDMapsResume(t *testing.T) {
	if !effectiveIDMapsResume(true, false) {
		t.Fatalf("resume=true, force=false should keep resume")
	}
	if effectiveIDMapsResume(false, false) {
		t.Fatalf("resume=false, force=false should not resume")
	}
	if effectiveIDMapsResume(true, true) {
		t.Fatalf("resume=true, force=true should disable resume")
	}
	if effectiveIDMapsResume(false, true) {
		t.Fatalf("resume=false, force=true should remain false")
	}
}
