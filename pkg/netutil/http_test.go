package netutil

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientRetriesOn500ThenSucceeds(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n < 3 {
			http.Error(w, "nope", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	sleepCalls := int32(0)
	c := NewClient(Options{
		HTTPClient:  srv.Client(),
		MaxAttempts: 4,
		BaseBackoff: 1 * time.Millisecond,
		MaxBackoff:  1 * time.Millisecond,
		Sleep: func(time.Duration) {
			atomic.AddInt32(&sleepCalls, 1)
		},
	})

	req, _ := http.NewRequest("GET", srv.URL, nil)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if string(b) != "ok" {
		t.Fatalf("body=%q", string(b))
	}
	if got := atomic.LoadInt32(&hits); got != 3 {
		t.Fatalf("hits=%d, want 3", got)
	}
	if got := atomic.LoadInt32(&sleepCalls); got == 0 {
		t.Fatalf("expected backoff sleep to be called")
	}
}

func TestClientRetriesPOSTWhenBodyReplayable(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		r.Body.Close()
		n := atomic.AddInt32(&hits, 1)
		if n == 1 {
			http.Error(w, "fail", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient(Options{
		HTTPClient:  srv.Client(),
		MaxAttempts: 3,
		BaseBackoff: 1 * time.Millisecond,
		MaxBackoff:  1 * time.Millisecond,
		Sleep:       func(time.Duration) {},
	})

	req, _ := http.NewRequest("POST", srv.URL, strings.NewReader("abc"))
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()
	if got := atomic.LoadInt32(&hits); got != 2 {
		t.Fatalf("hits=%d, want 2", got)
	}
}

func TestClientDoesNotRetryNonReplayableBody(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		http.Error(w, "fail", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(Options{
		HTTPClient:  srv.Client(),
		MaxAttempts: 3,
		BaseBackoff: 1 * time.Millisecond,
		MaxBackoff:  1 * time.Millisecond,
		Sleep:       func(time.Duration) {},
	})

	req := &http.Request{
		Method: http.MethodPost,
		URL:    mustParseURL(t, srv.URL),
		Body:   io.NopCloser(strings.NewReader("x")),
		Header: make(http.Header),
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("hits=%d, want 1", got)
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	return u
}
