package database

import (
	"archive/zip"
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestReactomeSpeciesPrefixMap(t *testing.T) {
	tests := map[string]string{
		"hsa": "R-HSA",
		"mmu": "R-MMU",
		"rno": "R-RNO",
	}

	for species, want := range tests {
		if got := reactomeSpeciesPrefixMap[species]; got != want {
			t.Fatalf("species %s prefix = %q, want %q", species, got, want)
		}
	}
}

func TestLoadReactomeUsesGMTNameAsPathwayName(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reactome_hsa.gmt")
	content := "R-HSA-12345\tApoptosis\tTP53\tCASP3\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write gmt: %v", err)
	}

	data, err := LoadReactome("hsa", dir)
	if err != nil {
		t.Fatalf("LoadReactome failed: %v", err)
	}
	pw := data.Pathways["R-HSA-12345"]
	if pw == nil {
		t.Fatalf("expected R-HSA-12345 pathway")
	}
	if pw.Name != "Apoptosis" {
		t.Fatalf("pathway name=%q want %q", pw.Name, "Apoptosis")
	}
}

func TestDownloadReactomeWithOptionsRetries(t *testing.T) {
	zipPayload := buildReactomeGMTZip(t, "Apoptosis\tR-HSA-12345\tTP53\tCASP3\n")

	var hits int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(zipPayload)
	}))
	defer ts.Close()

	oldURL := reactomeDownloadURL
	oldClient := reactomeHTTPClient
	defer func() {
		reactomeDownloadURL = oldURL
		reactomeHTTPClient = oldClient
	}()
	reactomeDownloadURL = ts.URL
	reactomeHTTPClient = ts.Client()

	data, err := DownloadReactomeWithOptions("hsa", "", &ReactomeDownloadOptions{
		AutoRetry:    true,
		MaxRetries:   1,
		RetryBackoff: 0,
	})
	if err != nil {
		t.Fatalf("DownloadReactomeWithOptions failed: %v", err)
	}
	if atomic.LoadInt32(&hits) != 2 {
		t.Fatalf("expected 2 attempts, got %d", atomic.LoadInt32(&hits))
	}
	if data == nil || len(data.Pathways) == 0 {
		t.Fatalf("expected non-empty pathways after retry success")
	}
	if _, ok := data.Pathways["R-HSA-12345"]; !ok {
		t.Fatalf("expected R-HSA-12345 in downloaded pathways")
	}
}

func TestDownloadReactomeWithOptionsRetryDisabled(t *testing.T) {
	var hits int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	oldURL := reactomeDownloadURL
	oldClient := reactomeHTTPClient
	defer func() {
		reactomeDownloadURL = oldURL
		reactomeHTTPClient = oldClient
	}()
	reactomeDownloadURL = ts.URL
	reactomeHTTPClient = ts.Client()

	_, err := DownloadReactomeWithOptions("hsa", "", &ReactomeDownloadOptions{
		AutoRetry:    false,
		MaxRetries:   3,
		RetryBackoff: 0,
	})
	if err == nil {
		t.Fatalf("expected error when retry disabled and server returns 503")
	}
	if atomic.LoadInt32(&hits) != 1 {
		t.Fatalf("expected 1 attempt when retry disabled, got %d", atomic.LoadInt32(&hits))
	}
}

func buildReactomeGMTZip(t *testing.T, gmtContent string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	f, err := zw.Create("ReactomePathways.gmt")
	if err != nil {
		t.Fatalf("create zip entry: %v", err)
	}
	if _, err := f.Write([]byte(gmtContent)); err != nil {
		t.Fatalf("write zip entry: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}
	return buf.Bytes()
}
