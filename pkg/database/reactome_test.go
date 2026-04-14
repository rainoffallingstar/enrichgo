package database

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
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

func TestDownloadReactomeFallbackToNCBIPathwayMapForMMU(t *testing.T) {
	zipPayload := buildReactomeGMTZip(t, "Apoptosis\tR-HSA-12345\tTP53\tCASP3\n")
	ncbi2Pathway := "100009614\tR-MMU-6810244\tKrtap12-22 [cytosol]\tR-MMU-6805567\thttps://reactome.org/PathwayBrowser/#/R-MMU-6805567\tKeratinization\tIEA\tMus musculus\n"
	geneInfoGZ := buildGzipPayload(t, "#header\n10090\t100009614\tKrtap12-22\t-\t-\t-\t-\t-\t-\t-\t-\t-\n")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/reactome.zip":
			w.Header().Set("Content-Type", "application/zip")
			_, _ = w.Write(zipPayload)
		case "/ncbi2.txt":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(ncbi2Pathway))
		case "/gene_info.gz":
			w.Header().Set("Content-Type", "application/gzip")
			_, _ = w.Write(geneInfoGZ)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	oldURL := reactomeDownloadURL
	oldMapURL := reactomeNCBI2PathwayURL
	oldClient := reactomeHTTPClient
	oldGeneInfo := ncbiGeneInfoURLBySpecies["mmu"]
	defer func() {
		reactomeDownloadURL = oldURL
		reactomeNCBI2PathwayURL = oldMapURL
		reactomeHTTPClient = oldClient
		ncbiGeneInfoURLBySpecies["mmu"] = oldGeneInfo
	}()

	reactomeDownloadURL = ts.URL + "/reactome.zip"
	reactomeNCBI2PathwayURL = ts.URL + "/ncbi2.txt"
	reactomeHTTPClient = ts.Client()
	ncbiGeneInfoURLBySpecies["mmu"] = ts.URL + "/gene_info.gz"

	data, err := DownloadReactomeWithOptions("mmu", "", &ReactomeDownloadOptions{
		AutoRetry:    false,
		MaxRetries:   0,
		RetryBackoff: 0,
	})
	if err != nil {
		t.Fatalf("DownloadReactomeWithOptions fallback failed: %v", err)
	}
	if data == nil || len(data.Pathways) == 0 {
		t.Fatalf("expected non-empty pathways from fallback mapping")
	}
	pw := data.Pathways["R-MMU-6805567"]
	if pw == nil {
		t.Fatalf("expected R-MMU-6805567 in fallback pathways")
	}
	if !pw.Genes["Krtap12-22"] {
		t.Fatalf("expected Krtap12-22 in fallback pathway genes")
	}
}

func buildGzipPayload(t *testing.T, content string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write([]byte(content)); err != nil {
		t.Fatalf("write gzip payload: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	return buf.Bytes()
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
