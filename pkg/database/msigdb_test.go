package database

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrDownloadMSigDBUsesLocalCache(t *testing.T) {
	dir := t.TempDir()
	cacheFile := filepath.Join(dir, "msigdb_c1.gmt")
	content := "SET_A\tNA\tGENE1\tGENE2\n"
	if err := os.WriteFile(cacheFile, []byte(content), 0644); err != nil {
		t.Fatalf("write cache file: %v", err)
	}

	sets, err := LoadOrDownloadMSigDB(MSigDBC1, dir)
	if err != nil {
		t.Fatalf("LoadOrDownloadMSigDB failed: %v", err)
	}
	if len(sets) != 1 {
		t.Fatalf("expected 1 gene set, got %d", len(sets))
	}
	if !sets[0].Genes["GENE1"] {
		t.Fatalf("expected GENE1 in cached gene set")
	}
}

func TestLoadOrDownloadMSigDBCollectionsMergeDedup(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "msigdb_c1.gmt"), []byte("SET_X\tNA\tA\tB\n"), 0644); err != nil {
		t.Fatalf("write c1 cache: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "msigdb_c2.gmt"), []byte("SET_X\tNA\tA\tB\nSET_Y\tNA\tC\tD\n"), 0644); err != nil {
		t.Fatalf("write c2 cache: %v", err)
	}

	sets, err := LoadOrDownloadMSigDBCollections([]MSigDBCollection{MSigDBC1, MSigDBC2}, dir)
	if err != nil {
		t.Fatalf("LoadOrDownloadMSigDBCollections failed: %v", err)
	}
	if len(sets) != 2 {
		t.Fatalf("expected 2 unique gene sets, got %d", len(sets))
	}
}
