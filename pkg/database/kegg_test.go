package database

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadKEGGUsesGMTNameAsPathwayName(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hsa.gmt")
	if err := os.WriteFile(path, []byte("hsa00010\tGlycolysis / Gluconeogenesis\t1\t2\n"), 0644); err != nil {
		t.Fatalf("write gmt: %v", err)
	}

	data, err := LoadKEGG("hsa", dir)
	if err != nil {
		t.Fatalf("LoadKEGG failed: %v", err)
	}
	pw := data.Pathways["hsa00010"]
	if pw == nil {
		t.Fatalf("expected hsa00010 pathway")
	}
	if pw.Name != "Glycolysis / Gluconeogenesis" {
		t.Fatalf("pathway name=%q want %q", pw.Name, "Glycolysis / Gluconeogenesis")
	}
}
