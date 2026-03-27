package database

import (
	"os"
	"path/filepath"
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
