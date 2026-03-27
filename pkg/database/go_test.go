package database

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseGAFUsesGeneSymbol(t *testing.T) {
	data := &GOData{
		Terms:      make(map[string]*GOTerm),
		Gene2Terms: make(map[string][]string),
	}

	// DB Object ID=P04637, Symbol=TP53
	line := "UniProtKB\tP04637\tTP53\t\tGO:0003677\tPMID:1\tIDA\t\tF\tname\t\tprotein\ttaxon:9606\t20240101\tUniProt\n"
	if err := parseGAF(strings.NewReader(line), data); err != nil {
		t.Fatalf("parseGAF failed: %v", err)
	}

	terms := data.Gene2Terms["TP53"]
	if len(terms) != 1 || terms[0] != "GO:0003677" {
		t.Fatalf("expected TP53->GO:0003677 mapping, got %+v", terms)
	}
}

func TestLoadGOUsesGMTNameAsTermName(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "go_hsa_BP.gmt")
	content := "GO:0000001\tMitochondrion inheritance\tGENE1\tGENE2\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write gmt: %v", err)
	}

	data, err := LoadGO(path)
	if err != nil {
		t.Fatalf("LoadGO failed: %v", err)
	}
	term := data.Terms["GO:0000001"]
	if term == nil {
		t.Fatalf("expected GO:0000001 term")
	}
	if term.Name != "Mitochondrion inheritance" {
		t.Fatalf("term name=%q want %q", term.Name, "Mitochondrion inheritance")
	}
}
