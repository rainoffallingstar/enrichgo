package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"enrichgo/pkg/analysis"
	"enrichgo/pkg/store"
)

func TestHydrateDisplayMapForKEGGMergesMissingSQLiteMappings(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "idmap.db")
	st, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer st.Close()

	err = st.ReplaceIDMap(context.Background(), "hsa", "kegg_list", "SYMBOL", "ENTREZID", []store.IDMapRow{
		{From: "TP53", To: "7157"},
		{From: "EGFR", To: "1956"},
	})
	if err != nil {
		t.Fatalf("replace idmap: %v", err)
	}

	display := map[string]string{"7157": "TP53_INPUT"}
	hydrateDisplayMapForKEGG(display, "kegg", "hsa", t.TempDir(), st)

	if got := display["7157"]; got != "TP53_INPUT" {
		t.Fatalf("display[7157]=%q, want existing value preserved", got)
	}
	if got := display["1956"]; got != "EGFR" {
		t.Fatalf("display[1956]=%q, want EGFR", got)
	}
}

func TestHydrateKEGGGeneSetMetadataUsesLocalNamesForPathPrefixedIDs(t *testing.T) {
	dataDir := t.TempDir()
	gmtPath := filepath.Join(dataDir, "hsa.gmt")
	content := "hsa00010\tGlycolysis / Gluconeogenesis\t1\t2\n"
	if err := os.WriteFile(gmtPath, []byte(content), 0644); err != nil {
		t.Fatalf("write gmt: %v", err)
	}

	geneSets := analysis.GeneSets{
		&analysis.GeneSet{
			ID:          "path:hsa00010",
			Name:        "path:hsa00010",
			Description: "-",
			Genes:       map[string]bool{"1": true, "2": true},
		},
	}

	hydrateKEGGGeneSetMetadata(geneSets, "hsa", dataDir)

	if got := geneSets[0].Name; got != "Glycolysis / Gluconeogenesis" {
		t.Fatalf("geneSets[0].Name=%q, want Glycolysis / Gluconeogenesis", got)
	}
	if got := geneSets[0].Description; got != "Glycolysis / Gluconeogenesis" {
		t.Fatalf("geneSets[0].Description=%q, want Glycolysis / Gluconeogenesis", got)
	}
}
