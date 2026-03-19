package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"enrichgo/pkg/types"
)

func TestSQLiteStore_GeneSetsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	sets := types.GeneSets{
		&types.GeneSet{ID: "p1", Name: "Pathway 1", Description: "d1", Genes: map[string]bool{"1": true, "2": true}},
		&types.GeneSet{ID: "p2", Name: "Pathway 2", Description: "d2", Genes: map[string]bool{"3": true}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := st.ReplaceGeneSets(ctx, GeneSetFilter{DB: "kegg", Species: "hsa"}, "ENTREZID", sets, "v1"); err != nil {
		t.Fatalf("ReplaceGeneSets: %v", err)
	}

	got, geneIDType, err := st.LoadGeneSets(ctx, GeneSetFilter{DB: "kegg", Species: "hsa"})
	if err != nil {
		t.Fatalf("LoadGeneSets: %v", err)
	}
	if geneIDType != "ENTREZID" {
		t.Fatalf("geneIDType=%q want %q", geneIDType, "ENTREZID")
	}
	if len(got) != 2 {
		t.Fatalf("len(got)=%d want 2", len(got))
	}
	if !got[0].Genes["1"] || !got[0].Genes["2"] {
		t.Fatalf("missing genes in set %q", got[0].ID)
	}
}

func TestSQLiteStore_IDMapLookup(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pairs := []IDMapRow{
		{From: "TP53", To: "7157"},
		{From: "EGFR", To: "1956"},
	}
	if err := st.ReplaceIDMap(ctx, "hsa", "test", "SYMBOL", "ENTREZID", pairs); err != nil {
		t.Fatalf("ReplaceIDMap: %v", err)
	}

	m, err := st.LookupIDMap(ctx, "hsa", "SYMBOL", "ENTREZID", []string{"TP53", "EGFR", "MISSING"})
	if err != nil {
		t.Fatalf("LookupIDMap: %v", err)
	}
	if m["TP53"][0] != "7157" {
		t.Fatalf("TP53 -> %v, want 7157", m["TP53"])
	}
	if m["EGFR"][0] != "1956" {
		t.Fatalf("EGFR -> %v, want 1956", m["EGFR"])
	}
	if _, ok := m["MISSING"]; ok {
		t.Fatalf("expected no key for MISSING, got %v", m["MISSING"])
	}
}

