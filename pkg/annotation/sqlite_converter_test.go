package annotation

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"enrichgo/pkg/store"
)

func TestSQLiteIDConverter_MultiHop(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// SYMBOL <-> ENTREZ
	if err := st.ReplaceIDMap(ctx, "hsa", "test", string(IDSymbol), string(IDEntrez), []store.IDMapRow{
		{From: "TP53", To: "7157"},
	}); err != nil {
		t.Fatalf("ReplaceIDMap symbol->entrez: %v", err)
	}
	if err := st.ReplaceIDMap(ctx, "hsa", "test", string(IDEntrez), string(IDSymbol), []store.IDMapRow{
		{From: "7157", To: "TP53"},
	}); err != nil {
		t.Fatalf("ReplaceIDMap entrez->symbol: %v", err)
	}

	// UNIPROT <-> ENTREZ
	if err := st.ReplaceIDMap(ctx, "hsa", "test", string(IDUniprot), string(IDEntrez), []store.IDMapRow{
		{From: "P04637", To: "7157"},
	}); err != nil {
		t.Fatalf("ReplaceIDMap uniprot->entrez: %v", err)
	}
	if err := st.ReplaceIDMap(ctx, "hsa", "test", string(IDEntrez), string(IDUniprot), []store.IDMapRow{
		{From: "7157", To: "P04637"},
	}); err != nil {
		t.Fatalf("ReplaceIDMap entrez->uniprot: %v", err)
	}

	converter := NewSQLiteIDConverter(st)

	m1, err := converter.Convert([]string{"TP53"}, IDSymbol, IDEntrez, "hsa")
	if err != nil {
		t.Fatalf("Convert SYMBOL->ENTREZ failed: %v", err)
	}
	if m1["TP53"][0] != "7157" {
		t.Fatalf("TP53 -> %v, want 7157", m1["TP53"])
	}

	m2, err := converter.Convert([]string{"P04637"}, IDUniprot, IDSymbol, "hsa")
	if err != nil {
		t.Fatalf("Convert UNIPROT->SYMBOL failed: %v", err)
	}
	if m2["P04637"][0] != "TP53" {
		t.Fatalf("P04637 -> %v, want TP53", m2["P04637"])
	}

	m3, err := converter.Convert([]string{"7157"}, IDEntrez, IDKEGG, "hsa")
	if err != nil {
		t.Fatalf("Convert ENTREZ->KEGG failed: %v", err)
	}
	if m3["7157"][0] != "hsa:7157" {
		t.Fatalf("7157 -> %v, want hsa:7157", m3["7157"])
	}

	m4, err := converter.Convert([]string{"hsa:7157"}, IDKEGG, IDSymbol, "hsa")
	if err != nil {
		t.Fatalf("Convert KEGG->SYMBOL failed: %v", err)
	}
	if m4["hsa:7157"][0] != "TP53" {
		t.Fatalf("hsa:7157 -> %v, want TP53", m4["hsa:7157"])
	}
}

