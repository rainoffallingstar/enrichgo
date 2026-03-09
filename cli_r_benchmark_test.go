package main

import "testing"

func TestResolveRResultFile(t *testing.T) {
	tests := []struct {
		command string
		db      string
		want    string
		wantErr bool
	}{
		{"enrich", "kegg", "r_ora_kegg.tsv", false},
		{"enrich", "go", "r_ora_go.tsv", false},
		{"gsea", "reactome", "r_gsea_reactome.tsv", false},
		{"gsea", "msigdb", "r_gsea_msigdb.tsv", false},
		{"gsea", "custom", "", true},
	}

	for _, tc := range tests {
		got, err := resolveRResultFile(tc.command, tc.db)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("expected error for %s/%s", tc.command, tc.db)
			}
			continue
		}
		if err != nil {
			t.Fatalf("unexpected error for %s/%s: %v", tc.command, tc.db, err)
		}
		if got != tc.want {
			t.Fatalf("resolveRResultFile(%q,%q)=%q, want %q", tc.command, tc.db, got, tc.want)
		}
	}
}

func TestDeriveOutputPath(t *testing.T) {
	tests := []struct {
		in     string
		suffix string
		want   string
	}{
		{"out.tsv", ".r", "out.r.tsv"},
		{"out", ".benchmark", "out.benchmark.tsv"},
		{"dir/result.json", ".benchmark", "dir/result.benchmark.json"},
	}

	for _, tc := range tests {
		got := deriveOutputPath(tc.in, tc.suffix)
		if got != tc.want {
			t.Fatalf("deriveOutputPath(%q,%q)=%q, want %q", tc.in, tc.suffix, got, tc.want)
		}
	}
}
