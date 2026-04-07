package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

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

func TestNormalizeBenchmarkSubprocessArgs(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "legacy enrich",
			in:   []string{"enrich", "-i", "x.tsv"},
			want: []string{"analyze", "ora", "-i", "x.tsv"},
		},
		{
			name: "legacy gsea",
			in:   []string{"gsea", "-i", "x.tsv"},
			want: []string{"analyze", "gsea", "-i", "x.tsv"},
		},
		{
			name: "legacy download",
			in:   []string{"download", "-d", "kegg"},
			want: []string{"data", "sync", "-d", "kegg"},
		},
		{
			name: "legacy db-audit",
			in:   []string{"db-audit", "--db", "x.db"},
			want: []string{"db", "audit", "--db", "x.db"},
		},
		{
			name: "already public",
			in:   []string{"analyze", "ora", "-i", "x.tsv"},
			want: []string{"analyze", "ora", "-i", "x.tsv"},
		},
		{
			name: "empty",
			in:   []string{},
			want: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeBenchmarkSubprocessArgs(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("len(got)=%d, want=%d; got=%v want=%v", len(got), len(tc.want), got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("got[%d]=%q, want=%q; got=%v want=%v", i, got[i], tc.want[i], got, tc.want)
				}
			}
		})
	}
}

func TestResolveRSBinary(t *testing.T) {
	prev, had := os.LookupEnv(envRSBinary)
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(envRSBinary, prev)
		} else {
			_ = os.Unsetenv(envRSBinary)
		}
	})

	t.Run("env override missing", func(t *testing.T) {
		_ = os.Setenv(envRSBinary, "definitely-not-a-real-rs-binary")
		_, err := resolveRSBinary()
		if err == nil {
			t.Fatal("expected error for missing override binary")
		}
		if !strings.Contains(err.Error(), envRSBinary) {
			t.Fatalf("error %q does not mention %s", err, envRSBinary)
		}
	})

	t.Run("env override valid", func(t *testing.T) {
		if _, err := exec.LookPath("sh"); err != nil {
			t.Skip("sh not found in PATH")
		}
		_ = os.Setenv(envRSBinary, "sh")
		got, err := resolveRSBinary()
		if err != nil {
			t.Fatalf("resolveRSBinary with valid override returned error: %v", err)
		}
		if strings.TrimSpace(got) == "" {
			t.Fatal("resolveRSBinary returned empty path")
		}
	})
}
