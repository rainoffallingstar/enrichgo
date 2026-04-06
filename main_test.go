package main

import (
	"reflect"
	"testing"
)

func TestRewriteAnalyzeArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    []string
		wantErr bool
	}{
		{name: "ora", args: []string{"enrichgo", "analyze", "ora", "-i", "a.txt"}, want: []string{"enrichgo", "enrich", "-i", "a.txt"}},
		{name: "gsea", args: []string{"enrichgo", "analyze", "gsea", "-i", "b.txt"}, want: []string{"enrichgo", "gsea", "-i", "b.txt"}},
		{name: "missing", args: []string{"enrichgo", "analyze"}, wantErr: true},
		{name: "unknown", args: []string{"enrichgo", "analyze", "abc"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rewriteAnalyzeArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("rewriteAnalyzeArgs mismatch\n got=%v\nwant=%v", got, tc.want)
			}
		})
	}
}

func TestRewriteDataArgs(t *testing.T) {
	got, err := rewriteDataArgs([]string{"enrichgo", "data", "sync", "-d", "kegg"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"enrichgo", "download", "-d", "kegg"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rewriteDataArgs mismatch\n got=%v\nwant=%v", got, want)
	}
	if _, err := rewriteDataArgs([]string{"enrichgo", "data", "bad"}); err == nil {
		t.Fatalf("expected error for unknown data subcommand")
	}
}

func TestRewriteDBArgs(t *testing.T) {
	got, err := rewriteDBArgs([]string{"enrichgo", "db", "audit", "--db", "a.db"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"enrichgo", "db-audit", "--db", "a.db"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rewriteDBArgs mismatch\n got=%v\nwant=%v", got, want)
	}
	if _, err := rewriteDBArgs([]string{"enrichgo", "db", "bad"}); err == nil {
		t.Fatalf("expected error for unknown db subcommand")
	}
}

func TestRewriteBenchArgs(t *testing.T) {
	t.Run("adds benchmark flag", func(t *testing.T) {
		got, err := rewriteBenchArgs([]string{"enrichgo", "bench", "run", "ora", "-i", "x.tsv"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{"enrichgo", "enrich", "-i", "x.tsv", "--benchmark=true"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("rewriteBenchArgs mismatch\n got=%v\nwant=%v", got, want)
		}
	})

	t.Run("keeps explicit benchmark", func(t *testing.T) {
		got, err := rewriteBenchArgs([]string{"enrichgo", "bench", "run", "gsea", "-i", "x.tsv", "--benchmark=false"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{"enrichgo", "gsea", "-i", "x.tsv", "--benchmark=false"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("rewriteBenchArgs mismatch\n got=%v\nwant=%v", got, want)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		if _, err := rewriteBenchArgs([]string{"enrichgo", "bench"}); err == nil {
			t.Fatalf("expected usage error")
		}
		if _, err := rewriteBenchArgs([]string{"enrichgo", "bench", "bad", "ora"}); err == nil {
			t.Fatalf("expected subcommand error")
		}
		if _, err := rewriteBenchArgs([]string{"enrichgo", "bench", "run", "bad"}); err == nil {
			t.Fatalf("expected analysis error")
		}
	})
}
