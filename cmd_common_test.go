package main

import (
	"os"
	"path/filepath"
	"testing"

	"enrichgo/pkg/annotation"
)

func TestParseMSigDBCollections(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantLen int
		wantErr bool
	}{
		{name: "default range", input: "c1-c8", wantLen: 8},
		{name: "all", input: "all", wantLen: 9},
		{name: "single", input: "c3", wantLen: 1},
		{name: "multi", input: "c1,c2,c2", wantLen: 2},
		{name: "invalid", input: "c9", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cols, err := parseMSigDBCollections(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(cols) != tc.wantLen {
				t.Fatalf("len(cols)=%d, want %d", len(cols), tc.wantLen)
			}
		})
	}
}

func TestTargetIDTypeForDatabase(t *testing.T) {
	tests := []struct {
		db   string
		want annotation.IDType
	}{
		{db: "kegg", want: annotation.IDEntrez},
		{db: "go", want: annotation.IDSymbol},
		{db: "reactome", want: annotation.IDSymbol},
		{db: "msigdb", want: annotation.IDSymbol},
		{db: "custom", want: annotation.IDUnknown},
	}

	for _, tc := range tests {
		t.Run(tc.db, func(t *testing.T) {
			got := targetIDTypeForDatabase(tc.db)
			if got != tc.want {
				t.Fatalf("targetIDTypeForDatabase(%q)=%v, want %v", tc.db, got, tc.want)
			}
		})
	}
}

func TestMapIDsForDisplay(t *testing.T) {
	ids := []string{"1", "2", "X"}
	display := map[string]string{
		"1": "TP53",
		"2": "EGFR",
	}
	got := mapIDsForDisplay(ids, display)
	want := []string{"TP53", "EGFR", "X"}
	if len(got) != len(want) {
		t.Fatalf("len(got)=%d, want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d]=%q, want=%q", i, got[i], want[i])
		}
	}
}

func TestLoadEntrezSymbolMapFromIDMap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kegg_hsa_idmap.tsv")
	content := "1\tTP53\n2\tEGFR\n2\tEGFR_DUP\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write temp idmap: %v", err)
	}
	got, err := loadEntrezSymbolMapFromIDMap(path)
	if err != nil {
		t.Fatalf("loadEntrezSymbolMapFromIDMap error: %v", err)
	}
	if got["1"] != "TP53" {
		t.Fatalf("got[1]=%q, want TP53", got["1"])
	}
	// duplicate ENTREZ keeps first symbol
	if got["2"] != "EGFR" {
		t.Fatalf("got[2]=%q, want EGFR", got["2"])
	}
}

func TestResolveKEGGIDCacheMaxEntries(t *testing.T) {
	prev, had := os.LookupEnv(envKEGGIDCacheMaxEntries)
	t.Cleanup(func() {
		if had {
			os.Setenv(envKEGGIDCacheMaxEntries, prev)
		} else {
			os.Unsetenv(envKEGGIDCacheMaxEntries)
		}
	})

	os.Unsetenv(envKEGGIDCacheMaxEntries)
	if v, ok, err := resolveKEGGIDCacheMaxEntries(0); err != nil || ok || v != 0 {
		t.Fatalf("no flag/env: v=%d ok=%v err=%v", v, ok, err)
	}

	os.Setenv(envKEGGIDCacheMaxEntries, "123")
	if v, ok, err := resolveKEGGIDCacheMaxEntries(0); err != nil || !ok || v != 123 {
		t.Fatalf("env: v=%d ok=%v err=%v", v, ok, err)
	}

	os.Setenv(envKEGGIDCacheMaxEntries, "bad")
	if _, ok, err := resolveKEGGIDCacheMaxEntries(0); err == nil || !ok {
		t.Fatalf("invalid env should error")
	}

	os.Setenv(envKEGGIDCacheMaxEntries, "999")
	if v, ok, err := resolveKEGGIDCacheMaxEntries(77); err != nil || !ok || v != 77 {
		t.Fatalf("flag overrides env: v=%d ok=%v err=%v", v, ok, err)
	}
}

func TestParseConversionPolicy(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    annotation.ConversionPolicy
		wantErr bool
	}{
		{name: "default threshold", input: "", want: annotation.ConversionPolicyThreshold},
		{name: "strict", input: "strict", want: annotation.ConversionPolicyStrict},
		{name: "best effort dash", input: "best-effort", want: annotation.ConversionPolicyBestEffort},
		{name: "best effort underscore", input: "best_effort", want: annotation.ConversionPolicyBestEffort},
		{name: "invalid", input: "foo", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseConversionPolicy(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("parseConversionPolicy(%q)=%q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestApplyStrictModeOverrides(t *testing.T) {
	t.Run("disabled keeps values", func(t *testing.T) {
		p := strictModePolicy{
			AutoUpdateDB:              true,
			EnableOnlineIDMapFallback: true,
			AllowIDFallback:           true,
			IDConversionPolicy:        "best-effort",
			MinConversionRate:         0.50,
		}
		applyStrictModeOverrides(false, &p)
		if !p.AutoUpdateDB || !p.EnableOnlineIDMapFallback || !p.AllowIDFallback {
			t.Fatalf("policy unexpectedly changed when strict mode disabled: %+v", p)
		}
		if p.IDConversionPolicy != "best-effort" || p.MinConversionRate != 0.50 {
			t.Fatalf("policy unexpectedly changed when strict mode disabled: %+v", p)
		}
	})

	t.Run("enabled enforces fail-fast defaults", func(t *testing.T) {
		p := strictModePolicy{
			AutoUpdateDB:              true,
			EnableOnlineIDMapFallback: true,
			AllowIDFallback:           true,
			IDConversionPolicy:        "best-effort",
			MinConversionRate:         0.50,
		}
		applyStrictModeOverrides(true, &p)
		if p.AutoUpdateDB || p.EnableOnlineIDMapFallback || p.AllowIDFallback {
			t.Fatalf("strict mode should disable fallback toggles: %+v", p)
		}
		if p.IDConversionPolicy != "threshold" {
			t.Fatalf("strict mode policy=%q, want threshold", p.IDConversionPolicy)
		}
		if p.MinConversionRate != 0.90 {
			t.Fatalf("strict mode min conversion rate=%.2f, want 0.90", p.MinConversionRate)
		}
	})

	t.Run("enabled keeps higher threshold", func(t *testing.T) {
		p := strictModePolicy{
			AutoUpdateDB:              true,
			EnableOnlineIDMapFallback: true,
			AllowIDFallback:           true,
			IDConversionPolicy:        "strict",
			MinConversionRate:         0.95,
		}
		applyStrictModeOverrides(true, &p)
		if p.MinConversionRate != 0.95 {
			t.Fatalf("strict mode should keep higher threshold, got %.2f", p.MinConversionRate)
		}
	})
}
