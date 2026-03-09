package main

import (
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
