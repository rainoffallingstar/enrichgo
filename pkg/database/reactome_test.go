package database

import "testing"

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
