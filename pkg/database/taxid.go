package database

import (
	"fmt"
	"strings"
)

var taxIDBySpecies = map[string]int{
	"hsa": 9606,
	"mmu": 10090,
	"rno": 10116,
	"dre": 7955,
	"dme": 7227,
	"cel": 6239,
	"sce": 559292,
	"ath": 3702,
	"eco": 562,
	"bta": 9913,
	"gga": 9031,
}

func TaxIDForSpecies(species string) (int, error) {
	species = strings.ToLower(strings.TrimSpace(species))
	if species == "" {
		return 0, fmt.Errorf("empty species")
	}
	tax, ok := taxIDBySpecies[species]
	if !ok {
		return 0, fmt.Errorf("unsupported species for extended idmaps: %s", species)
	}
	return tax, nil
}

