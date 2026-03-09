package main

import (
	"fmt"
	"strings"

	"enrichgo/pkg/annotation"
	"enrichgo/pkg/database"
)

func targetIDTypeForDatabase(db string) annotation.IDType {
	switch strings.ToLower(db) {
	case "kegg":
		return annotation.IDEntrez
	case "go", "reactome", "msigdb":
		return annotation.IDSymbol
	default:
		// custom/unknown: 不强制转换，沿用输入 ID
		return annotation.IDUnknown
	}
}

func parseMSigDBCollections(raw string) ([]database.MSigDBCollection, error) {
	v := strings.TrimSpace(strings.ToLower(raw))
	if v == "" {
		return database.DefaultMSigDBCollections(), nil
	}

	if v == "c1-c8" {
		return database.DefaultMSigDBCollections(), nil
	}
	if v == "all" {
		return []database.MSigDBCollection{
			database.MSigDBH,
			database.MSigDBC1,
			database.MSigDBC2,
			database.MSigDBC3,
			database.MSigDBC4,
			database.MSigDBC5,
			database.MSigDBC6,
			database.MSigDBC7,
			database.MSigDBC8,
		}, nil
	}

	parts := strings.Split(v, ",")
	seen := make(map[database.MSigDBCollection]bool)
	var cols []database.MSigDBCollection
	for _, part := range parts {
		col := database.MSigDBCollection(strings.TrimSpace(part))
		switch col {
		case database.MSigDBH, database.MSigDBC1, database.MSigDBC2, database.MSigDBC3,
			database.MSigDBC4, database.MSigDBC5, database.MSigDBC6, database.MSigDBC7, database.MSigDBC8:
			if !seen[col] {
				seen[col] = true
				cols = append(cols, col)
			}
		default:
			return nil, fmt.Errorf("unsupported MSigDB collection: %q", strings.TrimSpace(part))
		}
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("no valid MSigDB collection in %q", raw)
	}
	return cols, nil
}
