package database

import "strings"

func nameFromGMTGeneSet(gs *GeneSet) string {
	if gs == nil {
		return ""
	}
	if name := strings.TrimSpace(gs.Description); name != "" && !strings.EqualFold(name, "NA") {
		return name
	}
	if name := strings.TrimSpace(gs.Name); name != "" {
		return name
	}
	return strings.TrimSpace(gs.ID)
}
