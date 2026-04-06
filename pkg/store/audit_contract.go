package store

import (
	"context"
	"fmt"
	"strings"
)

type auditIDMapRequirement struct {
	Species  string
	FromType string
	ToType   string
	MinRows  int64
}

type auditContract struct {
	Profile      string
	MinTableRows map[string]int64
	IDMapMins    []auditIDMapRequirement
}

func resolveAuditContract(profile string) (*auditContract, error) {
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case "embedded-hsa-seed":
		// Seed profile only checks core schema health; row counts are intentionally not constrained.
		return &auditContract{Profile: profile}, nil
	case "embedded-hsa-basic", "embedded-hsa-extended":
		return &auditContract{
			Profile: profile,
			MinTableRows: map[string]int64{
				"geneset":      1,
				"geneset_gene": 1,
				"idmap":        1,
			},
			IDMapMins: []auditIDMapRequirement{
				{Species: "hsa", FromType: "SYMBOL", ToType: "ENTREZID", MinRows: 1},
				{Species: "hsa", FromType: "ENTREZID", ToType: "SYMBOL", MinRows: 1},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown audit profile %q", profile)
	}
}

func evaluateAuditContract(ctx context.Context, s *SQLiteStore, report *AuditReport, contract *auditContract) ([]string, error) {
	if contract == nil {
		return nil, nil
	}

	violations := make([]string, 0)
	for table, minRows := range contract.MinTableRows {
		got := report.RowCounts[table]
		if got < minRows {
			violations = append(violations, fmt.Sprintf("table %s row count %d < %d", table, got, minRows))
		}
	}

	for _, req := range contract.IDMapMins {
		got, err := countIDMapRows(ctx, s, req.Species, req.FromType, req.ToType)
		if err != nil {
			return nil, err
		}
		if got < req.MinRows {
			violations = append(
				violations,
				fmt.Sprintf("idmap %s %s->%s row count %d < %d", req.Species, req.FromType, req.ToType, got, req.MinRows),
			)
		}
	}

	return violations, nil
}

func countIDMapRows(ctx context.Context, s *SQLiteStore, species, fromType, toType string) (int64, error) {
	species = strings.ToLower(strings.TrimSpace(species))
	fromType = normalizeIDType(fromType)
	toType = normalizeIDType(toType)
	canonicalFromType, _, err := canonicalIDMapDirection(fromType, toType)
	if err != nil {
		return 0, nil
	}

	var n int64
	err = s.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM idmap_canon WHERE species=? AND from_type=?`,
		species,
		canonicalFromType,
	).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count idmap rows (%s %s->%s): %w", species, fromType, toType, err)
	}
	return n, nil
}
