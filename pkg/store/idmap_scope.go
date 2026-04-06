package store

import (
	"context"
	"fmt"
	"strings"
)

// CountIDMapScope returns row count for a canonical idmap scope.
// The (fromType,toType) pair accepts both X->ENTREZID and ENTREZID->X directions.
func (s *SQLiteStore) CountIDMapScope(ctx context.Context, species, source, fromType, toType string) (int64, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("store not initialized")
	}
	species = strings.ToLower(strings.TrimSpace(species))
	source = strings.TrimSpace(source)
	fromType = normalizeIDType(fromType)
	toType = normalizeIDType(toType)
	if species == "" || source == "" || fromType == "" || toType == "" {
		return 0, fmt.Errorf("invalid idmap count args")
	}

	canonicalFromType, _, err := canonicalIDMapDirection(fromType, toType)
	if err != nil {
		return 0, err
	}

	var n int64
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM idmap_canon
		WHERE species=? AND source=? AND from_type=?
	`, species, source, canonicalFromType).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
