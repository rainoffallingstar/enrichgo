package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const entrezIDType = "ENTREZID"

type IDMapRow struct {
	From string
	To   string
}

func (s *SQLiteStore) ReplaceIDMap(ctx context.Context, species, source, fromType, toType string, pairs []IDMapRow) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store not initialized")
	}
	species = strings.ToLower(strings.TrimSpace(species))
	source = strings.TrimSpace(source)
	fromType = normalizeIDType(fromType)
	toType = normalizeIDType(toType)
	if species == "" || source == "" || fromType == "" || toType == "" {
		return fmt.Errorf("invalid idmap replace args")
	}

	canonicalFromType, direction, err := canonicalIDMapDirection(fromType, toType)
	if err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM idmap_canon WHERE species=? AND source=? AND from_type=?`,
		species, source, canonicalFromType,
	); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	stmt, err := tx.PrepareContext(ctx, `
		INSERT OR IGNORE INTO idmap_canon (species, from_type, from_id, entrez_id, source, downloaded_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, p := range pairs {
		from, entrez := canonicalizeIDMapPair(direction, p)
		if from == "" || entrez == "" || from == entrez {
			continue
		}
		if _, err := stmt.ExecContext(ctx, species, canonicalFromType, from, entrez, source, now); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) LookupIDMap(ctx context.Context, species, fromType, toType string, fromIDs []string) (map[string][]string, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store not initialized")
	}
	species = strings.ToLower(strings.TrimSpace(species))
	fromType = normalizeIDType(fromType)
	toType = normalizeIDType(toType)
	if species == "" || fromType == "" || toType == "" {
		return nil, fmt.Errorf("invalid idmap lookup args")
	}
	if len(fromIDs) == 0 {
		return map[string][]string{}, nil
	}

	canonicalFromType, direction, err := canonicalIDMapDirection(fromType, toType)
	if err != nil {
		return nil, err
	}

	const chunkSize = 400
	out := make(map[string][]string, len(fromIDs))
	for start := 0; start < len(fromIDs); start += chunkSize {
		end := start + chunkSize
		if end > len(fromIDs) {
			end = len(fromIDs)
		}
		chunk := fromIDs[start:end]
		placeholders := make([]string, len(chunk))
		args := make([]any, 0, 2+len(chunk))
		args = append(args, species, canonicalFromType)
		for i, id := range chunk {
			placeholders[i] = "?"
			args = append(args, strings.TrimSpace(id))
		}

		var q string
		if direction == idMapDirectionToEntrez {
			q = fmt.Sprintf(`
				SELECT from_id, entrez_id
				FROM idmap_canon
				WHERE species=? AND from_type=? AND from_id IN (%s)
			`, strings.Join(placeholders, ","))
		} else {
			q = fmt.Sprintf(`
				SELECT entrez_id, from_id
				FROM idmap_canon
				WHERE species=? AND from_type=? AND entrez_id IN (%s)
			`, strings.Join(placeholders, ","))
		}

		rows, err := s.db.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var fromID, toID string
			if err := rows.Scan(&fromID, &toID); err != nil {
				rows.Close()
				return nil, err
			}
			appendUniqueString(out, strings.TrimSpace(fromID), strings.TrimSpace(toID))
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return out, nil
}

// ScanIDMap loads all mappings for a species/fromType/toType pair.
func (s *SQLiteStore) ScanIDMap(ctx context.Context, species, fromType, toType string) (map[string][]string, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store not initialized")
	}
	species = strings.ToLower(strings.TrimSpace(species))
	fromType = normalizeIDType(fromType)
	toType = normalizeIDType(toType)
	if species == "" || fromType == "" || toType == "" {
		return nil, fmt.Errorf("invalid idmap scan args")
	}

	canonicalFromType, direction, err := canonicalIDMapDirection(fromType, toType)
	if err != nil {
		return nil, err
	}

	var (
		rows *sql.Rows
	)
	if direction == idMapDirectionToEntrez {
		rows, err = s.db.QueryContext(ctx, `
			SELECT from_id, entrez_id
			FROM idmap_canon
			WHERE species=? AND from_type=?
			ORDER BY from_id, entrez_id
		`, species, canonicalFromType)
	} else {
		rows, err = s.db.QueryContext(ctx, `
			SELECT entrez_id, from_id
			FROM idmap_canon
			WHERE species=? AND from_type=?
			ORDER BY entrez_id, from_id
		`, species, canonicalFromType)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string][]string)
	for rows.Next() {
		var fromID, toID string
		if err := rows.Scan(&fromID, &toID); err != nil {
			return nil, err
		}
		appendUniqueString(out, strings.TrimSpace(fromID), strings.TrimSpace(toID))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type idMapDirection int

const (
	idMapDirectionToEntrez idMapDirection = iota + 1
	idMapDirectionFromEntrez
)

func normalizeIDType(v string) string {
	return strings.ToUpper(strings.TrimSpace(v))
}

func canonicalIDMapDirection(fromType, toType string) (string, idMapDirection, error) {
	if fromType == toType {
		return "", 0, fmt.Errorf("unsupported idmap direction %s -> %s", fromType, toType)
	}
	if toType == entrezIDType {
		if fromType == entrezIDType {
			return "", 0, fmt.Errorf("unsupported idmap direction %s -> %s", fromType, toType)
		}
		return fromType, idMapDirectionToEntrez, nil
	}
	if fromType == entrezIDType {
		if toType == entrezIDType {
			return "", 0, fmt.Errorf("unsupported idmap direction %s -> %s", fromType, toType)
		}
		return toType, idMapDirectionFromEntrez, nil
	}
	return "", 0, fmt.Errorf("unsupported idmap direction %s -> %s (canonical storage requires ENTREZID)", fromType, toType)
}

func canonicalizeIDMapPair(direction idMapDirection, p IDMapRow) (fromID string, entrezID string) {
	left := strings.TrimSpace(p.From)
	right := strings.TrimSpace(p.To)
	switch direction {
	case idMapDirectionToEntrez:
		return left, right
	case idMapDirectionFromEntrez:
		return right, left
	default:
		return "", ""
	}
}

func appendUniqueString(dst map[string][]string, key, val string) {
	if key == "" || val == "" {
		return
	}
	existing := dst[key]
	for _, v := range existing {
		if v == val {
			return
		}
	}
	dst[key] = append(existing, val)
}
