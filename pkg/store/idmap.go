package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

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
	fromType = strings.TrimSpace(fromType)
	toType = strings.TrimSpace(toType)
	if species == "" || source == "" || fromType == "" || toType == "" {
		return fmt.Errorf("invalid idmap replace args")
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM idmap WHERE species=? AND source=? AND from_type=? AND to_type=?`,
		species, source, fromType, toType,
	); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO idmap (species, from_type, from_id, to_type, to_id, source, downloaded_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, p := range pairs {
		from := strings.TrimSpace(p.From)
		to := strings.TrimSpace(p.To)
		if from == "" || to == "" || from == to {
			continue
		}
		if _, err := stmt.ExecContext(ctx, species, fromType, from, toType, to, source, now); err != nil {
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
	fromType = strings.TrimSpace(fromType)
	toType = strings.TrimSpace(toType)
	if species == "" || fromType == "" || toType == "" {
		return nil, fmt.Errorf("invalid idmap lookup args")
	}
	if len(fromIDs) == 0 {
		return map[string][]string{}, nil
	}

	// SQLite has a bound-variable limit; chunk to be safe.
	const chunkSize = 400
	out := make(map[string][]string, len(fromIDs))
	for start := 0; start < len(fromIDs); start += chunkSize {
		end := start + chunkSize
		if end > len(fromIDs) {
			end = len(fromIDs)
		}
		chunk := fromIDs[start:end]
		placeholders := make([]string, len(chunk))
		args := make([]any, 0, 3+len(chunk))
		args = append(args, species, fromType, toType)
		for i, id := range chunk {
			placeholders[i] = "?"
			args = append(args, id)
		}
		q := fmt.Sprintf(`
			SELECT from_id, to_id
			FROM idmap
			WHERE species=? AND from_type=? AND to_type=? AND from_id IN (%s)
		`, strings.Join(placeholders, ","))
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
			out[fromID] = append(out[fromID], toID)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return out, nil
}

