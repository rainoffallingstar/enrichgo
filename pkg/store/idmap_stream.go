package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type IDMapEmit func(from, to string) error

// ReplaceIDMapStream replaces id mappings for (species, source, fromType, toType) using a streaming producer.
// It is intended for very large mapping sources (NCBI/UniProt), avoiding holding all pairs in memory.
func (s *SQLiteStore) ReplaceIDMapStream(ctx context.Context, species, source, fromType, toType string, produce func(emit IDMapEmit) error) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store not initialized")
	}
	species = strings.ToLower(strings.TrimSpace(species))
	source = strings.TrimSpace(source)
	fromType = strings.TrimSpace(fromType)
	toType = strings.TrimSpace(toType)
	if species == "" || source == "" || fromType == "" || toType == "" {
		return fmt.Errorf("invalid idmap stream args")
	}

	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM idmap WHERE species=? AND source=? AND from_type=? AND to_type=?`,
		species, source, fromType, toType,
	); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)

	const batchSize = 50000
	var tx *sql.Tx
	var stmt *sql.Stmt
	rowsInBatch := 0

	openBatch := func() error {
		var err error
		tx, err = s.db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			return err
		}
		stmt, err = tx.PrepareContext(ctx, `
			INSERT OR IGNORE INTO idmap (species, from_type, from_id, to_type, to_id, source, downloaded_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			_ = tx.Rollback()
			tx = nil
			return err
		}
		rowsInBatch = 0
		return nil
	}

	commitBatch := func() error {
		if stmt != nil {
			_ = stmt.Close()
			stmt = nil
		}
		if tx != nil {
			if err := tx.Commit(); err != nil {
				_ = tx.Rollback()
				tx = nil
				return err
			}
			tx = nil
		}
		return nil
	}

	if err := openBatch(); err != nil {
		return err
	}
	defer func() { _ = commitBatch() }()

	emit := func(from, to string) error {
		from = strings.TrimSpace(from)
		to = strings.TrimSpace(to)
		if from == "" || to == "" || from == to {
			return nil
		}
		if _, err := stmt.ExecContext(ctx, species, fromType, from, toType, to, source, now); err != nil {
			return err
		}
		rowsInBatch++
		if rowsInBatch >= batchSize {
			if err := commitBatch(); err != nil {
				return err
			}
			return openBatch()
		}
		return nil
	}

	if err := produce(emit); err != nil {
		return err
	}
	return commitBatch()
}

