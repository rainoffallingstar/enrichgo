package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type IDMapEmit func(from, to string) error

const (
	defaultIDMapStreamStageBatchSize = 20000
	idMapStageCleanupTimeout         = 30 * time.Second
)

var idMapStreamStageBatchSize = defaultIDMapStreamStageBatchSize

// ReplaceIDMapStream replaces id mappings for (species, source, fromType, toType) using a streaming producer.
// It is intended for very large mapping sources (NCBI/UniProt), avoiding holding all pairs in memory.
//
// Canonical storage rule:
// - only mappings in canonical direction (X -> ENTREZID) are stored in idmap_canon.
// - when caller provides ENTREZID -> X, pairs are automatically reversed before persistence.
//
// Atomicity guarantee:
// - producer rows are durably buffered in idmap_stage with chunked commits.
// - old idmap_canon rows are replaced only in a short final transaction.
// - on producer/insert error old data is preserved.
func (s *SQLiteStore) ReplaceIDMapStream(ctx context.Context, species, source, fromType, toType string, produce func(emit IDMapEmit) error) (retErr error) {
	if s == nil || s.db == nil {
		return fmt.Errorf("store not initialized")
	}
	species = strings.ToLower(strings.TrimSpace(species))
	source = strings.TrimSpace(source)
	fromType = normalizeIDType(fromType)
	toType = normalizeIDType(toType)
	if species == "" || source == "" || fromType == "" || toType == "" {
		return fmt.Errorf("invalid idmap stream args")
	}

	canonicalFromType, direction, err := canonicalIDMapDirection(fromType, toType)
	if err != nil {
		return err
	}

	if err := s.ensureIDMapStageTable(ctx); err != nil {
		return err
	}
	if err := s.cleanupIDMapStageScope(ctx, species, source, canonicalFromType); err != nil {
		return err
	}

	runID := fmt.Sprintf("%s|%s|%s|%d", species, source, canonicalFromType, time.Now().UTC().UnixNano())
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), idMapStageCleanupTimeout)
		defer cancel()
		if err := s.cleanupIDMapStageRun(cleanupCtx, runID); err != nil {
			if retErr == nil {
				retErr = fmt.Errorf("cleanup idmap stage: %w", err)
				return
			}
			retErr = fmt.Errorf("%v; cleanup idmap stage: %w", retErr, err)
		}
	}()

	batchSize := idMapStreamStageBatchSize
	if batchSize <= 0 {
		batchSize = defaultIDMapStreamStageBatchSize
	}
	now := time.Now().UTC().Format(time.RFC3339)
	buffer := make([]IDMapRow, 0, batchSize)

	flush := func() error {
		if len(buffer) == 0 {
			return nil
		}
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		stmt, err := tx.PrepareContext(ctx, `
			INSERT OR IGNORE INTO idmap_stage (run_id, species, source, from_type, from_id, entrez_id, downloaded_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, row := range buffer {
			if _, err := stmt.ExecContext(ctx, runID, species, source, canonicalFromType, row.From, row.To, now); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		buffer = buffer[:0]
		return nil
	}

	emit := func(from, to string) error {
		canonFrom, canonEntrez := canonicalizeIDMapPair(direction, IDMapRow{From: from, To: to})
		if canonFrom == "" || canonEntrez == "" || canonFrom == canonEntrez {
			return nil
		}
		buffer = append(buffer, IDMapRow{From: canonFrom, To: canonEntrez})
		if len(buffer) < batchSize {
			return nil
		}
		return flush()
	}

	if err := produce(emit); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	return s.finalizeIDMapStage(ctx, runID, species, source, canonicalFromType)
}

func (s *SQLiteStore) ensureIDMapStageTable(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS idmap_stage (
			run_id TEXT NOT NULL,
			species TEXT NOT NULL,
			source TEXT NOT NULL,
			from_type TEXT NOT NULL,
			from_id TEXT NOT NULL,
			entrez_id TEXT NOT NULL,
			downloaded_at TEXT NOT NULL,
			PRIMARY KEY (run_id, species, source, from_type, from_id, entrez_id)
		)
	`); err != nil {
		return fmt.Errorf("ensure idmap_stage table: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS idx_idmap_stage_scope
		ON idmap_stage (species, source, from_type)
	`); err != nil {
		return fmt.Errorf("ensure idmap_stage index: %w", err)
	}
	return nil
}

func (s *SQLiteStore) cleanupIDMapStageScope(ctx context.Context, species, source, fromType string) error {
	if _, err := s.db.ExecContext(ctx, `
		DELETE FROM idmap_stage
		WHERE species=? AND source=? AND from_type=?
	`, species, source, fromType); err != nil {
		return fmt.Errorf("cleanup idmap_stage scope: %w", err)
	}
	return nil
}

func (s *SQLiteStore) cleanupIDMapStageRun(ctx context.Context, runID string) error {
	if strings.TrimSpace(runID) == "" {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM idmap_stage WHERE run_id=?`, runID); err != nil {
		return fmt.Errorf("cleanup idmap_stage run: %w", err)
	}
	return nil
}

func (s *SQLiteStore) finalizeIDMapStage(ctx context.Context, runID, species, source, canonicalFromType string) error {
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

	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO idmap_canon (species, from_type, from_id, entrez_id, source, downloaded_at)
		SELECT species, from_type, from_id, entrez_id, source, downloaded_at
		FROM idmap_stage
		WHERE run_id=?
	`, runID); err != nil {
		return err
	}
	return tx.Commit()
}
