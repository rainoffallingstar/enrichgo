package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteStore stores enrichgo caches (gene sets + ID mappings) in a single SQLite file.
// It uses the pure-Go modernc.org/sqlite driver so the binary can remain CGO-free.
type SQLiteStore struct {
	db *sql.DB
}

func OpenSQLite(path string) (*SQLiteStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("empty sqlite path")
	}
	if path != ":memory:" && !strings.HasPrefix(path, "file:") {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return nil, err
		}
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	s := &SQLiteStore{db: db}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.init(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLiteStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *SQLiteStore) init(ctx context.Context) error {
	if s.db == nil {
		return errors.New("nil db")
	}

	stmts := []string{
		`PRAGMA foreign_keys = ON;`,
		`PRAGMA busy_timeout = 10000;`,
		`CREATE TABLE IF NOT EXISTS geneset (
			db TEXT NOT NULL,
			species TEXT NOT NULL,
			ontology TEXT NOT NULL,
			collection TEXT NOT NULL,
			set_id TEXT NOT NULL,
			name TEXT NOT NULL,
			description TEXT NOT NULL,
			version TEXT NOT NULL,
			downloaded_at TEXT NOT NULL,
			PRIMARY KEY (db, species, ontology, collection, set_id)
		);`,
		`CREATE TABLE IF NOT EXISTS geneset_gene (
			db TEXT NOT NULL,
			species TEXT NOT NULL,
			ontology TEXT NOT NULL,
			collection TEXT NOT NULL,
			set_id TEXT NOT NULL,
			gene_id TEXT NOT NULL,
			gene_id_type TEXT NOT NULL,
			PRIMARY KEY (db, species, ontology, collection, set_id, gene_id, gene_id_type)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_geneset_gene_lookup
			ON geneset_gene (db, species, ontology, collection, set_id);`,
		`CREATE INDEX IF NOT EXISTS idx_geneset_gene_by_gene
			ON geneset_gene (db, species, ontology, collection, gene_id, gene_id_type);`,
		`CREATE TABLE IF NOT EXISTS idmap (
			species TEXT NOT NULL,
			from_type TEXT NOT NULL,
			from_id TEXT NOT NULL,
			to_type TEXT NOT NULL,
			to_id TEXT NOT NULL,
			source TEXT NOT NULL,
			downloaded_at TEXT NOT NULL,
			PRIMARY KEY (species, from_type, from_id, to_type, to_id, source)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_idmap_lookup
			ON idmap (species, from_type, to_type, from_id);`,
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT NOT NULL PRIMARY KEY,
			value TEXT NOT NULL
		);`,
	}

	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlite init failed: %w", err)
		}
	}
	return nil
}

func (s *SQLiteStore) DB() *sql.DB {
	return s.db
}

