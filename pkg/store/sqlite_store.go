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
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlite init failed: %w", err)
		}
	}
	if err := applySchema(ctx, s.db); err != nil {
		return fmt.Errorf("sqlite init failed: %w", err)
	}
	return nil
}
