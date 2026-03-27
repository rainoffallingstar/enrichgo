package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	envDefaultSQLitePath          = "ENRICHGO_DEFAULT_DB_PATH"
	embeddedSQLiteStateFileSuffix = ".embed.sha256"
)

var sha256HexPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

//go:embed assets/default_enrichgo.db
var embeddedDefaultSQLiteDB []byte

func embeddedDefaultSQLiteSHA256() string {
	sum := sha256.Sum256(embeddedDefaultSQLiteDB)
	return hex.EncodeToString(sum[:])
}

func defaultSQLiteRuntimePath() (string, error) {
	if p := strings.TrimSpace(os.Getenv(envDefaultSQLitePath)); p != "" {
		return p, nil
	}
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("resolve user cache dir: %w", err)
	}
	return filepath.Join(cacheDir, "enrichgo", "default_enrichgo.db"), nil
}

func embeddedSQLiteStatePath(dbPath string) string {
	return dbPath + embeddedSQLiteStateFileSuffix
}

func readEmbeddedSQLiteState(dbPath string) string {
	b, err := os.ReadFile(embeddedSQLiteStatePath(dbPath))
	if err != nil {
		return ""
	}
	v := strings.ToLower(strings.TrimSpace(string(b)))
	if !sha256HexPattern.MatchString(v) {
		return ""
	}
	return v
}

func writeEmbeddedSQLiteState(dbPath, sha string) {
	if !sha256HexPattern.MatchString(sha) {
		return
	}
	statePath := embeddedSQLiteStatePath(dbPath)
	tmp := statePath + ".tmp"
	if err := os.WriteFile(tmp, []byte(sha+"\n"), 0644); err != nil {
		return
	}
	if err := os.Rename(tmp, statePath); err != nil {
		_ = os.Remove(tmp)
	}
}

func markSQLiteDBAsUserManaged(dbPath string) {
	_ = os.Remove(embeddedSQLiteStatePath(dbPath))
}

func ensureEmbeddedDefaultSQLiteDBFile() (string, error) {
	if len(embeddedDefaultSQLiteDB) == 0 {
		return "", fmt.Errorf("embedded default sqlite db is empty")
	}

	path, err := defaultSQLiteRuntimePath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return "", fmt.Errorf("create sqlite cache dir: %w", err)
	}

	embeddedSHA := embeddedDefaultSQLiteSHA256()

	if fi, err := os.Stat(path); err == nil {
		if fi.Size() > 0 {
			existingState := readEmbeddedSQLiteState(path)
			if existingState == "" {
				// No embed marker means this DB is managed by user/update flow; keep it.
				return path, nil
			}
			if existingState == embeddedSHA {
				return path, nil
			}
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("stat sqlite db path: %w", err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, embeddedDefaultSQLiteDB, 0644); err != nil {
		return "", fmt.Errorf("write embedded sqlite temp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("install embedded sqlite file: %w", err)
	}
	writeEmbeddedSQLiteState(path, embeddedSHA)
	return path, nil
}

type dbUpdateOptions struct {
	Database    string
	Species     string
	Ontology    string
	Collection  string
	DBPath      string
	WithIDMaps  bool
	IDMapsLevel string
}

func runDownloadUpdateForDB(opts dbUpdateOptions) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable path: %w", err)
	}

	args, err := buildDownloadUpdateArgs(opts)
	if err != nil {
		return err
	}

	cmd := exec.Command(exe, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("download update failed: %w", err)
	}
	markSQLiteDBAsUserManaged(strings.TrimSpace(opts.DBPath))
	return nil
}

func buildDownloadUpdateArgs(opts dbUpdateOptions) ([]string, error) {
	dbPath := strings.TrimSpace(opts.DBPath)
	if dbPath == "" {
		return nil, fmt.Errorf("empty db path for update")
	}
	database := strings.ToLower(strings.TrimSpace(opts.Database))
	if database == "custom" {
		return nil, fmt.Errorf("--update-db is not supported for custom database")
	}
	if database == "" {
		return nil, fmt.Errorf("empty database for update")
	}

	args := []string{"download", "-d", database, "-s", strings.TrimSpace(opts.Species), "--db", dbPath, "--db-only"}
	if database == "go" {
		args = append(args, "-ont", strings.TrimSpace(opts.Ontology))
	}
	if database == "msigdb" {
		args = append(args, "-c", strings.TrimSpace(opts.Collection))
	}
	if opts.WithIDMaps {
		args = append(args, "--idmaps=true")
		lvl := strings.ToLower(strings.TrimSpace(opts.IDMapsLevel))
		if lvl == "" {
			lvl = "basic"
		}
		args = append(args, "--idmaps-level", lvl)
	} else {
		args = append(args, "--idmaps=false")
	}
	return args, nil
}
