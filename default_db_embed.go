package main

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

const (
	envDefaultSQLitePath          = "ENRICHGO_DEFAULT_DB_PATH"
	embeddedSQLiteStateFileSuffix = ".embed.sha256"
)

var sha256HexPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

//go:embed assets/default_enrichgo.db
var embeddedDefaultSQLiteDB []byte

//go:embed assets/default_enrichgo.db.manifest.json
var embeddedDefaultSQLiteManifestJSON []byte

type embeddedSQLiteManifest struct {
	SchemaVersion   int    `json:"schema_version"`
	Artifact        string `json:"artifact"`
	SHA256          string `json:"sha256"`
	ContractProfile string `json:"contract_profile"`
	Species         string `json:"species"`
	IDMapsLevel     string `json:"idmaps_level"`
}

var (
	embeddedManifestOnce sync.Once
	embeddedManifest     *embeddedSQLiteManifest
	embeddedManifestErr  error
)

func embeddedDefaultSQLiteSHA256() string {
	sum := sha256.Sum256(embeddedDefaultSQLiteDB)
	return hex.EncodeToString(sum[:])
}

func embeddedDefaultSQLiteManifest() (*embeddedSQLiteManifest, error) {
	embeddedManifestOnce.Do(func() {
		raw := bytes.TrimSpace(embeddedDefaultSQLiteManifestJSON)
		if len(raw) == 0 {
			embeddedManifestErr = fmt.Errorf("embedded manifest is empty")
			return
		}
		var m embeddedSQLiteManifest
		if err := json.Unmarshal(raw, &m); err != nil {
			embeddedManifestErr = fmt.Errorf("decode embedded manifest: %w", err)
			return
		}
		m.SHA256 = strings.ToLower(strings.TrimSpace(m.SHA256))
		if !sha256HexPattern.MatchString(m.SHA256) {
			embeddedManifestErr = fmt.Errorf("invalid manifest sha256 %q", m.SHA256)
			return
		}
		m.ContractProfile = strings.TrimSpace(m.ContractProfile)
		if m.ContractProfile == "" {
			embeddedManifestErr = fmt.Errorf("manifest contract_profile is empty")
			return
		}
		embeddedManifest = &m
	})
	if embeddedManifestErr != nil {
		return nil, embeddedManifestErr
	}
	out := *embeddedManifest
	return &out, nil
}

func verifyEmbeddedDefaultSQLiteContract() error {
	m, err := embeddedDefaultSQLiteManifest()
	if err != nil {
		return err
	}
	embeddedSHA := embeddedDefaultSQLiteSHA256()
	if m.SHA256 != embeddedSHA {
		return fmt.Errorf("embedded db sha256 %s does not match manifest %s", embeddedSHA, m.SHA256)
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open file for sha256: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("stream file for sha256: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
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
	if err := verifyEmbeddedDefaultSQLiteContract(); err != nil {
		return "", fmt.Errorf("verify embedded sqlite manifest: %w", err)
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
				fileSHA, shaErr := fileSHA256(path)
				if shaErr == nil && fileSHA == embeddedSHA {
					return path, nil
				}
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
	writtenSHA, err := fileSHA256(path)
	if err != nil {
		return "", fmt.Errorf("verify installed embedded sqlite file: %w", err)
	}
	if writtenSHA != embeddedSHA {
		_ = os.Remove(path)
		return "", fmt.Errorf("installed embedded sqlite sha256 %s does not match expected %s", writtenSHA, embeddedSHA)
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

	args := []string{"data", "sync", "-d", database, "-s", strings.TrimSpace(opts.Species), "--db", dbPath, "--db-only"}
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
