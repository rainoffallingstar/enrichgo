package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"enrichgo/pkg/store"
)

func TestBuildEmbeddedDBDeterministic(t *testing.T) {
	repoRoot := findRepoRoot(t)
	firstDir := filepath.Join(t.TempDir(), "first")
	secondDir := filepath.Join(t.TempDir(), "second")

	firstSHA, firstManifest := buildEmbeddedDBForTest(t, repoRoot, firstDir, "embedded-hsa-basic")
	secondSHA, secondManifest := buildEmbeddedDBForTest(t, repoRoot, secondDir, "embedded-hsa-basic")

	if firstSHA != secondSHA {
		t.Fatalf("embedded db sha mismatch across rebuilds: first=%s second=%s", firstSHA, secondSHA)
	}
	if firstManifest.SHA256 != firstSHA {
		t.Fatalf("first manifest sha=%s want %s", firstManifest.SHA256, firstSHA)
	}
	if secondManifest.SHA256 != secondSHA {
		t.Fatalf("second manifest sha=%s want %s", secondManifest.SHA256, secondSHA)
	}

	firstManifestBytes, err := os.ReadFile(filepath.Join(firstDir, "default_enrichgo.db.manifest.json"))
	if err != nil {
		t.Fatalf("read first manifest: %v", err)
	}
	secondManifestBytes, err := os.ReadFile(filepath.Join(secondDir, "default_enrichgo.db.manifest.json"))
	if err != nil {
		t.Fatalf("read second manifest: %v", err)
	}
	if string(firstManifestBytes) != string(secondManifestBytes) {
		t.Fatal("manifest payload should be deterministic across rebuilds")
	}
}

func TestNormalizeEmbeddedSQLiteSetsStableMetadata(t *testing.T) {
	repoRoot := findRepoRoot(t)
	outDir := t.TempDir()
	contractProfile := "embedded-hsa-basic"
	dbPath := filepath.Join(outDir, "default_enrichgo.db")

	sha, manifest := buildEmbeddedDBForTest(t, repoRoot, outDir, contractProfile)
	if manifest.SHA256 != sha {
		t.Fatalf("manifest sha=%s want %s", manifest.SHA256, sha)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	assertAllRowsEqual(t, ctx, db, `SELECT COUNT(*) FROM dataset WHERE downloaded_at != ?`, embeddedBuildTimestamp)
	assertAllRowsEqual(t, ctx, db, `SELECT COUNT(*) FROM idmap_canon WHERE downloaded_at != ?`, embeddedBuildTimestamp)
	assertAllRowsEqual(t, ctx, db, `SELECT COUNT(*) FROM dataset WHERE version != ?`, contractProfile)

	st, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("store.OpenSQLite: %v", err)
	}
	defer st.Close()
	report, err := st.AuditWithContract(ctx, contractProfile)
	if err != nil {
		t.Fatalf("AuditWithContract: %v", err)
	}
	if !report.ContractValid {
		t.Fatalf("contract invalid: %v", report.ContractViolations)
	}
}

func buildEmbeddedDBForTest(t *testing.T, repoRoot, outDir, contractProfile string) (string, manifest) {
	t.Helper()
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir out dir: %v", err)
	}

	dbPath := filepath.Join(outDir, "default_enrichgo.db")
	manifestPath := filepath.Join(outDir, "default_enrichgo.db.manifest.json")
	artifactPath := "assets/default_enrichgo.db"
	dataDir := filepath.Join(repoRoot, "data")

	if err := os.RemoveAll(dbPath); err != nil {
		t.Fatalf("remove existing db: %v", err)
	}
	st, err := store.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("store.OpenSQLite: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := writeKEGG(ctx, st, dataDir, "hsa", contractProfile); err != nil {
		_ = st.Close()
		t.Fatalf("writeKEGG: %v", err)
	}
	if err := writeGO(ctx, st, dataDir, "hsa", "BP", contractProfile); err != nil {
		_ = st.Close()
		t.Fatalf("writeGO: %v", err)
	}
	if err := writeBasicIDMap(ctx, st, filepath.Join(dataDir, "kegg_hsa_idmap.tsv"), "hsa"); err != nil {
		_ = st.Close()
		t.Fatalf("writeBasicIDMap: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}
	if err := normalizeEmbeddedSQLite(dbPath, contractProfile); err != nil {
		t.Fatalf("normalizeEmbeddedSQLite: %v", err)
	}

	sha, err := fileSHA256(dbPath)
	if err != nil {
		t.Fatalf("fileSHA256: %v", err)
	}
	m := manifest{
		SchemaVersion:   store.CurrentSchemaVersion,
		Artifact:        artifactPath,
		SHA256:          sha,
		ContractProfile: contractProfile,
		Species:         "hsa",
		IDMapsLevel:     "basic",
	}
	payload, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	payload = append(payload, byte('\n'))
	if err := os.WriteFile(manifestPath, payload, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return sha, m
}

func assertAllRowsEqual(t *testing.T, ctx context.Context, db *sql.DB, query, want string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, query, want).Scan(&count); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	if count != 0 {
		t.Fatalf("query %q returned %d mismatched rows for %q", query, count, want)
	}
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cur := wd
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(cur, "go.mod")); err == nil {
			return cur
		}
		next := filepath.Dir(cur)
		if next == cur {
			break
		}
		cur = next
	}
	t.Fatalf("repo root not found from %s", wd)
	return ""
}
