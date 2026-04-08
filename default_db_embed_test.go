package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"enrichgo/pkg/store"
	"enrichgo/pkg/types"

	_ "modernc.org/sqlite"
)

func TestEmbeddedDefaultSQLiteManifestMatchesEmbeddedDB(t *testing.T) {
	manifest, err := embeddedDefaultSQLiteManifest()
	if err != nil {
		t.Fatalf("embeddedDefaultSQLiteManifest: %v", err)
	}
	if manifest.SHA256 != embeddedDefaultSQLiteSHA256() {
		t.Fatalf("manifest sha256=%s, embedded sha256=%s", manifest.SHA256, embeddedDefaultSQLiteSHA256())
	}
	if manifest.ContractProfile == "" {
		t.Fatalf("manifest contract_profile should not be empty")
	}
}

func TestEnsureEmbeddedDefaultSQLiteDBFile_InstallAndReuse(t *testing.T) {
	if len(embeddedDefaultSQLiteDB) == 0 {
		t.Fatal("embedded default sqlite db should not be empty")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "cache", "default_enrichgo.db")
	t.Setenv(envDefaultSQLitePath, path)

	got, err := ensureEmbeddedDefaultSQLiteDBFile()
	if err != nil {
		t.Fatalf("ensureEmbeddedDefaultSQLiteDBFile error: %v", err)
	}
	if got != path {
		t.Fatalf("path=%q, want %q", got, path)
	}

	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read installed db: %v", err)
	}
	if !bytes.Equal(first, embeddedDefaultSQLiteDB) {
		t.Fatal("installed db bytes do not match embedded bytes")
	}

	got2, err := ensureEmbeddedDefaultSQLiteDBFile()
	if err != nil {
		t.Fatalf("second ensureEmbeddedDefaultSQLiteDBFile error: %v", err)
	}
	if got2 != path {
		t.Fatalf("second path=%q, want %q", got2, path)
	}
	second, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read reused db: %v", err)
	}
	if !bytes.Equal(second, embeddedDefaultSQLiteDB) {
		t.Fatal("reused db bytes do not match embedded bytes")
	}
}

func TestEnsureEmbeddedDefaultSQLiteDBFile_RefreshWhenEmbeddedStateChanges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cache", "default_enrichgo.db")
	t.Setenv(envDefaultSQLitePath, path)

	if _, err := ensureEmbeddedDefaultSQLiteDBFile(); err != nil {
		t.Fatalf("initial ensure error: %v", err)
	}
	if err := os.WriteFile(path, []byte("old-db-content"), 0644); err != nil {
		t.Fatalf("write old db: %v", err)
	}
	if err := os.WriteFile(embeddedSQLiteStatePath(path), []byte("0000000000000000000000000000000000000000000000000000000000000000\n"), 0644); err != nil {
		t.Fatalf("write old state: %v", err)
	}

	if _, err := ensureEmbeddedDefaultSQLiteDBFile(); err != nil {
		t.Fatalf("refresh ensure error: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read refreshed db: %v", err)
	}
	if !bytes.Equal(got, embeddedDefaultSQLiteDB) {
		t.Fatal("db should be refreshed from embedded bytes when embedded state hash changes")
	}
	state := readEmbeddedSQLiteState(path)
	if state != embeddedDefaultSQLiteSHA256() {
		t.Fatalf("state=%q, want %q", state, embeddedDefaultSQLiteSHA256())
	}
}

func TestEnsureEmbeddedDefaultSQLiteDBFile_UserManagedCurrentSchemaNotOverwritten(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cache", "default_enrichgo.db")
	t.Setenv(envDefaultSQLitePath, path)

	if _, err := ensureEmbeddedDefaultSQLiteDBFile(); err != nil {
		t.Fatalf("initial ensure error: %v", err)
	}
	markSQLiteDBAsUserManaged(path)

	st, err := store.OpenSQLite(path)
	if err != nil {
		t.Fatalf("OpenSQLite user-managed db: %v", err)
	}
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer writeCancel()
	sets := types.GeneSets{
		&types.GeneSet{ID: "user:set", Name: "User Set", Description: "custom", Genes: map[string]bool{"1": true}},
	}
	if err := st.ReplaceGeneSets(writeCtx, store.GeneSetFilter{DB: "reactome", Species: "hsa"}, "SYMBOL", sets, "user-v1"); err != nil {
		st.Close()
		t.Fatalf("ReplaceGeneSets: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close user-managed db: %v", err)
	}

	if _, err := ensureEmbeddedDefaultSQLiteDBFile(); err != nil {
		t.Fatalf("second ensure error: %v", err)
	}

	st, err = store.OpenSQLite(path)
	if err != nil {
		t.Fatalf("reopen user-managed db: %v", err)
	}
	defer st.Close()
	readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer readCancel()
	loaded, _, err := st.LoadGeneSets(readCtx, store.GeneSetFilter{DB: "reactome", Species: "hsa"})
	if err != nil {
		t.Fatalf("LoadGeneSets: %v", err)
	}
	found := false
	for _, gs := range loaded {
		if gs != nil && gs.ID == "user:set" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("user-managed current-schema db should not be overwritten")
	}
}

func TestEnsureEmbeddedDefaultSQLiteDBFile_UserManagedLegacySchemaReplaced(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cache", "default_enrichgo.db")
	t.Setenv(envDefaultSQLitePath, path)

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	createLegacySchemaDB(t, path)
	markSQLiteDBAsUserManaged(path)

	if _, err := ensureEmbeddedDefaultSQLiteDBFile(); err != nil {
		t.Fatalf("ensureEmbeddedDefaultSQLiteDBFile error: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read replaced db: %v", err)
	}
	if !bytes.Equal(got, embeddedDefaultSQLiteDB) {
		t.Fatal("legacy user-managed db should be replaced by embedded bytes")
	}
	if state := readEmbeddedSQLiteState(path); state != embeddedDefaultSQLiteSHA256() {
		t.Fatalf("state=%q, want %q", state, embeddedDefaultSQLiteSHA256())
	}
}

func TestEmbeddedDefaultSQLiteDBFile_ContractAndDataAvailable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cache", "default_enrichgo.db")
	t.Setenv(envDefaultSQLitePath, path)

	installedPath, err := ensureEmbeddedDefaultSQLiteDBFile()
	if err != nil {
		t.Fatalf("ensureEmbeddedDefaultSQLiteDBFile: %v", err)
	}
	manifest, err := embeddedDefaultSQLiteManifest()
	if err != nil {
		t.Fatalf("embeddedDefaultSQLiteManifest: %v", err)
	}
	st, err := store.OpenSQLite(installedPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	report, err := st.AuditWithContract(ctx, manifest.ContractProfile)
	if err != nil {
		t.Fatalf("AuditWithContract: %v", err)
	}
	if !report.ContractValid {
		t.Fatalf("embedded DB contract invalid: %v", report.ContractViolations)
	}
	keggSets, _, err := st.LoadGeneSets(ctx, store.GeneSetFilter{DB: "kegg", Species: "hsa"})
	if err != nil {
		t.Fatalf("LoadGeneSets kegg: %v", err)
	}
	if len(keggSets) == 0 {
		t.Fatal("embedded DB should provide at least one KEGG gene set")
	}
}

func TestEmbeddedDefaultSQLiteCLI_ORASmoke(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "ora.tsv")
	runCLIWithEmbeddedDefault(t,
		"analyze", "ora",
		"-i", "test-data/DE_results.csv",
		"-d", "kegg",
		"-s", "hsa",
		"--split-by-direction=false",
		"--auto-update-db=false",
		"-o", outPath,
	)
	assertNonEmptyOutputFile(t, outPath)
}

func TestEmbeddedDefaultSQLiteCLI_GSEASmoke(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "gsea.tsv")
	runCLIWithEmbeddedDefault(t,
		"analyze", "gsea",
		"-i", "test-data/DE_results.csv",
		"-d", "go",
		"-s", "hsa",
		"--auto-update-db=false",
		"-nPerm", "20",
		"-o", outPath,
	)
	assertNonEmptyOutputFile(t, outPath)
}

func TestCLIHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{"enrichgo"}, os.Args[i+1:]...)
			main()
			os.Exit(0)
		}
	}
	os.Exit(2)
}

func TestFileSHA256(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.txt")
	data := []byte("abc")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	got, err := fileSHA256(path)
	if err != nil {
		t.Fatalf("fileSHA256: %v", err)
	}
	sum := sha256.Sum256(data)
	want := hex.EncodeToString(sum[:])
	if got != want {
		t.Fatalf("sha256=%s, want %s", got, want)
	}
}

func TestBuildDownloadUpdateArgs(t *testing.T) {
	tests := []struct {
		name    string
		opts    dbUpdateOptions
		want    []string
		wantErr bool
	}{
		{
			name: "kegg no idmaps",
			opts: dbUpdateOptions{Database: "kegg", Species: "hsa", DBPath: "/tmp/a.db", WithIDMaps: false},
			want: []string{"data", "sync", "-d", "kegg", "-s", "hsa", "--db", "/tmp/a.db", "--db-only", "--idmaps=false"},
		},
		{
			name: "go with idmaps default level",
			opts: dbUpdateOptions{Database: "go", Species: "mmu", Ontology: "BP", DBPath: "/tmp/b.db", WithIDMaps: true},
			want: []string{"data", "sync", "-d", "go", "-s", "mmu", "--db", "/tmp/b.db", "--db-only", "-ont", "BP", "--idmaps=true", "--idmaps-level", "basic"},
		},
		{
			name: "msigdb with extended idmaps",
			opts: dbUpdateOptions{Database: "msigdb", Species: "hsa", Collection: "c2", DBPath: "/tmp/c.db", WithIDMaps: true, IDMapsLevel: "EXTENDED"},
			want: []string{"data", "sync", "-d", "msigdb", "-s", "hsa", "--db", "/tmp/c.db", "--db-only", "-c", "c2", "--idmaps=true", "--idmaps-level", "extended"},
		},
		{
			name:    "custom not supported",
			opts:    dbUpdateOptions{Database: "custom", Species: "hsa", DBPath: "/tmp/d.db"},
			wantErr: true,
		},
		{
			name:    "empty db path",
			opts:    dbUpdateOptions{Database: "kegg", Species: "hsa", DBPath: ""},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildDownloadUpdateArgs(tc.opts)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("args mismatch\n got=%v\nwant=%v", got, tc.want)
			}
		})
	}
}

func createLegacySchemaDB(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS meta (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);`,
		`DELETE FROM meta WHERE key='schema_version';`,
		`INSERT INTO meta(key, value) VALUES('schema_version', '1');`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}
}

func runCLIWithEmbeddedDefault(t *testing.T, args ...string) {
	t.Helper()
	runtimeDBPath := filepath.Join(t.TempDir(), "runtime-default.db")
	cmdArgs := append([]string{"-test.run=TestCLIHelperProcess", "--"}, args...)
	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(),
		"GO_WANT_HELPER_PROCESS=1",
		"ENRICHGO_DEFAULT_DB_PATH="+runtimeDBPath,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("CLI failed: %v\n%s", err, string(output))
	}
}

func assertNonEmptyOutputFile(t *testing.T, path string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if len(bytes.TrimSpace(content)) == 0 {
		t.Fatalf("output file is empty: %s", path)
	}
}
