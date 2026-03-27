package main

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

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

func TestEnsureEmbeddedDefaultSQLiteDBFile_UserManagedNotOverwritten(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cache", "default_enrichgo.db")
	t.Setenv(envDefaultSQLitePath, path)

	if _, err := ensureEmbeddedDefaultSQLiteDBFile(); err != nil {
		t.Fatalf("initial ensure error: %v", err)
	}
	markSQLiteDBAsUserManaged(path)

	custom := []byte("custom-user-managed-db")
	if err := os.WriteFile(path, custom, 0644); err != nil {
		t.Fatalf("write custom db: %v", err)
	}

	if _, err := ensureEmbeddedDefaultSQLiteDBFile(); err != nil {
		t.Fatalf("second ensure error: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read db: %v", err)
	}
	if !bytes.Equal(got, custom) {
		t.Fatal("user-managed db should not be overwritten")
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
			want: []string{"download", "-d", "kegg", "-s", "hsa", "--db", "/tmp/a.db", "--db-only", "--idmaps=false"},
		},
		{
			name: "go with idmaps default level",
			opts: dbUpdateOptions{Database: "go", Species: "mmu", Ontology: "BP", DBPath: "/tmp/b.db", WithIDMaps: true},
			want: []string{"download", "-d", "go", "-s", "mmu", "--db", "/tmp/b.db", "--db-only", "-ont", "BP", "--idmaps=true", "--idmaps-level", "basic"},
		},
		{
			name: "msigdb with extended idmaps",
			opts: dbUpdateOptions{Database: "msigdb", Species: "hsa", Collection: "c2", DBPath: "/tmp/c.db", WithIDMaps: true, IDMapsLevel: "EXTENDED"},
			want: []string{"download", "-d", "msigdb", "-s", "hsa", "--db", "/tmp/c.db", "--db-only", "-c", "c2", "--idmaps=true", "--idmaps-level", "extended"},
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
