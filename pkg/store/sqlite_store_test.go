package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"enrichgo/pkg/types"
)

func TestSQLiteStore_GeneSetsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	sets := types.GeneSets{
		&types.GeneSet{ID: "p1", Name: "Pathway 1", Description: "d1", Genes: map[string]bool{"1": true, "2": true}},
		&types.GeneSet{ID: "p2", Name: "Pathway 2", Description: "d2", Genes: map[string]bool{"3": true}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := st.ReplaceGeneSets(ctx, GeneSetFilter{DB: "kegg", Species: "hsa"}, "ENTREZID", sets, "v1"); err != nil {
		t.Fatalf("ReplaceGeneSets: %v", err)
	}

	got, geneIDType, err := st.LoadGeneSets(ctx, GeneSetFilter{DB: "kegg", Species: "hsa"})
	if err != nil {
		t.Fatalf("LoadGeneSets: %v", err)
	}
	if geneIDType != "ENTREZID" {
		t.Fatalf("geneIDType=%q want %q", geneIDType, "ENTREZID")
	}
	if len(got) != 2 {
		t.Fatalf("len(got)=%d want 2", len(got))
	}
	if !got[0].Genes["1"] || !got[0].Genes["2"] {
		t.Fatalf("missing genes in set %q", got[0].ID)
	}
}

func TestSQLiteStore_IDMapLookup(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pairs := []IDMapRow{
		{From: "TP53", To: "7157"},
		{From: "EGFR", To: "1956"},
	}
	if err := st.ReplaceIDMap(ctx, "hsa", "test", "SYMBOL", "ENTREZID", pairs); err != nil {
		t.Fatalf("ReplaceIDMap: %v", err)
	}

	m, err := st.LookupIDMap(ctx, "hsa", "SYMBOL", "ENTREZID", []string{"TP53", "EGFR", "MISSING"})
	if err != nil {
		t.Fatalf("LookupIDMap: %v", err)
	}
	if m["TP53"][0] != "7157" {
		t.Fatalf("TP53 -> %v, want 7157", m["TP53"])
	}
	if m["EGFR"][0] != "1956" {
		t.Fatalf("EGFR -> %v, want 1956", m["EGFR"])
	}
	if _, ok := m["MISSING"]; ok {
		t.Fatalf("expected no key for MISSING, got %v", m["MISSING"])
	}
}

func TestSQLiteStore_ScanIDMap(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pairs := []IDMapRow{
		{From: "A", To: "1"},
		{From: "A", To: "2"},
		{From: "B", To: "3"},
	}
	if err := st.ReplaceIDMap(ctx, "hsa", "scan_test", "SYMBOL", "ENTREZID", pairs); err != nil {
		t.Fatalf("ReplaceIDMap: %v", err)
	}

	got, err := st.ScanIDMap(ctx, "hsa", "SYMBOL", "ENTREZID")
	if err != nil {
		t.Fatalf("ScanIDMap: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(got)=%d want 2", len(got))
	}
	if len(got["A"]) != 2 {
		t.Fatalf("A -> %v want 2 targets", got["A"])
	}
	if len(got["B"]) != 1 || got["B"][0] != "3" {
		t.Fatalf("B -> %v want [3]", got["B"])
	}
}

func TestSQLiteStore_IDMapStreamRollbackOnProducerError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := st.ReplaceIDMap(ctx, "hsa", "stream_test", "SYMBOL", "ENTREZID", []IDMapRow{{From: "OLD", To: "1"}}); err != nil {
		t.Fatalf("seed ReplaceIDMap: %v", err)
	}

	err = st.ReplaceIDMapStream(ctx, "hsa", "stream_test", "SYMBOL", "ENTREZID", func(emit IDMapEmit) error {
		if err := emit("NEW", "2"); err != nil {
			return err
		}
		return fmt.Errorf("producer failed")
	})
	if err == nil {
		t.Fatalf("ReplaceIDMapStream expected error, got nil")
	}
	if !strings.Contains(err.Error(), "producer failed") {
		t.Fatalf("unexpected stream error: %v", err)
	}

	got, err := st.ScanIDMap(ctx, "hsa", "SYMBOL", "ENTREZID")
	if err != nil {
		t.Fatalf("ScanIDMap: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(got)=%d want 1", len(got))
	}
	if len(got["OLD"]) != 1 || got["OLD"][0] != "1" {
		t.Fatalf("OLD -> %v want [1]", got["OLD"])
	}
	if _, ok := got["NEW"]; ok {
		t.Fatalf("unexpected NEW mapping after rollback: %v", got["NEW"])
	}
}

func TestSQLiteStore_SchemaVersionInitialized(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	version, err := st.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if version != CurrentSchemaVersion {
		t.Fatalf("schema version=%d want %d", version, CurrentSchemaVersion)
	}
}

func TestSQLiteStore_BackfillsLegacySchemaVersion(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "legacy.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	stmts := []string{testDDLGeneset, testDDLGenesetGene, testDDLIDMap}
	mustExecSQL(t, db, stmts...)

	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite legacy: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	version, err := st.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if version != CurrentSchemaVersion {
		t.Fatalf("schema version=%d want %d", version, CurrentSchemaVersion)
	}
}

func TestSQLiteStore_RejectsIncompatibleLegacySchema(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "legacy_bad.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}

	badGeneset := strings.Replace(testDDLGeneset, "version TEXT NOT NULL,", "", 1)
	mustExecSQL(t, db, badGeneset, testDDLGenesetGene, testDDLIDMap)
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	_, err = OpenSQLite(dbPath)
	if err == nil {
		t.Fatalf("OpenSQLite expected error for incompatible legacy schema")
	}
	if !strings.Contains(err.Error(), "legacy schema is incompatible") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSQLiteStore_RejectsFutureSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "future.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}

	mustExecSQL(t, db,
		testDDLMeta,
		`INSERT INTO meta(key, value) VALUES('schema_version', '999');`,
	)
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	_, err = OpenSQLite(dbPath)
	if err == nil {
		t.Fatalf("OpenSQLite expected error for future schema version")
	}
	if !strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSQLiteStore_RepairsMissingSecondaryIndexes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "missing_indexes.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	mustExecSQL(t, db,
		testDDLGeneset,
		testDDLGenesetGene,
		testDDLIDMap,
		testDDLMeta,
		`INSERT INTO meta(key, value) VALUES('schema_version', '1');`,
	)
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite should auto-repair missing indexes: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open verify: %v", err)
	}
	defer db.Close()

	for _, idx := range []string{"idx_geneset_gene_lookup", "idx_geneset_gene_by_gene", "idx_idmap_lookup"} {
		if !indexExists(t, db, idx) {
			t.Fatalf("expected repaired index %s to exist", idx)
		}
	}
}

func TestSQLiteStore_RejectsIndexDefinitionMismatch(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "bad_index.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	mustExecSQL(t, db,
		testDDLGeneset,
		testDDLGenesetGene,
		testDDLIDMap,
		testDDLMeta,
		testDDLIdxGenesetGeneLookup,
		testDDLIdxGenesetGeneByGene,
		`CREATE INDEX idx_idmap_lookup ON idmap (species, from_type, from_id);`,
		`INSERT INTO meta(key, value) VALUES('schema_version', '1');`,
	)
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	_, err = OpenSQLite(dbPath)
	if err == nil {
		t.Fatalf("OpenSQLite expected index definition mismatch error")
	}
	if !strings.Contains(err.Error(), "columns mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSQLiteStore_RejectsSchemaColumnTypeMismatch(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "bad_type.db")
	badGeneset := strings.Replace(testDDLGeneset, "version TEXT NOT NULL,", "version INTEGER NOT NULL,", 1)
	createV1SchemaWithCustomTables(t, dbPath, badGeneset, testDDLGenesetGene, testDDLIDMap, []string{
		testDDLIdxGenesetGeneLookup,
		testDDLIdxGenesetGeneByGene,
		testDDLIdxIDMapLookup,
	})

	_, err := OpenSQLite(dbPath)
	if err == nil {
		t.Fatalf("OpenSQLite expected type mismatch error")
	}
	if !strings.Contains(err.Error(), "type mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSQLiteStore_RejectsSchemaColumnNotNullMismatch(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "bad_notnull.db")
	badIDMap := strings.Replace(testDDLIDMap, "source TEXT NOT NULL,", "source TEXT,", 1)
	createV1SchemaWithCustomTables(t, dbPath, testDDLGeneset, testDDLGenesetGene, badIDMap, []string{
		testDDLIdxGenesetGeneLookup,
		testDDLIdxGenesetGeneByGene,
		testDDLIdxIDMapLookup,
	})

	_, err := OpenSQLite(dbPath)
	if err == nil {
		t.Fatalf("OpenSQLite expected NOT NULL mismatch error")
	}
	if !strings.Contains(err.Error(), "NOT NULL mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSQLiteStore_RejectsSchemaPrimaryKeyMismatch(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "bad_pk.db")
	badGeneset := strings.Replace(
		testDDLGeneset,
		"PRIMARY KEY (db, species, ontology, collection, set_id)",
		"PRIMARY KEY (db, species, ontology, set_id)",
		1,
	)
	createV1SchemaWithCustomTables(t, dbPath, badGeneset, testDDLGenesetGene, testDDLIDMap, []string{
		testDDLIdxGenesetGeneLookup,
		testDDLIdxGenesetGeneByGene,
		testDDLIdxIDMapLookup,
	})

	_, err := OpenSQLite(dbPath)
	if err == nil {
		t.Fatalf("OpenSQLite expected primary-key mismatch error")
	}
	if !strings.Contains(err.Error(), "primary-key position mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func mustExecSQL(t *testing.T, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec failed: %v\nSQL:\n%s", err, stmt)
		}
	}
}

func indexExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var exists int
	err := db.QueryRow(`
		SELECT 1
		FROM sqlite_master
		WHERE type='index' AND name=?
	`, name).Scan(&exists)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		t.Fatalf("check index %s: %v", name, err)
	}
	return true
}

func createV1SchemaWithCustomTables(t *testing.T, dbPath, genesetDDL, genesetGeneDDL, idmapDDL string, indexDDLs []string) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	stmts := []string{genesetDDL, genesetGeneDDL, idmapDDL, testDDLMeta}
	stmts = append(stmts, indexDDLs...)
	stmts = append(stmts, `INSERT INTO meta(key, value) VALUES('schema_version', '1');`)
	mustExecSQL(t, db, stmts...)
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
}

const testDDLGeneset = `CREATE TABLE geneset (
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
);`

const testDDLGenesetGene = `CREATE TABLE geneset_gene (
	db TEXT NOT NULL,
	species TEXT NOT NULL,
	ontology TEXT NOT NULL,
	collection TEXT NOT NULL,
	set_id TEXT NOT NULL,
	gene_id TEXT NOT NULL,
	gene_id_type TEXT NOT NULL,
	PRIMARY KEY (db, species, ontology, collection, set_id, gene_id, gene_id_type)
);`

const testDDLIDMap = `CREATE TABLE idmap (
	species TEXT NOT NULL,
	from_type TEXT NOT NULL,
	from_id TEXT NOT NULL,
	to_type TEXT NOT NULL,
	to_id TEXT NOT NULL,
	source TEXT NOT NULL,
	downloaded_at TEXT NOT NULL,
	PRIMARY KEY (species, from_type, from_id, to_type, to_id, source)
);`

const testDDLMeta = `CREATE TABLE meta (
	key TEXT NOT NULL PRIMARY KEY,
	value TEXT NOT NULL
);`

const testDDLIdxGenesetGeneLookup = `CREATE INDEX idx_geneset_gene_lookup
	ON geneset_gene (db, species, ontology, collection, set_id);`

const testDDLIdxGenesetGeneByGene = `CREATE INDEX idx_geneset_gene_by_gene
	ON geneset_gene (db, species, ontology, collection, gene_id, gene_id_type);`

const testDDLIdxIDMapLookup = `CREATE INDEX idx_idmap_lookup
	ON idmap (species, from_type, to_type, from_id);`
