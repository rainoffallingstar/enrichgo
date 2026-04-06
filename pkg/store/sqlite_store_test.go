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

	if err := st.ReplaceGeneSets(ctx, GeneSetFilter{DB: "kegg", Species: "hsa"}, "ENTREZID", sets, "v2"); err != nil {
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

func TestSQLiteStore_IDMapLookupForwardAndReverse(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pairs := []IDMapRow{{From: "TP53", To: "7157"}, {From: "EGFR", To: "1956"}}
	if err := st.ReplaceIDMap(ctx, "hsa", "test", "SYMBOL", "ENTREZID", pairs); err != nil {
		t.Fatalf("ReplaceIDMap SYMBOL->ENTREZID: %v", err)
	}

	forward, err := st.LookupIDMap(ctx, "hsa", "SYMBOL", "ENTREZID", []string{"TP53", "EGFR", "MISSING"})
	if err != nil {
		t.Fatalf("LookupIDMap forward: %v", err)
	}
	if forward["TP53"][0] != "7157" {
		t.Fatalf("TP53 -> %v, want 7157", forward["TP53"])
	}
	if forward["EGFR"][0] != "1956" {
		t.Fatalf("EGFR -> %v, want 1956", forward["EGFR"])
	}
	if _, ok := forward["MISSING"]; ok {
		t.Fatalf("expected no key for MISSING, got %v", forward["MISSING"])
	}

	reverse, err := st.LookupIDMap(ctx, "hsa", "ENTREZID", "SYMBOL", []string{"7157", "1956", "0"})
	if err != nil {
		t.Fatalf("LookupIDMap reverse: %v", err)
	}
	if reverse["7157"][0] != "TP53" {
		t.Fatalf("7157 -> %v, want TP53", reverse["7157"])
	}
	if reverse["1956"][0] != "EGFR" {
		t.Fatalf("1956 -> %v, want EGFR", reverse["1956"])
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

	pairs := []IDMapRow{{From: "A", To: "1"}, {From: "A", To: "2"}, {From: "B", To: "3"}}
	if err := st.ReplaceIDMap(ctx, "hsa", "scan_test", "SYMBOL", "ENTREZID", pairs); err != nil {
		t.Fatalf("ReplaceIDMap: %v", err)
	}

	got, err := st.ScanIDMap(ctx, "hsa", "SYMBOL", "ENTREZID")
	if err != nil {
		t.Fatalf("ScanIDMap forward: %v", err)
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

	reverse, err := st.ScanIDMap(ctx, "hsa", "ENTREZID", "SYMBOL")
	if err != nil {
		t.Fatalf("ScanIDMap reverse: %v", err)
	}
	if len(reverse["1"]) == 0 || reverse["1"][0] != "A" {
		t.Fatalf("1 -> %v want [A]", reverse["1"])
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

func TestSQLiteStore_RejectsFutureSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "future.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}

	mustExecSQL(t, db, testDDLMeta)
	if _, err := db.Exec("INSERT INTO meta(key, value) VALUES(?, ?)", "schema_version", "999"); err != nil {
		t.Fatalf("insert schema_version: %v", err)
	}
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

func TestSQLiteStore_RejectsOlderSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "old.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}

	mustExecSQL(t, db, testDDLMeta)
	if _, err := db.Exec("INSERT INTO meta(key, value) VALUES(?, ?)", "schema_version", "1"); err != nil {
		t.Fatalf("insert schema_version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	_, err = OpenSQLite(dbPath)
	if err == nil {
		t.Fatalf("OpenSQLite expected error for old schema version")
	}
	if !strings.Contains(err.Error(), "no longer supported") {
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
	mustExecSQL(t, db, testDDLDataset, testDDLTerm, testDDLGeneDict, testDDLTermGene, testDDLIDMapCanon, testDDLMeta)
	if _, err := db.Exec("INSERT INTO meta(key, value) VALUES(?, ?)", "schema_version", "2"); err != nil {
		t.Fatalf("insert schema_version: %v", err)
	}
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

	for _, idx := range []string{"idx_dataset_lookup", "idx_term_gene_by_term", "idx_term_gene_by_gene", "idx_idmap_canon_forward", "idx_idmap_canon_reverse"} {
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
		testDDLDataset,
		testDDLTerm,
		testDDLGeneDict,
		testDDLTermGene,
		testDDLIDMapCanon,
		testDDLMeta,
		testDDLIdxDatasetLookup,
		testDDLIdxTermGeneByTerm,
		testDDLIdxTermGeneByGene,
		testDDLIdxIDMapCanonForward,
		"CREATE INDEX idx_idmap_canon_reverse ON idmap_canon (species, entrez_id);",
	)
	if _, err := db.Exec("INSERT INTO meta(key, value) VALUES(?, ?)", "schema_version", "2"); err != nil {
		t.Fatalf("insert schema_version: %v", err)
	}
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
	badDataset := strings.Replace(testDDLDataset, "version TEXT NOT NULL,", "version INTEGER NOT NULL,", 1)
	createV2SchemaWithCustomTables(t, dbPath, badDataset, testDDLTerm, testDDLGeneDict, testDDLTermGene, testDDLIDMapCanon, []string{
		testDDLIdxDatasetLookup,
		testDDLIdxTermGeneByTerm,
		testDDLIdxTermGeneByGene,
		testDDLIdxIDMapCanonForward,
		testDDLIdxIDMapCanonReverse,
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
	badIDMap := strings.Replace(testDDLIDMapCanon, "source TEXT NOT NULL,", "source TEXT,", 1)
	createV2SchemaWithCustomTables(t, dbPath, testDDLDataset, testDDLTerm, testDDLGeneDict, testDDLTermGene, badIDMap, []string{
		testDDLIdxDatasetLookup,
		testDDLIdxTermGeneByTerm,
		testDDLIdxTermGeneByGene,
		testDDLIdxIDMapCanonForward,
		testDDLIdxIDMapCanonReverse,
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
	badTermGene := strings.Replace(testDDLTermGene, "PRIMARY KEY (dataset_id, term_id, gene_pk)", "PRIMARY KEY (dataset_id, term_id)", 1)
	createV2SchemaWithCustomTables(t, dbPath, testDDLDataset, testDDLTerm, testDDLGeneDict, badTermGene, testDDLIDMapCanon, []string{
		testDDLIdxDatasetLookup,
		testDDLIdxTermGeneByTerm,
		testDDLIdxTermGeneByGene,
		testDDLIdxIDMapCanonForward,
		testDDLIdxIDMapCanonReverse,
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
	err := db.QueryRow("SELECT 1 FROM sqlite_master WHERE type=? AND name=?", "index", name).Scan(&exists)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		t.Fatalf("check index %s: %v", name, err)
	}
	return true
}

func createV2SchemaWithCustomTables(
	t *testing.T,
	dbPath, datasetDDL, termDDL, geneDictDDL, termGeneDDL, idmapCanonDDL string,
	indexDDLs []string,
) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	stmts := []string{datasetDDL, termDDL, geneDictDDL, termGeneDDL, idmapCanonDDL, testDDLMeta}
	stmts = append(stmts, indexDDLs...)
	mustExecSQL(t, db, stmts...)
	if _, err := db.Exec("INSERT INTO meta(key, value) VALUES(?, ?)", "schema_version", "2"); err != nil {
		t.Fatalf("insert schema_version: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
}

const testDDLDataset = `CREATE TABLE dataset (
	id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	db TEXT NOT NULL,
	species TEXT NOT NULL,
	ontology TEXT NOT NULL,
	collection TEXT NOT NULL,
	gene_id_type TEXT NOT NULL,
	version TEXT NOT NULL,
	downloaded_at TEXT NOT NULL
);`

const testDDLTerm = `CREATE TABLE term (
	dataset_id INTEGER NOT NULL,
	term_id TEXT NOT NULL,
	name TEXT NOT NULL,
	description TEXT NOT NULL,
	PRIMARY KEY (dataset_id, term_id)
);`

const testDDLGeneDict = `CREATE TABLE gene_dict (
	gene_pk INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	gene_id TEXT NOT NULL UNIQUE
);`

const testDDLTermGene = `CREATE TABLE term_gene (
	dataset_id INTEGER NOT NULL,
	term_id TEXT NOT NULL,
	gene_pk INTEGER NOT NULL,
	PRIMARY KEY (dataset_id, term_id, gene_pk)
);`

const testDDLIDMapCanon = `CREATE TABLE idmap_canon (
	species TEXT NOT NULL,
	from_type TEXT NOT NULL,
	from_id TEXT NOT NULL,
	entrez_id TEXT NOT NULL,
	source TEXT NOT NULL,
	downloaded_at TEXT NOT NULL,
	PRIMARY KEY (species, from_type, from_id, entrez_id, source)
);`

const testDDLMeta = `CREATE TABLE meta (
	key TEXT NOT NULL PRIMARY KEY,
	value TEXT NOT NULL
);`

const testDDLIdxDatasetLookup = `CREATE UNIQUE INDEX idx_dataset_lookup
	ON dataset (db, species, ontology, collection);`

const testDDLIdxTermGeneByTerm = `CREATE INDEX idx_term_gene_by_term
	ON term_gene (dataset_id, term_id);`

const testDDLIdxTermGeneByGene = `CREATE INDEX idx_term_gene_by_gene
	ON term_gene (dataset_id, gene_pk);`

const testDDLIdxIDMapCanonForward = `CREATE INDEX idx_idmap_canon_forward
	ON idmap_canon (species, from_type, from_id);`

const testDDLIdxIDMapCanonReverse = `CREATE INDEX idx_idmap_canon_reverse
	ON idmap_canon (species, from_type, entrez_id);`

func TestSQLiteStore_IDMapStreamStageCleanedAfterError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := st.ReplaceIDMap(ctx, "hsa", "stream_stage_err", "SYMBOL", "ENTREZID", []IDMapRow{{From: "OLD", To: "1"}}); err != nil {
		t.Fatalf("seed ReplaceIDMap: %v", err)
	}

	oldBatchSize := idMapStreamStageBatchSize
	idMapStreamStageBatchSize = 2
	defer func() { idMapStreamStageBatchSize = oldBatchSize }()

	err = st.ReplaceIDMapStream(ctx, "hsa", "stream_stage_err", "SYMBOL", "ENTREZID", func(emit IDMapEmit) error {
		if err := emit("NEW1", "2"); err != nil {
			return err
		}
		if err := emit("NEW2", "3"); err != nil {
			return err
		}
		return fmt.Errorf("stream failed after chunk commit")
	})
	if err == nil {
		t.Fatalf("ReplaceIDMapStream expected error, got nil")
	}
	if !strings.Contains(err.Error(), "stream failed") {
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
	if _, ok := got["NEW1"]; ok {
		t.Fatalf("unexpected NEW1 mapping after failed stream: %v", got["NEW1"])
	}

	if n := idMapStageRowCount(t, st); n != 0 {
		t.Fatalf("idmap_stage rows=%d want 0", n)
	}
}

func TestSQLiteStore_IDMapStreamChunkedWrites(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := st.ReplaceIDMap(ctx, "hsa", "stream_chunk", "SYMBOL", "ENTREZID", []IDMapRow{{From: "OLD", To: "1"}}); err != nil {
		t.Fatalf("seed ReplaceIDMap: %v", err)
	}

	oldBatchSize := idMapStreamStageBatchSize
	idMapStreamStageBatchSize = 3
	defer func() { idMapStreamStageBatchSize = oldBatchSize }()

	err = st.ReplaceIDMapStream(ctx, "hsa", "stream_chunk", "SYMBOL", "ENTREZID", func(emit IDMapEmit) error {
		for i := 0; i < 10; i++ {
			from := fmt.Sprintf("G%d", i)
			to := fmt.Sprintf("%d", i+100)
			if err := emit(from, to); err != nil {
				return err
			}
		}
		if err := emit("G0", "100"); err != nil {
			return err
		}
		if err := emit("", "100"); err != nil {
			return err
		}
		if err := emit("SAME", "SAME"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ReplaceIDMapStream: %v", err)
	}

	got, err := st.ScanIDMap(ctx, "hsa", "SYMBOL", "ENTREZID")
	if err != nil {
		t.Fatalf("ScanIDMap: %v", err)
	}
	if _, ok := got["OLD"]; ok {
		t.Fatalf("old mapping should be replaced, got OLD=%v", got["OLD"])
	}
	if len(got) != 10 {
		t.Fatalf("len(got)=%d want 10", len(got))
	}
	for i := 0; i < 10; i++ {
		from := fmt.Sprintf("G%d", i)
		want := fmt.Sprintf("%d", i+100)
		vals := got[from]
		if len(vals) != 1 || vals[0] != want {
			t.Fatalf("%s -> %v want [%s]", from, vals, want)
		}
	}

	if n := idMapStageRowCount(t, st); n != 0 {
		t.Fatalf("idmap_stage rows=%d want 0", n)
	}
}

func idMapStageRowCount(t *testing.T, st *SQLiteStore) int64 {
	t.Helper()
	if st == nil || st.db == nil {
		t.Fatalf("store not initialized")
	}
	exists, err := tableExists(context.Background(), st.db, "idmap_stage")
	if err != nil {
		t.Fatalf("tableExists idmap_stage: %v", err)
	}
	if !exists {
		return 0
	}
	var n int64
	if err := st.db.QueryRow("SELECT COUNT(*) FROM idmap_stage").Scan(&n); err != nil {
		t.Fatalf("count idmap_stage: %v", err)
	}
	return n
}

func TestSQLiteStore_CountIDMapScope(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pairs := []IDMapRow{{From: "TP53", To: "7157"}, {From: "EGFR", To: "1956"}, {From: "TP53", To: "7157"}}
	if err := st.ReplaceIDMap(ctx, "hsa", "scope_test", "SYMBOL", "ENTREZID", pairs); err != nil {
		t.Fatalf("ReplaceIDMap: %v", err)
	}

	n, err := st.CountIDMapScope(ctx, "hsa", "scope_test", "SYMBOL", "ENTREZID")
	if err != nil {
		t.Fatalf("CountIDMapScope forward: %v", err)
	}
	if n != 2 {
		t.Fatalf("CountIDMapScope forward=%d want 2", n)
	}

	n, err = st.CountIDMapScope(ctx, "hsa", "scope_test", "ENTREZID", "SYMBOL")
	if err != nil {
		t.Fatalf("CountIDMapScope reverse: %v", err)
	}
	if n != 2 {
		t.Fatalf("CountIDMapScope reverse=%d want 2", n)
	}
}
