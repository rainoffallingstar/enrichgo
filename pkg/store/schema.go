package store

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

const (
	CurrentSchemaVersion = 2
	schemaVersionKey     = "schema_version"
)

type schemaMigration struct {
	version    int
	statements []string
}

type columnSpec struct {
	Type    string
	NotNull bool
	PKPos   int
}

type indexSpec struct {
	Table     string
	Columns   []string
	Unique    bool
	CreateSQL string
}

type tableColumnInfo struct {
	Type    string
	NotNull bool
	PKPos   int
}

type indexDefinitionInfo struct {
	Table   string
	Columns []string
	Unique  bool
}

var schemaV2CoreTableOrder = []string{"dataset", "term", "gene_dict", "term_gene", "idmap_canon"}

var schemaV2CoreTableSpecs = map[string]map[string]columnSpec{
	"dataset": {
		"id":            {Type: "INTEGER", NotNull: true, PKPos: 1},
		"db":            {Type: "TEXT", NotNull: true, PKPos: 0},
		"species":       {Type: "TEXT", NotNull: true, PKPos: 0},
		"ontology":      {Type: "TEXT", NotNull: true, PKPos: 0},
		"collection":    {Type: "TEXT", NotNull: true, PKPos: 0},
		"gene_id_type":  {Type: "TEXT", NotNull: true, PKPos: 0},
		"version":       {Type: "TEXT", NotNull: true, PKPos: 0},
		"downloaded_at": {Type: "TEXT", NotNull: true, PKPos: 0},
	},
	"term": {
		"dataset_id":  {Type: "INTEGER", NotNull: true, PKPos: 1},
		"term_id":     {Type: "TEXT", NotNull: true, PKPos: 2},
		"name":        {Type: "TEXT", NotNull: true, PKPos: 0},
		"description": {Type: "TEXT", NotNull: true, PKPos: 0},
	},
	"gene_dict": {
		"gene_pk": {Type: "INTEGER", NotNull: true, PKPos: 1},
		"gene_id": {Type: "TEXT", NotNull: true, PKPos: 0},
	},
	"term_gene": {
		"dataset_id": {Type: "INTEGER", NotNull: true, PKPos: 1},
		"term_id":    {Type: "TEXT", NotNull: true, PKPos: 2},
		"gene_pk":    {Type: "INTEGER", NotNull: true, PKPos: 3},
	},
	"idmap_canon": {
		"species":       {Type: "TEXT", NotNull: true, PKPos: 1},
		"from_type":     {Type: "TEXT", NotNull: true, PKPos: 2},
		"from_id":       {Type: "TEXT", NotNull: true, PKPos: 3},
		"entrez_id":     {Type: "TEXT", NotNull: true, PKPos: 4},
		"source":        {Type: "TEXT", NotNull: true, PKPos: 5},
		"downloaded_at": {Type: "TEXT", NotNull: true, PKPos: 0},
	},
}

var schemaV2MetaTableOrder = []string{"meta"}

var schemaV2MetaTableSpecs = map[string]map[string]columnSpec{
	"meta": {
		"key":   {Type: "TEXT", NotNull: true, PKPos: 1},
		"value": {Type: "TEXT", NotNull: true, PKPos: 0},
	},
}

var schemaV2IndexOrder = []string{
	"idx_dataset_lookup",
	"idx_term_gene_by_term",
	"idx_term_gene_by_gene",
	"idx_idmap_canon_forward",
	"idx_idmap_canon_reverse",
}

var schemaV2IndexSpecs = map[string]indexSpec{
	"idx_dataset_lookup": {
		Table:   "dataset",
		Columns: []string{"db", "species", "ontology", "collection"},
		Unique:  true,
		CreateSQL: `CREATE UNIQUE INDEX IF NOT EXISTS idx_dataset_lookup
			ON dataset (db, species, ontology, collection);`,
	},
	"idx_term_gene_by_term": {
		Table:   "term_gene",
		Columns: []string{"dataset_id", "term_id"},
		Unique:  false,
		CreateSQL: `CREATE INDEX IF NOT EXISTS idx_term_gene_by_term
			ON term_gene (dataset_id, term_id);`,
	},
	"idx_term_gene_by_gene": {
		Table:   "term_gene",
		Columns: []string{"dataset_id", "gene_pk"},
		Unique:  false,
		CreateSQL: `CREATE INDEX IF NOT EXISTS idx_term_gene_by_gene
			ON term_gene (dataset_id, gene_pk);`,
	},
	"idx_idmap_canon_forward": {
		Table:   "idmap_canon",
		Columns: []string{"species", "from_type", "from_id"},
		Unique:  false,
		CreateSQL: `CREATE INDEX IF NOT EXISTS idx_idmap_canon_forward
			ON idmap_canon (species, from_type, from_id);`,
	},
	"idx_idmap_canon_reverse": {
		Table:   "idmap_canon",
		Columns: []string{"species", "from_type", "entrez_id"},
		Unique:  false,
		CreateSQL: `CREATE INDEX IF NOT EXISTS idx_idmap_canon_reverse
			ON idmap_canon (species, from_type, entrez_id);`,
	},
}

var schemaMigrations = []schemaMigration{
	{
		version: CurrentSchemaVersion,
		statements: []string{
			`CREATE TABLE IF NOT EXISTS dataset (
				id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
				db TEXT NOT NULL,
				species TEXT NOT NULL,
				ontology TEXT NOT NULL,
				collection TEXT NOT NULL,
				gene_id_type TEXT NOT NULL,
				version TEXT NOT NULL,
				downloaded_at TEXT NOT NULL
			);`,
			`CREATE TABLE IF NOT EXISTS term (
				dataset_id INTEGER NOT NULL,
				term_id TEXT NOT NULL,
				name TEXT NOT NULL,
				description TEXT NOT NULL,
				PRIMARY KEY (dataset_id, term_id),
				FOREIGN KEY (dataset_id) REFERENCES dataset(id) ON DELETE CASCADE
			);`,
			`CREATE TABLE IF NOT EXISTS gene_dict (
				gene_pk INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
				gene_id TEXT NOT NULL UNIQUE
			);`,
			`CREATE TABLE IF NOT EXISTS term_gene (
				dataset_id INTEGER NOT NULL,
				term_id TEXT NOT NULL,
				gene_pk INTEGER NOT NULL,
				PRIMARY KEY (dataset_id, term_id, gene_pk),
				FOREIGN KEY (dataset_id, term_id) REFERENCES term(dataset_id, term_id) ON DELETE CASCADE,
				FOREIGN KEY (gene_pk) REFERENCES gene_dict(gene_pk) ON DELETE CASCADE
			);`,
			`CREATE TABLE IF NOT EXISTS idmap_canon (
				species TEXT NOT NULL,
				from_type TEXT NOT NULL,
				from_id TEXT NOT NULL,
				entrez_id TEXT NOT NULL,
				source TEXT NOT NULL,
				downloaded_at TEXT NOT NULL,
				PRIMARY KEY (species, from_type, from_id, entrez_id, source)
			);`,
			schemaV2IndexSpecs["idx_dataset_lookup"].CreateSQL,
			schemaV2IndexSpecs["idx_term_gene_by_term"].CreateSQL,
			schemaV2IndexSpecs["idx_term_gene_by_gene"].CreateSQL,
			schemaV2IndexSpecs["idx_idmap_canon_forward"].CreateSQL,
			schemaV2IndexSpecs["idx_idmap_canon_reverse"].CreateSQL,
		},
	},
}

func applySchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("nil db")
	}
	if err := ensureMetaTable(ctx, db); err != nil {
		return err
	}

	version, hasVersion, err := readSchemaVersion(ctx, db)
	if err != nil {
		return err
	}

	if hasVersion {
		if version < 0 {
			return fmt.Errorf("invalid schema version %d", version)
		}
		if version > CurrentSchemaVersion {
			return fmt.Errorf("db schema version %d is newer than supported %d", version, CurrentSchemaVersion)
		}
		if version < CurrentSchemaVersion {
			return fmt.Errorf("db schema version %d is no longer supported; rebuild database with enrichgo data sync", version)
		}
		if err := ensureSchemaIndexes(ctx, db, version); err != nil {
			return err
		}
		if err := validateSchemaForVersion(ctx, db, version, true); err != nil {
			return err
		}
		return nil
	}

	hasUserTables, err := hasUnversionedUserTables(ctx, db)
	if err != nil {
		return err
	}
	if hasUserTables {
		return fmt.Errorf("unversioned sqlite schema is unsupported; rebuild database with enrichgo data sync")
	}

	for _, migration := range schemaMigrations {
		for _, stmt := range migration.statements {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("apply schema v%d: %w", migration.version, err)
			}
		}
		if err := writeSchemaVersion(ctx, db, migration.version); err != nil {
			return err
		}
	}

	if err := ensureSchemaIndexes(ctx, db, CurrentSchemaVersion); err != nil {
		return err
	}
	if err := validateSchemaForVersion(ctx, db, CurrentSchemaVersion, true); err != nil {
		return err
	}
	return nil
}

func hasUnversionedUserTables(ctx context.Context, db *sql.DB) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM sqlite_master
		WHERE type='table'
		  AND name NOT LIKE 'sqlite_%'
		  AND name <> 'meta'
	`).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("check unversioned tables: %w", err)
	}
	return n > 0, nil
}

func ensureSchemaIndexes(ctx context.Context, db *sql.DB, version int) error {
	switch version {
	case 2:
		return ensureIndexes(ctx, db, schemaV2IndexOrder, schemaV2IndexSpecs)
	default:
		return fmt.Errorf("unsupported schema version %d", version)
	}
}

func ensureMetaTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS meta (
		key TEXT NOT NULL PRIMARY KEY,
		value TEXT NOT NULL
	);`)
	if err != nil {
		return fmt.Errorf("ensure meta table: %w", err)
	}
	return nil
}

func readSchemaVersion(ctx context.Context, db *sql.DB) (int, bool, error) {
	var raw string
	err := db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key=?`, schemaVersionKey).Scan(&raw)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read schema version: %w", err)
	}
	version, convErr := strconv.Atoi(raw)
	if convErr != nil {
		return 0, true, fmt.Errorf("invalid schema version %q: %w", raw, convErr)
	}
	return version, true, nil
}

func writeSchemaVersion(ctx context.Context, db *sql.DB, version int) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO meta (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value
	`, schemaVersionKey, strconv.Itoa(version))
	if err != nil {
		return fmt.Errorf("write schema version: %w", err)
	}
	return nil
}

func tableExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var exists int
	err := db.QueryRowContext(ctx, `
		SELECT 1
		FROM sqlite_master
		WHERE type='table' AND name=?
	`, name).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check table %s: %w", name, err)
	}
	return true, nil
}

func validateSchemaForVersion(ctx context.Context, db *sql.DB, version int, strict bool) error {
	switch version {
	case 2:
		return validateSchemaV2(ctx, db, strict)
	default:
		return fmt.Errorf("unsupported schema version %d", version)
	}
}

func validateSchemaV2(ctx context.Context, db *sql.DB, strict bool) error {
	if err := validateSchemaV2CoreTables(ctx, db); err != nil {
		return err
	}
	if !strict {
		return nil
	}
	if err := validateSchemaV2MetaTable(ctx, db); err != nil {
		return err
	}
	if err := validateSchemaV2Indexes(ctx, db); err != nil {
		return err
	}
	return nil
}

func validateSchemaV2CoreTables(ctx context.Context, db *sql.DB) error {
	return validateTableSpecs(ctx, db, schemaV2CoreTableOrder, schemaV2CoreTableSpecs)
}

func validateSchemaV2MetaTable(ctx context.Context, db *sql.DB) error {
	return validateTableSpecs(ctx, db, schemaV2MetaTableOrder, schemaV2MetaTableSpecs)
}

func validateSchemaV2Indexes(ctx context.Context, db *sql.DB) error {
	return validateIndexes(ctx, db, schemaV2IndexOrder, schemaV2IndexSpecs)
}

func validateTableSpecs(ctx context.Context, db *sql.DB, tableOrder []string, specs map[string]map[string]columnSpec) error {
	for _, table := range tableOrder {
		exists, err := tableExists(ctx, db, table)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("missing required table %q", table)
		}

		actual, err := tableColumns(ctx, db, table)
		if err != nil {
			return err
		}

		required := specs[table]
		for colName, colSpec := range required {
			col, ok := actual[colName]
			if !ok {
				return fmt.Errorf("table %q missing required column %q", table, colName)
			}
			if !sameType(col.Type, colSpec.Type) {
				return fmt.Errorf("table %q column %q type mismatch: got %q want %q", table, colName, col.Type, colSpec.Type)
			}
			if col.NotNull != colSpec.NotNull {
				return fmt.Errorf("table %q column %q NOT NULL mismatch: got %v want %v", table, colName, col.NotNull, colSpec.NotNull)
			}
			if col.PKPos != colSpec.PKPos {
				return fmt.Errorf("table %q column %q primary-key position mismatch: got %d want %d", table, colName, col.PKPos, colSpec.PKPos)
			}
		}
	}
	return nil
}

func tableColumns(ctx context.Context, db *sql.DB, table string) (map[string]tableColumnInfo, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, quoteIdent(table)))
	if err != nil {
		return nil, fmt.Errorf("read table info %s: %w", table, err)
	}
	defer rows.Close()

	cols := make(map[string]tableColumnInfo)
	for rows.Next() {
		var (
			cid        int
			name       string
			typ        string
			notNull    int
			defaultVal any
			pk         int
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultVal, &pk); err != nil {
			return nil, fmt.Errorf("scan table info %s: %w", table, err)
		}
		cols[name] = tableColumnInfo{
			Type:    typ,
			NotNull: notNull != 0,
			PKPos:   pk,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("table info rows %s: %w", table, err)
	}
	return cols, nil
}

func validateIndexes(ctx context.Context, db *sql.DB, indexOrder []string, specs map[string]indexSpec) error {
	for _, name := range indexOrder {
		expected := specs[name]
		actual, exists, err := readIndexDefinition(ctx, db, name)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("missing required index %q", name)
		}
		if actual.Table != expected.Table {
			return fmt.Errorf("index %q table mismatch: got %q want %q", name, actual.Table, expected.Table)
		}
		if actual.Unique != expected.Unique {
			return fmt.Errorf("index %q uniqueness mismatch: got %v want %v", name, actual.Unique, expected.Unique)
		}
		if !sameStringSlice(actual.Columns, expected.Columns) {
			return fmt.Errorf("index %q columns mismatch: got (%s) want (%s)", name, strings.Join(actual.Columns, ","), strings.Join(expected.Columns, ","))
		}
	}
	return nil
}

func ensureIndexes(ctx context.Context, db *sql.DB, indexOrder []string, specs map[string]indexSpec) error {
	for _, name := range indexOrder {
		_, exists, err := readIndexDefinition(ctx, db, name)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if _, err := db.ExecContext(ctx, specs[name].CreateSQL); err != nil {
			return fmt.Errorf("create required index %q: %w", name, err)
		}
	}
	return nil
}

func readIndexDefinition(ctx context.Context, db *sql.DB, name string) (indexDefinitionInfo, bool, error) {
	var info indexDefinitionInfo
	var tableName string
	var sqlText sql.NullString

	err := db.QueryRowContext(ctx, `
		SELECT tbl_name, sql
		FROM sqlite_master
		WHERE type='index' AND name=?
	`, name).Scan(&tableName, &sqlText)
	if err == sql.ErrNoRows {
		return info, false, nil
	}
	if err != nil {
		return info, false, fmt.Errorf("read index %s: %w", name, err)
	}
	info.Table = tableName
	if sqlText.Valid {
		upper := strings.ToUpper(sqlText.String)
		info.Unique = strings.Contains(upper, "CREATE UNIQUE INDEX")
	}
	cols, err := indexColumns(ctx, db, name)
	if err != nil {
		return info, false, err
	}
	info.Columns = cols
	return info, true, nil
}

func indexColumns(ctx context.Context, db *sql.DB, name string) ([]string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`PRAGMA index_info(%s)`, quoteIdent(name)))
	if err != nil {
		return nil, fmt.Errorf("read index info %s: %w", name, err)
	}
	defer rows.Close()

	cols := make([]string, 0)
	for rows.Next() {
		var seqNo, cid int
		var colName string
		if err := rows.Scan(&seqNo, &cid, &colName); err != nil {
			return nil, fmt.Errorf("scan index info %s: %w", name, err)
		}
		cols = append(cols, colName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("index info rows %s: %w", name, err)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("index %q has no columns", name)
	}
	return cols, nil
}

func sameType(actual, expected string) bool {
	return normalizeSQLiteType(actual) == normalizeSQLiteType(expected)
}

func normalizeSQLiteType(s string) string {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return ""
	}
	return strings.Join(strings.Fields(s), " ")
}

func sameStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (s *SQLiteStore) SchemaVersion(ctx context.Context) (int, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("store not initialized")
	}
	version, _, err := readSchemaVersion(ctx, s.db)
	return version, err
}
