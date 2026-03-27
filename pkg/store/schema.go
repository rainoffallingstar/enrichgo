package store

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

const (
	CurrentSchemaVersion = 1
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

var schemaV1CoreTableOrder = []string{"geneset", "geneset_gene", "idmap"}

var schemaV1CoreTableSpecs = map[string]map[string]columnSpec{
	"geneset": {
		"db":            {Type: "TEXT", NotNull: true, PKPos: 1},
		"species":       {Type: "TEXT", NotNull: true, PKPos: 2},
		"ontology":      {Type: "TEXT", NotNull: true, PKPos: 3},
		"collection":    {Type: "TEXT", NotNull: true, PKPos: 4},
		"set_id":        {Type: "TEXT", NotNull: true, PKPos: 5},
		"name":          {Type: "TEXT", NotNull: true, PKPos: 0},
		"description":   {Type: "TEXT", NotNull: true, PKPos: 0},
		"version":       {Type: "TEXT", NotNull: true, PKPos: 0},
		"downloaded_at": {Type: "TEXT", NotNull: true, PKPos: 0},
	},
	"geneset_gene": {
		"db":           {Type: "TEXT", NotNull: true, PKPos: 1},
		"species":      {Type: "TEXT", NotNull: true, PKPos: 2},
		"ontology":     {Type: "TEXT", NotNull: true, PKPos: 3},
		"collection":   {Type: "TEXT", NotNull: true, PKPos: 4},
		"set_id":       {Type: "TEXT", NotNull: true, PKPos: 5},
		"gene_id":      {Type: "TEXT", NotNull: true, PKPos: 6},
		"gene_id_type": {Type: "TEXT", NotNull: true, PKPos: 7},
	},
	"idmap": {
		"species":       {Type: "TEXT", NotNull: true, PKPos: 1},
		"from_type":     {Type: "TEXT", NotNull: true, PKPos: 2},
		"from_id":       {Type: "TEXT", NotNull: true, PKPos: 3},
		"to_type":       {Type: "TEXT", NotNull: true, PKPos: 4},
		"to_id":         {Type: "TEXT", NotNull: true, PKPos: 5},
		"source":        {Type: "TEXT", NotNull: true, PKPos: 6},
		"downloaded_at": {Type: "TEXT", NotNull: true, PKPos: 0},
	},
}

var schemaV1MetaTableOrder = []string{"meta"}

var schemaV1MetaTableSpecs = map[string]map[string]columnSpec{
	"meta": {
		"key":   {Type: "TEXT", NotNull: true, PKPos: 1},
		"value": {Type: "TEXT", NotNull: true, PKPos: 0},
	},
}

var schemaV1IndexOrder = []string{
	"idx_geneset_gene_lookup",
	"idx_geneset_gene_by_gene",
	"idx_idmap_lookup",
}

var schemaV1IndexSpecs = map[string]indexSpec{
	"idx_geneset_gene_lookup": {
		Table:   "geneset_gene",
		Columns: []string{"db", "species", "ontology", "collection", "set_id"},
		Unique:  false,
		CreateSQL: `CREATE INDEX IF NOT EXISTS idx_geneset_gene_lookup
			ON geneset_gene (db, species, ontology, collection, set_id);`,
	},
	"idx_geneset_gene_by_gene": {
		Table:   "geneset_gene",
		Columns: []string{"db", "species", "ontology", "collection", "gene_id", "gene_id_type"},
		Unique:  false,
		CreateSQL: `CREATE INDEX IF NOT EXISTS idx_geneset_gene_by_gene
			ON geneset_gene (db, species, ontology, collection, gene_id, gene_id_type);`,
	},
	"idx_idmap_lookup": {
		Table:   "idmap",
		Columns: []string{"species", "from_type", "to_type", "from_id"},
		Unique:  false,
		CreateSQL: `CREATE INDEX IF NOT EXISTS idx_idmap_lookup
			ON idmap (species, from_type, to_type, from_id);`,
	},
}

var schemaMigrations = []schemaMigration{
	{
		version: CurrentSchemaVersion,
		statements: []string{
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
			schemaV1IndexSpecs["idx_geneset_gene_lookup"].CreateSQL,
			schemaV1IndexSpecs["idx_geneset_gene_by_gene"].CreateSQL,
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
			schemaV1IndexSpecs["idx_idmap_lookup"].CreateSQL,
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

	version, ok, err := readSchemaVersion(ctx, db)
	if err != nil {
		return err
	}
	if ok {
		if version < 0 {
			return fmt.Errorf("invalid schema version %d", version)
		}
		if version > CurrentSchemaVersion {
			return fmt.Errorf("db schema version %d is newer than supported %d", version, CurrentSchemaVersion)
		}
		// Allow missing secondary indexes on existing DBs; they are repaired below.
		if err := validateSchemaForVersion(ctx, db, version, false); err != nil {
			return err
		}
		if version == 1 {
			if err := validateSchemaV1MetaTable(ctx, db); err != nil {
				return err
			}
		}
	} else {
		if _, err := detectCompatibleLegacySchema(ctx, db); err != nil {
			return err
		}
		version = 0
	}

	for _, migration := range schemaMigrations {
		if migration.version <= version {
			continue
		}
		for _, stmt := range migration.statements {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("apply schema v%d: %w", migration.version, err)
			}
		}
		if err := writeSchemaVersion(ctx, db, migration.version); err != nil {
			return err
		}
		version = migration.version
	}

	if err := ensureSchemaIndexes(ctx, db, version); err != nil {
		return err
	}
	if err := validateSchemaForVersion(ctx, db, version, true); err != nil {
		return err
	}
	return nil
}

func ensureSchemaIndexes(ctx context.Context, db *sql.DB, version int) error {
	switch version {
	case 0:
		return nil
	case 1:
		return ensureIndexes(ctx, db, schemaV1IndexOrder, schemaV1IndexSpecs)
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

func detectCompatibleLegacySchema(ctx context.Context, db *sql.DB) (bool, error) {
	required := schemaV1CoreTableOrder
	present := make([]string, 0, len(required))
	for _, name := range required {
		exists, err := tableExists(ctx, db, name)
		if err != nil {
			return false, err
		}
		if exists {
			present = append(present, name)
		}
	}

	if len(present) == 0 {
		return false, nil
	}
	if len(present) != len(required) {
		return false, fmt.Errorf("legacy schema is incompatible: partial tables present (%s)", strings.Join(present, ","))
	}
	if err := validateSchemaForVersion(ctx, db, CurrentSchemaVersion, false); err != nil {
		return false, fmt.Errorf("legacy schema is incompatible: %w", err)
	}
	return true, nil
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
	case 0:
		return nil
	case 1:
		return validateSchemaV1(ctx, db, strict)
	default:
		return fmt.Errorf("unsupported schema version %d", version)
	}
}

func validateSchemaV1(ctx context.Context, db *sql.DB, strict bool) error {
	if err := validateSchemaV1CoreTables(ctx, db); err != nil {
		return err
	}
	if !strict {
		return nil
	}
	if err := validateSchemaV1MetaTable(ctx, db); err != nil {
		return err
	}
	if err := validateSchemaV1Indexes(ctx, db); err != nil {
		return err
	}
	return nil
}

func validateSchemaV1CoreTables(ctx context.Context, db *sql.DB) error {
	return validateTableSpecs(ctx, db, schemaV1CoreTableOrder, schemaV1CoreTableSpecs)
}

func validateSchemaV1MetaTable(ctx context.Context, db *sql.DB) error {
	return validateTableSpecs(ctx, db, schemaV1MetaTableOrder, schemaV1MetaTableSpecs)
}

func validateSchemaV1Indexes(ctx context.Context, db *sql.DB) error {
	return validateIndexes(ctx, db, schemaV1IndexOrder, schemaV1IndexSpecs)
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
