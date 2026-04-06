package store

import (
	"context"
	"fmt"
	"strings"
)

type AuditReport struct {
	HasSchemaVersion     bool             `json:"has_schema_version"`
	SchemaVersion        int              `json:"schema_version"`
	CurrentSchemaVersion int              `json:"current_schema_version"`
	TablesValid          bool             `json:"tables_valid"`
	IndexesValid         bool             `json:"indexes_valid"`
	ValidationError      string           `json:"validation_error,omitempty"`
	RowCounts            map[string]int64 `json:"row_counts"`

	ContractProfile    string   `json:"contract_profile,omitempty"`
	ContractValid      bool     `json:"contract_valid"`
	ContractViolations []string `json:"contract_violations,omitempty"`
}

func (s *SQLiteStore) Audit(ctx context.Context) (*AuditReport, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("store not initialized")
	}

	r := &AuditReport{
		CurrentSchemaVersion: CurrentSchemaVersion,
		RowCounts:            make(map[string]int64),
		ContractValid:        true,
	}

	version, hasVersion, err := readSchemaVersion(ctx, s.db)
	if err != nil {
		return nil, err
	}
	r.HasSchemaVersion = hasVersion
	r.SchemaVersion = version

	var tableErr error
	var indexErr error
	switch {
	case !hasVersion:
		tableErr = fmt.Errorf("missing schema version marker")
		indexErr = tableErr
	case version == CurrentSchemaVersion:
		tableErr = validateSchemaV2CoreTables(ctx, s.db)
		if tableErr == nil {
			tableErr = validateSchemaV2MetaTable(ctx, s.db)
		}
		indexErr = validateSchemaV2Indexes(ctx, s.db)
	case version < CurrentSchemaVersion:
		tableErr = fmt.Errorf("schema version %d is no longer supported", version)
		indexErr = tableErr
	default:
		tableErr = fmt.Errorf("unsupported schema version %d", version)
		indexErr = tableErr
	}

	r.TablesValid = tableErr == nil
	r.IndexesValid = indexErr == nil
	if tableErr != nil && indexErr != nil && tableErr.Error() != indexErr.Error() {
		r.ValidationError = tableErr.Error() + "; " + indexErr.Error()
	} else if tableErr != nil {
		r.ValidationError = tableErr.Error()
	} else if indexErr != nil {
		r.ValidationError = indexErr.Error()
	}

	for _, table := range []string{"meta", "dataset", "term", "gene_dict", "term_gene", "idmap_canon"} {
		count, exists, err := countRows(ctx, s, table)
		if err != nil {
			return nil, err
		}
		if exists {
			r.RowCounts[table] = count
		}
	}

	// Backward-compatible aliases for downstream consumers expecting v1 names.
	if n, ok := r.RowCounts["term"]; ok {
		r.RowCounts["geneset"] = n
	}
	if n, ok := r.RowCounts["term_gene"]; ok {
		r.RowCounts["geneset_gene"] = n
	}
	if n, ok := r.RowCounts["idmap_canon"]; ok {
		r.RowCounts["idmap"] = n
	}

	return r, nil
}

func (s *SQLiteStore) AuditWithContract(ctx context.Context, profile string) (*AuditReport, error) {
	r, err := s.Audit(ctx)
	if err != nil {
		return nil, err
	}

	profile = strings.TrimSpace(strings.ToLower(profile))
	if profile == "" {
		r.ContractValid = true
		return r, nil
	}
	r.ContractProfile = profile

	contract, err := resolveAuditContract(profile)
	if err != nil {
		return nil, err
	}
	violations, err := evaluateAuditContract(ctx, s, r, contract)
	if err != nil {
		return nil, err
	}
	r.ContractViolations = violations
	r.ContractValid = len(violations) == 0
	return r, nil
}

func countRows(ctx context.Context, s *SQLiteStore, table string) (int64, bool, error) {
	exists, err := tableExists(ctx, s.db, table)
	if err != nil {
		return 0, false, err
	}
	if !exists {
		return 0, false, nil
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdent(table))
	var n int64
	if err := s.db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return 0, true, fmt.Errorf("count rows %s: %w", table, err)
	}
	return n, true, nil
}

func (r *AuditReport) CoreHealthy() bool {
	if r == nil {
		return false
	}
	if !r.HasSchemaVersion {
		return false
	}
	if r.SchemaVersion != r.CurrentSchemaVersion {
		return false
	}
	if !r.TablesValid || !r.IndexesValid {
		return false
	}
	return strings.TrimSpace(r.ValidationError) == ""
}

func (r *AuditReport) Healthy() bool {
	if !r.CoreHealthy() {
		return false
	}
	if strings.TrimSpace(r.ContractProfile) != "" && !r.ContractValid {
		return false
	}
	return true
}
