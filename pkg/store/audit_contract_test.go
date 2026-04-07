package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"enrichgo/pkg/types"
)

func TestAuditWithContractFailsOnEmptyDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	report, err := st.AuditWithContract(ctx, "embedded-hsa-basic")
	if err != nil {
		t.Fatalf("AuditWithContract: %v", err)
	}
	if report.ContractValid {
		t.Fatalf("expected contract invalid for empty DB")
	}
	if len(report.ContractViolations) == 0 {
		t.Fatalf("expected contract violations")
	}
}

func TestAuditWithContractSeedPassesOnEmptyDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	report, err := st.AuditWithContract(ctx, "embedded-hsa-seed")
	if err != nil {
		t.Fatalf("AuditWithContract: %v", err)
	}
	if !report.ContractValid {
		t.Fatalf("expected seed contract valid, violations=%v", report.ContractViolations)
	}
}

func TestAuditWithContractPassesWhenMinimumDataPresent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "enrichgo.db")
	st, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sets := types.GeneSets{
		&types.GeneSet{ID: "p1", Name: "Pathway 1", Description: "d1", Genes: map[string]bool{"7157": true}},
	}
	if err := st.ReplaceGeneSets(ctx, GeneSetFilter{DB: "kegg", Species: "hsa"}, "ENTREZID", sets, "v1"); err != nil {
		t.Fatalf("ReplaceGeneSets: %v", err)
	}
	if err := st.ReplaceIDMap(ctx, "hsa", "test", "SYMBOL", "ENTREZID", []IDMapRow{{From: "TP53", To: "7157"}}); err != nil {
		t.Fatalf("ReplaceIDMap SYMBOL->ENTREZID: %v", err)
	}
	if err := st.ReplaceIDMap(ctx, "hsa", "test", "ENTREZID", "SYMBOL", []IDMapRow{{From: "7157", To: "TP53"}}); err != nil {
		t.Fatalf("ReplaceIDMap ENTREZID->SYMBOL: %v", err)
	}

	report, err := st.AuditWithContract(ctx, "embedded-hsa-basic")
	if err != nil {
		t.Fatalf("AuditWithContract: %v", err)
	}
	if !report.ContractValid {
		t.Fatalf("expected contract valid, violations=%v", report.ContractViolations)
	}
}

func TestResolveAuditContractExtendedAliasCanonicalizes(t *testing.T) {
	contract, err := resolveAuditContract("embedded-hsa-extended")
	if err != nil {
		t.Fatalf("resolveAuditContract: %v", err)
	}
	if contract.Profile != "embedded-hsa-extended-sru" {
		t.Fatalf("canonical profile=%q want embedded-hsa-extended-sru", contract.Profile)
	}
}
