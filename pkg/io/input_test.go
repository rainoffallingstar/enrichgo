package io

import (
	"os"
	"path/filepath"
	"testing"
)

const testDataPath = "../../test-data/DE_results.csv"

// TestParseDiffGeneTable verifies that DE_results.csv is parsed correctly.
func TestParseDiffGeneTable(t *testing.T) {
	input, err := ParseDiffGeneTable(testDataPath, true)
	if err != nil {
		t.Fatalf("ParseDiffGeneTable failed: %v", err)
	}

	if len(input.Genes) == 0 {
		t.Fatal("Expected non-empty gene list")
	}

	// Spot-check first gene from the file
	if input.Genes[0] != "THBS1" {
		t.Errorf("Expected first gene to be THBS1, got %q", input.Genes[0])
	}

	// GeneValues should be populated with logFC values
	val, ok := input.GeneValues["THBS1"]
	if !ok {
		t.Error("GeneValues missing entry for THBS1")
	}
	if val <= 0 {
		t.Errorf("Expected positive logFC for THBS1 (up-regulated), got %v", val)
	}

	// All genes must have corresponding value entries
	for _, gene := range input.Genes {
		if _, ok := input.GeneValues[gene]; !ok {
			t.Errorf("GeneValues missing entry for gene %q", gene)
		}
	}
}

// TestParseDiffGeneTableGeneCount verifies a reasonable number of genes is parsed.
func TestParseDiffGeneTableGeneCount(t *testing.T) {
	input, err := ParseDiffGeneTable(testDataPath, true)
	if err != nil {
		t.Fatalf("ParseDiffGeneTable failed: %v", err)
	}

	// The test file should contain at least a handful of genes
	if len(input.Genes) < 5 {
		t.Errorf("Expected at least 5 genes, got %d", len(input.Genes))
	}

	// Genes list and GeneValues map must stay in sync
	if len(input.Genes) != len(input.GeneValues) {
		t.Errorf("len(Genes)=%d != len(GeneValues)=%d", len(input.Genes), len(input.GeneValues))
	}
}

func TestParseRankedGeneFileGeneThenRank(t *testing.T) {
	path := writeTempRankedFile(t, "GENE1\t2.5\nGENE2\t1.5\n")
	input, err := ParseRankedGeneFile(path)
	if err != nil {
		t.Fatalf("ParseRankedGeneFile failed: %v", err)
	}
	if len(input.Genes) != 2 {
		t.Fatalf("len(Genes)=%d, want 2", len(input.Genes))
	}
	if input.Genes[0] != "GENE1" || input.Genes[1] != "GENE2" {
		t.Fatalf("Genes=%v, want [GENE1 GENE2]", input.Genes)
	}
	if input.GeneValues["GENE1"] != 2.5 || input.GeneValues["GENE2"] != 1.5 {
		t.Fatalf("GeneValues=%v", input.GeneValues)
	}
}

func TestParseRankedGeneFileRankThenGene(t *testing.T) {
	path := writeTempRankedFile(t, "2.5\tGENE1\n1.5\tGENE2\n")
	input, err := ParseRankedGeneFile(path)
	if err != nil {
		t.Fatalf("ParseRankedGeneFile failed: %v", err)
	}
	if len(input.Genes) != 2 {
		t.Fatalf("len(Genes)=%d, want 2", len(input.Genes))
	}
	if input.Genes[0] != "GENE1" || input.Genes[1] != "GENE2" {
		t.Fatalf("Genes=%v, want [GENE1 GENE2]", input.Genes)
	}
	if input.GeneValues["GENE1"] != 2.5 || input.GeneValues["GENE2"] != 1.5 {
		t.Fatalf("GeneValues=%v", input.GeneValues)
	}
}

func TestParseRankedGeneFileSingleColumnAndComments(t *testing.T) {
	path := writeTempRankedFile(t, "# comment\nGENE1\n\nGENE2\n")
	input, err := ParseRankedGeneFile(path)
	if err != nil {
		t.Fatalf("ParseRankedGeneFile failed: %v", err)
	}
	if len(input.Genes) != 2 {
		t.Fatalf("len(Genes)=%d, want 2", len(input.Genes))
	}
	if input.GeneValues["GENE1"] != 1.0 || input.GeneValues["GENE2"] != 1.0 {
		t.Fatalf("GeneValues=%v", input.GeneValues)
	}
}

func writeTempRankedFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ranked.tsv")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("os.WriteFile: %v", err)
	}
	return path
}
