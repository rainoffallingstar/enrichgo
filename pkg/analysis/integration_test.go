package analysis

import (
	"testing"
)

// TestRunORAWorkflow tests the end-to-end ORA flow with synthetic gene sets.
func TestRunORAWorkflow(t *testing.T) {
	// Build a small synthetic gene set database
	geneSet := &GeneSet{
		ID:   "PATHWAY:001",
		Name: "Test pathway",
		Genes: map[string]bool{
			"GENE1": true, "GENE2": true, "GENE3": true,
			"GENE4": true, "GENE5": true,
		},
		Description: "Synthetic test pathway",
	}
	geneSets := GeneSets{geneSet}

	// Input gene list with strong overlap
	inputGenes := []string{"GENE1", "GENE2", "GENE3", "GENE4", "GENEOTHER"}

	params := &ORAParams{
		GeneList:     inputGenes,
		GeneSets:     geneSets,
		MinGSSize:    2,
		MaxGSSize:    1000,
		PValueCutoff: 1.0, // accept all for testing
		QValueCutoff: 1.0,
	}

	results := RunORA(params)

	if len(results) == 0 {
		t.Fatal("Expected at least one ORA result, got none")
	}

	r := results[0]
	if r.ID != "PATHWAY:001" {
		t.Errorf("Expected result ID PATHWAY:001, got %q", r.ID)
	}
	if r.Count != 4 {
		t.Errorf("Expected overlap count 4, got %d", r.Count)
	}
	if r.PValue <= 0 || r.PValue > 1 {
		t.Errorf("PValue out of [0,1]: %v", r.PValue)
	}
	if r.PAdjust <= 0 || r.PAdjust > 1 {
		t.Errorf("PAdjust out of [0,1]: %v", r.PAdjust)
	}
}

// TestRunGSEAWorkflow tests the end-to-end GSEA flow with synthetic data.
func TestRunGSEAWorkflow(t *testing.T) {
	geneSet := &GeneSet{
		ID:   "SET:001",
		Name: "Test gene set",
		Genes: map[string]bool{
			"GENE1": true, "GENE2": true, "GENE3": true,
		},
		Description: "Synthetic set",
	}
	geneSets := GeneSets{geneSet}

	// Ranked gene list: GENE1-3 have highest values
	geneValues := map[string]float64{
		"GENE1": 4.0, "GENE2": 3.5, "GENE3": 3.0,
		"GENE4": 1.0, "GENE5": 0.5,
	}
	geneList := []string{"GENE1", "GENE2", "GENE3", "GENE4", "GENE5"}

	gseaInput := &GSEAInput{
		GeneList:      geneList,
		GeneValues:    geneValues,
		GeneSets:      geneSets,
		Permutations:  100,
		MinGSSize:     2,
		MaxGSSize:     1000,
		PAdjustCutoff: 1.0, // accept all for testing
		Seed:          42,
	}

	results := RunGSEA(gseaInput)

	// With PAdjustCutoff=1.0 and all genes in set near top, expect a result
	if len(results) == 0 {
		t.Fatal("Expected at least one GSEA result, got none")
	}

	r := results[0]
	if r.PValue <= 0 || r.PValue > 1 {
		t.Errorf("PValue out of [0,1]: %v", r.PValue)
	}
	if r.EnrichmentScore == 0 {
		t.Error("Expected non-zero enrichment score")
	}
}

// TestGSEAPAdjustCutoffRespected verifies that adjusted p-value cutoff is applied.
func TestGSEAPAdjustCutoffRespected(t *testing.T) {
	// Build many gene sets so FDR correction yields high adjusted p-values
	var geneSets GeneSets
	for i := 0; i < 50; i++ {
		gs := &GeneSet{
			ID:    "SET",
			Name:  "set",
			Genes: map[string]bool{"GENEOTHER1": true, "GENEOTHER2": true},
		}
		geneSets = append(geneSets, gs)
	}

	geneValues := map[string]float64{"GENE1": 1.0, "GENE2": 0.5}
	geneList := []string{"GENE1", "GENE2"}

	gseaInput := &GSEAInput{
		GeneList:      geneList,
		GeneValues:    geneValues,
		GeneSets:      geneSets,
		Permutations:  10,
		MinGSSize:     1,
		MaxGSSize:     1000,
		PAdjustCutoff: 0.0, // nothing should pass
		Seed:          1,
	}

	results := RunGSEA(gseaInput)
	if len(results) != 0 {
		t.Errorf("Expected 0 results with PAdjustCutoff=0.0, got %d", len(results))
	}
}
