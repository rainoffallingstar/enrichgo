package annotation

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDetectIDType(t *testing.T) {
	tests := []struct {
		input    string
		expected IDType
	}{
		// Entrez ID (纯数字)
		{"1234", IDEntrez},
		{"1001", IDEntrez},
		{"999999", IDEntrez},

		// 基因符号 (首字母大写)
		{"TP53", IDSymbol},
		{"BRCA1", IDSymbol},
		{"ACTB", IDSymbol},
		{"GAPDH", IDSymbol},

		// ENSEMBL ID
		{"ENSG00000141510", IDEnsembl},
		{"ENSMUSG00000027387", IDEnsembl},
		{"ENSG000001", IDEnsembl},

		// UniProt ID
		{"P12345", IDUniprot},
		{"Q9BQN5", IDUniprot},
		{"P04637", IDUniprot},

		// KEGG ID
		{"hsa:10458", IDKEGG},
		{"mmu:12345", IDKEGG},
		{"eco:b0001", IDKEGG},

		// RefSeq
		{"NM_001", IDRefSeq},
		{"NR_024", IDRefSeq},
		{"NP_001", IDRefSeq},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := DetectIDType(tt.input)
			if result != tt.expected {
				t.Errorf("DetectIDType(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDetectIDTypeUnknown(t *testing.T) {
	tests := []string{
		"",
		"abc123xyz",
		"12abc34",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			result := DetectIDType(input)
			if result != IDUnknown {
				t.Errorf("DetectIDType(%q) = %v, want %v", input, result, IDUnknown)
			}
		})
	}
}

func TestBatchDetectIDType(t *testing.T) {
	tests := []struct {
		name     string
		geneIDs  []string
		expected IDType
	}{
		{
			name:     "all entrez",
			geneIDs:  []string{"1001", "1002", "1003", "1004", "1005"},
			expected: IDEntrez,
		},
		{
			name:     "all symbols",
			geneIDs:  []string{"TP53", "BRCA1", "EGFR", "MYC", "GAPDH"},
			expected: IDSymbol,
		},
		{
			name:     "mixed",
			geneIDs:  []string{"TP53", "1234", "BRCA1", "1001", "EGFR"},
			expected: IDSymbol, // 3 symbols vs 2 entrez
		},
		{
			name:     "low signal",
			geneIDs:  []string{"TP53", "abc123", "xyz789"}, // 都是 unknown
			expected: IDUnknown,
		},
		{
			name:     "all unknown",
			geneIDs:  []string{"abc123", "xyz789", "unknown"},
			expected: IDUnknown,
		},
		{
			name:     "empty",
			geneIDs:  []string{},
			expected: IDUnknown,
		},
		{
			name:     "single entrez",
			geneIDs:  []string{"1234"},
			expected: IDEntrez,
		},
		{
			name:     "single symbol",
			geneIDs:  []string{"TP53"},
			expected: IDSymbol,
		},
		{
			name:     "all ensembl",
			geneIDs:  []string{"ENSG00000141510", "ENSG00000123456", "ENSG0000078910"},
			expected: IDEnsembl,
		},
		{
			name:     "all uniprot",
			geneIDs:  []string{"P12345", "Q9BQN5", "P04637"},
			expected: IDUniprot,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BatchDetectIDType(tt.geneIDs)
			if result != tt.expected {
				t.Errorf("BatchDetectIDType(%v) = %v, want %v", tt.geneIDs, result, tt.expected)
			}
		})
	}
}

func TestUniqueStrings(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "no duplicates",
			input:    []string{"a", "b", "c"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "with duplicates",
			input:    []string{"a", "b", "a", "c", "b"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "with spaces",
			input:    []string{"  a  ", "b", "c"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "with empty",
			input:    []string{"a", "", "b"},
			expected: []string{"a", "b"},
		},
		{
			name:     "empty",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "all duplicates",
			input:    []string{"a", "a", "a"},
			expected: []string{"a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := uniqueStrings(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("uniqueStrings(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIDTypeToKEGG(t *testing.T) {
	tests := []struct {
		input    IDType
		expected string
	}{
		{IDEntrez, "ncbi-geneid"},
		{IDSymbol, "genesymbol"},
		{IDUniprot, "uniprot"},
		{IDKEGG, "kegg"},
		{IDUnknown, "unknown"},
		{IDEnsembl, "ENSEMBL"},
		{IDRefSeq, "REFSEQ"},
	}

	for _, tt := range tests {
		t.Run(string(tt.input), func(t *testing.T) {
			result := idTypeToKEGG(tt.input)
			if result != tt.expected {
				t.Errorf("idTypeToKEGG(%v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIDConverterInterface(t *testing.T) {
	// Test that KEGGIDConverter implements IDConverter interface
	var _ IDConverter = &KEGGIDConverter{}
}

func TestKEGGIDConverterCreation(t *testing.T) {
	converter := NewKEGGIDConverter()
	if converter == nil {
		t.Error("NewKEGGIDConverter() returned nil")
	}
	if converter.cache == nil {
		t.Error("converter.cache is nil")
	}
}

func TestGetCacheKey(t *testing.T) {
	converter := NewKEGGIDConverter()

	tests := []struct {
		species  string
		fromType IDType
		toType   IDType
		expected string
	}{
		{"hsa", IDEntrez, IDSymbol, "hsa:ENTREZID:SYMBOL"},
		{"mmu", IDSymbol, IDUniprot, "mmu:SYMBOL:UNIPROT"},
		{"hsa", IDKEGG, IDEntrez, "hsa:KEGG:ENTREZID"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := converter.getCacheKey(tt.species, tt.fromType, tt.toType)
			if result != tt.expected {
				t.Errorf("getCacheKey(%q, %v, %v) = %q, want %q",
					tt.species, tt.fromType, tt.toType, result, tt.expected)
			}
		})
	}
}

func TestConvertUsesLocalSpeciesMapCache(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kegg_hsa_idmap.tsv")
	content := "7157\tTP53\n1956\tEGFR\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write mapping file: %v", err)
	}

	converter := NewKEGGIDConverter(dir)

	got, err := converter.Convert([]string{"TP53", "EGFR"}, IDSymbol, IDEntrez, "hsa")
	if err != nil {
		t.Fatalf("Convert failed: %v", err)
	}

	if got["TP53"][0] != "7157" {
		t.Fatalf("TP53 -> %v, want 7157", got["TP53"])
	}
	if got["EGFR"][0] != "1956" {
		t.Fatalf("EGFR -> %v, want 1956", got["EGFR"])
	}
}

func TestConvertBetweenSymbolEntrezAndKEGG(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kegg_hsa_idmap.tsv")
	content := "7157\tTP53\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write mapping file: %v", err)
	}

	converter := NewKEGGIDConverter(dir)

	m1, err := converter.Convert([]string{"TP53"}, IDSymbol, IDKEGG, "hsa")
	if err != nil {
		t.Fatalf("symbol->kegg failed: %v", err)
	}
	if m1["TP53"][0] != "hsa:7157" {
		t.Fatalf("TP53 -> %v, want hsa:7157", m1["TP53"])
	}

	m2, err := converter.Convert([]string{"hsa:7157"}, IDKEGG, IDSymbol, "hsa")
	if err != nil {
		t.Fatalf("kegg->symbol failed: %v", err)
	}
	if m2["hsa:7157"][0] != "TP53" {
		t.Fatalf("hsa:7157 -> %v, want TP53", m2["hsa:7157"])
	}
}

func TestLoadSpeciesMapWithMultiColumnIDMap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kegg_hsa_idmap.tsv")
	content := "1\tCDS\t19:complement(1..2)\tA1BG\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write mapping file: %v", err)
	}

	converter := NewKEGGIDConverter(dir)
	got, err := converter.Convert([]string{"A1BG"}, IDSymbol, IDEntrez, "hsa")
	if err != nil {
		t.Fatalf("Convert failed: %v", err)
	}
	if got["A1BG"][0] != "1" {
		t.Fatalf("A1BG -> %v, want 1", got["A1BG"])
	}
}

func TestKEGGIDConverterPartialCacheFill(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kegg_hsa_idmap.tsv")
	content := "7157\tTP53\n1956\tEGFR\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write mapping file: %v", err)
	}

	converter := NewKEGGIDConverter(dir)
	converter.SetMaxCacheEntries(100)

	key := converter.getCacheKey("hsa", IDSymbol, IDEntrez)

	if _, err := converter.Convert([]string{"TP53"}, IDSymbol, IDEntrez, "hsa"); err != nil {
		t.Fatalf("first Convert: %v", err)
	}
	if _, ok := converter.getCached(key, "TP53"); !ok {
		t.Fatalf("expected TP53 cached after first Convert")
	}

	if got, err := converter.Convert([]string{"TP53", "EGFR"}, IDSymbol, IDEntrez, "hsa"); err != nil {
		t.Fatalf("second Convert: %v", err)
	} else {
		if got["TP53"][0] != "7157" || got["EGFR"][0] != "1956" {
			t.Fatalf("unexpected mapping: %+v", got)
		}
	}
	if _, ok := converter.getCached(key, "EGFR"); !ok {
		t.Fatalf("expected EGFR cached after second Convert")
	}
}

func TestKEGGIDConverterCacheIsBounded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kegg_hsa_idmap.tsv")
	content := "7157\tTP53\n1956\tEGFR\n1\tA1BG\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write mapping file: %v", err)
	}

	converter := NewKEGGIDConverter(dir)
	converter.SetMaxCacheEntries(2)

	_, err := converter.Convert([]string{"TP53", "EGFR", "A1BG"}, IDSymbol, IDEntrez, "hsa")
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}

	key := converter.getCacheKey("hsa", IDSymbol, IDEntrez)
	converter.mu.RLock()
	cc := converter.cache[key]
	converter.mu.RUnlock()
	if cc == nil {
		t.Fatalf("expected cache bucket for %s", key)
	}
	if cc.Len() > 2 {
		t.Fatalf("cache len=%d, want <=2", cc.Len())
	}
}

type mockConverter struct {
	mapping map[string][]string
	err     error
}

func (m mockConverter) Convert(geneIDs []string, fromType, toType IDType, species string) (map[string][]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.mapping, nil
}

func TestConvertGeneIDFailsWhenUnconverted(t *testing.T) {
	genes := []string{"TP53", "EGFR"}
	converter := mockConverter{
		mapping: map[string][]string{
			"TP53": {"7157"},
			"EGFR": {"EGFR"},
		},
	}

	_, _, err := ConvertGeneID(genes, IDEntrez, "hsa", converter)
	if err == nil {
		t.Fatalf("expected conversion error, got nil")
	}
	if !strings.Contains(err.Error(), "not converted") {
		t.Fatalf("unexpected error: %v", err)
	}
}
