package analysis

import (
	"math"
	"testing"
)

func TestHypergeometricTest(t *testing.T) {
	tests := []struct {
		name string
		N    int // 总体数量
		K    int // 总体中成功数量
		n    int // 样本数量
		k    int // 样本中成功数量
		minP float64
		maxP float64
	}{
		{
			name: "basic case",
			N:    1000,
			K:    100,
			n:    50,
			k:    10,
			minP: 0,
			maxP: 1,
		},
		{
			name: "no overlap",
			N:    1000,
			K:    100,
			n:    50,
			k:    0,
			minP: 0.9,
			maxP: 1.0,
		},
		{
			name: "full overlap",
			N:    1000,
			K:    100,
			n:    50,
			k:    50,
			minP: 0,
			maxP: 0.1,
		},
		{
			name: "edge case: n > N",
			N:    100,
			K:    50,
			n:    200,
			k:    10,
			minP: 0.99,
			maxP: 1.0,
		},
		{
			name: "small sample",
			N:    100,
			K:    10,
			n:    5,
			k:    2,
			minP: 0,
			maxP: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := HypergeometricTestCumulative(tt.N, tt.K, tt.n, tt.k)
			if p < tt.minP || p > tt.maxP {
				t.Errorf("p-value %v out of expected range [%v, %v]", p, tt.minP, tt.maxP)
			}
			// p-value should always be in [0, 1]
			if p < 0 || p > 1 {
				t.Errorf("p-value %v out of valid range [0, 1]", p)
			}
		})
	}
}

func TestHypergeometricTestKnownResult(t *testing.T) {
	// Test with known results
	// When N=1000, K=100, n=50, k=10:
	// Expected p-value should be relatively small (significant enrichment)
	p := HypergeometricTestCumulative(1000, 100, 50, 10)

	// With 10/50 genes in a pathway that contains 100/1000 total genes,
	// this is a significant enrichment
	if p > 0.5 {
		t.Errorf("Expected significant enrichment, got p=%v", p)
	}
}

func TestFDR(t *testing.T) {
	tests := []struct {
		name          string
		pValues       []float64
		checkRange    bool
		checkMonotone bool
		minAdjusted   float64
		maxAdjusted   float64
	}{
		{
			name:          "simple case",
			pValues:       []float64{0.01, 0.02, 0.03, 0.04, 0.05},
			checkRange:    true,
			checkMonotone: true,
			minAdjusted:   0,
			maxAdjusted:   0.25,
		},
		{
			name:          "already sorted",
			pValues:       []float64{0.001, 0.01, 0.05, 0.1, 0.5},
			checkRange:    true,
			checkMonotone: true,
			minAdjusted:   0,
			maxAdjusted:   0.5,
		},
		{
			name:          "unsorted",
			pValues:       []float64{0.05, 0.01, 0.03, 0.02, 0.001},
			checkRange:    true,
			checkMonotone: false, // BH preserves original order; output is not sorted for unsorted input
			minAdjusted:   0,
			maxAdjusted:   0.25,
		},
		{
			name:        "empty",
			pValues:     []float64{},
			checkRange:  false,
			minAdjusted: 0,
			maxAdjusted: 0,
		},
		{
			name:          "single value",
			pValues:       []float64{0.05},
			checkRange:    true,
			checkMonotone: true,
			minAdjusted:   0.05,
			maxAdjusted:   0.05,
		},
		{
			name:          "large p-values",
			pValues:       []float64{0.6, 0.7, 0.8, 0.9, 1.0},
			checkRange:    true,
			checkMonotone: true,
			minAdjusted:   0.6,
			maxAdjusted:   1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adjusted := AdjustFDR(tt.pValues)

			if len(tt.pValues) == 0 {
				if adjusted != nil {
					t.Error("Expected nil for empty input")
				}
				return
			}

			if len(adjusted) != len(tt.pValues) {
				t.Errorf("Length mismatch: got %v, want %v", len(adjusted), len(tt.pValues))
			}

			if tt.checkRange {
				for i, p := range adjusted {
					if p < tt.minAdjusted || p > tt.maxAdjusted {
						t.Errorf("Adjusted p-value[%d]=%v out of expected range [%v, %v]",
							i, p, tt.minAdjusted, tt.maxAdjusted)
					}
					// FDR adjusted p-values should be monotonically increasing (only for sorted input)
					if tt.checkMonotone && i > 0 && adjusted[i] < adjusted[i-1] {
						t.Errorf("Adjusted p-values not monotonically increasing at index %d", i)
					}
				}
			}
		})
	}
}

func TestFDRProperties(t *testing.T) {
	// Test that FDR has certain mathematical properties
	pValues := []float64{0.01, 0.02, 0.03, 0.04, 0.05}
	adjusted := AdjustFDR(pValues)

	// Property 1: Adjusted p-values should be >= original p-values
	for i, p := range pValues {
		if adjusted[i] < p {
			t.Errorf("Adjusted p-value[%d]=%v < original=%v", i, adjusted[i], p)
		}
	}

	// Property 2: All adjusted values should be in [0, 1]
	for i, p := range adjusted {
		if p < 0 || p > 1 {
			t.Errorf("Adjusted p-value[%d]=%v out of [0, 1]", i, p)
		}
	}

	// Property 3: For very small p-values, adjusted should be approximately equal
	if adjusted[0] > 0.05 {
		t.Errorf("Smallest p-value should remain significant after FDR: %v", adjusted[0])
	}
}

func TestLogFactorial(t *testing.T) {
	tests := []struct {
		n   int
		min float64
		max float64
	}{
		{0, -0.1, 0.1},
		{1, -0.1, 0.1},
		{2, 0, 1},
		{10, 15, 25},
		{100, 300, 400},
		{170, 700, 800},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := logFactorial(tt.n)
			if result < tt.min || result > tt.max {
				t.Errorf("logFactorial(%d) = %v, expected range [%v, %v]", tt.n, result, tt.min, tt.max)
			}
		})
	}
}

func TestMin(t *testing.T) {
	if min(1, 2) != 1 {
		t.Error("min(1, 2) should be 1")
	}
	if min(2, 1) != 1 {
		t.Error("min(2, 1) should be 1")
	}
	if min(5, 5) != 5 {
		t.Error("min(5, 5) should be 5")
	}
	if min(-1, 1) != -1 {
		t.Error("min(-1, 1) should be -1")
	}
}

func TestMathutil(t *testing.T) {
	// Test that math functions are available
	if math.Log(1) != 0 {
		t.Error("math.Log(1) should be 0")
	}
	if math.Exp(0) != 1 {
		t.Error("math.Exp(0) should be 1")
	}
}

func TestRunORAFiltersByQValueOnly(t *testing.T) {
	gs := &GeneSet{
		ID:   "PATHWAY:QONLY",
		Name: "Q-only filter test",
		Genes: map[string]bool{
			"A": true, "B": true, "C": true, "D": true, "E": true,
		},
	}

	params := &ORAParams{
		GeneList:     []string{"A", "B", "C", "X", "Y"},
		GeneSets:     GeneSets{gs},
		MinGSSize:    1,
		MaxGSSize:    1000,
		PValueCutoff: 0.0, // 若仍按 pvalue 过滤将得到 0 结果
		QValueCutoff: 1.0, // 放开 FDR
	}

	results := RunORA(params)
	if len(results) == 0 {
		t.Fatalf("expected non-empty result when qvalue allows all")
	}
}
