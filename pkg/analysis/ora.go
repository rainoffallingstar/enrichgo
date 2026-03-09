package analysis

import (
	"fmt"
	"math"
	"sort"

	"enrichgo/pkg/types"
)

// GeneSet 是 types.GeneSet 的别名
type GeneSet = types.GeneSet

// GeneSets 是 types.GeneSets 的别名
type GeneSets = types.GeneSets

// EnrichmentResult 富集分析结果
type EnrichmentResult struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	GeneRatio   string   `json:"geneRatio"`
	BgRatio     string   `json:"bgRatio"`
	PValue      float64  `json:"pValue"`
	PAdjust     float64  `json:"pAdjust"`
	QValue      float64  `json:"qValue"`
	Genes       []string `json:"genes"`
	Count       int      `json:"count"`
	Description string   `json:"description"`
}

// ORAParams ORA 参数
type ORAParams struct {
	GeneList     []string // 输入基因列表
	GeneSets     GeneSets // 基因集
	Universe     []string // 背景基因列表 (可选)
	PAdjust      string   // p 值校正方法 ("BH", "bonferroni", "holm")
	MinGSSize    int      // 最小基因集大小
	MaxGSSize    int      // 最大基因集大小
	PValueCutoff float64  // 兼容保留；当前不过滤 raw p-value
	QValueCutoff float64  // FDR(q-value) 阈值
}

// HypergeometricTest 超几何检验
// N: 总体数量 (背景基因数)
// K: 总体中成功数量 (基因集中基因数)
// n: 样本数量 (输入基因数)
// k: 样本中成功数量 (输入基因在基因集中的数量)
func HypergeometricTest(N, K, n, k int) float64 {
	if n == 0 || K == 0 || N == 0 {
		return 1.0
	}

	// 确保参数合法
	if n > N {
		return 1.0
	}
	if k > K {
		k = K
	}
	if k > n {
		k = n
	}

	// 超几何分布概率
	// P(X = k) = (C(K, k) * C(N-K, n-k)) / C(N, n)
	logProb := logBinomial(K, k) + logBinomial(N-K, n-k) - logBinomial(N, n)
	return math.Exp(logProb)
}

// HypergeometricTestCumulative 累计超几何检验 (>= k)
func HypergeometricTestCumulative(N, K, n, k int) float64 {
	if n == 0 || K == 0 || N == 0 {
		return 1.0
	}

	if n > N {
		n = N
	}
	if k > K {
		k = K
	}
	if k > n {
		k = n
	}

	// 累计计算
	var pValue float64
	for i := k; i <= min(n, K); i++ {
		pValue += math.Exp(logBinomial(K, i) + logBinomial(N-K, n-i) - logBinomial(N, n))
	}

	if pValue > 1.0 {
		pValue = 1.0
	}
	return pValue
}

func logBinomial(n, k int) float64 {
	if k > n || k < 0 {
		return 0
	}
	if k == 0 || k == n {
		return 0
	}
	return logFactorial(n) - logFactorial(k) - logFactorial(n-k)
}

var logFactorialCache = make([]float64, 171)
var logFactorialInitialized = false

func logFactorial(n int) float64 {
	if !logFactorialInitialized {
		for i := range logFactorialCache {
			logFactorialCache[i] = float64(i)
		}
		logFactorialCache[0] = 0
		logFactorialCache[1] = 0
		for i := 2; i <= 170; i++ {
			logFactorialCache[i] = logFactorialCache[i-1] + math.Log(float64(i))
		}
		logFactorialInitialized = true
	}

	if n <= 170 {
		return logFactorialCache[n]
	}
	// 使用 Stirling 近似
	return float64(n)*math.Log(float64(n)) - float64(n) + 0.5*math.Log(2*math.Pi*float64(n))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// RunORA 执行 ORA 分析
func RunORA(params *ORAParams) []*EnrichmentResult {
	// 构建基因集合
	geneSetMap := make(map[string]bool)
	for _, gene := range params.GeneList {
		geneSetMap[gene] = true
	}

	// 确定背景基因
	var universe []string
	if params.Universe != nil && len(params.Universe) > 0 {
		universe = params.Universe
	} else {
		// 从所有基因集构建背景
		allGenes := make(map[string]bool)
		for _, gs := range params.GeneSets {
			for gene := range gs.Genes {
				allGenes[gene] = true
			}
		}
		for gene := range geneSetMap {
			allGenes[gene] = true
		}
		for gene := range allGenes {
			universe = append(universe, gene)
		}
	}

	N := len(universe) // 总体数量
	universeMap := make(map[string]bool)
	for _, gene := range universe {
		universeMap[gene] = true
	}

	// 对每个基因集进行富集分析
	var results []*EnrichmentResult
	for _, gs := range params.GeneSets {
		// 检查基因集大小
		gsSize := len(gs.Genes)
		if gsSize < params.MinGSSize || gsSize > params.MaxGSSize {
			continue
		}

		// 计算交集
		var overlap []string
		for gene := range gs.Genes {
			if geneSetMap[gene] {
				overlap = append(overlap, gene)
			}
		}
		k := len(overlap) // 交集数量

		if k == 0 {
			continue
		}

		n := len(params.GeneList) // 输入基因数量
		K := gsSize               // 基因集大小

		// 超几何检验
		pValue := HypergeometricTestCumulative(N, K, n, k)

		// GeneRatio: k/n
		// BgRatio: K/N
		geneRatio := fmt.Sprintf("%d/%d", k, n)
		bgRatio := fmt.Sprintf("%d/%d", K, N)

		result := &EnrichmentResult{
			ID:          gs.ID,
			Name:        gs.Name,
			GeneRatio:   geneRatio,
			BgRatio:     bgRatio,
			PValue:      pValue,
			Genes:       overlap,
			Count:       k,
			Description: gs.Description,
		}
		results = append(results, result)
	}

	// p 值校正 (Benjamini-Hochberg)
	if len(results) > 0 {
		pValues := make([]float64, len(results))
		for i, r := range results {
			pValues[i] = r.PValue
		}
		pAdjusted := AdjustFDR(pValues)
		for i, r := range results {
			r.PAdjust = pAdjusted[i]
		}

		// q 值 (FDR) 就是调整后的 p 值
		for _, r := range results {
			r.QValue = r.PAdjust
		}

		// 过滤：仅按 FDR(q-value) 过滤，不再叠加 raw p-value 条件
		var filtered []*EnrichmentResult
		for _, r := range results {
			if r.QValue <= params.QValueCutoff {
				filtered = append(filtered, r)
			}
		}
		results = filtered
	}

	// 按 p 值排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].PValue < results[j].PValue
	})

	return results
}
