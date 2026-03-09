package analysis

import (
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"time"
)

// GSEAInput GSEA 输入
type GSEAInput struct {
	GeneList          []string           // 排序后的基因列表 (按数值降序)
	GeneValues        map[string]float64 // 基因 -> 值 (如 log2FC)
	GeneSets          GeneSets           // 基因集
	Workers           int                // 并行 worker 数，<=0 时自动使用 GOMAXPROCS
	Permutations      int                // 置换次数
	MinGSSize         int                // 最小基因集大小
	MaxGSSize         int                // 最大基因集大小
	PAdjustCutoff     float64            // 默认按校正后 p 值过滤（对齐 fgsea 输出域）
	PValueCutoff      float64            // 可选 raw p-value 过滤阈值（<=0 表示关闭）
	ApplyPValueCutoff bool               // 是否启用 raw p-value 过滤
	PValueMethod      string             // p 值估计方法: simple(默认), adaptive
	MaxPermutations   int                // adaptive 模式下单 pathway 最大置换次数
	UseSharedNESPool  bool               // 是否启用按基因集大小共享置换池进行 NES 归一化（实验特性）
	Seed              int64              // 随机种子
}

// GSEAResult GSEA 结果
type GSEAResult struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	NES             float64  `json:"nes"`             // Normalized Enrichment Score
	PValue          float64  `json:"pValue"`          // p 值
	PAdjust         float64  `json:"pAdjust"`         // 校正后 p 值
	QValue          float64  `json:"qValue"`          // q 值 (FDR)
	EnrichmentScore float64  `json:"enrichmentScore"` // 原始 Enrichment Score
	LeadGenes       []string `json:"leadGenes"`       // 领先基因
	Description     string   `json:"description"`
}

// RunGSEA 执行 GSEA 分析
func RunGSEA(input *GSEAInput) []*GSEAResult {
	baseSeed := input.Seed
	if baseSeed == 0 {
		baseSeed = time.Now().UnixNano()
	}

	// 确保基因列表已排序
	sortedGenes := make([]string, len(input.GeneList))
	copy(sortedGenes, input.GeneList)

	// 按值排序 (降序)
	sort.SliceStable(sortedGenes, func(i, j int) bool {
		left := input.GeneValues[sortedGenes[i]]
		right := input.GeneValues[sortedGenes[j]]
		if left != right {
			return left > right
		}
		return sortedGenes[i] < sortedGenes[j]
	})

	N := len(sortedGenes)
	if N == 0 || input.Permutations <= 0 {
		return nil
	}
	geneAbsValues := make([]float64, N)
	for i, g := range sortedGenes {
		geneAbsValues[i] = math.Abs(input.GeneValues[g])
	}

	type gseaCandidate struct {
		gs         *GeneSet
		hitIndices []int
	}
	var candidates []gseaCandidate
	for _, gs := range input.GeneSets {
		// 只按与排序基因列表重叠后的大小进行过滤，和 fgsea 口径一致。
		hitIndices := collectHitIndices(sortedGenes, gs.Genes)
		gsSize := len(hitIndices)
		if gsSize == 0 || gsSize < input.MinGSSize || gsSize > input.MaxGSSize || gsSize >= N {
			continue
		}
		candidates = append(candidates, gseaCandidate{gs: gs, hitIndices: hitIndices})
	}
	if len(candidates) == 0 {
		return nil
	}

	pMethod := input.PValueMethod
	if pMethod == "" {
		pMethod = "simple"
	}
	maxPerm := input.MaxPermutations
	if maxPerm <= 0 {
		maxPerm = input.Permutations
	}
	targetP := input.PAdjustCutoff / float64(len(candidates))
	if targetP <= 0 {
		targetP = 1e-6
	}
	workers := input.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers < 1 {
		workers = 1
	}
	if workers > len(candidates) {
		workers = len(candidates)
	}

	// Shared permutation pools by pathway size to stabilize NES normalization
	// (closer to fgseaSimpleImpl's per-size denominator behavior).
	sharedPermPools := make(map[int][]float64)
	if input.UseSharedNESPool {
		sizeSeen := make(map[int]struct{})
		var sizes []int
		for _, cand := range candidates {
			size := len(cand.hitIndices)
			if _, ok := sizeSeen[size]; ok {
				continue
			}
			sizeSeen[size] = struct{}{}
			sizes = append(sizes, size)
		}
		sort.Ints(sizes)
		nPool := input.Permutations
		if nPool < 500 {
			nPool = 500
		}
		for _, gsSize := range sizes {
			pool := make([]float64, nPool)
			poolRNG := rand.New(rand.NewSource(baseSeed + int64(gsSize)*1469598103934665603))
			poolSampler := newIndexSampler(N, poolRNG)
			for i := 0; i < nPool; i++ {
				permHit := poolSampler.Sample(gsSize)
				es, _ := calculateEnrichmentScore(geneAbsValues, permHit)
				pool[i] = es
			}
			sharedPermPools[gsSize] = pool
		}
	}

	// 对每个基因集进行 GSEA
	results := make([]*GSEAResult, len(candidates))
	jobs := make(chan int, len(candidates))
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				cand := candidates[idx]
				gs := cand.gs
				hitIndices := cand.hitIndices
				workerRNG := rand.New(rand.NewSource(baseSeed + int64(idx+1)*1099511628211))
				workerSampler := newIndexSampler(N, workerRNG)

				// 观测 ES（按排名值加权）。
				es, peakIdx := calculateEnrichmentScore(geneAbsValues, hitIndices)

				var pValue, nes float64
				switch pMethod {
				case "adaptive":
					pValue, nes = calculateAdaptivePermutationStats(
						es, len(hitIndices), geneAbsValues, workerSampler,
						input.Permutations, maxPerm, targetP,
					)
					// Optional shared-by-size NES normalization (experimental).
					if input.UseSharedNESPool {
						if pool := sharedPermPools[len(hitIndices)]; len(pool) > 0 {
							nes = normalizeNESByPool(es, pool)
						}
					}
				default:
					// 基于随机基因集置换生成该基因集的零分布（按同号计算 p 值和 NES）。
					permES := make([]float64, input.Permutations)
					for p := 0; p < input.Permutations; p++ {
						permHit := workerSampler.Sample(len(hitIndices))
						permES[p], _ = calculateEnrichmentScore(geneAbsValues, permHit)
					}
					pValue, nes = calculatePermutationStats(es, permES)
				}

				results[idx] = &GSEAResult{
					ID:              gs.ID,
					Name:            gs.Name,
					NES:             nes,
					EnrichmentScore: es,
					PValue:          pValue,
					LeadGenes:       calculateLeadingEdge(sortedGenes, hitIndices, peakIdx, es),
					Description:     gs.Description,
				}
			}
		}()
	}
	for i := range candidates {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	// 清理空项（防御性处理）
	filteredResults := results[:0]
	for _, r := range results {
		if r != nil {
			filteredResults = append(filteredResults, r)
		}
	}
	results = filteredResults

	// p 值校正
	if len(results) > 0 {
		pValues := make([]float64, len(results))
		for i, r := range results {
			pValues[i] = r.PValue
		}
		pAdjusted := AdjustFDR(pValues)
		for i, r := range results {
			r.PAdjust = pAdjusted[i]
			r.QValue = pAdjusted[i]
		}

		// 过滤：默认按校正后 p 值（与 clusterProfiler/fgsea 输出口径一致）。
		// raw p-value 过滤仅在显式开启时叠加应用。
		var filtered []*GSEAResult
		for _, r := range results {
			if r.PAdjust > input.PAdjustCutoff {
				continue
			}
			if input.ApplyPValueCutoff && r.PValue > input.PValueCutoff {
				continue
			}
			filtered = append(filtered, r)
		}
		results = filtered
	}

	// 按显著性优先排序，次序稳定便于对齐比较。
	sort.Slice(results, func(i, j int) bool {
		if results[i].PAdjust != results[j].PAdjust {
			return results[i].PAdjust < results[j].PAdjust
		}
		if results[i].PValue != results[j].PValue {
			return results[i].PValue < results[j].PValue
		}
		return results[i].ID < results[j].ID
	})

	return results
}

func normalizeNESByPool(observedES float64, permES []float64) float64 {
	if len(permES) == 0 {
		return observedES
	}
	geZero := 0
	leZero := 0
	sumPos := 0.0
	sumNegAbs := 0.0
	for _, es := range permES {
		if es >= 0 {
			geZero++
		}
		if es <= 0 {
			leZero++
		}
		if es > 0 {
			sumPos += es
		} else if es < 0 {
			sumNegAbs += math.Abs(es)
		}
	}
	nes := observedES
	if observedES > 0 && geZero > 0 {
		meanPos := sumPos / float64(geZero)
		if meanPos > 0 {
			nes = observedES / meanPos
		}
	} else if observedES < 0 && leZero > 0 {
		meanNegAbs := sumNegAbs / float64(leZero)
		if meanNegAbs > 0 {
			nes = observedES / meanNegAbs
		}
	}
	return nes
}

func calculateAdaptivePermutationStats(
	observedES float64,
	gsSize int,
	geneAbsValues []float64,
	sampler *indexSampler,
	initPerm int,
	maxPerm int,
	targetP float64,
) (float64, float64) {
	if initPerm <= 0 {
		initPerm = 1000
	}
	if maxPerm < initPerm {
		maxPerm = initPerm
	}
	if targetP <= 0 {
		targetP = 1e-6
	}

	total := 0
	geEs := 0
	leEs := 0
	geZero := 0
	leZero := 0
	sumPos := 0.0
	sumNegAbs := 0.0
	posVals := make([]float64, 0, initPerm)
	negAbsVals := make([]float64, 0, initPerm)

	updateByES := func(es float64) {
		total++
		if es >= observedES {
			geEs++
		}
		if es <= observedES {
			leEs++
		}
		if es >= 0 {
			geZero++
		}
		if es <= 0 {
			leZero++
		}
		if es > 0 {
			sumPos += es
			posVals = append(posVals, es)
		} else if es < 0 {
			sumNegAbs += math.Abs(es)
			negAbsVals = append(negAbsVals, math.Abs(es))
		}
	}

	evalP := func() float64 {
		pGe := 1.0
		pLe := 1.0
		if geZero > 0 {
			pGe = float64(geEs+1) / float64(geZero+1)
		}
		if leZero > 0 {
			pLe = float64(leEs+1) / float64(leZero+1)
		}
		return math.Min(pGe, pLe)
	}

	// 初始置换
	for i := 0; i < initPerm; i++ {
		permHit := sampler.Sample(gsSize)
		es, _ := calculateEnrichmentScore(geneAbsValues, permHit)
		updateByES(es)
	}

	batch := initPerm
	if batch < 1000 {
		batch = 1000
	}
	for total < maxPerm {
		pNow := evalP()
		// 已达到目标精度后停止。
		if pNow <= targetP && total >= initPerm*2 {
			break
		}
		// 明显不显著时在完成最低采样后停止。
		if pNow > 0.2 && total >= initPerm*2 {
			break
		}

		toRun := batch
		if total+toRun > maxPerm {
			toRun = maxPerm - total
		}
		for i := 0; i < toRun; i++ {
			permHit := sampler.Sample(gsSize)
			es, _ := calculateEnrichmentScore(geneAbsValues, permHit)
			updateByES(es)
		}
	}

	pValue := evalP()
	// Tail extrapolation to break the 1/nPerm floor and approximate multilevel behavior.
	if observedES > 0 {
		pTail := estimateTailPValue(observedES, posVals)
		if pTail > 0 && pTail < pValue {
			pValue = pTail
		}
	} else if observedES < 0 {
		pTail := estimateTailPValue(math.Abs(observedES), negAbsVals)
		if pTail > 0 && pTail < pValue {
			pValue = pTail
		}
	}

	nes := observedES
	if observedES > 0 && geZero > 0 {
		meanPos := sumPos / float64(geZero)
		if meanPos > 0 {
			nes = observedES / meanPos
		}
	} else if observedES < 0 && leZero > 0 {
		meanNegAbs := sumNegAbs / float64(leZero)
		if meanNegAbs > 0 {
			nes = observedES / meanNegAbs
		}
	}
	return pValue, nes
}

func estimateTailPValue(observedAbs float64, vals []float64) float64 {
	n := len(vals)
	if n < 200 || observedAbs <= 0 {
		return 0
	}
	sort.Float64s(vals)
	// 90th percentile threshold
	uIdx := int(0.9 * float64(n))
	if uIdx < 0 {
		uIdx = 0
	}
	if uIdx >= n {
		uIdx = n - 1
	}
	u := vals[uIdx]
	if observedAbs <= u {
		return 0
	}
	tailVals := vals[uIdx:]
	m := len(tailVals)
	if m < 30 {
		return 0
	}
	sumExcess := 0.0
	for _, v := range tailVals {
		sumExcess += (v - u)
	}
	meanExcess := sumExcess / float64(m)
	if meanExcess <= 0 {
		return 0
	}
	// Exponential POT approximation (GPD with xi=0)
	tailProb := float64(m) / float64(n)
	est := tailProb * math.Exp(-(observedAbs-u)/meanExcess)
	if est < 1e-300 {
		est = 1e-300
	}
	if est > 1 {
		est = 1
	}
	return est
}

// calculateEnrichmentScore 计算加权 ES，并返回峰值位置（用于 leading edge）。
// hitIndices 需为升序（由 collectHitIndices / sampler 保证）。
func calculateEnrichmentScore(geneAbsValues []float64, hitIndices []int) (float64, int) {
	N := len(geneAbsValues)
	Nh := len(hitIndices)
	if Nh == 0 || Nh >= N {
		return 0, -1
	}

	maxES := 0.0
	minES := 0.0
	maxIdx := -1
	minIdx := -1
	missStep := 1.0 / float64(N-Nh)

	normHit := 0.0
	for _, idx := range hitIndices {
		normHit += geneAbsValues[idx]
	}
	if normHit == 0 {
		normHit = float64(Nh)
	}

	running := 0.0
	prevHit := -1
	for _, hit := range hitIndices {
		gap := hit - prevHit - 1
		if gap > 0 {
			running -= float64(gap) * missStep
			if running < minES {
				minES = running
				minIdx = hit - 1
			}
		}

		w := geneAbsValues[hit]
		if w == 0 {
			w = 1.0
		}
		running += w / normHit
		if running > maxES {
			maxES = running
			maxIdx = hit
		}
		if running < minES {
			minES = running
			minIdx = hit
		}
		prevHit = hit
	}
	if prevHit < N-1 {
		running -= float64(N-prevHit-1) * missStep
		if running < minES {
			minES = running
			minIdx = N - 1
		}
	}

	if math.Abs(maxES) >= math.Abs(minES) {
		return maxES, maxIdx
	}
	return minES, minIdx
}

// calculatePermutationStats 计算同号置换的 p 值和 NES。
func calculatePermutationStats(observedES float64, permES []float64) (float64, float64) {
	if len(permES) == 0 {
		return 1.0, observedES
	}

	geEs := 0
	leEs := 0
	geZero := 0
	leZero := 0
	sumPos := 0.0
	sumNegAbs := 0.0
	for _, es := range permES {
		if es >= observedES {
			geEs++
		}
		if es <= observedES {
			leEs++
		}
		if es >= 0 {
			geZero++
		}
		if es <= 0 {
			leZero++
		}
		if es > 0 {
			sumPos += es
		} else if es < 0 {
			sumNegAbs += math.Abs(es)
		}
	}

	pGe := 1.0
	pLe := 1.0
	if geZero > 0 {
		pGe = float64(geEs+1) / float64(geZero+1)
	}
	if leZero > 0 {
		pLe = float64(leEs+1) / float64(leZero+1)
	}
	pValue := math.Min(pGe, pLe)

	nes := observedES
	if observedES > 0 && geZero > 0 {
		meanPos := sumPos / float64(geZero)
		if meanPos > 0 {
			nes = observedES / meanPos
		}
	} else if observedES < 0 && leZero > 0 {
		meanNegAbs := sumNegAbs / float64(leZero)
		if meanNegAbs > 0 {
			nes = observedES / meanNegAbs
		}
	}

	return pValue, nes
}

func calculateLeadingEdge(genes []string, hitIndices []int, peakIdx int, es float64) []string {
	if len(hitIndices) == 0 || peakIdx < 0 {
		return nil
	}
	var leadGenes []string
	for _, idx := range hitIndices {
		if es >= 0 && idx <= peakIdx {
			leadGenes = append(leadGenes, genes[idx])
		}
		if es < 0 && idx >= peakIdx {
			leadGenes = append(leadGenes, genes[idx])
		}
	}
	return leadGenes
}

func collectHitIndices(genes []string, geneSet map[string]bool) []int {
	hits := make([]int, 0, len(geneSet))
	for i, g := range genes {
		if geneSet[g] {
			hits = append(hits, i)
		}
	}
	return hits
}

// indexSampler 复用标记数组，降低每次置换采样的分配和哈希开销。
type indexSampler struct {
	n      int
	rng    *rand.Rand
	marks  []uint32
	epoch  uint32
	buffer []int
}

func newIndexSampler(n int, rng *rand.Rand) *indexSampler {
	return &indexSampler{
		n:     n,
		rng:   rng,
		marks: make([]uint32, n),
	}
}

func (s *indexSampler) Sample(k int) []int {
	if k <= 0 || s.n <= 0 {
		return nil
	}
	if k >= s.n {
		out := make([]int, s.n)
		for i := 0; i < s.n; i++ {
			out[i] = i
		}
		return out
	}

	s.epoch++
	if s.epoch == 0 {
		for i := range s.marks {
			s.marks[i] = 0
		}
		s.epoch = 1
	}

	if cap(s.buffer) < k {
		s.buffer = make([]int, 0, k)
	}
	out := s.buffer[:0]
	for len(out) < k {
		idx := s.rng.Intn(s.n)
		if s.marks[idx] == s.epoch {
			continue
		}
		s.marks[idx] = s.epoch
		out = append(out, idx)
	}
	sort.Ints(out)
	s.buffer = out
	return out
}
