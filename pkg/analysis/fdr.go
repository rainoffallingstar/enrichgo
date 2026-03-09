package analysis

import "sort"

// AdjustFDR Benjamini-Hochberg FDR 校正
func AdjustFDR(pValues []float64) []float64 {
	n := len(pValues)
	if n == 0 {
		return nil
	}

	// 按 p 值排序
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}
	sortedP := make([]float64, n)
	for i, p := range pValues {
		sortedP[i] = p
	}
	sort.Slice(indices, func(i, j int) bool {
		return sortedP[indices[i]] < sortedP[indices[j]]
	})

	// BH 方法
	adjusted := make([]float64, n)
	for i := 0; i < n; i++ {
		rank := i + 1
		adjusted[indices[i]] = sortedP[indices[i]] * float64(n) / float64(rank)
	}

	// 确保单调 (从后向前，按排序后的下标)
	for i := n - 2; i >= 0; i-- {
		if adjusted[indices[i]] > adjusted[indices[i+1]] {
			adjusted[indices[i]] = adjusted[indices[i+1]]
		}
	}

	// 限制在 [0, 1]
	for i := range adjusted {
		if adjusted[i] > 1 {
			adjusted[i] = 1
		}
		if adjusted[i] < 0 {
			adjusted[i] = 0
		}
	}

	return adjusted
}
