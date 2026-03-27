package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"enrichgo/pkg/annotation"
	"enrichgo/pkg/database"
)

func targetIDTypeForDatabase(db string) annotation.IDType {
	switch strings.ToLower(db) {
	case "kegg":
		return annotation.IDEntrez
	case "go", "reactome", "msigdb":
		return annotation.IDSymbol
	default:
		// custom/unknown: 不强制转换，沿用输入 ID
		return annotation.IDUnknown
	}
}

func parseMSigDBCollections(raw string) ([]database.MSigDBCollection, error) {
	v := strings.TrimSpace(strings.ToLower(raw))
	if v == "" {
		return database.DefaultMSigDBCollections(), nil
	}

	if v == "c1-c8" {
		return database.DefaultMSigDBCollections(), nil
	}
	if v == "all" {
		return []database.MSigDBCollection{
			database.MSigDBH,
			database.MSigDBC1,
			database.MSigDBC2,
			database.MSigDBC3,
			database.MSigDBC4,
			database.MSigDBC5,
			database.MSigDBC6,
			database.MSigDBC7,
			database.MSigDBC8,
		}, nil
	}

	parts := strings.Split(v, ",")
	seen := make(map[database.MSigDBCollection]bool)
	var cols []database.MSigDBCollection
	for _, part := range parts {
		col := database.MSigDBCollection(strings.TrimSpace(part))
		switch col {
		case database.MSigDBH, database.MSigDBC1, database.MSigDBC2, database.MSigDBC3,
			database.MSigDBC4, database.MSigDBC5, database.MSigDBC6, database.MSigDBC7, database.MSigDBC8:
			if !seen[col] {
				seen[col] = true
				cols = append(cols, col)
			}
		default:
			return nil, fmt.Errorf("unsupported MSigDB collection: %q", strings.TrimSpace(part))
		}
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("no valid MSigDB collection in %q", raw)
	}
	return cols, nil
}

// mergeDisplayMapFromConversion 将 orig->newIDs 的转换映射合并为 newID->orig 显示映射。
// 当一个 newID 对应多个 orig 时，保留首次出现的 orig（稳定且可复现）。
func mergeDisplayMapFromConversion(display map[string]string, mapping map[string][]string) {
	for orig, newIDs := range mapping {
		for _, id := range newIDs {
			if id == "" {
				continue
			}
			if _, exists := display[id]; !exists {
				display[id] = orig
			}
		}
	}
}

// mapIDsForDisplay 将结果中的基因 ID 映射为更易读的显示值（通常是 SYMBOL）。
func mapIDsForDisplay(ids []string, display map[string]string) []string {
	if len(ids) == 0 || len(display) == 0 {
		return ids
	}
	out := make([]string, len(ids))
	for i, id := range ids {
		if sym, ok := display[id]; ok && sym != "" {
			out[i] = sym
		} else {
			out[i] = id
		}
	}
	return out
}

// loadEntrezSymbolMapFromIDMap 从 kegg_<species>_idmap.tsv 读取 ENTREZ->SYMBOL 映射。
// 文件格式：第一列 ENTREZID，第二列 SYMBOL。
func loadEntrezSymbolMapFromIDMap(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	m := make(map[string]string)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) < 2 {
			continue
		}
		entrez := strings.TrimSpace(parts[0])
		symbol := strings.TrimSpace(parts[1])
		if entrez == "" || symbol == "" {
			continue
		}
		if _, exists := m[entrez]; !exists {
			m[entrez] = symbol
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return m, nil
}

const envKEGGIDCacheMaxEntries = "ENRICHGO_KEGG_ID_CACHE_MAX_ENTRIES"
const envKEGGIDCacheMetricsTSV = "ENRICHGO_KEGG_ID_CACHE_METRICS_TSV"

// resolveKEGGIDCacheMaxEntries resolves the max entries for KEGG ID conversion cache.
// Precedence: flag value (non-zero) > env (non-empty).
// Return values:
// - max: the resolved value
// - ok:  whether a non-default value should be applied
func resolveKEGGIDCacheMaxEntries(flagVal int) (max int, ok bool, err error) {
	if flagVal != 0 {
		return flagVal, true, nil
	}
	raw, exists := os.LookupEnv(envKEGGIDCacheMaxEntries)
	if !exists {
		return 0, false, nil
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "0" {
		return 0, false, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, true, fmt.Errorf("%s must be an integer (got %q): %w", envKEGGIDCacheMaxEntries, raw, err)
	}
	return v, true, nil
}

func writeKEGGIDCacheMetricsIfRequested(conv *annotation.KEGGIDConverter) {
	path := strings.TrimSpace(os.Getenv(envKEGGIDCacheMetricsTSV))
	if path == "" {
		return
	}
	var st annotation.KEGGIDCacheStats
	if conv != nil {
		st = conv.Stats()
	}
	content := "hits\tmisses\tevictions\tentries\tbuckets\tmax_entries\n"
	content += fmt.Sprintf("%d\t%d\t%d\t%d\t%d\t%d\n", st.Hits, st.Misses, st.Evictions, st.Entries, st.Buckets, st.MaxEntries)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to write KEGG ID cache metrics to %s: %v\n", path, err)
	}
}
