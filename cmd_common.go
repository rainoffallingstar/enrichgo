package main

import (
	"bufio"
	"fmt"
	"os"
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
