package annotation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"enrichgo/pkg/store"
)

// SQLiteIDConverter converts IDs using an offline SQLite idmap table.
// It performs multi-hop conversion through ENTREZ when needed.
type SQLiteIDConverter struct {
	st      *store.SQLiteStore
	timeout time.Duration
}

func NewSQLiteIDConverter(st *store.SQLiteStore) *SQLiteIDConverter {
	return &SQLiteIDConverter{st: st, timeout: 30 * time.Second}
}

func (c *SQLiteIDConverter) Convert(geneIDs []string, fromType, toType IDType, species string) (map[string][]string, error) {
	if c == nil || c.st == nil {
		return nil, fmt.Errorf("sqlite converter not initialized")
	}
	if len(geneIDs) == 0 {
		return map[string][]string{}, nil
	}
	species = strings.ToLower(strings.TrimSpace(species))
	if species == "" {
		return nil, fmt.Errorf("empty species")
	}

	// Handle KEGG as a formatting layer over ENTREZ-like gene IDs.
	if fromType == IDKEGG {
		fromType = IDEntrez
	}
	if toType == IDKEGG {
		// Keep as a final formatting step.
	}

	// Step 1: map input -> ENTREZ IDs (or normalize ENTREZ directly).
	entrezByOrig, err := c.toEntrez(geneIDs, fromType, species)
	if err != nil {
		return nil, err
	}

	// Step 2: ENTREZ -> target.
	if toType == IDEntrez {
		return entrezByOrig, nil
	}
	if toType == IDKEGG {
		out := make(map[string][]string, len(entrezByOrig))
		for orig, entrezIDs := range entrezByOrig {
			for _, e := range entrezIDs {
				e = strings.TrimSpace(e)
				if e == "" {
					continue
				}
				out[orig] = append(out[orig], species+":"+e)
			}
			out[orig] = uniqueStrings(out[orig])
		}
		return out, nil
	}

	// Collect unique ENTREZ IDs for a batched lookup.
	var allEntrez []string
	seen := make(map[string]bool)
	for _, ids := range entrezByOrig {
		for _, e := range ids {
			e = strings.TrimSpace(e)
			if e == "" || seen[e] {
				continue
			}
			seen[e] = true
			allEntrez = append(allEntrez, e)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	targetMap, err := c.st.LookupIDMap(ctx, species, string(IDEntrez), string(toType), allEntrez)
	if err != nil {
		return nil, err
	}

	out := make(map[string][]string, len(entrezByOrig))
	for orig, entrezIDs := range entrezByOrig {
		for _, e := range entrezIDs {
			for _, v := range targetMap[e] {
				v = strings.TrimSpace(v)
				if v != "" {
					out[orig] = append(out[orig], v)
				}
			}
		}
		out[orig] = uniqueStrings(out[orig])
	}
	return out, nil
}

func (c *SQLiteIDConverter) toEntrez(geneIDs []string, fromType IDType, species string) (map[string][]string, error) {
	out := make(map[string][]string, len(geneIDs))
	switch fromType {
	case IDEntrez:
		for _, orig := range geneIDs {
			e := normalizeEntrezLocal(orig, species)
			if e != "" {
				out[orig] = []string{e}
			} else {
				out[orig] = nil
			}
		}
		return out, nil
	case IDSymbol, IDUniprot, IDEnsembl, IDRefSeq:
		// continue
	default:
		return nil, fmt.Errorf("unsupported fromType for sqlite conversion: %s", fromType)
	}

	// Normalize input IDs for lookup while keeping the original keys.
	normByOrig := make(map[string]string, len(geneIDs))
	var norms []string
	seen := make(map[string]bool)
	for _, orig := range geneIDs {
		n := normalizeByTypeLocal(orig, fromType, species)
		normByOrig[orig] = n
		if n == "" || seen[n] {
			continue
		}
		seen[n] = true
		norms = append(norms, n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	entrezMap, err := c.st.LookupIDMap(ctx, species, string(fromType), string(IDEntrez), norms)
	if err != nil {
		return nil, err
	}
	for orig, n := range normByOrig {
		out[orig] = uniqueStrings(entrezMap[n])
	}
	return out, nil
}

func normalizeByTypeLocal(s string, t IDType, species string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	switch t {
	case IDSymbol:
		return strings.ToUpper(s)
	case IDEntrez:
		return normalizeEntrezLocal(s, species)
	case IDKEGG:
		return normalizeEntrezLocal(s, species)
	case IDUniprot:
		// Common forms: "P12345", "uniprot:P12345", "up:P12345"
		s = strings.TrimPrefix(s, "uniprot:")
		s = strings.TrimPrefix(s, "up:")
		return strings.TrimSpace(s)
	case IDEnsembl:
		s = strings.TrimPrefix(strings.ToUpper(s), "ENSEMBL:")
		return strings.TrimSpace(s)
	case IDRefSeq:
		s = strings.TrimPrefix(strings.ToUpper(s), "REFSEQ:")
		return strings.TrimSpace(s)
	default:
		return s
	}
}

func normalizeEntrezLocal(s, species string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, species+":")
	s = strings.TrimPrefix(s, "ncbi-geneid:")
	return strings.TrimSpace(s)
}

