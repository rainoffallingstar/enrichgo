package database

import (
	"bufio"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// KEGGSpeciesIDMap holds a best-effort offline mapping derived from KEGG's gene list endpoint.
// It includes:
// - Entrez-like gene ID (right of "<species>:") -> primary SYMBOL
// - SYMBOL/aliases -> Entrez-like gene ID
type KEGGSpeciesIDMap struct {
	EntrezToSymbol map[string]string
	SymbolToEntrez map[string]string
}

func FetchKEGGSpeciesIDMap(species string) (*KEGGSpeciesIDMap, error) {
	species = strings.ToLower(strings.TrimSpace(species))
	if species == "" {
		return nil, fmt.Errorf("empty species")
	}

	url := fmt.Sprintf("https://rest.kegg.jp/list/%s", species)
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("KEGG API error: %s", resp.Status)
	}

	m := &KEGGSpeciesIDMap{
		EntrezToSymbol: make(map[string]string),
		SymbolToEntrez: make(map[string]string),
	}

	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		fields := strings.Split(line, "\t")
		if len(fields) < 2 {
			continue
		}
		left := strings.TrimSpace(fields[0]) // "<species>:<gene>"
		right := strings.TrimSpace(fields[len(fields)-1])
		if left == "" || right == "" {
			continue
		}
		entrez := strings.TrimPrefix(left, species+":")
		entrez = strings.TrimSpace(entrez)
		if entrez == "" {
			continue
		}

		genesPart := right
		if idx := strings.Index(right, ";"); idx >= 0 {
			genesPart = right[:idx]
		}
		aliases := strings.Split(genesPart, ",")
		if len(aliases) == 0 {
			continue
		}
		primary := strings.TrimSpace(aliases[0])
		if primary == "" {
			continue
		}

		if _, ok := m.EntrezToSymbol[entrez]; !ok {
			m.EntrezToSymbol[entrez] = primary
		}
		for _, a := range aliases {
			sym := strings.ToUpper(strings.TrimSpace(a))
			if sym == "" {
				continue
			}
			if _, exists := m.SymbolToEntrez[sym]; !exists {
				m.SymbolToEntrez[sym] = entrez
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(m.EntrezToSymbol) == 0 {
		return nil, fmt.Errorf("empty mapping returned from KEGG list")
	}
	return m, nil
}

type KEGGLinkPair struct {
	Entrez   string
	External string
}

// FetchKEGGLinks fetches KEGG link mappings for a species, returning pairs of:
// - Entrez-like gene ID (right of "<species>:") and
// - external identifier (prefix stripped if present).
//
// Known targets that tend to work for common species include: "uniprot", "ensembl", "refseq".
func FetchKEGGLinks(species, target string) ([]KEGGLinkPair, error) {
	species = strings.ToLower(strings.TrimSpace(species))
	target = strings.ToLower(strings.TrimSpace(target))
	if species == "" || target == "" {
		return nil, fmt.Errorf("empty species/target")
	}

	url := fmt.Sprintf("https://rest.kegg.jp/link/%s/%s", target, species)
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("KEGG API error: %s", resp.Status)
	}

	var pairs []KEGGLinkPair
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) < 2 {
			continue
		}
		left := strings.TrimSpace(parts[0])  // "<species>:<gene>"
		right := strings.TrimSpace(parts[1]) // "<db>:<id>" or "<id>"
		if left == "" || right == "" {
			continue
		}
		entrez := strings.TrimPrefix(left, species+":")
		entrez = strings.TrimSpace(entrez)
		if entrez == "" {
			continue
		}
		ext := right
		if idx := strings.Index(ext, ":"); idx >= 0 {
			ext = ext[idx+1:]
		}
		ext = strings.TrimSpace(ext)
		if ext == "" {
			continue
		}
		pairs = append(pairs, KEGGLinkPair{Entrez: entrez, External: ext})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return pairs, nil
}

