package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"enrichgo/pkg/database"
	"enrichgo/pkg/store"
	"enrichgo/pkg/types"
)

const embeddedBuildTimestamp = "2000-01-01T00:00:00Z"

type manifest struct {
	SchemaVersion   int    `json:"schema_version"`
	Artifact        string `json:"artifact"`
	SHA256          string `json:"sha256"`
	ContractProfile string `json:"contract_profile"`
	Species         string `json:"species"`
	IDMapsLevel     string `json:"idmaps_level"`
}

func main() {
	var (
		outDBPath    string
		outManifest  string
		artifactPath string
		dataDir      string
		species      string
		contract     string
		idMapsLevel  string
		goOntology   string
	)
	flag.StringVar(&outDBPath, "db", "assets/default_enrichgo.db", "output SQLite DB path")
	flag.StringVar(&outManifest, "manifest", "assets/default_enrichgo.db.manifest.json", "output manifest path")
	flag.StringVar(&artifactPath, "artifact", "assets/default_enrichgo.db", "manifest artifact path")
	flag.StringVar(&dataDir, "data-dir", "data", "local data directory")
	flag.StringVar(&species, "species", "hsa", "species code")
	flag.StringVar(&contract, "contract-profile", "embedded-hsa-basic", "manifest contract profile")
	flag.StringVar(&idMapsLevel, "idmaps-level", "basic", "manifest idmaps level")
	flag.StringVar(&goOntology, "go-ontology", "BP", "GO ontology to embed")
	flag.Parse()

	if err := os.MkdirAll(filepath.Dir(outDBPath), 0755); err != nil {
		fail("mkdir db dir", err)
	}
	if err := os.MkdirAll(filepath.Dir(outManifest), 0755); err != nil {
		fail("mkdir manifest dir", err)
	}
	if err := os.RemoveAll(outDBPath); err != nil {
		fail("remove existing db", err)
	}

	st, err := store.OpenSQLite(outDBPath)
	if err != nil {
		fail("open sqlite", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	version := strings.TrimSpace(contract)
	if version == "" {
		version = "embedded-static"
	}

	if err := writeKEGG(ctx, st, dataDir, species, version); err != nil {
		_ = st.Close()
		fail("write KEGG", err)
	}
	if err := writeGO(ctx, st, dataDir, species, goOntology, version); err != nil {
		_ = st.Close()
		fail("write GO", err)
	}
	if err := writeBasicIDMap(ctx, st, filepath.Join(dataDir, fmt.Sprintf("kegg_%s_idmap.tsv", strings.ToLower(strings.TrimSpace(species)))), species); err != nil {
		_ = st.Close()
		fail("write idmap", err)
	}
	if err := st.Close(); err != nil {
		fail("close sqlite", err)
	}
	if err := normalizeEmbeddedSQLite(outDBPath, version); err != nil {
		fail("normalize sqlite", err)
	}

	sum, err := fileSHA256(outDBPath)
	if err != nil {
		fail("hash sqlite", err)
	}
	m := manifest{
		SchemaVersion:   store.CurrentSchemaVersion,
		Artifact:        artifactPath,
		SHA256:          sum,
		ContractProfile: contract,
		Species:         species,
		IDMapsLevel:     idMapsLevel,
	}
	payload, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		fail("marshal manifest", err)
	}
	payload = append(payload, byte('\n'))
	if err := os.WriteFile(outManifest, payload, 0644); err != nil {
		fail("write manifest", err)
	}
}

func writeKEGG(ctx context.Context, st *store.SQLiteStore, dataDir, species, version string) error {
	data, err := database.LoadKEGG(species, dataDir)
	if err != nil {
		return err
	}
	if data == nil || len(data.Pathways) == 0 {
		return fmt.Errorf("local KEGG data missing for species=%s in %s", species, dataDir)
	}
	sets := make(types.GeneSets, 0, len(data.Pathways))
	for _, pw := range data.Pathways {
		sets = append(sets, &types.GeneSet{ID: pw.ID, Name: pw.Name, Description: pw.Description, Genes: pw.Genes})
	}
	sortGeneSets(sets)
	return st.ReplaceGeneSets(ctx, store.GeneSetFilter{DB: "kegg", Species: species}, "ENTREZID", sets, version)
}

func writeGO(ctx context.Context, st *store.SQLiteStore, dataDir, species, ontology, version string) error {
	path := filepath.Join(dataDir, fmt.Sprintf("go_%s_%s.gmt", species, strings.ToUpper(strings.TrimSpace(ontology))))
	data, err := database.LoadGO(path)
	if err != nil {
		return err
	}
	if data == nil || len(data.Terms) == 0 {
		return fmt.Errorf("local GO data missing at %s", path)
	}
	termToGenes := make(map[string]map[string]bool)
	for gene, terms := range data.Gene2Terms {
		for _, termID := range terms {
			if termToGenes[termID] == nil {
				termToGenes[termID] = make(map[string]bool)
			}
			termToGenes[termID][gene] = true
		}
	}
	sets := make(types.GeneSets, 0, len(data.Terms))
	for termID, term := range data.Terms {
		genes := termToGenes[termID]
		if len(genes) == 0 {
			continue
		}
		sets = append(sets, &types.GeneSet{ID: termID, Name: term.Name, Description: term.Definition, Genes: genes})
	}
	if len(sets) == 0 {
		return fmt.Errorf("no GO gene sets built from %s", path)
	}
	sortGeneSets(sets)
	return st.ReplaceGeneSets(ctx, store.GeneSetFilter{DB: "go", Species: species, Ontology: strings.ToUpper(strings.TrimSpace(ontology))}, "SYMBOL", sets, version)
}

func writeBasicIDMap(ctx context.Context, st *store.SQLiteStore, path, species string) error {
	symbolToEntrez, entrezToSymbol, err := loadKEGGIDMapTSV(path, species)
	if err != nil {
		return err
	}
	symbolRows := make([]store.IDMapRow, 0, len(symbolToEntrez))
	for sym, entrez := range symbolToEntrez {
		symbolRows = append(symbolRows, store.IDMapRow{From: sym, To: entrez})
	}
	sortIDMapRows(symbolRows)
	if err := st.ReplaceIDMap(ctx, species, "kegg_list", "SYMBOL", "ENTREZID", symbolRows); err != nil {
		return err
	}
	entrezRows := make([]store.IDMapRow, 0, len(entrezToSymbol))
	for entrez, sym := range entrezToSymbol {
		entrezRows = append(entrezRows, store.IDMapRow{From: entrez, To: sym})
	}
	sortIDMapRows(entrezRows)
	return st.ReplaceIDMap(ctx, species, "kegg_list", "ENTREZID", "SYMBOL", entrezRows)
}

func loadKEGGIDMapTSV(path, species string) (map[string]string, map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	symbolToEntrez := make(map[string]string)
	entrezToSymbol := make(map[string]string)
	species = strings.ToLower(strings.TrimSpace(species))
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "	")
		if len(fields) < 2 {
			continue
		}
		entrez := strings.TrimSpace(fields[0])
		symbol := strings.TrimSpace(fields[len(fields)-1])
		if species != "" && strings.HasPrefix(strings.ToLower(entrez), species+":") {
			entrez = strings.TrimSpace(entrez[len(species)+1:])
		} else if idx := strings.Index(entrez, ":"); idx >= 0 {
			entrez = strings.TrimSpace(entrez[idx+1:])
		}
		if strings.HasPrefix(strings.ToLower(entrez), "ncbi-geneid:") {
			entrez = strings.TrimSpace(entrez[len("ncbi-geneid:"):])
		}
		if entrez == "" || symbol == "" {
			continue
		}
		if _, ok := entrezToSymbol[entrez]; !ok {
			entrezToSymbol[entrez] = symbol
		}
		symKey := strings.ToUpper(symbol)
		if _, ok := symbolToEntrez[symKey]; !ok {
			symbolToEntrez[symKey] = entrez
		}
	}
	if err := sc.Err(); err != nil {
		return nil, nil, err
	}
	if len(symbolToEntrez) == 0 || len(entrezToSymbol) == 0 {
		return nil, nil, fmt.Errorf("empty KEGG idmap in %s", path)
	}
	return symbolToEntrez, entrezToSymbol, nil
}

func sortGeneSets(sets types.GeneSets) {
	sort.Slice(sets, func(i, j int) bool {
		left := sets[i]
		right := sets[j]
		if left == nil {
			return right != nil
		}
		if right == nil {
			return false
		}
		if left.ID != right.ID {
			return left.ID < right.ID
		}
		if left.Name != right.Name {
			return left.Name < right.Name
		}
		return left.Description < right.Description
	})
}

func sortIDMapRows(rows []store.IDMapRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].From != rows[j].From {
			return rows[i].From < rows[j].From
		}
		return rows[i].To < rows[j].To
	})
}

func normalizeEmbeddedSQLite(path, version string) error {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `UPDATE dataset SET version=?, downloaded_at=?`, version, embeddedBuildTimestamp); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE idmap_canon SET downloaded_at=?`, embeddedBuildTimestamp); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `VACUUM`)
	return err
}

func fileSHA256(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:]), nil
}

func fail(step string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", step, err)
	os.Exit(1)
}
