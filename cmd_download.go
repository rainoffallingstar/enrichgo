package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"enrichgo/pkg/annotation"
	"enrichgo/pkg/database"
	"enrichgo/pkg/store"
	"enrichgo/pkg/types"
)

// downloadCmd 数据库下载命令
type downloadCmd struct {
	database    string
	species     string
	ontology    string
	collection  string
	outputDir   string
	all         bool
	dbPath      string
	dbOnly      bool
	withIDMaps  bool
	idMapsLevel string
}

func runDownload(cmd *flag.FlagSet) {
	c := &downloadCmd{}

	cmd.StringVar(&c.database, "d", "kegg", "Database: kegg, go, msigdb, reactome, all")
	cmd.StringVar(&c.species, "s", "hsa", "Species code (e.g., hsa, mmu)")
	cmd.StringVar(&c.ontology, "ont", "BP", "GO ontology: BP, MF, CC, ALL")
	cmd.StringVar(&c.collection, "c", "c1-c8", "MSigDB collection(s): h, c1-c8, c1,c2,..., all")
	cmd.StringVar(&c.outputDir, "o", "data", "Output directory")
	cmd.BoolVar(&c.all, "all", false, "Download all collections (h,c1-c8) for msigdb")
	cmd.StringVar(&c.dbPath, "db", "", "Optional SQLite cache DB path (offline bundle). When set, downloaded data is stored into this DB")
	cmd.BoolVar(&c.dbOnly, "db-only", false, "When used with --db, skip writing GMT/TSV cache files and only write into the SQLite DB")
	cmd.BoolVar(&c.withIDMaps, "idmaps", true, "When used with --db, also fetch and store offline ID mappings (SYMBOL/ENTREZ/UNIPROT/ENSEMBL/REFSEQ)")
	cmd.StringVar(&c.idMapsLevel, "idmaps-level", "basic", "ID mapping level when used with --db --idmaps: basic (KEGG list/link) or extended (NCBI+UniProt dumps; larger but more complete)")

	cmd.Parse(os.Args[2:])

	outputDir := c.outputDir
	if c.dbOnly {
		outputDir = ""
	}
	if outputDir != "" {
		// 创建输出目录
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating output directory: %v\n", err)
			os.Exit(1)
		}
	}

	var st *store.SQLiteStore
	if strings.TrimSpace(c.dbPath) != "" {
		var err error
		st, err = store.OpenSQLite(c.dbPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening sqlite db: %v\n", err)
			os.Exit(1)
		}
		defer st.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	snapshotVersion := sqliteSnapshotVersion(time.Now())

	runKEGG := func() {
		fmt.Printf("Downloading KEGG data for %s...\n", c.species)
		data, err := database.DownloadKEGG(c.species, outputDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Downloaded %d pathways\n", len(data.Pathways))
		if st != nil {
			sets := keggToGeneSets(data)
			if err := st.ReplaceGeneSets(ctx, store.GeneSetFilter{DB: "kegg", Species: c.species}, string(annotation.IDEntrez), sets, snapshotVersion); err != nil {
				fmt.Fprintf(os.Stderr, "Error writing KEGG to sqlite: %v\n", err)
				os.Exit(1)
			}
		}
	}

	runGO := func() {
		ontologies := []string{"BP", "MF", "CC"}
		if strings.ToUpper(c.ontology) != "ALL" {
			ontologies = []string{c.ontology}
		}
		for _, ont := range ontologies {
			ont = strings.ToUpper(strings.TrimSpace(ont))
			fmt.Printf("Downloading GO %s data for %s...\n", ont, c.species)
			data, err := database.DownloadGO(c.species, ont, outputDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Downloaded %d GO terms\n", len(data.Terms))
			if st != nil {
				sets := goToGeneSets(data)
				if err := st.ReplaceGeneSets(ctx, store.GeneSetFilter{DB: "go", Species: c.species, Ontology: ont}, string(annotation.IDSymbol), sets, snapshotVersion); err != nil {
					fmt.Fprintf(os.Stderr, "Error writing GO to sqlite: %v\n", err)
					os.Exit(1)
				}
			}
		}
	}

	runMSigDB := func() {
		selection := c.collection
		if c.all {
			selection = "all"
		}
		collections, err := parseMSigDBCollections(selection)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		total := 0
		for _, col := range collections {
			fmt.Printf("Downloading MSigDB %s...\n", col)
			sets, err := database.DownloadMSigDB(col, outputDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error downloading %s: %v\n", col, err)
				os.Exit(1)
			}
			total += len(sets)
			if st != nil {
				if err := st.ReplaceGeneSets(ctx, store.GeneSetFilter{DB: "msigdb", Species: c.species, Collection: string(col)}, string(annotation.IDSymbol), types.GeneSets(sets), snapshotVersion); err != nil {
					fmt.Fprintf(os.Stderr, "Error writing MSigDB to sqlite: %v\n", err)
					os.Exit(1)
				}
			}
		}
		fmt.Printf("Downloaded %d gene sets across %d collection(s)\n", total, len(collections))
	}

	runReactome := func() {
		fmt.Printf("Downloading Reactome data for %s...\n", c.species)
		data, err := database.LoadOrDownloadReactome(c.species, outputDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Downloaded %d pathways\n", len(data.Pathways))
		if st != nil {
			sets := reactomeToGeneSets(data)
			if err := st.ReplaceGeneSets(ctx, store.GeneSetFilter{DB: "reactome", Species: c.species}, string(annotation.IDSymbol), sets, snapshotVersion); err != nil {
				fmt.Fprintf(os.Stderr, "Error writing Reactome to sqlite: %v\n", err)
				os.Exit(1)
			}
		}
	}

	switch strings.ToLower(strings.TrimSpace(c.database)) {
	case "kegg":
		runKEGG()
	case "go":
		runGO()
	case "msigdb":
		runMSigDB()
	case "reactome":
		runReactome()
	case "all":
		runKEGG()
		runGO()
		runReactome()
		runMSigDB()
	default:
		fmt.Printf("Error: unknown database: %s\n", c.database)
		os.Exit(1)
	}

	if st != nil && c.withIDMaps {
		level := strings.ToLower(strings.TrimSpace(c.idMapsLevel))
		if level == "extended" {
			// Extended mappings can take a long time on slow networks.
			cancel()
			ctx, cancel = context.WithTimeout(context.Background(), 240*time.Minute)
			defer cancel()
		}
		switch level {
		case "", "basic":
			if err := writeBasicIDMapsToSQLite(ctx, st, c.species); err != nil {
				fmt.Fprintf(os.Stderr, "Error writing idmaps to sqlite: %v\n", err)
				os.Exit(1)
			}
		case "extended":
			if err := writeExtendedIDMapsToSQLite(ctx, st, c.species); err != nil {
				fmt.Fprintf(os.Stderr, "Error writing idmaps to sqlite: %v\n", err)
				os.Exit(1)
			}
		default:
			fmt.Fprintf(os.Stderr, "Error: unknown --idmaps-level %q (use basic or extended)\n", c.idMapsLevel)
			os.Exit(1)
		}
	}

	fmt.Println("Done!")
}

func sqliteSnapshotVersion(now time.Time) string {
	return "snapshot-" + now.UTC().Format("20060102T150405Z")
}

func keggToGeneSets(data *database.KEGGData) types.GeneSets {
	var sets types.GeneSets
	for _, pw := range data.Pathways {
		sets = append(sets, &types.GeneSet{
			ID:          pw.ID,
			Name:        pw.Name,
			Genes:       pw.Genes,
			Description: pw.Description,
		})
	}
	return sets
}

func reactomeToGeneSets(data *database.ReactomeData) types.GeneSets {
	var sets types.GeneSets
	for _, pw := range data.Pathways {
		sets = append(sets, &types.GeneSet{
			ID:          pw.ID,
			Name:        pw.Name,
			Genes:       pw.Genes,
			Description: pw.Description,
		})
	}
	return sets
}

func goToGeneSets(data *database.GOData) types.GeneSets {
	term2genes := make(map[string]map[string]bool)
	for gene, terms := range data.Gene2Terms {
		for _, termID := range terms {
			if term2genes[termID] == nil {
				term2genes[termID] = make(map[string]bool)
			}
			term2genes[termID][gene] = true
		}
	}
	var sets types.GeneSets
	for termID, term := range data.Terms {
		sets = append(sets, &types.GeneSet{
			ID:          termID,
			Name:        term.Name,
			Genes:       term2genes[termID],
			Description: term.Definition,
		})
	}
	return sets
}

func writeBasicIDMapsToSQLite(ctx context.Context, st *store.SQLiteStore, species string) error {
	m, err := database.FetchKEGGSpeciesIDMap(species)
	if err != nil {
		return err
	}

	// SYMBOL <-> ENTREZID derived from KEGG list.
	{
		rows := make([]store.IDMapRow, 0, len(m.SymbolToEntrez))
		for sym, entrez := range m.SymbolToEntrez {
			rows = append(rows, store.IDMapRow{From: sym, To: entrez})
		}
		if err := st.ReplaceIDMap(ctx, species, "kegg_list", string(annotation.IDSymbol), string(annotation.IDEntrez), rows); err != nil {
			return err
		}
	}
	{
		rows := make([]store.IDMapRow, 0, len(m.EntrezToSymbol))
		for entrez, sym := range m.EntrezToSymbol {
			rows = append(rows, store.IDMapRow{From: entrez, To: sym})
		}
		if err := st.ReplaceIDMap(ctx, species, "kegg_list", string(annotation.IDEntrez), string(annotation.IDSymbol), rows); err != nil {
			return err
		}
	}

	type linkSpec struct {
		target string
		typ    annotation.IDType
	}
	links := []linkSpec{
		{target: "uniprot", typ: annotation.IDUniprot},
		{target: "ensembl", typ: annotation.IDEnsembl},
		{target: "refseq", typ: annotation.IDRefSeq},
	}
	for _, spec := range links {
		pairs, err := database.FetchKEGGLinks(species, spec.target)
		if err != nil {
			// Not all targets exist for all species; keep going.
			fmt.Fprintf(os.Stderr, "Warning: failed to fetch KEGG link %s for %s: %v\n", spec.target, species, err)
			continue
		}
		if len(pairs) == 0 {
			continue
		}

		entrezToExt := make([]store.IDMapRow, 0, len(pairs))
		extToEntrez := make([]store.IDMapRow, 0, len(pairs))
		for _, p := range pairs {
			entrezToExt = append(entrezToExt, store.IDMapRow{From: p.Entrez, To: p.External})
			extToEntrez = append(extToEntrez, store.IDMapRow{From: p.External, To: p.Entrez})
		}
		source := "kegg_link_" + spec.target
		if err := st.ReplaceIDMap(ctx, species, source, string(annotation.IDEntrez), string(spec.typ), entrezToExt); err != nil {
			return err
		}
		if err := st.ReplaceIDMap(ctx, species, source, string(spec.typ), string(annotation.IDEntrez), extToEntrez); err != nil {
			return err
		}
	}
	return nil
}

func writeExtendedIDMapsToSQLite(ctx context.Context, st *store.SQLiteStore, species string) error {
	taxID, err := database.TaxIDForSpecies(species)
	if err != nil {
		return err
	}

	// NCBI gene_info: SYMBOL <-> ENTREZ
	if err := st.ReplaceIDMapStream(ctx, species, "ncbi_gene_info", string(annotation.IDEntrez), string(annotation.IDSymbol), func(emit store.IDMapEmit) error {
		return database.StreamNCBIGeneInfoForSpecies(species, taxID, nil,
			func(entrez, symbol string) error { return emit(entrez, symbol) },
			func(symbol, entrez string) error { return nil },
		)
	}); err != nil {
		return err
	}
	if err := st.ReplaceIDMapStream(ctx, species, "ncbi_gene_info", string(annotation.IDSymbol), string(annotation.IDEntrez), func(emit store.IDMapEmit) error {
		return database.StreamNCBIGeneInfoForSpecies(species, taxID, nil,
			func(entrez, symbol string) error { return nil },
			func(symbol, entrez string) error { return emit(symbol, entrez) },
		)
	}); err != nil {
		return err
	}

	// NCBI gene2ensembl: ENSEMBL <-> ENTREZ
	if err := st.ReplaceIDMapStream(ctx, species, "ncbi_gene2ensembl", string(annotation.IDEnsembl), string(annotation.IDEntrez), func(emit store.IDMapEmit) error {
		return database.StreamNCBIGene2Ensembl(taxID, nil,
			func(ensembl, entrez string) error { return emit(ensembl, entrez) },
			func(entrez, ensembl string) error { return nil },
		)
	}); err != nil {
		return err
	}
	if err := st.ReplaceIDMapStream(ctx, species, "ncbi_gene2ensembl", string(annotation.IDEntrez), string(annotation.IDEnsembl), func(emit store.IDMapEmit) error {
		return database.StreamNCBIGene2Ensembl(taxID, nil,
			func(ensembl, entrez string) error { return nil },
			func(entrez, ensembl string) error { return emit(entrez, ensembl) },
		)
	}); err != nil {
		return err
	}

	// NCBI gene2refseq: REFSEQ <-> ENTREZ
	if err := st.ReplaceIDMapStream(ctx, species, "ncbi_gene2refseq", string(annotation.IDRefSeq), string(annotation.IDEntrez), func(emit store.IDMapEmit) error {
		return database.StreamNCBIGene2RefSeq(taxID, nil,
			func(refseq, entrez string) error { return emit(refseq, entrez) },
			func(entrez, refseq string) error { return nil },
		)
	}); err != nil {
		return err
	}
	if err := st.ReplaceIDMapStream(ctx, species, "ncbi_gene2refseq", string(annotation.IDEntrez), string(annotation.IDRefSeq), func(emit store.IDMapEmit) error {
		return database.StreamNCBIGene2RefSeq(taxID, nil,
			func(refseq, entrez string) error { return nil },
			func(entrez, refseq string) error { return emit(entrez, refseq) },
		)
	}); err != nil {
		return err
	}

	// UniProt idmapping_selected: UNIPROT <-> ENTREZ
	if err := st.ReplaceIDMapStream(ctx, species, "uniprot_idmapping_selected", string(annotation.IDUniprot), string(annotation.IDEntrez), func(emit store.IDMapEmit) error {
		return database.StreamUniProtIDMappingSelected(taxID, nil,
			func(uniprot, entrez string) error { return emit(uniprot, entrez) },
			func(entrez, uniprot string) error { return nil },
		)
	}); err != nil {
		return err
	}
	if err := st.ReplaceIDMapStream(ctx, species, "uniprot_idmapping_selected", string(annotation.IDEntrez), string(annotation.IDUniprot), func(emit store.IDMapEmit) error {
		return database.StreamUniProtIDMappingSelected(taxID, nil,
			func(uniprot, entrez string) error { return nil },
			func(entrez, uniprot string) error { return emit(entrez, uniprot) },
		)
	}); err != nil {
		return err
	}

	// Keep KEGG-derived maps as additional fallback (best-effort).
	if err := writeBasicIDMapsToSQLite(ctx, st, species); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to write KEGG fallback idmaps: %v\n", err)
	}
	return nil
}
