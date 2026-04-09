package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"enrichgo/pkg/analysis"
	"enrichgo/pkg/annotation"
	"enrichgo/pkg/database"
	"enrichgo/pkg/io"
	"enrichgo/pkg/store"
)

type runtimeSQLiteOptions struct {
	Database       string
	Species        string
	Ontology       string
	Collection     string
	DBPath         string
	UseEmbeddedDB  bool
	UpdateDB       bool
	AutoUpdateDB   bool
	UpdateDBIDMaps bool
	UpdateDBIDMode string
}

func prepareRuntimeSQLite(opts runtimeSQLiteOptions) (*store.SQLiteStore, string, error) {
	effectiveDBPath := strings.TrimSpace(opts.DBPath)
	if effectiveDBPath == "" && opts.UseEmbeddedDB {
		path, embedErr := ensureEmbeddedDefaultSQLiteDBFile()
		if embedErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to prepare embedded SQLite DB (%s): %v\n", embeddedDefaultSQLiteSHA256(), embedErr)
		} else {
			effectiveDBPath = path
			fmt.Printf("Using embedded SQLite DB: %s\n", effectiveDBPath)
		}
	}

	if opts.UpdateDB {
		if effectiveDBPath == "" {
			return nil, "", fmt.Errorf("--update-db requires --db or --use-embedded-db")
		}
		fmt.Printf("Updating SQLite DB before analysis (db=%s, species=%s)...\n", opts.Database, opts.Species)
		if err := runDownloadUpdateForDB(dbUpdateOptions{
			Database:    opts.Database,
			Species:     opts.Species,
			Ontology:    opts.Ontology,
			Collection:  opts.Collection,
			DBPath:      effectiveDBPath,
			WithIDMaps:  opts.UpdateDBIDMaps,
			IDMapsLevel: opts.UpdateDBIDMode,
		}); err != nil {
			return nil, "", fmt.Errorf("updating sqlite db: %w", err)
		}
	}

	if effectiveDBPath == "" {
		return nil, "", nil
	}
	st, err := store.OpenSQLite(effectiveDBPath)
	if err != nil {
		return nil, "", fmt.Errorf("opening sqlite db: %w", err)
	}

	if opts.AutoUpdateDB && !opts.UpdateDB && strings.TrimSpace(opts.DBPath) == "" {
		needUpdate, reason, err := shouldAutoExpandRuntimeSQLite(st, effectiveDBPath, opts)
		if err != nil {
			_ = st.Close()
			return nil, "", err
		}
		if needUpdate {
			fmt.Printf("Auto-updating runtime SQLite DB (%s)...\n", reason)
			_ = st.Close()
			if err := runDownloadUpdateForDB(dbUpdateOptions{
				Database:    opts.Database,
				Species:     opts.Species,
				Ontology:    opts.Ontology,
				Collection:  opts.Collection,
				DBPath:      effectiveDBPath,
				WithIDMaps:  opts.UpdateDBIDMaps,
				IDMapsLevel: opts.UpdateDBIDMode,
			}); err != nil {
				return nil, "", fmt.Errorf("auto-updating sqlite db: %w", err)
			}
			st, err = store.OpenSQLite(effectiveDBPath)
			if err != nil {
				return nil, "", fmt.Errorf("reopening sqlite db after auto-update: %w", err)
			}
		}
	}

	return st, effectiveDBPath, nil
}

func shouldAutoExpandRuntimeSQLite(st *store.SQLiteStore, dbPath string, opts runtimeSQLiteOptions) (bool, string, error) {
	if st == nil {
		return false, "", nil
	}
	database := strings.ToLower(strings.TrimSpace(opts.Database))
	if database == "" || database == "custom" {
		return false, "", nil
	}

	manifest, manifestErr := embeddedDefaultSQLiteManifest()
	if manifestErr == nil {
		profile := strings.ToLower(strings.TrimSpace(manifest.ContractProfile))
		if strings.Contains(profile, "seed") && readEmbeddedSQLiteState(dbPath) != "" {
			return true, "embedded seed profile", nil
		}
	}

	covered, err := sqliteCoverageAvailable(st, database, opts.Species, opts.Ontology, opts.Collection)
	if err != nil {
		return false, "", fmt.Errorf("checking sqlite coverage: %w", err)
	}
	if !covered {
		return true, fmt.Sprintf("missing %s coverage", database), nil
	}
	return false, "", nil
}

func sqliteCoverageAvailable(st *store.SQLiteStore, database, species, ontology, collection string) (bool, error) {
	switch database {
	case "kegg":
		return sqliteHasAnyGeneSet(st, store.GeneSetFilter{DB: "kegg", Species: species})
	case "go":
		return sqliteHasAnyGeneSet(st, store.GeneSetFilter{DB: "go", Species: species, Ontology: ontology})
	case "reactome":
		return sqliteHasAnyGeneSet(st, store.GeneSetFilter{DB: "reactome", Species: species})
	case "msigdb":
		cols, err := parseMSigDBCollections(collection)
		if err != nil {
			return false, err
		}
		for _, col := range cols {
			ok, err := sqliteHasAnyGeneSet(st, store.GeneSetFilter{DB: "msigdb", Species: species, Collection: string(col)})
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
		}
		return true, nil
	default:
		return true, nil
	}
}

func sqliteHasAnyGeneSet(st *store.SQLiteStore, filter store.GeneSetFilter) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sets, _, err := st.LoadGeneSets(ctx, filter)
	if err != nil {
		return false, err
	}
	return len(sets) > 0, nil
}

type idConversionOptions struct {
	Database                  string
	Species                   string
	DataDir                   string
	IDType                    string
	AllowIDFallback           bool
	ConversionPolicy          annotation.ConversionPolicy
	MinConversionRate         float64
	EnableOnlineIDMapFallback bool
	ApplyKEGGCacheMax         bool
	KEGGCacheMax              int
}

func applyIDConversionToInput(
	input *io.GeneInput,
	st *store.SQLiteStore,
	opts idConversionOptions,
	convertAllGenes bool,
	displayGeneMap map[string]string,
) (*annotation.KEGGIDConverter, error) {
	if input == nil {
		return nil, fmt.Errorf("nil gene input")
	}

	targetIDType := targetIDTypeForDatabase(opts.Database)
	shouldConvert := targetIDType != annotation.IDUnknown
	switch strings.ToLower(strings.TrimSpace(opts.IDType)) {
	case "", "auto":
	case "entrez":
		targetIDType = annotation.IDEntrez
		shouldConvert = true
	case "symbol":
		targetIDType = annotation.IDSymbol
		shouldConvert = true
	case "uniprot":
		targetIDType = annotation.IDUniprot
		shouldConvert = true
	case "kegg":
		targetIDType = annotation.IDKEGG
		shouldConvert = true
	default:
		fmt.Fprintf(os.Stderr, "Warning: unknown id-type '%s', using auto-detection\n", opts.IDType)
	}

	detectedType := annotation.BatchDetectIDType(input.Genes)
	if !shouldConvert || detectedType == annotation.IDUnknown || detectedType == targetIDType {
		return nil, nil
	}
	fmt.Printf("Detected ID type: %s, converting to %s...\n", detectedType, targetIDType)

	chain := annotation.NewChainIDConverter()
	if st != nil {
		chain.AddLayer("sqlite", annotation.NewSQLiteIDConverter(st))
	}
	localConv := annotation.NewKEGGIDConverter(opts.DataDir)
	localConv.SetAllowOnlineFetch(false)
	if opts.ApplyKEGGCacheMax {
		localConv.SetMaxCacheEntries(opts.KEGGCacheMax)
	}
	chain.AddLayer("local_tsv", localConv)

	var keggConv *annotation.KEGGIDConverter
	if opts.EnableOnlineIDMapFallback {
		onlineConv := annotation.NewKEGGIDConverter(opts.DataDir)
		onlineConv.SetAllowOnlineFetch(true)
		if opts.ApplyKEGGCacheMax {
			onlineConv.SetMaxCacheEntries(opts.KEGGCacheMax)
		}
		chain.AddLayer("kegg_online", onlineConv)
		keggConv = onlineConv
	} else if opts.ApplyKEGGCacheMax {
		keggConv = localConv
	}

	sourceIDs := input.Genes
	if convertAllGenes {
		if len(input.AllGenes) > 0 {
			sourceIDs = input.AllGenes
		}
	}
	converted, mapping, report, err := annotation.ConvertGeneIDWithPolicy(
		sourceIDs,
		targetIDType,
		opts.Species,
		chain,
		opts.ConversionPolicy,
		opts.MinConversionRate,
	)
	printConversionReport(report, opts.MinConversionRate)
	if err != nil {
		if !opts.AllowIDFallback {
			return keggConv, fmt.Errorf("ID conversion failed: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Warning: ID conversion failed: %v, using original IDs\n", err)
		return keggConv, nil
	}

	mergeDisplayMapFromConversion(displayGeneMap, mapping)
	if convertAllGenes {
		convertedSig := make([]string, 0, len(input.Genes))
		for _, orig := range input.Genes {
			convertedSig = append(convertedSig, mapping[orig]...)
		}
		input.Genes = uniqueStringsPreserveOrder(convertedSig)
		input.AllGenes = converted
		fmt.Printf("Converted %d significant genes (universe: %d)\n", len(input.Genes), len(input.AllGenes))
	} else {
		input.Genes = converted
		fmt.Printf("Converted %d genes\n", len(converted))
	}

	input.GeneValues = remapFloatValues(input.GeneValues, mapping, sourceIDs)
	if len(input.GeneDirections) > 0 {
		input.GeneDirections = remapStringValues(input.GeneDirections, mapping, sourceIDs)
	}
	return keggConv, nil
}

func remapFloatValues(old map[string]float64, mapping map[string][]string, order []string) map[string]float64 {
	if len(old) == 0 {
		return old
	}
	out := make(map[string]float64)
	for _, orig := range order {
		v, ok := old[orig]
		if !ok {
			continue
		}
		for _, newID := range mapping[orig] {
			newID = strings.TrimSpace(newID)
			if newID == "" {
				continue
			}
			if _, exists := out[newID]; !exists {
				out[newID] = v
			}
		}
	}
	return out
}

func remapStringValues(old map[string]string, mapping map[string][]string, order []string) map[string]string {
	if len(old) == 0 {
		return old
	}
	out := make(map[string]string)
	for _, orig := range order {
		v, ok := old[orig]
		if !ok {
			continue
		}
		for _, newID := range mapping[orig] {
			newID = strings.TrimSpace(newID)
			if newID == "" {
				continue
			}
			if _, exists := out[newID]; !exists {
				out[newID] = v
			}
		}
	}
	return out
}

func uniqueStringsPreserveOrder(ids []string) []string {
	seen := make(map[string]bool, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out
}

func hydrateDisplayMapForKEGG(display map[string]string, database, species, dataDir string, st *store.SQLiteStore) {
	if !strings.EqualFold(database, "kegg") {
		return
	}
	if st != nil {
		if m, err := loadEntrezSymbolMapFromSQLite(st, species); err == nil {
			mergeDisplayMapMissing(display, m)
		}
	}
	idmapPath := filepath.Join(dataDir, fmt.Sprintf("kegg_%s_idmap.tsv", species))
	if m, err := loadEntrezSymbolMapFromIDMap(idmapPath); err == nil {
		mergeDisplayMapMissing(display, m)
	}
}

func mergeDisplayMapMissing(display, extra map[string]string) {
	if len(extra) == 0 {
		return
	}
	for k, v := range extra {
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k == "" || v == "" {
			continue
		}
		if _, exists := display[k]; !exists {
			display[k] = v
		}
	}
}

func hydrateKEGGGeneSetMetadata(geneSets analysis.GeneSets, species, dataDir string) {
	if !keggGeneSetsNeedMetadataHydration(geneSets) {
		return
	}
	mergeKEGGGeneSetMetadata(geneSets, loadKEGGPathwayNamesFromLocal(species, dataDir))
	if !keggGeneSetsNeedMetadataHydration(geneSets) {
		return
	}
	names, err := database.FetchKEGGPathwayNames(species)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to hydrate KEGG pathway metadata: %v\n", err)
		return
	}
	mergeKEGGGeneSetMetadata(geneSets, names)
}

func loadKEGGPathwayNamesFromLocal(species, dataDir string) map[string]string {
	if strings.TrimSpace(dataDir) == "" {
		return nil
	}
	data, err := database.LoadKEGG(species, dataDir)
	if err != nil || data == nil {
		return nil
	}
	names := make(map[string]string, len(data.Pathways))
	for _, pw := range data.Pathways {
		if pw == nil {
			continue
		}
		key := database.NormalizeKEGGPathwayID(pw.ID)
		name := strings.TrimSpace(pw.Name)
		if key == "" || keggPathwayNamePlaceholder(pw.ID, name) {
			continue
		}
		if _, exists := names[key]; !exists {
			names[key] = name
		}
	}
	return names
}

func mergeKEGGGeneSetMetadata(geneSets analysis.GeneSets, names map[string]string) {
	if len(names) == 0 {
		return
	}
	for _, gs := range geneSets {
		if gs == nil {
			continue
		}
		name := strings.TrimSpace(names[database.NormalizeKEGGPathwayID(gs.ID)])
		if name == "" {
			continue
		}
		if keggPathwayNamePlaceholder(gs.ID, gs.Name) {
			gs.Name = name
		}
		if keggPathwayDescriptionPlaceholder(gs.Description) {
			gs.Description = name
		}
	}
}

func keggGeneSetsNeedMetadataHydration(geneSets analysis.GeneSets) bool {
	for _, gs := range geneSets {
		if gs == nil {
			continue
		}
		if keggPathwayNamePlaceholder(gs.ID, gs.Name) || keggPathwayDescriptionPlaceholder(gs.Description) {
			return true
		}
	}
	return false
}

func keggPathwayNamePlaceholder(id, name string) bool {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" || trimmed == "-" {
		return true
	}
	if strings.EqualFold(trimmed, strings.TrimSpace(id)) {
		return true
	}
	normalizedID := database.NormalizeKEGGPathwayID(id)
	if normalizedID == "" {
		return false
	}
	return strings.EqualFold(database.NormalizeKEGGPathwayID(trimmed), normalizedID)
}

func keggPathwayDescriptionPlaceholder(desc string) bool {
	trimmed := strings.TrimSpace(desc)
	return trimmed == "" || trimmed == "-"
}

type geneSetLoadOptions struct {
	Database   string
	Species    string
	Ontology   string
	Collection string
	GMTFile    string
	DataDir    string
	Store      *store.SQLiteStore
}

type geneSetLoadResult struct {
	GeneSets       analysis.GeneSets
	AnnotatedGenes map[string]bool
}

func loadGeneSetsWithAnnotations(opts geneSetLoadOptions) (*geneSetLoadResult, error) {
	result := &geneSetLoadResult{}

	switch strings.ToLower(strings.TrimSpace(opts.Database)) {
	case "kegg":
		if opts.Store != nil {
			sets, err := loadGeneSetsFromSQLite(opts.Store, store.GeneSetFilter{DB: "kegg", Species: opts.Species})
			if err != nil {
				return nil, err
			}
			result.GeneSets = sets
			hydrateKEGGGeneSetMetadata(result.GeneSets, opts.Species, opts.DataDir)
			return result, nil
		}
		data, err := database.LoadOrDownloadKEGG(opts.Species, opts.DataDir)
		if err != nil {
			return nil, fmt.Errorf("loading KEGG data: %w", err)
		}
		if data == nil {
			return nil, fmt.Errorf("no KEGG data available")
		}
		for _, pw := range data.Pathways {
			result.GeneSets = append(result.GeneSets, &analysis.GeneSet{
				ID:          pw.ID,
				Name:        pw.Name,
				Genes:       pw.Genes,
				Description: pw.Description,
			})
		}
		hydrateKEGGGeneSetMetadata(result.GeneSets, opts.Species, opts.DataDir)
		return result, nil

	case "go":
		if opts.Store != nil {
			sets, err := loadGeneSetsFromSQLite(opts.Store, store.GeneSetFilter{DB: "go", Species: opts.Species, Ontology: opts.Ontology})
			if err != nil {
				return nil, err
			}
			result.GeneSets = sets
			result.AnnotatedGenes = make(map[string]bool)
			for _, gs := range sets {
				for gene := range gs.Genes {
					result.AnnotatedGenes[gene] = true
				}
			}
			return result, nil
		}
		data, err := database.LoadOrDownloadGO(opts.Species, opts.Ontology, opts.DataDir)
		if err != nil {
			return nil, fmt.Errorf("loading GO data: %w", err)
		}
		result.AnnotatedGenes = make(map[string]bool, len(data.Gene2Terms))
		for gene, terms := range data.Gene2Terms {
			if len(terms) > 0 {
				result.AnnotatedGenes[gene] = true
			}
		}
		term2genes := make(map[string]map[string]bool)
		for gene, terms := range data.Gene2Terms {
			for _, termID := range terms {
				if term2genes[termID] == nil {
					term2genes[termID] = make(map[string]bool)
				}
				term2genes[termID][gene] = true
			}
		}
		for termID, term := range data.Terms {
			result.GeneSets = append(result.GeneSets, &analysis.GeneSet{
				ID:          termID,
				Name:        term.Name,
				Genes:       term2genes[termID],
				Description: term.Definition,
			})
		}
		return result, nil

	case "reactome":
		if opts.Store != nil {
			sets, err := loadGeneSetsFromSQLite(opts.Store, store.GeneSetFilter{DB: "reactome", Species: opts.Species})
			if err != nil {
				return nil, err
			}
			result.GeneSets = sets
			return result, nil
		}
		data, err := database.LoadOrDownloadReactome(opts.Species, opts.DataDir)
		if err != nil {
			return nil, fmt.Errorf("loading Reactome data: %w", err)
		}
		if data == nil || len(data.Pathways) == 0 {
			return nil, fmt.Errorf("no Reactome data available")
		}
		for _, pw := range data.Pathways {
			result.GeneSets = append(result.GeneSets, &analysis.GeneSet{
				ID:          pw.ID,
				Name:        pw.Name,
				Genes:       pw.Genes,
				Description: pw.Description,
			})
		}
		return result, nil

	case "msigdb":
		collections, err := parseMSigDBCollections(opts.Collection)
		if err != nil {
			return nil, err
		}
		if opts.Store != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			seen := make(map[string]bool)
			for _, col := range collections {
				sets, _, err := opts.Store.LoadGeneSets(ctx, store.GeneSetFilter{
					DB:         "msigdb",
					Species:    opts.Species,
					Collection: string(col),
				})
				if err != nil {
					return nil, fmt.Errorf("loading MSigDB from sqlite: %w", err)
				}
				for _, gs := range sets {
					if gs == nil || seen[gs.ID] {
						continue
					}
					seen[gs.ID] = true
					result.GeneSets = append(result.GeneSets, gs)
				}
			}
			return result, nil
		}
		sets, err := database.LoadOrDownloadMSigDBCollections(collections, opts.DataDir)
		if err != nil {
			return nil, fmt.Errorf("loading MSigDB: %w", err)
		}
		result.GeneSets = sets
		return result, nil

	case "custom":
		if strings.TrimSpace(opts.GMTFile) == "" {
			return nil, fmt.Errorf("-gmt is required for custom database")
		}
		sets, err := database.LoadGMTFile(opts.GMTFile)
		if err != nil {
			return nil, fmt.Errorf("loading GMT file: %w", err)
		}
		result.GeneSets = sets
		return result, nil

	default:
		return nil, fmt.Errorf("unknown database: %s", opts.Database)
	}
}

func loadGeneSetsFromSQLite(st *store.SQLiteStore, filter store.GeneSetFilter) (analysis.GeneSets, error) {
	if st == nil {
		return nil, fmt.Errorf("nil sqlite store")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sets, _, err := st.LoadGeneSets(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("loading %s from sqlite: %w", filter.DB, err)
	}
	return analysis.GeneSets(sets), nil
}
