package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"enrichgo/pkg/annotation"
	"enrichgo/pkg/database"
	"enrichgo/pkg/netutil"
	"enrichgo/pkg/store"
	"enrichgo/pkg/types"
)

// downloadCmd 数据库下载命令
type downloadCmd struct {
	database             string
	species              string
	ontology             string
	collection           string
	outputDir            string
	all                  bool
	dbPath               string
	dbOnly               bool
	withIDMaps           bool
	idMapsLevel          string
	idMapsRetries        int
	idMapsRetryBackoff   time.Duration
	idMapsTimeout        time.Duration
	idMapsHTTPTimeout    time.Duration
	idMapsResume         bool
	idMapsForceRefresh   bool
	idMapsLocalDir       string
	reactomeAutoRetry    bool
	reactomeRetries      int
	reactomeRetryBackoff time.Duration
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
	cmd.BoolVar(&c.withIDMaps, "idmaps", true, "When used with --db, also fetch and store offline ID mappings (SYMBOL/ENTREZ)")
	cmd.StringVar(&c.idMapsLevel, "idmaps-level", "basic", "ID mapping level when used with --db --idmaps: basic (KEGG symbol list) or extended (NCBI gene_info symbol map; larger but more complete)")
	cmd.IntVar(&c.idMapsRetries, "idmaps-retries", 2, "Retry count for --idmaps sync on timeout/transient network errors")
	cmd.DurationVar(&c.idMapsRetryBackoff, "idmaps-retry-backoff", 20*time.Second, "Backoff between --idmaps retries")
	cmd.DurationVar(&c.idMapsTimeout, "idmaps-timeout", 240*time.Minute, "Per-attempt timeout for writing --idmaps into SQLite")
	cmd.DurationVar(&c.idMapsHTTPTimeout, "idmaps-http-timeout", 45*time.Minute, "HTTP timeout for a single --idmaps download request (extended mode)")
	cmd.BoolVar(&c.idMapsResume, "idmaps-resume", true, "Resume extended idmaps by skipping already populated source scopes in SQLite")
	cmd.BoolVar(&c.idMapsForceRefresh, "idmaps-force-refresh", false, "Force refresh idmaps by disabling resume-skip and rewriting all idmap scopes")
	cmd.StringVar(&c.idMapsLocalDir, "idmaps-local-dir", "data", "Local directory for offline KEGG idmap TSV fallback (kegg_<species>_idmap.tsv)")
	cmd.BoolVar(&c.reactomeAutoRetry, "reactome-auto-retry", true, "Automatically retry Reactome download on transient network/server failures")
	cmd.IntVar(&c.reactomeRetries, "reactome-retries", 2, "Retry count for Reactome download when --reactome-auto-retry=true")
	cmd.DurationVar(&c.reactomeRetryBackoff, "reactome-retry-backoff", 15*time.Second, "Backoff between Reactome download retries")

	cmd.Parse(os.Args[2:])

	if c.idMapsRetries < 0 {
		fmt.Fprintf(os.Stderr, "Error: --idmaps-retries must be >= 0 (got %d)\n", c.idMapsRetries)
		os.Exit(1)
	}
	if c.idMapsRetryBackoff < 0 {
		fmt.Fprintf(os.Stderr, "Error: --idmaps-retry-backoff must be >= 0 (got %s)\n", c.idMapsRetryBackoff)
		os.Exit(1)
	}
	if c.idMapsTimeout <= 0 {
		fmt.Fprintf(os.Stderr, "Error: --idmaps-timeout must be > 0 (got %s)\n", c.idMapsTimeout)
		os.Exit(1)
	}
	if c.idMapsHTTPTimeout <= 0 {
		fmt.Fprintf(os.Stderr, "Error: --idmaps-http-timeout must be > 0 (got %s)\n", c.idMapsHTTPTimeout)
		os.Exit(1)
	}
	if c.reactomeRetries < 0 {
		fmt.Fprintf(os.Stderr, "Error: --reactome-retries must be >= 0 (got %d)\n", c.reactomeRetries)
		os.Exit(1)
	}
	if c.reactomeRetryBackoff < 0 {
		fmt.Fprintf(os.Stderr, "Error: --reactome-retry-backoff must be >= 0 (got %s)\n", c.reactomeRetryBackoff)
		os.Exit(1)
	}

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
		opts := &database.ReactomeDownloadOptions{
			AutoRetry:    c.reactomeAutoRetry,
			MaxRetries:   c.reactomeRetries,
			RetryBackoff: c.reactomeRetryBackoff,
		}
		data, err := database.LoadOrDownloadReactomeWithOptions(c.species, outputDir, opts)
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
		if level == "" {
			level = "basic"
		}
		var idMapClient database.HTTPClient
		if level == "extended" {
			idMapClient = netutil.NewClient(netutil.Options{Timeout: c.idMapsHTTPTimeout})
		}
		if err := writeIDMapsToSQLiteWithRetryConfig(
			st,
			c.species,
			level,
			c.idMapsTimeout,
			c.idMapsRetries,
			c.idMapsRetryBackoff,
			idMapClient,
			effectiveIDMapsResume(c.idMapsResume, c.idMapsForceRefresh),
			c.idMapsLocalDir,
		); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing idmaps to sqlite: %v\n", err)
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

	return nil
}

func writeExtendedIDMapsToSQLite(ctx context.Context, st *store.SQLiteStore, species string, client database.HTTPClient) error {
	taxID, err := database.TaxIDForSpecies(species)
	if err != nil {
		return err
	}

	for _, step := range buildExtendedIDMapSteps(species, taxID, client) {
		if err := st.ReplaceIDMapStream(ctx, species, step.source, step.fromType, step.toType, step.produce); err != nil {
			return err
		}
		logExtendedIDMapStepStats(step)
	}

	// Keep KEGG-derived maps as additional fallback (best-effort).
	if err := writeKEGGFallbackIDMapsBestEffort(ctx, species, 2, 5*time.Second, func(innerCtx context.Context, sp string) error {
		return writeBasicIDMapsToSQLite(innerCtx, st, sp)
	}); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to write KEGG fallback idmaps: %v\n", err)
	}
	return nil
}
func writeKEGGFallbackIDMapsBestEffort(
	ctx context.Context,
	species string,
	retries int,
	backoff time.Duration,
	write func(context.Context, string) error,
) error {
	if write == nil {
		return fmt.Errorf("kegg fallback writer is nil")
	}
	if retries < 0 {
		return fmt.Errorf("invalid kegg fallback retries %d", retries)
	}
	if backoff < 0 {
		return fmt.Errorf("invalid kegg fallback backoff %s", backoff)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Info: skipping KEGG fallback idmaps because context is done: %v\n", err)
		return nil
	}

	totalAttempts := retries + 1
	var lastErr error
	for attempt := 1; attempt <= totalAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Info: skipping KEGG fallback idmaps because context is done: %v\n", err)
			return nil
		}
		err := write(ctx, species)
		if err == nil {
			if attempt > 1 {
				fmt.Fprintf(os.Stderr, "Info: KEGG fallback idmaps succeeded on retry %d/%d\n", attempt, totalAttempts)
			}
			return nil
		}
		lastErr = err
		if attempt == totalAttempts || !isRetryableIDMapError(err) {
			break
		}
		fmt.Fprintf(os.Stderr, "Info: KEGG fallback idmaps attempt %d/%d failed: %v\n", attempt, totalAttempts, err)
		if backoff > 0 {
			fmt.Fprintf(os.Stderr, "Info: retrying KEGG fallback idmaps in %s...\n", backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				fmt.Fprintf(os.Stderr, "Info: skipping KEGG fallback idmaps because context is done: %v\n", ctx.Err())
				return nil
			}
		}
	}
	if lastErr == nil {
		return nil
	}
	if errors.Is(lastErr, context.Canceled) || errors.Is(lastErr, context.DeadlineExceeded) || isRetryableIDMapError(lastErr) {
		fmt.Fprintf(os.Stderr, "Info: skipping KEGG fallback idmaps after retry budget: %v\n", lastErr)
		return nil
	}
	return lastErr
}

func writeIDMapsToSQLiteWithRetry(
	st *store.SQLiteStore,
	species, level string,
	attemptTimeout time.Duration,
	retries int,
	backoff time.Duration,
	client database.HTTPClient,
) error {
	if st == nil {
		return fmt.Errorf("sqlite store is nil")
	}
	if attemptTimeout <= 0 {
		return fmt.Errorf("invalid idmaps timeout %s", attemptTimeout)
	}
	if retries < 0 {
		return fmt.Errorf("invalid idmaps retries %d", retries)
	}
	if backoff < 0 {
		return fmt.Errorf("invalid idmaps retry backoff %s", backoff)
	}

	level = strings.ToLower(strings.TrimSpace(level))
	if level == "" {
		level = "basic"
	}
	if level != "basic" && level != "extended" {
		return fmt.Errorf("unknown --idmaps-level %q (use basic or extended)", level)
	}

	totalAttempts := retries + 1
	var lastErr error
	for attempt := 1; attempt <= totalAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
		var err error
		switch level {
		case "basic":
			err = writeBasicIDMapsToSQLite(attemptCtx, st, species)
		case "extended":
			err = writeExtendedIDMapsToSQLite(attemptCtx, st, species, client)
		}
		cancel()
		if err == nil {
			if attempt > 1 {
				fmt.Fprintf(os.Stderr, "Info: idmaps sync succeeded on retry %d/%d\n", attempt, totalAttempts)
			}
			return nil
		}

		lastErr = err
		if attempt == totalAttempts || !isRetryableIDMapError(err) {
			break
		}
		fmt.Fprintf(os.Stderr, "Warning: idmaps sync attempt %d/%d failed: %v\n", attempt, totalAttempts, err)
		if backoff > 0 {
			fmt.Fprintf(os.Stderr, "Warning: retrying idmaps sync in %s...\n", backoff)
			time.Sleep(backoff)
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("idmaps sync failed without explicit error")
	}
	return lastErr
}

func isRetryableIDMapError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"context deadline exceeded",
		"client.timeout",
		"timeout",
		"i/o timeout",
		"tls handshake timeout",
		"connection reset",
		"connection refused",
		"temporary failure",
		"temporarily unavailable",
		"unexpected eof",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}
