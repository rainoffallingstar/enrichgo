package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"enrichgo/pkg/analysis"
	"enrichgo/pkg/annotation"
	"enrichgo/pkg/io"
)

// gseaCmd GSEA 分析命令
type gseaCmd struct {
	inputFile                 string
	outputFile                string
	dataDir                   string
	dbPath                    string
	database                  string
	species                   string
	ontology                  string
	collection                string
	gmtFile                   string
	format                    string
	idType                    string
	seed                      int64
	minGSSize                 int
	maxGSSize                 int
	permutations              int
	workers                   int
	pvalue                    float64
	padjCutoff                float64
	applyPValueCut            bool
	pvalueMethod              string
	maxPermutations           int
	useSharedNESPool          bool
	ranked                    bool    // 是否已排序
	rankCol                   string  // 排名列名（用于 DEG 表格）
	allowIDFallback           bool    // ID 转换失败时允许回退到原始 ID
	idConversionPolicy        string  // ID 转换策略: strict/threshold/best-effort
	minConversionRate         float64 // threshold 策略下最小转换率
	enableOnlineIDMapFallback bool    // 是否启用在线 ID 映射回退
	idCacheMax                int     // KEGG ID 转换缓存上限（每个 bucket）
	useEmbeddedDB             bool    // --db 未提供时是否使用嵌入默认 SQLite
	autoUpdateDB              bool    // 自动扩容默认 runtime SQLite（可显式关闭）
	strictMode                bool    // 严格模式：关闭自动兜底并启用失败即退出
	updateDB                  bool    // 运行前是否先更新 SQLite 数据
	updateDBIDMaps            bool    // 更新时是否同步刷新 ID 映射
	updateDBIDLevel           string  // 更新时 ID 映射级别（basic/extended）
	debugRankedOut            string  // 输出排序后的 ranked gene 列表
	useR                      bool
	benchmark                 bool
	benchmarkOut              string
}

func runGSEA(cmd *flag.FlagSet) {
	c := &gseaCmd{}
	displayGeneMap := make(map[string]string) // result gene ID -> SYMBOL
	var keggConv *annotation.KEGGIDConverter

	cmd.StringVar(&c.inputFile, "i", "", "Input ranked gene file (required)")
	cmd.StringVar(&c.outputFile, "o", "gsea_result.tsv", "Output file")
	cmd.StringVar(&c.dataDir, "data-dir", "data", "Database cache directory")
	cmd.StringVar(&c.dbPath, "db", "", "Optional SQLite cache DB path (offline bundle). When set, gene sets + ID mappings are loaded from this DB")
	cmd.StringVar(&c.database, "d", "kegg", "Database: kegg, go, reactome, msigdb, custom")
	cmd.StringVar(&c.species, "s", "hsa", "Species code (e.g., hsa, mmu)")
	cmd.StringVar(&c.ontology, "ont", "BP", "GO ontology: BP, MF, CC")
	cmd.StringVar(&c.collection, "c", "c1-c8", "MSigDB collection(s): h, c1-c8, c1,c2,..., all")
	cmd.StringVar(&c.gmtFile, "gmt", "", "Custom GMT file path")
	cmd.StringVar(&c.format, "fmt", "tsv", "Output format: tsv, csv, json")
	cmd.StringVar(&c.idType, "id-type", "auto", "Input ID type: auto, entrez, symbol, uniprot, kegg")
	cmd.Int64Var(&c.seed, "seed", 0, "Random seed for GSEA (0 = use system time)")
	cmd.IntVar(&c.minGSSize, "minGSSize", 10, "Minimum gene set size")
	cmd.IntVar(&c.maxGSSize, "maxGSSize", 500, "Maximum gene set size")
	cmd.IntVar(&c.permutations, "nPerm", 1000, "Number of permutations")
	cmd.IntVar(&c.workers, "workers", 0, "Number of parallel workers for pathway scoring (0 = auto)")
	cmd.Float64Var(&c.padjCutoff, "padj-cutoff", 0.05, "Adjusted p-value cutoff (primary GSEA filter)")
	cmd.Float64Var(&c.pvalue, "pvalue", 0.05, "Raw p-value cutoff (used only when --apply-pvalue-cutoff=true)")
	cmd.BoolVar(&c.applyPValueCut, "apply-pvalue-cutoff", false, "Apply raw p-value cutoff in addition to --padj-cutoff")
	cmd.StringVar(&c.pvalueMethod, "pvalue-method", "simple", "P-value estimation: simple or adaptive")
	cmd.IntVar(&c.maxPermutations, "max-perm", 20000, "Maximum permutations per pathway when --pvalue-method=adaptive")
	cmd.BoolVar(&c.useSharedNESPool, "shared-nes-pool", false, "Use shared permutation pool by gene-set size for NES normalization (experimental)")
	cmd.BoolVar(&c.ranked, "ranked", false, "Input is already ranked (descending)")
	cmd.StringVar(&c.rankCol, "rank-col", "logFC", "Column name to use as ranking metric for GSEA (from DEG table)")
	cmd.BoolVar(&c.allowIDFallback, "allow-id-fallback", true, "Continue with original IDs when ID conversion is incomplete")
	cmd.StringVar(&c.idConversionPolicy, "id-conversion-policy", "best-effort", "ID conversion policy: strict, threshold, best-effort")
	cmd.Float64Var(&c.minConversionRate, "min-conversion-rate", 0.50, "Minimum acceptable conversion rate when --id-conversion-policy=threshold")
	cmd.BoolVar(&c.enableOnlineIDMapFallback, "enable-online-idmap-fallback", true, "Enable online KEGG fallback when offline ID mappings are missing")
	cmd.IntVar(&c.idCacheMax, "kegg-id-cache-max-entries", 0, "Max entries per KEGG ID conversion cache bucket. 0=default; <0=disable eviction. Env: "+envKEGGIDCacheMaxEntries)
	cmd.BoolVar(&c.useEmbeddedDB, "use-embedded-db", true, "When --db is empty, use bundled embedded SQLite DB by default")
	cmd.BoolVar(&c.autoUpdateDB, "auto-update-db", true, "Auto-expand runtime SQLite DB when requested database coverage is missing (set false to disable)")
	cmd.BoolVar(&c.strictMode, "strict-mode", false, "Disable automatic fallback behavior and enforce fail-fast conversion policy")
	cmd.BoolVar(&c.updateDB, "update-db", false, "Before analysis, run download update into target SQLite DB")
	cmd.BoolVar(&c.updateDBIDMaps, "update-db-idmaps", true, "When --update-db, also refresh offline ID mappings")
	cmd.StringVar(&c.updateDBIDLevel, "update-db-idmaps-level", "basic", "When --update-db-idmaps, choose basic or extended")
	cmd.StringVar(&c.debugRankedOut, "debug-ranked-out", "", "Write ranked genes after preprocessing to this TSV path")
	cmd.BoolVar(&c.useR, "use-r", false, "Run analysis via R clusterProfiler baseline instead of Go implementation")
	cmd.BoolVar(&c.benchmark, "benchmark", false, "Run both Go and R implementations and emit benchmark report")
	cmd.StringVar(&c.benchmarkOut, "benchmark-out", "", "Benchmark report path (TSV). Default: derive from -o")

	cmd.Parse(os.Args[2:])

	policy := strictModePolicy{
		AutoUpdateDB:              c.autoUpdateDB,
		EnableOnlineIDMapFallback: c.enableOnlineIDMapFallback,
		AllowIDFallback:           c.allowIDFallback,
		IDConversionPolicy:        c.idConversionPolicy,
		MinConversionRate:         c.minConversionRate,
	}
	applyStrictModeOverrides(c.strictMode, &policy)
	c.autoUpdateDB = policy.AutoUpdateDB
	c.enableOnlineIDMapFallback = policy.EnableOnlineIDMapFallback
	c.allowIDFallback = policy.AllowIDFallback
	c.idConversionPolicy = policy.IDConversionPolicy
	c.minConversionRate = policy.MinConversionRate

	keggCacheMax, applyKEGGCacheMax, resolveErr := resolveKEGGIDCacheMaxEntries(c.idCacheMax)
	if resolveErr != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", resolveErr)
		os.Exit(1)
	}
	convPolicy, policyErr := parseConversionPolicy(c.idConversionPolicy)
	if policyErr != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", policyErr)
		os.Exit(1)
	}
	if c.minConversionRate <= 0 || c.minConversionRate > 1 {
		fmt.Fprintf(os.Stderr, "Error: --min-conversion-rate must be in (0,1] (got %.4f)\n", c.minConversionRate)
		os.Exit(1)
	}

	// 验证参数
	if c.inputFile == "" {
		fmt.Println("Error: -i (input file) is required")
		cmd.Usage()
		os.Exit(1)
	}
	if c.benchmark {
		if err := runBenchmarkMode("gsea", c.database, c.outputFile, c.benchmarkOut); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if c.useR {
		opts := &rRunOptions{
			Command:          "gsea",
			Database:         c.database,
			Species:          c.species,
			Ontology:         c.ontology,
			Collection:       c.collection,
			InputFile:        c.inputFile,
			OutputFile:       c.outputFile,
			DataDir:          c.dataDir,
			NPerm:            c.permutations,
			Format:           c.format,
			RankCol:          c.rankCol,
			SplitByDirection: false,
		}
		if err := runRMode(opts); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Done (R mode)!")
		return
	}

	st, _, err := prepareRuntimeSQLite(runtimeSQLiteOptions{
		Database:       c.database,
		Species:        c.species,
		Ontology:       c.ontology,
		Collection:     c.collection,
		DBPath:         c.dbPath,
		UseEmbeddedDB:  c.useEmbeddedDB,
		AutoUpdateDB:   c.autoUpdateDB,
		UpdateDB:       c.updateDB,
		UpdateDBIDMaps: c.updateDBIDMaps,
		UpdateDBIDMode: c.updateDBIDLevel,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if st != nil {
		defer st.Close()
	}

	// 1. 读取输入
	fmt.Println("Reading input genes...")
	var input *io.GeneInput

	if c.ranked {
		input, err = io.ParseRankedGeneFile(c.inputFile)
	} else {
		// 解析 DEG 表格，使用指定列作为排名
		opts := &io.ParseDiffGeneTableOptions{
			ValueCol: c.rankCol,
			// 不过滤——GSEA 使用全部基因排名
		}
		input, err = io.ParseDiffGeneTableWithOptions(c.inputFile, opts)
		if err != nil {
			// 回退到基因列表
			input, err = io.ParseGeneListFile(c.inputFile)
		}
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input file: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d genes for GSEA (rank column: %s)\n", len(input.Genes), c.rankCol)

	// 1.5 自动 ID 类型检测和转换
	keggConv, err = applyIDConversionToInput(
		input,
		st,
		idConversionOptions{
			Database:                  c.database,
			Species:                   c.species,
			DataDir:                   c.dataDir,
			IDType:                    c.idType,
			AllowIDFallback:           c.allowIDFallback,
			ConversionPolicy:          convPolicy,
			MinConversionRate:         c.minConversionRate,
			EnableOnlineIDMapFallback: c.enableOnlineIDMapFallback,
			ApplyKEGGCacheMax:         applyKEGGCacheMax,
			KEGGCacheMax:              keggCacheMax,
		},
		false,
		displayGeneMap,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		if !c.strictMode {
			fmt.Fprintln(os.Stderr, "Hint: use --allow-id-fallback to continue with original IDs")
		}
		os.Exit(1)
	}
	hydrateDisplayMapForKEGG(displayGeneMap, c.database, c.species, c.dataDir, st)

	// 对基因按值排序
	sortedGenes := make([]string, len(input.Genes))
	copy(sortedGenes, input.Genes)
	sort.Slice(sortedGenes, func(i, j int) bool {
		left := input.GeneValues[sortedGenes[i]]
		right := input.GeneValues[sortedGenes[j]]
		if left != right {
			return left > right
		}
		return sortedGenes[i] < sortedGenes[j]
	})

	if c.debugRankedOut != "" {
		lines := []string{"gene\trank"}
		for _, g := range sortedGenes {
			lines = append(lines, fmt.Sprintf("%s\t%.15g", g, input.GeneValues[g]))
		}
		if err := os.WriteFile(c.debugRankedOut, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing debug ranked output: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Wrote debug ranked genes: %s\n", c.debugRankedOut)
	}

	// 2. 加载基因集数据库
	fmt.Printf("Loading database: %s...\n", c.database)
	loaded, err := loadGeneSetsWithAnnotations(geneSetLoadOptions{
		Database:   c.database,
		Species:    c.species,
		Ontology:   c.ontology,
		Collection: c.collection,
		GMTFile:    c.gmtFile,
		DataDir:    c.dataDir,
		Store:      st,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading database: %v\n", err)
		os.Exit(1)
	}
	geneSets := loaded.GeneSets

	if len(geneSets) == 0 {
		fmt.Fprintf(os.Stderr, "Error: no gene sets loaded for database=%s species=%s\n", c.database, c.species)
		if !c.strictMode {
			fmt.Fprintln(os.Stderr, "Hint: try --update-db or verify requested coverage in --db")
		}
		os.Exit(1)
	}

	fmt.Printf("Loaded %d gene sets\n", len(geneSets))

	// 3. 执行 GSEA
	fmt.Printf("Running GSEA (permutations: %d, workers: %d)...\n", c.permutations, c.workers)
	gseaInput := &analysis.GSEAInput{
		GeneList:          sortedGenes,
		GeneValues:        input.GeneValues,
		GeneSets:          geneSets,
		Workers:           c.workers,
		Permutations:      c.permutations,
		MinGSSize:         c.minGSSize,
		MaxGSSize:         c.maxGSSize,
		PAdjustCutoff:     c.padjCutoff,
		PValueCutoff:      c.pvalue,
		ApplyPValueCutoff: c.applyPValueCut,
		PValueMethod:      c.pvalueMethod,
		MaxPermutations:   c.maxPermutations,
		UseSharedNESPool:  c.useSharedNESPool,
		Seed:              c.seed,
	}

	results := analysis.RunGSEA(gseaInput)
	fmt.Printf("Found %d enriched gene sets\n", len(results))

	// 4. 写入结果
	fmt.Printf("Writing results to %s...\n", c.outputFile)
	if err := io.WriteGSEAResults(convertGSEAResults(results, displayGeneMap), c.outputFile, io.OutputFormat(c.format)); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing output: %v\n", err)
		os.Exit(1)
	}

	writeKEGGIDCacheMetricsIfRequested(keggConv)
	fmt.Println("Done!")
}

func convertGSEAResults(results []*analysis.GSEAResult, displayGeneMap map[string]string) []*io.GSEAResult {
	var converted []*io.GSEAResult
	for _, r := range results {
		converted = append(converted, &io.GSEAResult{
			ID:              r.ID,
			Name:            r.Name,
			NES:             r.NES,
			PValue:          r.PValue,
			PAdjust:         r.PAdjust,
			QValue:          r.QValue,
			EnrichmentScore: r.EnrichmentScore,
			LeadGenes:       mapIDsForDisplay(r.LeadGenes, displayGeneMap),
			Description:     r.Description,
		})
	}
	return converted
}
