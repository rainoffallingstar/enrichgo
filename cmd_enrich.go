package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"enrichgo/pkg/analysis"
	"enrichgo/pkg/annotation"
	"enrichgo/pkg/io"
	"enrichgo/pkg/store"
)

// enrichCmd ORA 富集分析命令
type enrichCmd struct {
	inputFile    string
	outputFile   string
	dataDir      string
	dbPath       string
	universeFile string
	database     string
	species      string
	ontology     string
	collection   string
	gmtFile      string
	format       string
	idType       string
	minGSSize    int
	maxGSSize    int
	pvalue       float64
	qvalue       float64
	noFilter     bool
	// 显著基因过滤参数
	sigCol                    string  // 字符串过滤列名（如 "significant"）
	sigVal                    string  // 字符串过滤值（如 "TRUE"）
	fdrCol                    string  // 数值过滤列名（如 "FDR"）
	fdrThreshold              float64 // 数值过滤阈值（如 0.05）
	useAllBackground          bool    // 使用表格全部基因作为 ORA 背景
	allowIDFallback           bool    // ID 转换失败时允许回退到原始 ID
	idConversionPolicy        string  // ID 转换策略: strict/threshold/best-effort
	minConversionRate         float64 // threshold 策略下最小转换率
	enableOnlineIDMapFallback bool    // 是否启用在线 ID 映射回退
	idCacheMax                int     // KEGG ID 转换缓存上限（每个 bucket）
	useEmbeddedDB             bool    // --db 未提供时是否使用嵌入的默认 SQLite
	autoUpdateDB              bool    // 自动扩容默认 runtime SQLite（可显式关闭）
	strictMode                bool    // 严格模式：关闭自动兜底并启用失败即退出
	updateDB                  bool    // 运行前是否先更新 SQLite 数据
	updateDBIDMaps            bool    // 更新时是否同步刷新 ID 映射
	updateDBIDLevel           string  // 更新时 ID 映射级别（basic/extended）
	// 方向性分析参数
	splitByDirection bool    // 是否按方向分别做 ORA
	dirCol           string  // 方向列名
	upVal            string  // 上调标记值
	downVal          string  // 下调标记值
	logFCCol         string  // logFC 列名（用于自动推断方向，空=第二列）
	logFCThresh      float64 // logFC 绝对值阈值，|logFC| > thresh 才赋予方向（默认 0）
	useR             bool
	benchmark        bool
	benchmarkOut     string
}

func runEnrich(cmd *flag.FlagSet) {
	c := &enrichCmd{}
	displayGeneMap := make(map[string]string) // result gene ID -> SYMBOL
	var keggConv *annotation.KEGGIDConverter

	cmd.StringVar(&c.inputFile, "i", "", "Input gene list file (required)")
	cmd.StringVar(&c.outputFile, "o", "enrichment_result.tsv", "Output file")
	cmd.StringVar(&c.dataDir, "data-dir", "data", "Database cache directory")
	cmd.StringVar(&c.dbPath, "db", "", "Optional SQLite cache DB path (offline bundle). When set, gene sets + ID mappings are loaded from this DB")
	cmd.StringVar(&c.universeFile, "universe-file", "", "Optional background gene list file (one gene per line)")
	cmd.StringVar(&c.database, "d", "kegg", "Database: kegg, go, reactome, msigdb, custom")
	cmd.StringVar(&c.species, "s", "hsa", "Species code (e.g., hsa, mmu)")
	cmd.StringVar(&c.ontology, "ont", "BP", "GO ontology: BP, MF, CC")
	cmd.StringVar(&c.collection, "c", "c1-c8", "MSigDB collection(s): h, c1-c8, c1,c2,..., all")
	cmd.StringVar(&c.gmtFile, "gmt", "", "Custom GMT file path")
	cmd.StringVar(&c.format, "fmt", "tsv", "Output format: tsv, csv, json")
	cmd.StringVar(&c.idType, "id-type", "auto", "Input ID type: auto, entrez, symbol, uniprot, kegg (auto-detect by default)")
	cmd.IntVar(&c.minGSSize, "minGSSize", 10, "Minimum gene set size")
	cmd.IntVar(&c.maxGSSize, "maxGSSize", 500, "Maximum gene set size")
	cmd.Float64Var(&c.pvalue, "pvalue", 0.05, "Deprecated: raw p-value cutoff (ignored; filtering uses qvalue/FDR)")
	cmd.Float64Var(&c.qvalue, "qvalue", 0.05, "FDR (q-value) cutoff")
	cmd.BoolVar(&c.noFilter, "noFilter", false, "Disable filtering by FDR (q-value)")
	// 显著基因过滤
	cmd.StringVar(&c.sigCol, "sig-col", "significant", "Column name for significance flag (string filter)")
	cmd.StringVar(&c.sigVal, "sig-val", "TRUE", "Value that marks a significant gene (used with --sig-col)")
	cmd.StringVar(&c.fdrCol, "fdr-col", "", "Column name for FDR/adjusted p-value (numeric filter; overrides --sig-col when set)")
	cmd.Float64Var(&c.fdrThreshold, "fdr-threshold", 0.05, "FDR threshold for significant genes (used with --fdr-col)")
	cmd.BoolVar(&c.useAllBackground, "use-all-background", true, "Use all genes in DEG table as ORA background (Universe)")
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
	// 方向性分析
	cmd.BoolVar(&c.splitByDirection, "split-by-direction", true, "Run separate ORA for Up/Down regulated genes")
	cmd.StringVar(&c.dirCol, "dir-col", "direction", "Column name for regulation direction")
	cmd.StringVar(&c.upVal, "up-val", "Up", "Value indicating up-regulation in direction column")
	cmd.StringVar(&c.downVal, "down-val", "Down", "Value indicating down-regulation in direction column")
	cmd.StringVar(&c.logFCCol, "logfc-col", "", "Column name for log fold-change (used to infer direction when --dir-col is absent; default: second column)")
	cmd.Float64Var(&c.logFCThresh, "logfc-threshold", 0, "Absolute logFC threshold for direction inference (|logFC| must exceed this value)")
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

	keggCacheMax, applyKEGGCacheMax, err := resolveKEGGIDCacheMaxEntries(c.idCacheMax)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
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
	if c.noFilter {
		c.qvalue = 1.0
	}
	if c.benchmark {
		if err := runBenchmarkMode("enrich", c.database, c.outputFile, c.benchmarkOut); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if c.useR {
		opts := &rRunOptions{
			Command:          "enrich",
			Database:         c.database,
			Species:          c.species,
			Ontology:         c.ontology,
			Collection:       c.collection,
			InputFile:        c.inputFile,
			OutputFile:       c.outputFile,
			DataDir:          c.dataDir,
			NPerm:            1000,
			Format:           c.format,
			SigCol:           c.sigCol,
			SigVal:           c.sigVal,
			FDRCol:           c.fdrCol,
			FDRThreshold:     c.fdrThreshold,
			RankCol:          c.logFCCol,
			SplitByDirection: c.splitByDirection,
			DirCol:           c.dirCol,
			UpVal:            c.upVal,
			DownVal:          c.downVal,
			LogFCCol:         c.logFCCol,
			LogFCThreshold:   c.logFCThresh,
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

	// 1. 读取输入基因列表
	fmt.Println("Reading input genes...")

	// 构建解析选项：按 fdr-col 或 sig-col 过滤显著基因
	opts := &io.ParseDiffGeneTableOptions{
		DirCol:   c.dirCol,
		ValueCol: c.logFCCol, // 空字符串时默认第二列
	}
	if c.fdrCol != "" {
		opts.FilterCol = c.fdrCol
		opts.FilterThresh = c.fdrThreshold
		opts.FilterMode = "numeric_lte"
	} else if c.sigCol != "" {
		opts.FilterCol = c.sigCol
		opts.FilterVal = c.sigVal
		opts.FilterMode = "string"
	}

	input, err := io.ParseDiffGeneTableWithOptions(c.inputFile, opts)
	if err != nil {
		// 回退到基因列表格式
		plainInput, err2 := io.ParseGeneListFile(c.inputFile)
		if err2 != nil {
			fmt.Fprintf(os.Stderr, "Error reading input file: %v\n", err)
			os.Exit(1)
		}
		input = plainInput
	}
	fmt.Printf("Loaded %d significant genes (universe: %d total)\n", len(input.Genes), len(input.AllGenes))

	if len(input.AllGenes) == 0 {
		input.AllGenes = input.Genes
	}

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
		true,
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

	// 1.6 如无方向信息，尝试从 logFC（GeneValues）自动推断 Up/Down
	if c.splitByDirection && len(input.GeneDirections) == 0 {
		// 检查是否有真实 logFC 数据（至少一个值不等于默认占位符 1.0）
		hasRealLogFC := false
		for _, g := range input.Genes {
			if v := input.GeneValues[g]; v != 1.0 {
				hasRealLogFC = true
				break
			}
		}
		if hasRealLogFC {
			colDesc := "second column"
			if c.logFCCol != "" {
				colDesc = c.logFCCol
			}
			fmt.Printf("Direction column not found; inferring Up/Down from logFC (%s, |logFC| > %.2f)...\n", colDesc, c.logFCThresh)
			for _, g := range input.Genes {
				v := input.GeneValues[g]
				if v > c.logFCThresh {
					input.GeneDirections[g] = c.upVal
				} else if v < -c.logFCThresh {
					input.GeneDirections[g] = c.downVal
				}
				// |v| <= threshold 的基因不参与方向分组（稀释的变化量）
			}
			fmt.Printf("Inferred %d Up / %d Down significant genes\n",
				func() int {
					n := 0
					for _, d := range input.GeneDirections {
						if d == c.upVal {
							n++
						}
					}
					return n
				}(),
				func() int {
					n := 0
					for _, d := range input.GeneDirections {
						if d == c.downVal {
							n++
						}
					}
					return n
				}(),
			)
		} else {
			fmt.Println("Warning: no direction column or logFC data found; running combined ORA without Up/Down split")
		}
	}

	// 2. 加载基因集数据库
	fmt.Printf("Loading database: %s...\n", c.database)
	var forcedUniverse []string
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

	if strings.EqualFold(c.database, "go") {
		if len(loaded.AnnotatedGenes) == 0 {
			fmt.Fprintln(os.Stderr, "Error: GO annotation background is empty")
			os.Exit(1)
		}
		if c.universeFile == "" {
			filteredSig := make([]string, 0, len(input.Genes))
			for _, g := range input.Genes {
				if loaded.AnnotatedGenes[g] {
					filteredSig = append(filteredSig, g)
				}
			}
			if len(filteredSig) != len(input.Genes) {
				fmt.Printf("GO annotation filter: kept %d/%d significant genes\n", len(filteredSig), len(input.Genes))
			}
			input.Genes = filteredSig
			if len(input.Genes) == 0 {
				fmt.Fprintln(os.Stderr, "Error: no significant genes remain after GO annotation filter")
				os.Exit(1)
			}
			for gene := range loaded.AnnotatedGenes {
				forcedUniverse = append(forcedUniverse, gene)
			}
		}
	}

	fmt.Printf("Loaded %d gene sets\n", len(geneSets))

	// 3. 执行 ORA
	fmt.Println("Running ORA...")

	// 确定 Universe
	var universe []string
	if c.universeFile != "" {
		bgInput, err := io.ParseGeneListFile(c.universeFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading universe-file: %v\n", err)
			os.Exit(1)
		}
		universe = bgInput.Genes
		if len(universe) == 0 {
			fmt.Fprintln(os.Stderr, "Error: universe-file is empty")
			os.Exit(1)
		}
		// 与 clusterProfiler 一致：输入基因必须属于 universe。
		uSet := make(map[string]bool, len(universe))
		for _, g := range universe {
			uSet[g] = true
		}
		filteredSig := make([]string, 0, len(input.Genes))
		for _, g := range input.Genes {
			if uSet[g] {
				filteredSig = append(filteredSig, g)
			}
		}
		if len(filteredSig) != len(input.Genes) {
			fmt.Printf("Universe filter: kept %d/%d significant genes\n", len(filteredSig), len(input.Genes))
		}
		input.Genes = filteredSig
		if len(input.Genes) == 0 {
			fmt.Fprintln(os.Stderr, "Error: no significant genes remain after universe filter")
			os.Exit(1)
		}
	} else if len(forcedUniverse) > 0 {
		universe = forcedUniverse
	} else if c.useAllBackground && len(input.AllGenes) > 0 {
		universe = input.AllGenes
	}

	var allConverted []*io.EnrichmentResult

	if c.splitByDirection && len(input.GeneDirections) > 0 {
		// 按方向分组
		var upGenes, downGenes []string
		for _, g := range input.Genes {
			switch input.GeneDirections[g] {
			case c.upVal:
				upGenes = append(upGenes, g)
			case c.downVal:
				downGenes = append(downGenes, g)
			}
		}

		if len(upGenes) > 0 {
			fmt.Printf("Running ORA for Up-regulated genes (%d genes)...\n", len(upGenes))
			upParams := &analysis.ORAParams{
				GeneList:     upGenes,
				GeneSets:     geneSets,
				Universe:     universe,
				MinGSSize:    c.minGSSize,
				MaxGSSize:    c.maxGSSize,
				PValueCutoff: c.pvalue,
				QValueCutoff: c.qvalue,
			}
			upResults := analysis.RunORA(upParams)
			fmt.Printf("Found %d enriched terms (Up)\n", len(upResults))
			allConverted = append(allConverted, convertResults(upResults, "Up", displayGeneMap)...)
		}

		if len(downGenes) > 0 {
			fmt.Printf("Running ORA for Down-regulated genes (%d genes)...\n", len(downGenes))
			downParams := &analysis.ORAParams{
				GeneList:     downGenes,
				GeneSets:     geneSets,
				Universe:     universe,
				MinGSSize:    c.minGSSize,
				MaxGSSize:    c.maxGSSize,
				PValueCutoff: c.pvalue,
				QValueCutoff: c.qvalue,
			}
			downResults := analysis.RunORA(downParams)
			fmt.Printf("Found %d enriched terms (Down)\n", len(downResults))
			allConverted = append(allConverted, convertResults(downResults, "Down", displayGeneMap)...)
		}
	} else {
		// 原有逻辑：全部显著基因一起做 ORA
		params := &analysis.ORAParams{
			GeneList:     input.Genes,
			GeneSets:     geneSets,
			Universe:     universe,
			MinGSSize:    c.minGSSize,
			MaxGSSize:    c.maxGSSize,
			PValueCutoff: c.pvalue,
			QValueCutoff: c.qvalue,
		}
		results := analysis.RunORA(params)
		fmt.Printf("Found %d enriched terms\n", len(results))
		allConverted = convertResults(results, "", displayGeneMap)
	}

	// 4. 写入结果
	fmt.Printf("Writing results to %s...\n", c.outputFile)
	if err := io.WriteEnrichmentResults(allConverted, c.outputFile, io.OutputFormat(c.format)); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing output: %v\n", err)
		os.Exit(1)
	}

	writeKEGGIDCacheMetricsIfRequested(keggConv)
	fmt.Println("Done!")
}

func loadEntrezSymbolMapFromSQLite(st *store.SQLiteStore, species string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	scanned, err := st.ScanIDMap(
		ctx,
		strings.ToLower(strings.TrimSpace(species)),
		string(annotation.IDEntrez),
		string(annotation.IDSymbol),
	)
	if err != nil {
		return nil, err
	}

	m := make(map[string]string, len(scanned))
	for entrez, symbols := range scanned {
		entrez = strings.TrimSpace(entrez)
		if entrez == "" {
			continue
		}
		for _, sym := range symbols {
			sym = strings.TrimSpace(sym)
			if sym == "" {
				continue
			}
			m[entrez] = sym
			break
		}
	}
	if len(m) == 0 {
		return nil, fmt.Errorf("empty ENTREZ->SYMBOL mapping in sqlite for %s", species)
	}
	return m, nil
}

// 转换结果格式
func convertResults(results []*analysis.EnrichmentResult, direction string, displayGeneMap map[string]string) []*io.EnrichmentResult {
	var converted []*io.EnrichmentResult
	for _, r := range results {
		converted = append(converted, &io.EnrichmentResult{
			Direction:   direction,
			ID:          r.ID,
			Name:        r.Name,
			GeneRatio:   r.GeneRatio,
			BgRatio:     r.BgRatio,
			PValue:      r.PValue,
			PAdjust:     r.PAdjust,
			QValue:      r.QValue,
			Genes:       mapIDsForDisplay(r.Genes, displayGeneMap),
			Count:       r.Count,
			Description: r.Description,
		})
	}
	return converted
}
