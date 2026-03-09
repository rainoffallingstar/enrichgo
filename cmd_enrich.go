package main

import (
	"flag"
	"fmt"
	"os"

	"enrichgo/pkg/analysis"
	"enrichgo/pkg/annotation"
	"enrichgo/pkg/database"
	"enrichgo/pkg/io"
)

// enrichCmd ORA 富集分析命令
type enrichCmd struct {
	inputFile    string
	outputFile   string
	dataDir      string
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
	sigCol           string  // 字符串过滤列名（如 "significant"）
	sigVal           string  // 字符串过滤值（如 "TRUE"）
	fdrCol           string  // 数值过滤列名（如 "FDR"）
	fdrThreshold     float64 // 数值过滤阈值（如 0.05）
	useAllBackground bool    // 使用表格全部基因作为 ORA 背景
	allowIDFallback  bool    // ID 转换失败时允许回退到原始 ID
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

	cmd.StringVar(&c.inputFile, "i", "", "Input gene list file (required)")
	cmd.StringVar(&c.outputFile, "o", "enrichment_result.tsv", "Output file")
	cmd.StringVar(&c.dataDir, "data-dir", "data", "Database cache directory")
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
	cmd.BoolVar(&c.allowIDFallback, "allow-id-fallback", false, "Continue with original IDs when ID conversion fails")
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
			Command:    "enrich",
			Database:   c.database,
			Species:    c.species,
			Ontology:   c.ontology,
			Collection: c.collection,
			InputFile:  c.inputFile,
			OutputFile: c.outputFile,
			DataDir:    c.dataDir,
			NPerm:      1000,
			Format:     c.format,
		}
		if err := runRMode(opts); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Done (R mode)!")
		return
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
	targetIDType := targetIDTypeForDatabase(c.database)
	shouldConvert := targetIDType != annotation.IDUnknown
	if c.idType != "auto" {
		switch c.idType {
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
			fmt.Fprintf(os.Stderr, "Warning: unknown id-type '%s', using auto-detection\n", c.idType)
		}
	}

	// 自动检测输入 ID 类型并在需要时转换
	detectedType := annotation.BatchDetectIDType(input.Genes)
	if shouldConvert && detectedType != annotation.IDUnknown && detectedType != targetIDType {
		fmt.Printf("Detected ID type: %s, converting to %s...\n", detectedType, targetIDType)
		converter := annotation.NewKEGGIDConverter(c.dataDir)

		// 转换全部基因（包括背景）
		convertedAll, allMapping, err := annotation.ConvertGeneID(input.AllGenes, targetIDType, c.species, converter)
		if err != nil {
			if !c.allowIDFallback {
				fmt.Fprintf(os.Stderr, "Error: ID conversion failed: %v\n", err)
				fmt.Fprintln(os.Stderr, "Hint: use --allow-id-fallback to continue with original IDs")
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "Warning: ID conversion failed: %v, using original IDs\n", err)
		} else {
			// 从映射中提取显著基因的转换 ID
			sigSet := make(map[string]bool, len(input.Genes))
			for _, g := range input.Genes {
				sigSet[g] = true
			}
			var convertedSig []string
			for origKey, newIDs := range allMapping {
				if sigSet[origKey] {
					convertedSig = append(convertedSig, newIDs...)
				}
			}
			input.Genes = convertedSig
			input.AllGenes = convertedAll
			fmt.Printf("Converted %d significant genes (universe: %d)\n", len(convertedSig), len(convertedAll))

			// 更新 GeneValues 的键
			newValues := make(map[string]float64)
			for origKey, newIDs := range allMapping {
				if v, ok := input.GeneValues[origKey]; ok {
					for _, newID := range newIDs {
						newValues[newID] = v
					}
				}
			}
			input.GeneValues = newValues

			// 更新 GeneDirections 的键
			newDirs := make(map[string]string)
			for origKey, newIDs := range allMapping {
				if d, ok := input.GeneDirections[origKey]; ok {
					for _, newID := range newIDs {
						newDirs[newID] = d
					}
				}
			}
			input.GeneDirections = newDirs
		}
	}

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
	var geneSets analysis.GeneSets
	var forcedUniverse []string

	switch c.database {
	case "kegg":
		data, err := database.LoadOrDownloadKEGG(c.species, c.dataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading KEGG data: %v\n", err)
			os.Exit(1)
		}
		if data == nil {
			fmt.Fprintln(os.Stderr, "Error: no KEGG data available")
			os.Exit(1)
		}
		for _, pw := range data.Pathways {
			gs := &analysis.GeneSet{
				ID:          pw.ID,
				Name:        pw.Name,
				Genes:       pw.Genes,
				Description: pw.Description,
			}
			geneSets = append(geneSets, gs)
		}

	case "go":
		data, err := database.LoadOrDownloadGO(c.species, c.ontology, c.dataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading GO data: %v\n", err)
			os.Exit(1)
		}
		annotatedGenes := make(map[string]bool, len(data.Gene2Terms))
		for gene, terms := range data.Gene2Terms {
			if len(terms) > 0 {
				annotatedGenes[gene] = true
			}
		}
		if len(annotatedGenes) == 0 {
			fmt.Fprintln(os.Stderr, "Error: GO annotation background is empty")
			os.Exit(1)
		}
		if c.universeFile == "" {
			filteredSig := make([]string, 0, len(input.Genes))
			for _, g := range input.Genes {
				if annotatedGenes[g] {
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
			// 默认口径：GO ORA 背景使用可注释基因全集。
			for gene := range annotatedGenes {
				forcedUniverse = append(forcedUniverse, gene)
			}
		}
		// 构建倒排索引 termID -> genes
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
			gs := &analysis.GeneSet{
				ID:          termID,
				Name:        term.Name,
				Genes:       term2genes[termID],
				Description: term.Definition,
			}
			geneSets = append(geneSets, gs)
		}

	case "reactome":
		data, err := database.LoadOrDownloadReactome(c.species, c.dataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading Reactome data: %v\n", err)
			os.Exit(1)
		}
		if data == nil || len(data.Pathways) == 0 {
			fmt.Fprintln(os.Stderr, "Error: no Reactome data available")
			os.Exit(1)
		}
		for _, pw := range data.Pathways {
			gs := &analysis.GeneSet{
				ID:          pw.ID,
				Name:        pw.Name,
				Genes:       pw.Genes,
				Description: pw.Description,
			}
			geneSets = append(geneSets, gs)
		}

	case "msigdb":
		collections, err := parseMSigDBCollections(c.collection)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		sets, err := database.LoadOrDownloadMSigDBCollections(collections, c.dataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading MSigDB: %v\n", err)
			os.Exit(1)
		}
		geneSets = sets

	case "custom":
		if c.gmtFile == "" {
			fmt.Println("Error: -gmt is required for custom database")
			os.Exit(1)
		}
		sets, err := database.LoadGMTFile(c.gmtFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading GMT file: %v\n", err)
			os.Exit(1)
		}
		geneSets = sets

	default:
		fmt.Printf("Error: unknown database: %s\n", c.database)
		os.Exit(1)
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
			allConverted = append(allConverted, convertResults(upResults, "Up")...)
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
			allConverted = append(allConverted, convertResults(downResults, "Down")...)
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
		allConverted = convertResults(results, "")
	}

	// 4. 写入结果
	fmt.Printf("Writing results to %s...\n", c.outputFile)
	if err := io.WriteEnrichmentResults(allConverted, c.outputFile, io.OutputFormat(c.format)); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing output: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Done!")
}

// 转换结果格式
func convertResults(results []*analysis.EnrichmentResult, direction string) []*io.EnrichmentResult {
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
			Genes:       r.Genes,
			Count:       r.Count,
			Description: r.Description,
		})
	}
	return converted
}
