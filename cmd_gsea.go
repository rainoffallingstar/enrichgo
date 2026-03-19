package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"enrichgo/pkg/analysis"
	"enrichgo/pkg/annotation"
	"enrichgo/pkg/database"
	"enrichgo/pkg/io"
	"enrichgo/pkg/store"
)

// gseaCmd GSEA 分析命令
type gseaCmd struct {
	inputFile        string
	outputFile       string
	dataDir          string
	dbPath           string
	database         string
	species          string
	ontology         string
	collection       string
	gmtFile          string
	format           string
	idType           string
	seed             int64
	minGSSize        int
	maxGSSize        int
	permutations     int
	workers          int
	pvalue           float64
	padjCutoff       float64
	applyPValueCut   bool
	pvalueMethod     string
	maxPermutations  int
	useSharedNESPool bool
	ranked           bool   // 是否已排序
	rankCol          string // 排名列名（用于 DEG 表格）
	allowIDFallback  bool   // ID 转换失败时允许回退到原始 ID
	debugRankedOut   string // 输出排序后的 ranked gene 列表
	useR             bool
	benchmark        bool
	benchmarkOut     string
}

func runGSEA(cmd *flag.FlagSet) {
	c := &gseaCmd{}
	displayGeneMap := make(map[string]string) // result gene ID -> SYMBOL

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
	cmd.BoolVar(&c.allowIDFallback, "allow-id-fallback", false, "Continue with original IDs when ID conversion fails")
	cmd.StringVar(&c.debugRankedOut, "debug-ranked-out", "", "Write ranked genes after preprocessing to this TSV path")
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
	if c.benchmark {
		if err := runBenchmarkMode("gsea", c.database, c.outputFile, c.benchmarkOut); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if c.useR {
		opts := &rRunOptions{
			Command:    "gsea",
			Database:   c.database,
			Species:    c.species,
			Ontology:   c.ontology,
			Collection: c.collection,
			InputFile:  c.inputFile,
			OutputFile: c.outputFile,
			DataDir:    c.dataDir,
			NPerm:      c.permutations,
			Format:     c.format,
		}
		if err := runRMode(opts); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Done (R mode)!")
		return
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

	// 1. 读取输入
	fmt.Println("Reading input genes...")
	var input *io.GeneInput
	var err error

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
	{
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

		detectedType := annotation.BatchDetectIDType(input.Genes)
		if shouldConvert && detectedType != annotation.IDUnknown && detectedType != targetIDType {
			fmt.Printf("Detected ID type: %s, converting to %s...\n", detectedType, targetIDType)
			var converter annotation.IDConverter
			if st != nil {
				converter = annotation.NewSQLiteIDConverter(st)
			} else {
				converter = annotation.NewKEGGIDConverter(c.dataDir)
			}
			converted, mapping, err := annotation.ConvertGeneID(input.Genes, targetIDType, c.species, converter)
			if err != nil {
				if !c.allowIDFallback {
					fmt.Fprintf(os.Stderr, "Error: ID conversion failed: %v\n", err)
					fmt.Fprintln(os.Stderr, "Hint: use --allow-id-fallback to continue with original IDs")
					os.Exit(1)
				}
				fmt.Fprintf(os.Stderr, "Warning: ID conversion failed: %v, using original IDs\n", err)
			} else {
				mergeDisplayMapFromConversion(displayGeneMap, mapping)
				input.Genes = converted
				fmt.Printf("Converted %d genes\n", len(converted))
				// 更新 GeneValues
				newValues := make(map[string]float64)
				for origKey, newIDs := range mapping {
					if v, ok := input.GeneValues[origKey]; ok {
						for _, newID := range newIDs {
							newValues[newID] = v
						}
					}
				}
				input.GeneValues = newValues
			}
		}
	}
	if len(displayGeneMap) == 0 && strings.EqualFold(c.database, "kegg") {
		if st != nil {
			if m, err := loadEntrezSymbolMapFromSQLite(st, c.species); err == nil {
				for k, v := range m {
					displayGeneMap[k] = v
				}
			}
		} else {
			idmapPath := filepath.Join(c.dataDir, fmt.Sprintf("kegg_%s_idmap.tsv", c.species))
			if m, err := loadEntrezSymbolMapFromIDMap(idmapPath); err == nil {
				for k, v := range m {
					displayGeneMap[k] = v
				}
			}
		}
	}

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
	var geneSets analysis.GeneSets

	switch c.database {
	case "kegg":
		if st != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			sets, _, err := st.LoadGeneSets(ctx, store.GeneSetFilter{DB: "kegg", Species: c.species})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error loading KEGG from sqlite: %v\n", err)
				os.Exit(1)
			}
			geneSets = analysis.GeneSets(sets)
		} else {
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
		}

	case "go":
		if st != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			sets, _, err := st.LoadGeneSets(ctx, store.GeneSetFilter{DB: "go", Species: c.species, Ontology: c.ontology})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error loading GO from sqlite: %v\n", err)
				os.Exit(1)
			}
			geneSets = analysis.GeneSets(sets)
		} else {
			data, err := database.LoadOrDownloadGO(c.species, c.ontology, c.dataDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error loading GO data: %v\n", err)
				os.Exit(1)
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
		}

	case "reactome":
		if st != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			sets, _, err := st.LoadGeneSets(ctx, store.GeneSetFilter{DB: "reactome", Species: c.species})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error loading Reactome from sqlite: %v\n", err)
				os.Exit(1)
			}
			geneSets = analysis.GeneSets(sets)
		} else {
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
		}

	case "msigdb":
		collections, err := parseMSigDBCollections(c.collection)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if st != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			seen := make(map[string]bool)
			for _, col := range collections {
				sets, _, err := st.LoadGeneSets(ctx, store.GeneSetFilter{DB: "msigdb", Species: c.species, Collection: string(col)})
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error loading MSigDB from sqlite: %v\n", err)
					os.Exit(1)
				}
				for _, gs := range sets {
					if gs == nil || seen[gs.ID] {
						continue
					}
					seen[gs.ID] = true
					geneSets = append(geneSets, gs)
				}
			}
		} else {
			sets, err := database.LoadOrDownloadMSigDBCollections(collections, c.dataDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error loading MSigDB: %v\n", err)
				os.Exit(1)
			}
			geneSets = sets
		}

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
