package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// 支持的数据库类型
type DatabaseType string

const (
	DBKEGG     DatabaseType = "kegg"
	DBGO       DatabaseType = "go"
	DBReactome DatabaseType = "reactome"
	DBMSigDB   DatabaseType = "msigdb"
	DBCustom   DatabaseType = "custom"
)

// 支持的物种代码 (KEGG 格式)
var KEGGSpecies = map[string]string{
	"hsa": "Homo sapiens",
	"mmu": "Mus musculus",
	"rno": "Rattus norvegicus",
	"dre": "Danio rerio",
	"cel": "Caenorhabditis elegans",
	"dme": "Drosophila melanogaster",
	"ath": "Arabidopsis thaliana",
	"sce": "Saccharomyces cerevisiae",
	"eco": "Escherichia coli",
	"bta": "Bos taurus",
	"gga": "Gallus gallus",
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "analyze":
		runAnalyzeEntry()
	case "data":
		runDataEntry()
	case "db":
		runDBEntry()
	case "bench":
		runBenchEntry()
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func runAnalyzeEntry() {
	rewritten, err := rewriteAnalyzeArgs(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		if strings.HasPrefix(err.Error(), "missing analyze subcommand") {
			printUsage()
		}
		os.Exit(1)
	}
	os.Args = rewritten
	switch os.Args[1] {
	case "enrich":
		runEnrich(flag.NewFlagSet("enrich", flag.ExitOnError))
	case "gsea":
		runGSEA(flag.NewFlagSet("gsea", flag.ExitOnError))
	}
}

func runDataEntry() {
	rewritten, err := rewriteDataArgs(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		if strings.HasPrefix(err.Error(), "missing data subcommand") {
			printUsage()
		}
		os.Exit(1)
	}
	os.Args = rewritten
	runDownload(flag.NewFlagSet("download", flag.ExitOnError))
}

func runDBEntry() {
	rewritten, err := rewriteDBArgs(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		if strings.HasPrefix(err.Error(), "missing db subcommand") {
			printUsage()
		}
		os.Exit(1)
	}
	os.Args = rewritten
	runDBAudit(flag.NewFlagSet("db-audit", flag.ExitOnError))
}

func runBenchEntry() {
	rewritten, err := rewriteBenchArgs(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	os.Args = rewritten
	switch os.Args[1] {
	case "enrich":
		runEnrich(flag.NewFlagSet("enrich", flag.ExitOnError))
	case "gsea":
		runGSEA(flag.NewFlagSet("gsea", flag.ExitOnError))
	}
}

func rewriteAnalyzeArgs(args []string) ([]string, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("missing analyze subcommand (ora|gsea)")
	}
	sub := strings.ToLower(strings.TrimSpace(args[2]))
	switch sub {
	case "ora":
		return append([]string{args[0], "enrich"}, args[3:]...), nil
	case "gsea":
		return append([]string{args[0], "gsea"}, args[3:]...), nil
	default:
		return nil, fmt.Errorf("unknown analyze subcommand: %s", args[2])
	}
}

func rewriteDataArgs(args []string) ([]string, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("missing data subcommand (sync)")
	}
	sub := strings.ToLower(strings.TrimSpace(args[2]))
	switch sub {
	case "sync":
		return append([]string{args[0], "download"}, args[3:]...), nil
	default:
		return nil, fmt.Errorf("unknown data subcommand: %s", args[2])
	}
}

func rewriteDBArgs(args []string) ([]string, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("missing db subcommand (audit)")
	}
	sub := strings.ToLower(strings.TrimSpace(args[2]))
	switch sub {
	case "audit":
		return append([]string{args[0], "db-audit"}, args[3:]...), nil
	default:
		return nil, fmt.Errorf("unknown db subcommand: %s", args[2])
	}
}

func rewriteBenchArgs(args []string) ([]string, error) {
	if len(args) < 4 {
		return nil, fmt.Errorf("usage: enrichgo bench run <ora|gsea> [flags]")
	}
	if strings.ToLower(strings.TrimSpace(args[2])) != "run" {
		return nil, fmt.Errorf("unknown bench subcommand: %s", args[2])
	}
	analysis := strings.ToLower(strings.TrimSpace(args[3]))
	forward := append([]string{args[0]}, args[4:]...)
	if !hasBenchmarkFlag(forward[1:]) {
		forward = append(forward, "--benchmark=true")
	}
	switch analysis {
	case "ora":
		return append([]string{args[0], "enrich"}, forward[1:]...), nil
	case "gsea":
		return append([]string{args[0], "gsea"}, forward[1:]...), nil
	default:
		return nil, fmt.Errorf("unknown analysis for bench run: %s", args[3])
	}
}

func hasBenchmarkFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--benchmark" || strings.HasPrefix(arg, "--benchmark=") {
			return true
		}
	}
	return false
}

func printUsage() {
	fmt.Println("Usage: enrichgo <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  analyze ora      Over-representation analysis (ORA)")
	fmt.Println("  analyze gsea     Gene Set Enrichment Analysis (GSEA)")
	fmt.Println("  data sync        Download/sync database files")
	fmt.Println("  db audit         Audit SQLite backend schema and persistence")
	fmt.Println("  bench run        Run Go-vs-R benchmark for ora/gsea")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  enrichgo analyze ora -i genes.txt -d kegg -s hsa -o result.tsv")
	fmt.Println("  enrichgo analyze gsea -i ranked_genes.txt -d go -s hsa -o gsea_result.tsv")
	fmt.Println("  enrichgo data sync -d kegg -s hsa -o data/")
	fmt.Println("  enrichgo db audit --db data/enrichgo.db")
	fmt.Println("  enrichgo bench run gsea -i test-data/DE_results.csv -d kegg -s hsa -o /tmp/gsea.tsv --benchmark-out /tmp/bench.tsv")
	fmt.Println()
	fmt.Println("For more information, use: enrichgo <command> -h")
}
