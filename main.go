package main

import (
	"flag"
	"fmt"
	"os"
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
	// 子命令
	enrichCmd := flag.NewFlagSet("enrich", flag.ExitOnError)
	gseaCmd := flag.NewFlagSet("gsea", flag.ExitOnError)
	downloadCmd := flag.NewFlagSet("download", flag.ExitOnError)

	if len(os.Args) < 2 {
		fmt.Println("Usage: enrichgo <command> [options]")
		fmt.Println("Commands:")
		fmt.Println("  enrich   Over-representation analysis (ORA)")
		fmt.Println("  gsea     Gene Set Enrichment Analysis (GSEA)")
		fmt.Println("  download Download database files")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "enrich":
		runEnrich(enrichCmd)
	case "gsea":
		runGSEA(gseaCmd)
	case "download":
		runDownload(downloadCmd)
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: enrichgo <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  enrich   Over-representation analysis (ORA)")
	fmt.Println("  gsea     Gene Set Enrichment Analysis (GSEA)")
	fmt.Println("  download Download database files")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  enrichgo enrich -i genes.txt -d kegg -s hsa -o result.tsv")
	fmt.Println("  enrichgo gsea -i ranked_genes.txt -d go -s hsa -o gsea_result.tsv")
	fmt.Println("  enrichgo download -d kegg -s hsa -o data/")
	fmt.Println()
	fmt.Println("For more information, use: enrichgo <command> -h")
}
