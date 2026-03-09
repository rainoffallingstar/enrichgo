package main

import (
	"flag"
	"fmt"
	"os"

	"enrichgo/pkg/database"
)

// downloadCmd 数据库下载命令
type downloadCmd struct {
	database   string
	species    string
	ontology   string
	collection string
	outputDir  string
	all        bool
}

func runDownload(cmd *flag.FlagSet) {
	c := &downloadCmd{}

	cmd.StringVar(&c.database, "d", "kegg", "Database: kegg, go, msigdb, reactome")
	cmd.StringVar(&c.species, "s", "hsa", "Species code (e.g., hsa, mmu)")
	cmd.StringVar(&c.ontology, "ont", "BP", "GO ontology: BP, MF, CC, ALL")
	cmd.StringVar(&c.collection, "c", "c1-c8", "MSigDB collection(s): h, c1-c8, c1,c2,..., all")
	cmd.StringVar(&c.outputDir, "o", "data", "Output directory")
	cmd.BoolVar(&c.all, "all", false, "Download all collections (h,c1-c8) for msigdb")

	cmd.Parse(os.Args[2:])

	// 创建输出目录
	if err := os.MkdirAll(c.outputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	switch c.database {
	case "kegg":
		fmt.Printf("Downloading KEGG data for %s...\n", c.species)
		data, err := database.DownloadKEGG(c.species, c.outputDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Downloaded %d pathways\n", len(data.Pathways))

	case "go":
		ontologies := []string{"BP", "MF", "CC"}
		if c.ontology != "ALL" {
			ontologies = []string{c.ontology}
		}

		for _, ont := range ontologies {
			fmt.Printf("Downloading GO %s data for %s...\n", ont, c.species)
			data, err := database.DownloadGO(c.species, ont, c.outputDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Downloaded %d GO terms\n", len(data.Terms))
		}

	case "msigdb":
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
			sets, err := database.DownloadMSigDB(col, c.outputDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error downloading %s: %v\n", col, err)
				os.Exit(1)
			}
			total += len(sets)
		}
		fmt.Printf("Downloaded %d gene sets across %d collection(s)\n", total, len(collections))

	case "reactome":
		fmt.Printf("Downloading Reactome data for %s...\n", c.species)
		data, err := database.LoadOrDownloadReactome(c.species, c.outputDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Downloaded %d pathways\n", len(data.Pathways))

	default:
		fmt.Printf("Error: unknown database: %s\n", c.database)
		os.Exit(1)
	}

	fmt.Println("Done!")
}
