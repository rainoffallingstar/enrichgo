# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

enrichgo is a Go CLI implementation of clusterProfiler for gene set enrichment analysis. It provides command-line tools for:
- **ORA (Over-Representation Analysis)**: Hypergeometric test for enriched pathways
- **GSEA (Gene Set Enrichment Analysis)**: Rank-based enrichment analysis
- **Database downloads**: KEGG, GO, Reactome, MSigDB

## Common Commands

```bash
# Build the project
go build -o enrichgo

# Run all tests
go test ./...

# Run tests for a specific package
go test ./pkg/analysis/...

# Run a specific test
go test -v -run TestHypergeometricTest ./pkg/analysis/

# Run with coverage
go test -cover ./...

# Run the CLI
./enrichgo enrich -i genes.txt -d kegg -s hsa -o result.tsv
./enrichgo gsea -i ranked_genes.txt -d kegg -s hsa -o gsea_result.tsv
./enrichgo download -d kegg -s hsa -o data/

# Get help
./enrichgo enrich -h
./enrichgo gsea -h
```

## Architecture

### Directory Structure
```
enrichgo/
├── main.go              # Entry point, command routing
├── cmd_enrich.go        # ORA command implementation
├── cmd_gsea.go          # GSEA command implementation
├── cmd_download.go      # Database download command
└── pkg/
    ├── analysis/        # Core algorithms (ORA, GSEA, FDR)
    ├── annotation/      # ID detection and conversion (bitr)
    ├── database/        # KEGG, GO, MSigDB, Reactome downloads
    ├── io/              # File parsing and output
    └── types/           # Shared types (GeneSet, Pathway)
```

### Key Modules

- **pkg/analysis/ora.go**: Hypergeometric test implementation with cumulative probability calculation, Benjamini-Hochberg FDR correction
- **pkg/analysis/gsea.go**: GSEA algorithm with enrichment score (ES), permutation testing, normalized enrichment score (NES)
- **pkg/annotation/bitr.go**: ID type detection (ENTREZ, SYMBOL, ENSEMBL, UNIPROT, KEGG, REFSEQ) and conversion via KEGG REST API
- **pkg/database/**: Download and parse gene sets from KEGG, GO, MSigDB, Reactome

### Data Flow

1. Input: Gene list file (TSV/CSV) or ranked gene list
2. ID conversion: Auto-detect and convert input IDs to database format (e.g., SYMBOL → ENTREZ)
3. Database loading: Load or download gene sets from selected database
4. Analysis: ORA (hypergeometric test) or GSEA (ranked enrichment)
5. Output: TSV/CSV file with enrichment results

### Supported Databases

- KEGG PATHWAY (species: hsa, mmu, rno, dre, cel, dme, ath, sce, eco, bta, gga)
- Gene Ontology (ontologies: BP, MF, CC)
- Reactome
- MSigDB (c1-c8)
- Custom GMT files
