# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

clusterProfiler is a Bioconductor R package for universal enrichment analysis of omics data. It supports:
- **Over-Representation Analysis (ORA)**: enrichGO, enrichKEGG, enrichWP, enrichDAVID, enricher
- **Gene Set Enrichment Analysis (GSEA)**: gseGO, gseKEGG, gseWP, gseMKEGG
- **Comparative analysis**: compareCluster for comparing multiple gene lists
- **LLM-based interpretation**: interpret(), interpret_hierarchical() for automated analysis using Large Language Models

## Common Commands

```bash
# Generate documentation (roxygen2)
make rd

# Build package
make build

# Install package
make install

# Run R CMD check
make check

# Run BiocCheck
make bioccheck

# Run interpret test (LLM functionality)
make test_interpret

# Run testthat tests
Rscript -e 'devtools::test()'

# Run a single test file
Rscript -e 'testthat::test_file("tests/testthat/test-bitr.R")'
```

## Architecture

### Core Classes (S4)
- `enrichResult`: Base class for ORA results (from DOSE package)
- `gseaResult`: Base class for GSEA results
- `compareClusterResult`: Results from compareCluster
- `groupGOResult`: Contains enrichResult, adds GO level information

### Key Files
- `R/enrichGO.R`, `R/enrichKEGG.R`: Core enrichment functions
- `R/gseAnalyzer.R`: GSEA implementation
- `R/compareCluster.R`: Multi-cluster comparison
- `R/interpret.R` (~51KB): LLM-based interpretation module
- `R/simplify.R`: GO enrichment simplification/semantic clustering
- `R/gson.R`: Gene set online (GSON) functionality for custom gene sets

### Data Flow
1. Input: Gene IDs (ENTREZ, UNIPROT, ENSEMBL, etc.)
2. Conversion: `bitr()` converts between ID types
3. Enrichment: Call enrich*/gse* functions
4. Results: S4 objects with tidyverse-like methods (filter, select, mutate, etc.)
5. Visualization: Uses enrichplot package

### Test Structure
- Uses testthat framework
- Tests in `tests/testthat/` directory
- Additional manual tests in `local_test/`

## Development Notes

- Package uses roxygen2 for documentation (run `make rd` after modifying roxygen comments)
- Depends on Bioconductor packages: DOSE, GOSemSim, enrichit, enrichplot, yulab.utils
- Supports KEGG, GO, WikiPathways, Reactome, and custom gene sets
- LLM interpretation uses OpenAI API (set via `OPENAI_API_KEY` env var)
- Development branch: `devel` (Bioconductor-style development workflow)
