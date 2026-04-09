# Enrichgo Command Templates

Use these templates when the user wants concrete commands for this repository.

## Build

```bash
go build -o enrichgo .
```

## ORA

Single result table from a DEG file:

```bash
./enrichgo analyze ora \
  -i test-data/DE_results.csv \
  -d go -s hsa \
  --fdr-col FDR --fdr-threshold 0.05 \
  --split-by-direction=false \
  -o /tmp/ora_go.tsv
```

Use a specific offline SQLite bundle:

```bash
./enrichgo analyze ora \
  -i test-data/DE_results.csv \
  -d kegg -s hsa \
  --db data/enrichgo.db \
  -o /tmp/ora_kegg.tsv
```

## GSEA

Run from a DEG table:

```bash
./enrichgo analyze gsea \
  -i test-data/DE_results.csv \
  -d kegg -s hsa \
  --rank-col logFC \
  -nPerm 1000 \
  -o /tmp/gsea_kegg.tsv
```

Run from a pre-ranked file:

```bash
./enrichgo analyze gsea \
  -i ranked_genes.tsv \
  --ranked \
  -d go -s hsa \
  -o /tmp/gsea_go.tsv
```

Use a custom GMT:

```bash
./enrichgo analyze gsea \
  -i test-data/DE_results.csv \
  -d custom \
  --gmt /abs/path/custom.gmt \
  --rank-col logFC \
  -o /tmp/gsea_custom.tsv
```

## Data Sync

Cache local database files:

```bash
./enrichgo data sync -d kegg -s hsa -o data/
./enrichgo data sync -d go -s hsa -ont BP -o data/
```

Build a reusable SQLite bundle:

```bash
./enrichgo data sync \
  -d all -s hsa -ont ALL -c all \
  --db data/enrichgo.db \
  --db-only \
  --idmaps \
  --idmaps-level extended
```

Force a full ID-map refresh:

```bash
./enrichgo data sync \
  -d all -s hsa \
  --db data/enrichgo.db \
  --db-only \
  --idmaps \
  --idmaps-level extended \
  --idmaps-force-refresh
```

## DB Audit

Audit a SQLite bundle:

```bash
./enrichgo db audit --db data/enrichgo.db
```

Audit against the embedded manifest:

```bash
./enrichgo db audit \
  --db data/enrichgo.db \
  --expect-embedded-manifest
```

## R Baseline and Benchmark

Run the R baseline directly:

```bash
./enrichgo analyze gsea \
  -i test-data/DE_results.csv \
  -d go -s hsa \
  --rank-col logFC \
  --use-r \
  -o /tmp/gsea_r.tsv
```

Run Go and R together with a benchmark report:

```bash
./enrichgo analyze gsea \
  -i test-data/DE_results.csv \
  -d kegg -s hsa \
  --rank-col logFC \
  -nPerm 100 \
  --benchmark \
  --benchmark-out /tmp/gsea_benchmark.tsv \
  -o /tmp/gsea_go.tsv
```

Alternative benchmark entrypoint:

```bash
./enrichgo bench run gsea \
  -i test-data/DE_results.csv \
  -d kegg -s hsa \
  --rank-col logFC \
  -o /tmp/gsea_go.tsv \
  --benchmark-out /tmp/gsea_benchmark.tsv
```
