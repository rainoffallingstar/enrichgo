---
name: enrichgo-cli
description: "Use when working on the enrichgo repository or CLI: run ORA or GSEA analyses, sync KEGG/GO/Reactome/MSigDB data, audit embedded or offline SQLite bundles, compare Go vs R clusterProfiler baselines, or troubleshoot ID conversion and alignment regressions."
---

# Enrichgo CLI

Use this skill when the task is specifically about this repository's `enrichgo` command-line tool rather than generic enrichment analysis.

## Before You Act

Confirm the workspace is the `enrichgo` repo before assuming repo-relative paths exist. Good anchors are:

- `go.mod`
- `main.go`
- `cmd_enrich.go`
- `cmd_gsea.go`
- `cmd_download.go`
- `docs/alignment.md`

If the user only wants biological interpretation of enrichment results and not this CLI or repo, do not rely on this skill.

## What This Tool Does

`enrichgo` is a Go CLI for:

- ORA: `enrichgo analyze ora`
- GSEA: `enrichgo analyze gsea`
- Data sync and offline SQLite bundle creation: `enrichgo data sync`
- SQLite contract and manifest audit: `enrichgo db audit`
- Go vs R benchmark mode: `enrichgo bench run`

The public subcommands above are the preferred surface. Internally the binary rewrites to legacy aliases like `enrich`, `gsea`, `download`, and `db-audit`, but user-facing guidance should normally use the public commands.

## Default Workflow

1. Read the user request and decide whether it is analysis, data sync, benchmark, DB audit, or code-change work.
2. When exact flag names matter, check the current command files instead of trusting memory:
   - `cmd_enrich.go`
   - `cmd_gsea.go`
   - `cmd_download.go`
   - `cmd_db_audit.go`
3. Prefer the embedded SQLite default when it covers the requested database and species.
4. Use `--db` when the user explicitly wants an offline bundle or reproducible local database artifact.
5. If a task touches R parity, benchmark, or regression diagnosis, read `references/validation.md`.
6. If the user needs runnable command templates, read `references/commands.md`.

## Analysis Guidance

For ORA:

- Default entrypoint: `enrichgo analyze ora`
- DEG table input often uses `--fdr-col` and `--fdr-threshold`
- `--split-by-direction` defaults to `true`; disable it when the user wants one combined ORA result
- Use `--db` only when the user wants a specific SQLite bundle; otherwise the embedded DB is usually the first choice

For GSEA:

- Default entrypoint: `enrichgo analyze gsea`
- DEG tables normally need `--rank-col`
- Pre-ranked inputs should use `--ranked`
- `--nPerm`, `--pvalue-method`, and `--max-perm` matter for parity and performance discussions

For custom gene sets:

- Use `-d custom --gmt <path>`
- Do not suggest `--update-db` for `-d custom`

## ID Conversion and Fallbacks

When a request involves SYMBOL, ENTREZID, KEGG, UniProt, or conversion failures:

- Check whether the task wants strict reproducibility or best-effort completion
- The default policy is permissive:
  - `--id-conversion-policy best-effort`
  - `--allow-id-fallback=true`
  - `--enable-online-idmap-fallback=true`
- For fail-fast or CI-like behavior, prefer `--strict-mode` or an explicit threshold policy
- For offline reproducibility, prefer a prepared SQLite bundle with `--db` and synced ID maps

## Data Sync and SQLite Guidance

Use `enrichgo data sync` when the user needs cached databases, offline delivery, or a reusable SQLite file.

Key cases:

- Cache GMT/TSV data into `data/`
- Build a single SQLite bundle with `--db`
- Store offline ID maps with `--idmaps`
- Use `--idmaps-level basic` for faster/smaller syncs
- Use `--idmaps-level extended` when SYMBOL to ENTREZ coverage matters more than speed

Use `enrichgo db audit` when the user needs:

- schema or manifest consistency checks
- contract-profile checks
- SHA256 verification of a SQLite DB

## Benchmark and Alignment Guidance

Use `--use-r` when the user wants the R `clusterProfiler` baseline only.

Use `--benchmark` or `enrichgo bench run` when the user wants Go and R outputs plus timing and memory comparison.

For repository-level parity work:

- Alignment workflow lives in `docs/alignment.md`
- Primary scripts live under `scripts/alignment/`
- Prefer the checked-in scripts over inventing ad hoc comparison commands

## Working Style

- Prefer repository examples and checked-in test data over made-up files
- Use `test-data/DE_results.csv` for local smoke checks unless the user supplied another dataset
- Use `examples/input/` and `examples/output/` when you need lightweight command examples
- If output semantics are unclear, inspect the command implementation before explaining behavior
- When modifying code, validate at least the narrowest relevant `go test` target or a CLI smoke run if feasible

## References

- `references/commands.md`: common command templates for build, analysis, sync, audit, and benchmark
- `references/validation.md`: parity, regression, alignment, and troubleshooting workflow
