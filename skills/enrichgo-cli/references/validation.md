# Enrichgo Validation and Troubleshooting

Use this reference when the task is about parity with `clusterProfiler`, regression checks, or debugging CLI behavior.

## Fast Validation Order

1. Build the binary.
2. Run the smallest relevant `go test` target.
3. Run a CLI smoke command against `test-data/DE_results.csv`.
4. Only then escalate to alignment scripts or Go vs R benchmark runs.

## Typical Checks

Build:

```bash
go build -o enrichgo .
```

Repository tests:

```bash
go test ./...
```

Narrower examples:

```bash
go test ./pkg/analysis/...
go test ./pkg/store/...
go test -run Test.* ./...
```

## Alignment Workflow

When the task is specifically about parity against `clusterProfiler`, use the repository scripts instead of inventing a new process.

Primary references:

- `docs/alignment.md`
- `scripts/alignment/run_alignment.sh`
- `scripts/alignment/run_alignment_ci.sh`
- `scripts/alignment/check_alignment_thresholds.py`
- `scripts/alignment/run_alignment_batch.py`

Common entrypoints:

```bash
./scripts/alignment/run_alignment.sh \
  test-data/DE_results.csv \
  /tmp/alignment \
  data
```

```bash
./scripts/alignment/run_alignment_ci.sh \
  /tmp/alignment_ci \
  data \
  test-data/DE_results.csv
```

## When to Inspect Which File

- Option mismatch or help text issue: inspect `cmd_enrich.go`, `cmd_gsea.go`, `cmd_download.go`, `cmd_db_audit.go`
- Command routing issue: inspect `main.go`
- ID conversion issue: inspect `pkg/annotation/`
- SQLite persistence or audit issue: inspect `pkg/store/` and `cmd_db_audit.go`
- Alignment threshold or report issue: inspect `docs/alignment.md` and `scripts/alignment/`

## Common Failure Modes

ID conversion problems:

- Check `--id-conversion-policy`
- Check `--min-conversion-rate`
- Check `--allow-id-fallback`
- Check `--enable-online-idmap-fallback`
- Prefer an explicit SQLite DB with synced ID maps for reproducible offline runs

Custom gene set problems:

- Use `-d custom`
- Pass `--gmt`
- Do not suggest `--update-db`

R parity problems:

- Confirm whether the user wants `--use-r` or `--benchmark`
- Check whether `rvx` or `Rscript` is available
- Follow the checked-in alignment workflow before claiming a parity regression

Embedded DB problems:

- Audit with `enrichgo db audit`
- Use `--expect-embedded-manifest` when checking the default bundled DB contract
