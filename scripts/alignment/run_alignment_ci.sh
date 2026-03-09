#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_BASE="${1:-/tmp/alignment_ci}"
DATA_DIR="${2:-$ROOT_DIR/data}"
INPUT_CSV="${3:-$ROOT_DIR/test-data/DE_results.csv}"

SMOKE_NPERM="${SMOKE_NPERM:-100}"
FORMAL_NPERM="${FORMAL_NPERM:-1000}"
ALIGN_SKIP_DOWNLOAD="${ALIGN_SKIP_DOWNLOAD:-1}"
ALIGN_SKIP_KEGG="${ALIGN_SKIP_KEGG:-0}"
SMOKE_GSEA_PVALUE_METHOD="${SMOKE_GSEA_PVALUE_METHOD:-simple}"
FORMAL_GSEA_PVALUE_METHOD="${FORMAL_GSEA_PVALUE_METHOD:-adaptive}"
FORMAL_GSEA_PVALUE_METHOD_MSIGDB="${FORMAL_GSEA_PVALUE_METHOD_MSIGDB:-simple}"
FORMAL_GSEA_MAX_PERM="${FORMAL_GSEA_MAX_PERM:-10000}"

SMOKE_DIR="$OUT_BASE/smoke"
FORMAL_DIR="$OUT_BASE/formal"

mkdir -p "$SMOKE_DIR" "$FORMAL_DIR"

run_one() {
  local nperm="$1"
  local out_dir="$2"
  local pvalue_method="$3"
  echo "[CI] run_alignment nPerm=$nperm out=$out_dir"
  ALIGN_SKIP_DOWNLOAD="$ALIGN_SKIP_DOWNLOAD" \
  ALIGN_SKIP_KEGG="$ALIGN_SKIP_KEGG" \
  ALIGN_DEBUG_GO_GSEA=0 \
  ALIGN_NPERM="$nperm" \
  ALIGN_GSEA_PVALUE_METHOD="$pvalue_method" \
  ALIGN_GSEA_PVALUE_METHOD_MSIGDB="$FORMAL_GSEA_PVALUE_METHOD_MSIGDB" \
  ALIGN_GSEA_MAX_PERM="$FORMAL_GSEA_MAX_PERM" \
  bash "$ROOT_DIR/scripts/alignment/run_alignment.sh" "$INPUT_CSV" "$out_dir" "$DATA_DIR"
}

gate_one() {
  local summary="$1"
  local kegg_top20="$2"
  local go_top20="$3"
  local enforce_l2="$4"
  echo "[CI] gate summary=$summary"
  python3 "$ROOT_DIR/scripts/alignment/check_alignment_thresholds.py" \
    "$summary" \
    --kegg-gsea-top20-min "$kegg_top20" \
    --go-gsea-top20-min "$go_top20" \
    --enforce-l2 "$enforce_l2"
}

run_one "$SMOKE_NPERM" "$SMOKE_DIR" "$SMOKE_GSEA_PVALUE_METHOD"
# Smoke uses L1 only to catch catastrophic regressions quickly.
gate_one "$SMOKE_DIR/comparison_summary.tsv" 0.50 0.40 0

run_one "$FORMAL_NPERM" "$FORMAL_DIR" "$FORMAL_GSEA_PVALUE_METHOD"
# Formal enables L2 top20 checks.
gate_one "$FORMAL_DIR/comparison_summary.tsv" 0.80 0.60 1

echo "[CI] smoke summary:  $SMOKE_DIR/comparison_summary.tsv"
echo "[CI] formal summary: $FORMAL_DIR/comparison_summary.tsv"
