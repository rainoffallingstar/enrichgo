#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
INPUT_CSV="${1:-$ROOT_DIR/test-data/DE_results.csv}"
OUT_DIR="${2:-$ROOT_DIR/artifacts/alignment}"
DATA_DIR="${3:-$ROOT_DIR/data}"
ALIGN_NPERM="${ALIGN_NPERM:-1000}"
ALIGN_GSEA_PVALUE_METHOD="${ALIGN_GSEA_PVALUE_METHOD:-simple}"
ALIGN_GSEA_PVALUE_METHOD_MSIGDB="${ALIGN_GSEA_PVALUE_METHOD_MSIGDB:-$ALIGN_GSEA_PVALUE_METHOD}"
ALIGN_GSEA_MAX_PERM="${ALIGN_GSEA_MAX_PERM:-20000}"
ALIGN_SKIP_DOWNLOAD="${ALIGN_SKIP_DOWNLOAD:-0}"
ALIGN_MIN_CONV_RATE="${ALIGN_MIN_CONV_RATE:-0.90}"
ALIGN_ONLY_ORA="${ALIGN_ONLY_ORA:-0}"
ALIGN_USE_R_GO_UNIVERSE="${ALIGN_USE_R_GO_UNIVERSE:-1}"
ALIGN_USE_R_GO_GMT="${ALIGN_USE_R_GO_GMT:-1}"
ALIGN_DEBUG_GO_GSEA="${ALIGN_DEBUG_GO_GSEA:-0}"
ALIGN_SKIP_KEGG="${ALIGN_SKIP_KEGG:-0}"
ALIGN_INCLUDE_REACTOME="${ALIGN_INCLUDE_REACTOME:-0}"
ALIGN_INCLUDE_MSIGDB="${ALIGN_INCLUDE_MSIGDB:-0}"
ALIGN_MSIGDB_COLLECTIONS="${ALIGN_MSIGDB_COLLECTIONS:-c1-c8}"

mkdir -p "$OUT_DIR" "$DATA_DIR"

if [[ ! -f "$INPUT_CSV" ]]; then
  echo "Input CSV not found: $INPUT_CSV" >&2
  exit 1
fi

echo "[ALIGN] input=$INPUT_CSV out=$OUT_DIR data=$DATA_DIR"
echo "[ALIGN] nperm=$ALIGN_NPERM gsea_pvalue_method=$ALIGN_GSEA_PVALUE_METHOD gsea_pvalue_method_msigdb=$ALIGN_GSEA_PVALUE_METHOD_MSIGDB gsea_max_perm=$ALIGN_GSEA_MAX_PERM skip_download=$ALIGN_SKIP_DOWNLOAD skip_kegg=$ALIGN_SKIP_KEGG include_reactome=$ALIGN_INCLUDE_REACTOME include_msigdb=$ALIGN_INCLUDE_MSIGDB msigdb_collections=$ALIGN_MSIGDB_COLLECTIONS only_ora=$ALIGN_ONLY_ORA debug_go_gsea=$ALIGN_DEBUG_GO_GSEA"

pushd "$ROOT_DIR" >/dev/null

if [[ ! -x "$ROOT_DIR/enrichgo" ]]; then
  echo "Building enrichgo binary..."
  go build -o enrichgo .
fi

# Ensure baseline databases are present in data dir.
# KEGG download also caches ID mapping file for offline SYMBOL->ENTREZ conversion.
if [[ "$ALIGN_SKIP_DOWNLOAD" != "1" ]]; then
  if [[ ! -f "$DATA_DIR/hsa.gmt" ]]; then
    echo "Downloading KEGG cache into $DATA_DIR..."
    ./enrichgo download -d kegg -s hsa -o "$DATA_DIR"
  fi
  if [[ ! -f "$DATA_DIR/go_hsa_BP.gmt" ]]; then
    echo "Downloading GO(BP) cache into $DATA_DIR..."
    ./enrichgo download -d go -s hsa -ont BP -o "$DATA_DIR"
  fi
  if [[ "$ALIGN_INCLUDE_REACTOME" == "1" && ! -f "$DATA_DIR/reactome_hsa.gmt" ]]; then
    echo "Downloading Reactome cache into $DATA_DIR..."
    ./enrichgo download -d reactome -s hsa -o "$DATA_DIR"
  fi
  if [[ "$ALIGN_INCLUDE_MSIGDB" == "1" ]]; then
    echo "Downloading MSigDB cache ($ALIGN_MSIGDB_COLLECTIONS) into $DATA_DIR..."
    ./enrichgo download -d msigdb -c "$ALIGN_MSIGDB_COLLECTIONS" -o "$DATA_DIR"
  fi
else
  echo "ALIGN_SKIP_DOWNLOAD=1: expecting pre-cached files in $DATA_DIR"
fi

if [[ ! -f "$DATA_DIR/hsa.gmt" ]]; then
  echo "Missing required KEGG cache: $DATA_DIR/hsa.gmt" >&2
  exit 1
fi
if [[ ! -f "$DATA_DIR/go_hsa_BP.gmt" ]]; then
  echo "Missing required GO cache: $DATA_DIR/go_hsa_BP.gmt" >&2
  exit 1
fi
if [[ "$ALIGN_SKIP_KEGG" != "1" && ! -f "$DATA_DIR/hsa.gmt" ]]; then
  echo "Missing required KEGG GMT for alignment: $DATA_DIR/hsa.gmt" >&2
  exit 1
fi
if [[ "$ALIGN_INCLUDE_REACTOME" == "1" && ! -f "$DATA_DIR/reactome_hsa.gmt" ]]; then
  echo "Missing required Reactome GMT: $DATA_DIR/reactome_hsa.gmt" >&2
  exit 1
fi

if [[ ! -f "$DATA_DIR/kegg_hsa_idmap.tsv" ]]; then
  echo "Warning: kegg_hsa_idmap.tsv missing; KEGG ID conversion may require network." >&2
fi

# 0) ID conversion quality gate (SYMBOL -> ENTREZ)
if [[ ! -f "$DATA_DIR/kegg_hsa_idmap.tsv" ]]; then
  echo "Missing required ID map for conversion gate: $DATA_DIR/kegg_hsa_idmap.tsv" >&2
  exit 1
fi
python3 "$ROOT_DIR/scripts/alignment/check_id_conversion.py" \
  "$INPUT_CSV" "$DATA_DIR/kegg_hsa_idmap.tsv" "$ALIGN_MIN_CONV_RATE" "$OUT_DIR/conversion_check.json"

# KEGG analyses require strict SYMBOL->ENTREZ conversion for all rows.
# Filter input to rows with mappable SYMBOL to keep strict conversion mode usable.
KEGG_INPUT_CSV="$OUT_DIR/input_kegg_mappable.csv"
python3 "$ROOT_DIR/scripts/alignment/filter_input_by_idmap.py" \
  "$INPUT_CSV" "$DATA_DIR/kegg_hsa_idmap.tsv" "$KEGG_INPUT_CSV"

GO_UNIVERSE_FILE=""
if [[ "$ALIGN_USE_R_GO_UNIVERSE" == "1" ]]; then
  GO_UNIVERSE_FILE="$OUT_DIR/r_go_bp_universe.txt"
  python3 - "$DATA_DIR/go_hsa_BP.gmt" "$GO_UNIVERSE_FILE" <<'PY'
import sys
gmt, out = sys.argv[1], sys.argv[2]
genes = set()
with open(gmt, "r", encoding="utf-8") as f:
    for line in f:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 3:
            continue
        for g in parts[2:]:
            g = g.strip()
            if g:
                genes.add(g)
with open(out, "w", encoding="utf-8") as w:
    for g in sorted(genes):
        w.write(g + "\n")
print(f"[GO-UNIVERSE] exported {len(genes)} symbols to {out}")
PY
fi

GO_GMT_FILE=""
if [[ "$ALIGN_USE_R_GO_GMT" == "1" ]]; then
  GO_GMT_FILE="$DATA_DIR/go_hsa_BP.gmt"
fi

REACTOME_GMT_FILE=""
if [[ "$ALIGN_INCLUDE_REACTOME" == "1" ]]; then
  REACTOME_GMT_FILE="$DATA_DIR/reactome_hsa.gmt"
fi

MSIGDB_GMT_FILE=""
if [[ "$ALIGN_INCLUDE_MSIGDB" == "1" ]]; then
  MSIGDB_GMT_FILE="$OUT_DIR/msigdb_merged.gmt"
  python3 - "$DATA_DIR" "$ALIGN_MSIGDB_COLLECTIONS" "$MSIGDB_GMT_FILE" <<'PY'
import os
import sys

data_dir, cols_raw, out_file = sys.argv[1], sys.argv[2], sys.argv[3]
if cols_raw == "c1-c8":
    cols = [f"c{i}" for i in range(1, 9)]
elif cols_raw == "all":
    cols = ["h"] + [f"c{i}" for i in range(1, 9)]
else:
    cols = [c.strip() for c in cols_raw.split(",") if c.strip()]

paths = [os.path.join(data_dir, f"msigdb_{c}.gmt") for c in cols]
missing = [p for p in paths if not os.path.exists(p)]
if missing:
    print("[MSIGDB] missing cache files:", file=sys.stderr)
    for p in missing:
        print(f"  - {p}", file=sys.stderr)
    sys.exit(1)

seen = set()
n = 0
with open(out_file, "w", encoding="utf-8") as fw:
    for p in paths:
        with open(p, "r", encoding="utf-8") as fr:
            for line in fr:
                line = line.rstrip("\n")
                if not line:
                    continue
                term = line.split("\t", 1)[0].strip()
                if not term or term in seen:
                    continue
                seen.add(term)
                fw.write(line + "\n")
                n += 1
print(f"[MSIGDB] merged {len(paths)} files into {out_file} with {n} unique terms")
PY
fi

# 1) Generate Go outputs (normalized settings for R alignment)
echo "Running Go ORA/GSEA baselines..."
./enrichgo enrich \
  -i "$KEGG_INPUT_CSV" -d kegg -s hsa --data-dir "$DATA_DIR" \
  --fdr-col FDR --fdr-threshold 0.05 --split-by-direction=false \
  -ont BP -o "$OUT_DIR/go_ora_kegg.tsv"

if [[ -n "$GO_GMT_FILE" && -n "$GO_UNIVERSE_FILE" ]]; then
  ./enrichgo enrich \
    -i "$INPUT_CSV" -d custom -s hsa --data-dir "$DATA_DIR" \
    --fdr-col FDR --fdr-threshold 0.05 --split-by-direction=false \
    --universe-file "$GO_UNIVERSE_FILE" -gmt "$GO_GMT_FILE" -o "$OUT_DIR/go_ora_go.tsv"
elif [[ -n "$GO_GMT_FILE" ]]; then
  ./enrichgo enrich \
    -i "$INPUT_CSV" -d custom -s hsa --data-dir "$DATA_DIR" \
    --fdr-col FDR --fdr-threshold 0.05 --split-by-direction=false \
    -gmt "$GO_GMT_FILE" -o "$OUT_DIR/go_ora_go.tsv"
elif [[ -n "$GO_UNIVERSE_FILE" ]]; then
  ./enrichgo enrich \
    -i "$INPUT_CSV" -d go -s hsa --data-dir "$DATA_DIR" \
    --fdr-col FDR --fdr-threshold 0.05 --split-by-direction=false \
    -ont BP --universe-file "$GO_UNIVERSE_FILE" -o "$OUT_DIR/go_ora_go.tsv"
else
  ./enrichgo enrich \
    -i "$INPUT_CSV" -d go -s hsa --data-dir "$DATA_DIR" \
    --fdr-col FDR --fdr-threshold 0.05 --split-by-direction=false \
    -ont BP -o "$OUT_DIR/go_ora_go.tsv"
fi

if [[ "$ALIGN_ONLY_ORA" != "1" ]]; then
    ./enrichgo gsea \
      -i "$KEGG_INPUT_CSV" -d kegg -s hsa --data-dir "$DATA_DIR" \
      -rank-col logFC -seed 42 -nPerm "$ALIGN_NPERM" -ont BP \
      -pvalue-method "$ALIGN_GSEA_PVALUE_METHOD" -max-perm "$ALIGN_GSEA_MAX_PERM" \
      -o "$OUT_DIR/go_gsea_kegg.tsv"

  if [[ -n "$GO_GMT_FILE" ]]; then
    ./enrichgo gsea \
      -i "$INPUT_CSV" -d custom -gmt "$GO_GMT_FILE" -s hsa --data-dir "$DATA_DIR" \
      -rank-col logFC -seed 42 -nPerm "$ALIGN_NPERM" \
      -pvalue-method "$ALIGN_GSEA_PVALUE_METHOD" -max-perm "$ALIGN_GSEA_MAX_PERM" \
      -o "$OUT_DIR/go_gsea_go.tsv"
    if [[ "$ALIGN_DEBUG_GO_GSEA" == "1" ]]; then
      ./enrichgo gsea \
        -i "$INPUT_CSV" -d custom -gmt "$GO_GMT_FILE" -s hsa --data-dir "$DATA_DIR" \
        -rank-col logFC -seed 42 -nPerm "$ALIGN_NPERM" -padj-cutoff 1.0 \
        -pvalue-method "$ALIGN_GSEA_PVALUE_METHOD" -max-perm "$ALIGN_GSEA_MAX_PERM" \
        -debug-ranked-out "$OUT_DIR/go_gsea_go_ranked.tsv" \
        -o "$OUT_DIR/go_gsea_go_unfiltered.tsv"
    fi
  else
    ./enrichgo gsea \
      -i "$INPUT_CSV" -d go -s hsa --data-dir "$DATA_DIR" \
      -rank-col logFC -seed 42 -nPerm "$ALIGN_NPERM" -ont BP \
      -pvalue-method "$ALIGN_GSEA_PVALUE_METHOD" -max-perm "$ALIGN_GSEA_MAX_PERM" \
      -o "$OUT_DIR/go_gsea_go.tsv"
    if [[ "$ALIGN_DEBUG_GO_GSEA" == "1" ]]; then
      ./enrichgo gsea \
        -i "$INPUT_CSV" -d go -s hsa --data-dir "$DATA_DIR" \
        -rank-col logFC -seed 42 -nPerm "$ALIGN_NPERM" -ont BP -padj-cutoff 1.0 \
        -pvalue-method "$ALIGN_GSEA_PVALUE_METHOD" -max-perm "$ALIGN_GSEA_MAX_PERM" \
        -debug-ranked-out "$OUT_DIR/go_gsea_go_ranked.tsv" \
        -o "$OUT_DIR/go_gsea_go_unfiltered.tsv"
    fi
  fi
else
  printf "ID\tName\tNES\tPValue\tPAdjust\tQValue\tEnrichmentScore\tLeadGenes\tDescription\n" > "$OUT_DIR/go_gsea_kegg.tsv"
  printf "ID\tName\tNES\tPValue\tPAdjust\tQValue\tEnrichmentScore\tLeadGenes\tDescription\n" > "$OUT_DIR/go_gsea_go.tsv"
fi

if [[ "$ALIGN_INCLUDE_REACTOME" == "1" ]]; then
  ./enrichgo enrich \
    -i "$INPUT_CSV" -d custom -gmt "$REACTOME_GMT_FILE" -s hsa --data-dir "$DATA_DIR" \
    --fdr-col FDR --fdr-threshold 0.05 --split-by-direction=false \
    -o "$OUT_DIR/go_ora_reactome.tsv"
fi

if [[ "$ALIGN_INCLUDE_MSIGDB" == "1" ]]; then
  ./enrichgo enrich \
    -i "$INPUT_CSV" -d custom -gmt "$MSIGDB_GMT_FILE" -s hsa --data-dir "$DATA_DIR" \
    --fdr-col FDR --fdr-threshold 0.05 --split-by-direction=false \
    -o "$OUT_DIR/go_ora_msigdb.tsv"
fi

if [[ "$ALIGN_ONLY_ORA" != "1" ]]; then
  if [[ "$ALIGN_INCLUDE_REACTOME" == "1" ]]; then
    ./enrichgo gsea \
      -i "$INPUT_CSV" -d custom -gmt "$REACTOME_GMT_FILE" -s hsa --data-dir "$DATA_DIR" \
      -rank-col logFC -seed 42 -nPerm "$ALIGN_NPERM" \
      -pvalue-method "$ALIGN_GSEA_PVALUE_METHOD" -max-perm "$ALIGN_GSEA_MAX_PERM" \
      -o "$OUT_DIR/go_gsea_reactome.tsv"
  fi
  if [[ "$ALIGN_INCLUDE_MSIGDB" == "1" ]]; then
    ./enrichgo gsea \
      -i "$INPUT_CSV" -d custom -gmt "$MSIGDB_GMT_FILE" -s hsa --data-dir "$DATA_DIR" \
      -rank-col logFC -seed 42 -nPerm "$ALIGN_NPERM" \
      -pvalue-method "$ALIGN_GSEA_PVALUE_METHOD_MSIGDB" -max-perm "$ALIGN_GSEA_MAX_PERM" \
      -o "$OUT_DIR/go_gsea_msigdb.tsv"
  fi
else
  if [[ "$ALIGN_INCLUDE_REACTOME" == "1" ]]; then
    printf "ID\tName\tNES\tPValue\tPAdjust\tQValue\tEnrichmentScore\tLeadGenes\tDescription\n" > "$OUT_DIR/go_gsea_reactome.tsv"
  fi
  if [[ "$ALIGN_INCLUDE_MSIGDB" == "1" ]]; then
    printf "ID\tName\tNES\tPValue\tPAdjust\tQValue\tEnrichmentScore\tLeadGenes\tDescription\n" > "$OUT_DIR/go_gsea_msigdb.tsv"
  fi
fi

cat > "$OUT_DIR/go_meta.json" <<META
{
  "input": "${INPUT_CSV}",
  "data_dir": "${DATA_DIR}",
  "analysis": ["ora_kegg", "ora_go", "gsea_kegg", "gsea_go"],
  "params": {
    "species": "hsa",
    "ontology": "BP",
    "fdr_threshold": 0.05,
    "rank_col": "logFC",
    "seed": 42,
    "nPerm": ${ALIGN_NPERM}
  }
}
META

# 2) Generate R(clusterProfiler) outputs
echo "Running R clusterProfiler baselines..."
ALIGN_NPERM="$ALIGN_NPERM" \
ALIGN_ONLY_ORA="$ALIGN_ONLY_ORA" \
ALIGN_SKIP_KEGG="$ALIGN_SKIP_KEGG" \
ALIGN_R_GO_UNIVERSE_FILE="$GO_UNIVERSE_FILE" \
ALIGN_R_GO_GMT_FILE="$GO_GMT_FILE" \
ALIGN_R_KEGG_GMT_FILE="$DATA_DIR/hsa.gmt" \
ALIGN_KEGG_INPUT_CSV="$KEGG_INPUT_CSV" \
ALIGN_KEGG_IDMAP_TSV="$DATA_DIR/kegg_hsa_idmap.tsv" \
ALIGN_INCLUDE_REACTOME="$ALIGN_INCLUDE_REACTOME" \
ALIGN_INCLUDE_MSIGDB="$ALIGN_INCLUDE_MSIGDB" \
ALIGN_R_REACTOME_GMT_FILE="$REACTOME_GMT_FILE" \
ALIGN_R_MSIGDB_GMT_FILE="$MSIGDB_GMT_FILE" \
Rscript "$ROOT_DIR/scripts/alignment/clusterprofiler_baseline.R" "$INPUT_CSV" "$OUT_DIR"

# 3) Compare & score
echo "Comparing Go vs R outputs..."
python3 "$ROOT_DIR/scripts/alignment/compare_results.py" "$OUT_DIR"

if [[ "$ALIGN_ONLY_ORA" != "1" ]]; then
  DIAG_GMT_FILE="$GO_GMT_FILE"
  if [[ -z "$DIAG_GMT_FILE" ]]; then
    DIAG_GMT_FILE="$DATA_DIR/go_hsa_BP.gmt"
  fi
  if [[ -f "$OUT_DIR/go_gsea_go.tsv" && -f "$OUT_DIR/r_gsea_go.tsv" && -f "$DIAG_GMT_FILE" ]]; then
    if [[ "$ALIGN_DEBUG_GO_GSEA" == "1" && -f "$OUT_DIR/go_gsea_go_unfiltered.tsv" ]]; then
      python3 "$ROOT_DIR/scripts/alignment/diagnose_gsea_go.py" \
        "$OUT_DIR/go_gsea_go.tsv" "$OUT_DIR/r_gsea_go.tsv" "$DIAG_GMT_FILE" "$INPUT_CSV" "$OUT_DIR/go_gsea_go_unfiltered.tsv"
    else
      python3 "$ROOT_DIR/scripts/alignment/diagnose_gsea_go.py" \
        "$OUT_DIR/go_gsea_go.tsv" "$OUT_DIR/r_gsea_go.tsv" "$DIAG_GMT_FILE" "$INPUT_CSV"
    fi
  fi
fi

echo "Done. Summary: $OUT_DIR/comparison_summary.tsv"
popd >/dev/null
