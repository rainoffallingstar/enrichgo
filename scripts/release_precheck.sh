#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

INPUT_CSV="${RELEASE_PRECHECK_INPUT:-$ROOT_DIR/test-data/DE_results.csv}"
DB_PATH="${RELEASE_PRECHECK_DB:-/tmp/enrichgo_release.db}"
OUT_DIR="${RELEASE_PRECHECK_OUT:-/tmp/enrichgo_release}"
NPERM="${RELEASE_PRECHECK_NPERM:-200}"
SKIP_SYNC="${RELEASE_PRECHECK_SKIP_SYNC:-0}"
IDMAPS_LEVEL="${RELEASE_PRECHECK_IDMAPS_LEVEL:-basic}"
IDMAPS_RESUME="${RELEASE_PRECHECK_IDMAPS_RESUME:-1}"
IDMAPS_FORCE_REFRESH="${RELEASE_PRECHECK_IDMAPS_FORCE_REFRESH:-0}"
EMBED_SPECIES="${RELEASE_PRECHECK_EMBED_SPECIES:-hsa}"
EMBED_IDMAPS_LEVEL="${RELEASE_PRECHECK_EMBED_IDMAPS_LEVEL:-basic}"
EMBED_GO_ONTOLOGY="${RELEASE_PRECHECK_EMBED_GO_ONTOLOGY:-BP}"
EMBED_CONTRACT_PROFILE="${RELEASE_PRECHECK_EMBED_CONTRACT_PROFILE:-embedded-${EMBED_SPECIES}-${EMBED_IDMAPS_LEVEL}}"

if [[ "$IDMAPS_LEVEL" != "basic" && "$IDMAPS_LEVEL" != "extended" ]]; then
  echo "[ERROR] RELEASE_PRECHECK_IDMAPS_LEVEL must be basic or extended (got: $IDMAPS_LEVEL)" >&2
  exit 1
fi
if [[ "$IDMAPS_RESUME" != "0" && "$IDMAPS_RESUME" != "1" ]]; then
  echo "[ERROR] RELEASE_PRECHECK_IDMAPS_RESUME must be 0 or 1 (got: $IDMAPS_RESUME)" >&2
  exit 1
fi
if [[ "$IDMAPS_FORCE_REFRESH" != "0" && "$IDMAPS_FORCE_REFRESH" != "1" ]]; then
  echo "[ERROR] RELEASE_PRECHECK_IDMAPS_FORCE_REFRESH must be 0 or 1 (got: $IDMAPS_FORCE_REFRESH)" >&2
  exit 1
fi

if [[ ! -f "$INPUT_CSV" ]]; then
  echo "[ERROR] input file not found: $INPUT_CSV" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

echo "[PRECHECK] root=$ROOT_DIR"
echo "[PRECHECK] input=$INPUT_CSV"
echo "[PRECHECK] db=$DB_PATH"
echo "[PRECHECK] out=$OUT_DIR"
echo "[PRECHECK] nperm=$NPERM skip_sync=$SKIP_SYNC"
echo "[PRECHECK] idmaps_level=$IDMAPS_LEVEL resume=$IDMAPS_RESUME force_refresh=$IDMAPS_FORCE_REFRESH"
echo "[PRECHECK] embedded_species=$EMBED_SPECIES embedded_idmaps_level=$EMBED_IDMAPS_LEVEL embedded_go_ontology=$EMBED_GO_ONTOLOGY"

echo "[STEP] go test ./..."
go test ./...

echo "[STEP] rebuild embedded DB asset from local data"
go run ./tools/build_embedded_db \
  --db assets/default_enrichgo.db \
  --manifest assets/default_enrichgo.db.manifest.json \
  --artifact assets/default_enrichgo.db \
  --data-dir data \
  --species "$EMBED_SPECIES" \
  --contract-profile "$EMBED_CONTRACT_PROFILE" \
  --idmaps-level "$EMBED_IDMAPS_LEVEL" \
  --go-ontology "$EMBED_GO_ONTOLOGY"

echo "[STEP] build enrichgo binary"
go build -o ./enrichgo .

echo "[STEP] verify top-level CLI surface"
help_text="$(./enrichgo help)"
for cmd in "analyze ora" "analyze gsea" "data sync" "db audit" "bench run"; do
  if ! printf '%s\n' "$help_text" | grep -Fq "$cmd"; then
    echo "[ERROR] missing command in help: $cmd" >&2
    exit 1
  fi
done

echo "[STEP] embedded manifest audit"
./enrichgo db audit --db assets/default_enrichgo.db --expect-embedded-manifest --strict-contract=true

echo "[STEP] embedded default-path smoke"
EMBEDDED_RUNTIME_DB="$OUT_DIR/embedded_runtime.db"
rm -f "$EMBEDDED_RUNTIME_DB" "$EMBEDDED_RUNTIME_DB.embed.sha256"
env ENRICHGO_DEFAULT_DB_PATH="$EMBEDDED_RUNTIME_DB" ./enrichgo analyze ora  -i "$INPUT_CSV" -d kegg -s hsa --split-by-direction=false --auto-update-db=false -o "$OUT_DIR/embedded_ora_kegg.tsv"
env ENRICHGO_DEFAULT_DB_PATH="$EMBEDDED_RUNTIME_DB" ./enrichgo analyze gsea -i "$INPUT_CSV" -d go   -s hsa --auto-update-db=false -nPerm "$NPERM" -o "$OUT_DIR/embedded_gsea_go.tsv"

if [[ "$SKIP_SYNC" != "1" ]]; then
  echo "[STEP] sync full release DB (all/hsa, idmaps=$IDMAPS_LEVEL)"
  if [[ -f "$DB_PATH" ]]; then
    echo "[STEP] remove stale DB before sync: $DB_PATH"
    rm -f "$DB_PATH"
  fi
  idmaps_args=(--idmaps --idmaps-level "$IDMAPS_LEVEL")
  if [[ "$IDMAPS_LEVEL" == "extended" ]]; then
    if [[ "$IDMAPS_RESUME" == "1" ]]; then
      idmaps_args+=(--idmaps-resume=true)
    else
      idmaps_args+=(--idmaps-resume=false)
    fi
    if [[ "$IDMAPS_FORCE_REFRESH" == "1" ]]; then
      idmaps_args+=(--idmaps-force-refresh)
    fi
  fi
  ./enrichgo data sync -d all -s hsa -ont ALL -c all --db "$DB_PATH" --db-only "${idmaps_args[@]}"
else
  echo "[STEP] skip db sync (RELEASE_PRECHECK_SKIP_SYNC=1)"
  if [[ ! -s "$DB_PATH" ]]; then
    echo "[ERROR] db not found or empty while sync skipped: $DB_PATH" >&2
    exit 1
  fi
fi

echo "[STEP] run default-mode matrix"
./enrichgo analyze ora  -i "$INPUT_CSV" -d kegg     -s hsa --db "$DB_PATH" -o "$OUT_DIR/ora_kegg.tsv"
./enrichgo analyze gsea -i "$INPUT_CSV" -d go       -s hsa --db "$DB_PATH" -o "$OUT_DIR/gsea_go.tsv" -nPerm "$NPERM"
./enrichgo analyze ora  -i "$INPUT_CSV" -d reactome -s hsa --db "$DB_PATH" -o "$OUT_DIR/ora_reactome.tsv"
./enrichgo analyze gsea -i "$INPUT_CSV" -d msigdb   -s hsa -c c2 --db "$DB_PATH" -o "$OUT_DIR/gsea_msigdb.tsv" -nPerm "$NPERM"

echo "[STEP] run strict-mode matrix"
./enrichgo analyze ora  -i "$INPUT_CSV" -d go   -s hsa --db "$DB_PATH" --strict-mode -o "$OUT_DIR/ora_go_strict.tsv"
./enrichgo analyze gsea -i "$INPUT_CSV" -d kegg -s hsa --db "$DB_PATH" --strict-mode -o "$OUT_DIR/gsea_kegg_strict.tsv" -nPerm "$NPERM"

echo "[STEP] validate output files"
for f in \
  "$OUT_DIR/embedded_ora_kegg.tsv" \
  "$OUT_DIR/embedded_gsea_go.tsv" \
  "$OUT_DIR/ora_kegg.tsv" \
  "$OUT_DIR/gsea_go.tsv" \
  "$OUT_DIR/ora_reactome.tsv" \
  "$OUT_DIR/gsea_msigdb.tsv" \
  "$OUT_DIR/ora_go_strict.tsv" \
  "$OUT_DIR/gsea_kegg_strict.tsv"; do
  if [[ ! -s "$f" ]]; then
    echo "[ERROR] output file missing or empty: $f" >&2
    exit 1
  fi
  first_line="$(head -n 1 "$f" || true)"
  if [[ -z "${first_line//[[:space:]]/}" ]]; then
    echo "[ERROR] output header is empty: $f" >&2
    exit 1
  fi
  echo "[OK] $f"
done

echo "[DONE] RELEASE PRECHECK PASSED"
