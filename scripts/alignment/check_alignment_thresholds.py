#!/usr/bin/env python3
import argparse
import csv
import sys
from typing import Dict, Tuple


def to_float(v: str):
    if v is None or v == "" or v == "NA":
        return None
    try:
        return float(v)
    except Exception:
        return None


def read_summary(path: str) -> Dict[Tuple[str, str], Dict[str, str]]:
    out: Dict[Tuple[str, str], Dict[str, str]] = {}
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            key = (row.get("analysis", ""), row.get("db", ""))
            out[key] = row
    return out


def require_row(rows: Dict[Tuple[str, str], Dict[str, str]], analysis: str, db: str) -> Dict[str, str]:
    key = (analysis, db)
    if key not in rows:
        raise ValueError(f"missing row: analysis={analysis}, db={db}")
    return rows[key]


def assert_non_empty(row: Dict[str, str], analysis: str, db: str) -> None:
    go_rows = int(row.get("go_rows", "0") or "0")
    r_rows = int(row.get("r_rows", "0") or "0")
    shared_rows = int(row.get("shared_rows", "0") or "0")
    if go_rows <= 0 or r_rows <= 0 or shared_rows <= 0:
        raise ValueError(
            f"{analysis}/{db} non-empty check failed: go_rows={go_rows}, r_rows={r_rows}, shared_rows={shared_rows}"
        )


def assert_threshold(row: Dict[str, str], metric: str, min_val=None, max_val=None, label="") -> None:
    val = to_float(row.get(metric, ""))
    if val is None:
        raise ValueError(f"{label} metric missing: {metric}")
    if min_val is not None and val < min_val:
        raise ValueError(f"{label} {metric}={val:.6f} < {min_val:.6f}")
    if max_val is not None and val > max_val:
        raise ValueError(f"{label} {metric}={val:.6f} > {max_val:.6f}")


def main() -> int:
    p = argparse.ArgumentParser(description="Gate alignment metrics from comparison_summary.tsv")
    p.add_argument("summary_tsv", help="Path to comparison_summary.tsv")
    # L1: core alignment stability
    p.add_argument("--kegg-gsea-shared-min", type=int, default=1)
    p.add_argument("--kegg-gsea-sig-jaccard-min", type=float, default=0.0)
    p.add_argument("--kegg-gsea-nes-max", type=float, default=0.02)
    p.add_argument("--go-gsea-shared-min", type=int, default=1)
    p.add_argument("--go-gsea-sig-jaccard-min", type=float, default=0.0)
    p.add_argument("--go-gsea-nes-max", type=float, default=0.02)
    p.add_argument("--reactome-gsea-shared-min", type=int, default=1)
    p.add_argument("--reactome-gsea-sig-jaccard-min", type=float, default=0.0)
    p.add_argument("--reactome-gsea-nes-max", type=float, default=0.05)
    p.add_argument("--msigdb-gsea-shared-min", type=int, default=1)
    p.add_argument("--msigdb-gsea-sig-jaccard-min", type=float, default=0.75)
    p.add_argument("--msigdb-gsea-nes-max", type=float, default=0.05)
    # L2: ranking quality (optional strictness)
    p.add_argument("--kegg-gsea-top20-min", type=float, default=0.80)
    p.add_argument("--go-gsea-top20-min", type=float, default=0.60)
    p.add_argument("--reactome-gsea-top20-min", type=float, default=0.40)
    p.add_argument("--msigdb-gsea-top20-min", type=float, default=0.10)
    p.add_argument("--enforce-l2", type=int, default=0, choices=[0, 1], help="Whether to fail on L2(top20) checks.")
    args = p.parse_args()

    rows = read_summary(args.summary_tsv)

    ora_kegg = require_row(rows, "ora", "kegg")
    gsea_kegg = require_row(rows, "gsea", "kegg")
    gsea_go = require_row(rows, "gsea", "go")

    assert_non_empty(ora_kegg, "ora", "kegg")
    assert_non_empty(gsea_kegg, "gsea", "kegg")
    assert_non_empty(gsea_go, "gsea", "go")

    # L1: required
    assert_threshold(gsea_kegg, "shared_rows", min_val=args.kegg_gsea_shared_min, label="gsea/kegg")
    assert_threshold(gsea_kegg, "sig_jaccard", min_val=args.kegg_gsea_sig_jaccard_min, label="gsea/kegg")
    assert_threshold(gsea_kegg, "nes_abs_err_median", max_val=args.kegg_gsea_nes_max, label="gsea/kegg")
    assert_threshold(gsea_go, "shared_rows", min_val=args.go_gsea_shared_min, label="gsea/go")
    assert_threshold(gsea_go, "sig_jaccard", min_val=args.go_gsea_sig_jaccard_min, label="gsea/go")
    assert_threshold(gsea_go, "nes_abs_err_median", max_val=args.go_gsea_nes_max, label="gsea/go")

    if ("gsea", "reactome") in rows:
        gsea_reactome = require_row(rows, "gsea", "reactome")
        assert_non_empty(gsea_reactome, "gsea", "reactome")
        assert_threshold(gsea_reactome, "shared_rows", min_val=args.reactome_gsea_shared_min, label="gsea/reactome")
        assert_threshold(gsea_reactome, "sig_jaccard", min_val=args.reactome_gsea_sig_jaccard_min, label="gsea/reactome")
        assert_threshold(gsea_reactome, "nes_abs_err_median", max_val=args.reactome_gsea_nes_max, label="gsea/reactome")

    if ("gsea", "msigdb") in rows:
        gsea_msigdb = require_row(rows, "gsea", "msigdb")
        assert_non_empty(gsea_msigdb, "gsea", "msigdb")
        assert_threshold(gsea_msigdb, "shared_rows", min_val=args.msigdb_gsea_shared_min, label="gsea/msigdb")
        assert_threshold(gsea_msigdb, "sig_jaccard", min_val=args.msigdb_gsea_sig_jaccard_min, label="gsea/msigdb")
        assert_threshold(gsea_msigdb, "nes_abs_err_median", max_val=args.msigdb_gsea_nes_max, label="gsea/msigdb")

    # L2: optional
    l2_failures = []
    try:
        assert_threshold(gsea_kegg, "top20_overlap", min_val=args.kegg_gsea_top20_min, label="gsea/kegg")
    except Exception as e:
        l2_failures.append(str(e))
    try:
        assert_threshold(gsea_go, "top20_overlap", min_val=args.go_gsea_top20_min, label="gsea/go")
    except Exception as e:
        l2_failures.append(str(e))
    if ("gsea", "reactome") in rows:
        try:
            assert_threshold(require_row(rows, "gsea", "reactome"), "top20_overlap", min_val=args.reactome_gsea_top20_min, label="gsea/reactome")
        except Exception as e:
            l2_failures.append(str(e))
    if ("gsea", "msigdb") in rows:
        try:
            assert_threshold(require_row(rows, "gsea", "msigdb"), "top20_overlap", min_val=args.msigdb_gsea_top20_min, label="gsea/msigdb")
        except Exception as e:
            l2_failures.append(str(e))

    if l2_failures and args.enforce_l2 == 1:
        raise ValueError("L2 threshold failure(s): " + " | ".join(l2_failures))
    if l2_failures:
        print("[WARN] L2 threshold warning(s):")
        for item in l2_failures:
            print(f"[WARN] {item}")

    print("[PASS] alignment thresholds satisfied")
    print(f"[PASS] source={args.summary_tsv}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"[FAIL] {e}", file=sys.stderr)
        raise SystemExit(1)
