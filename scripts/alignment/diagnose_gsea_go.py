#!/usr/bin/env python3
import csv
import json
import math
import os
import sys
from typing import Dict, List, Optional, Set, Tuple


def read_tsv(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            rows.append(row)
    return rows


def read_ranked_genes(path: str) -> List[str]:
    genes: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return genes
        gene_col = reader.fieldnames[0]
        for c in reader.fieldnames:
            if c.strip().lower() == "gene":
                gene_col = c
                break
        for row in reader:
            g = (row.get(gene_col) or "").strip()
            if g:
                genes.append(g)
    return genes


def read_gmt(path: str) -> Dict[str, Set[str]]:
    term2genes: Dict[str, Set[str]] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 3:
                continue
            tid = parts[0].strip()
            genes = {g.strip() for g in parts[2:] if g.strip()}
            if tid:
                term2genes[tid] = genes
    return term2genes


def to_float(v: Optional[str]) -> Optional[float]:
    if v is None:
        return None
    s = v.strip()
    if s == "" or s == "NA":
        return None
    try:
        return float(s)
    except Exception:
        return None


def norm_id(v: str) -> str:
    if v.startswith("path:"):
        return v[5:]
    return v


def build_index(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        tid = norm_id(r.get("ID", ""))
        if tid:
            out[tid] = r
    return out


def rank_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    def key(row: Dict[str, str]):
        padj = to_float(row.get("PAdjust", row.get("p.adjust", "")))
        pval = to_float(row.get("PValue", row.get("pvalue", "")))
        tid = norm_id(row.get("ID", ""))
        if padj is None:
            padj = math.inf
        if pval is None:
            pval = math.inf
        return (padj, pval, tid)

    return sorted(rows, key=key)


def topk_ids(rows: List[Dict[str, str]], k: int) -> List[str]:
    return [norm_id(r.get("ID", "")) for r in rows[:k] if r.get("ID", "").strip()]


def topk_overlap(go_rows: List[Dict[str, str]], r_rows: List[Dict[str, str]], k: int) -> float:
    go_top = topk_ids(go_rows, k)
    r_top = topk_ids(r_rows, k)
    if not go_top and not r_top:
        return 1.0
    denom = max(1, min(k, max(len(go_top), len(r_top))))
    inter = len(set(go_top).intersection(set(r_top)))
    return inter / float(denom)


def median(xs: List[float]) -> Optional[float]:
    if not xs:
        return None
    ys = sorted(xs)
    n = len(ys)
    if n % 2 == 1:
        return ys[n // 2]
    return 0.5 * (ys[n // 2 - 1] + ys[n // 2])


def reason_label(
    in_go: bool,
    in_r: bool,
    overlap_size: int,
    min_size: int,
    max_size: int,
    in_go_unfiltered: bool,
    go_unfiltered_padj: Optional[float],
) -> str:
    if overlap_size < min_size:
        return "filtered_by_minGSSize"
    if overlap_size > max_size:
        return "filtered_by_maxGSSize"
    if in_go and in_r:
        return "shared"
    if in_r and not in_go:
        if in_go_unfiltered:
            if go_unfiltered_padj is not None and go_unfiltered_padj > 0.05:
                return "filtered_by_go_padj_cutoff"
            return "go_unfiltered_present_but_filtered_or_sorted_out"
        return "missing_in_go_unfiltered"
    if in_go and not in_r:
        return "r_not_significant_or_ranking_diff"
    return "absent_both"


def main() -> None:
    if len(sys.argv) not in (5, 6):
        print(
            "Usage: diagnose_gsea_go.py <go_gsea_go.tsv> <r_gsea_go.tsv> <go_bp_gmt> <ranked_input_csv> [go_gsea_go_unfiltered.tsv]"
        )
        sys.exit(1)

    go_path, r_path, gmt_path, rank_path = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    go_unfiltered_path = sys.argv[5] if len(sys.argv) == 6 else ""
    out_dir = os.path.dirname(go_path) or "."
    out_json = os.path.join(out_dir, "gsea_go_diagnose.json")
    out_tsv = os.path.join(out_dir, "gsea_go_diagnose.tsv")

    min_size = 10
    max_size = 500

    go_rows = read_tsv(go_path)
    r_rows = read_tsv(r_path)
    go_unfiltered_rows = read_tsv(go_unfiltered_path) if go_unfiltered_path else []
    term2genes = read_gmt(gmt_path)
    ranked_genes = read_ranked_genes(rank_path)
    ranked_set = set(ranked_genes)

    go_rows_ranked = rank_rows(go_rows)
    r_rows_ranked = rank_rows(r_rows)
    go_by = build_index(go_rows_ranked)
    r_by = build_index(r_rows_ranked)
    go_unfiltered_by = build_index(go_unfiltered_rows)

    all_ids = sorted(set(go_by.keys()) | set(r_by.keys()))
    shared = sorted(set(go_by.keys()) & set(r_by.keys()))
    only_go = sorted(set(go_by.keys()) - set(r_by.keys()))
    only_r = sorted(set(r_by.keys()) - set(go_by.keys()))

    details: List[Dict[str, object]] = []
    shared_nes_diff: List[float] = []
    shared_padj_rel_diff: List[float] = []

    for tid in all_ids:
        genes = term2genes.get(tid, set())
        overlap_size = len(genes & ranked_set)

        g = go_by.get(tid, {})
        r = r_by.get(tid, {})
        in_go = tid in go_by
        in_r = tid in r_by
        in_go_unfiltered = tid in go_unfiltered_by

        go_nes = to_float(g.get("NES"))
        r_nes = to_float(r.get("NES"))
        go_padj = to_float(g.get("PAdjust"))
        r_padj = to_float(r.get("p.adjust"))
        go_p = to_float(g.get("PValue"))
        r_p = to_float(r.get("pvalue"))
        gu = go_unfiltered_by.get(tid, {})
        gu_padj = to_float(gu.get("PAdjust"))

        if in_go and in_r and go_nes is not None and r_nes is not None:
            shared_nes_diff.append(abs(go_nes - r_nes))
        if in_go and in_r and go_padj is not None and r_padj is not None:
            shared_padj_rel_diff.append(abs(go_padj - r_padj) / max(abs(r_padj), 1e-12))

        details.append(
            {
                "ID": tid,
                "category": "shared" if in_go and in_r else ("go_only" if in_go else ("r_only" if in_r else "none")),
                "reason": reason_label(
                    in_go,
                    in_r,
                    overlap_size,
                    min_size,
                    max_size,
                    in_go_unfiltered,
                    gu_padj,
                ),
                "set_size_total": len(genes),
                "set_size_overlap_ranked": overlap_size,
                "go_nes": go_nes,
                "r_nes": r_nes,
                "go_pvalue": go_p,
                "r_pvalue": r_p,
                "go_padj": go_padj,
                "r_padj": r_padj,
                "go_unfiltered_padj": gu_padj,
                "nes_abs_diff": None if go_nes is None or r_nes is None else abs(go_nes - r_nes),
                "padj_rel_diff": None if go_padj is None or r_padj is None else abs(go_padj - r_padj) / max(abs(r_padj), 1e-12),
            }
        )

    details.sort(
        key=lambda x: (
            0 if x["category"] == "r_only" else (1 if x["category"] == "go_only" else 2),
            x["ID"],
        )
    )

    reason_counts: Dict[str, int] = {}
    for d in details:
        reason = str(d["reason"])
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

    go_top20 = set(topk_ids(go_rows_ranked, 20))
    r_top20 = set(topk_ids(r_rows_ranked, 20))
    top20_go_only = go_top20 - r_top20
    top20_r_only = r_top20 - go_top20
    top20_reason_counts_go_only: Dict[str, int] = {}
    top20_reason_counts_r_only: Dict[str, int] = {}
    for d in details:
        tid = str(d["ID"])
        reason = str(d["reason"])
        if tid in top20_go_only:
            top20_reason_counts_go_only[reason] = top20_reason_counts_go_only.get(reason, 0) + 1
        if tid in top20_r_only:
            top20_reason_counts_r_only[reason] = top20_reason_counts_r_only.get(reason, 0) + 1

    result = {
        "go_rows": len(go_rows_ranked),
        "r_rows": len(r_rows_ranked),
        "shared_rows": len(shared),
        "go_only_rows": len(only_go),
        "r_only_rows": len(only_r),
        "go_unfiltered_rows": len(go_unfiltered_rows),
        "ranked_gene_rows": len(ranked_genes),
        "ranked_gene_unique": len(ranked_set),
        "minGSSize": min_size,
        "maxGSSize": max_size,
        "reason_counts": reason_counts,
        "shared_nes_abs_err_median": median(shared_nes_diff),
        "shared_padj_rel_err_median": median(shared_padj_rel_diff),
        "top10_overlap": topk_overlap(go_rows_ranked, r_rows_ranked, 10),
        "top20_overlap": topk_overlap(go_rows_ranked, r_rows_ranked, 20),
        "top20_go_ids": sorted(go_top20),
        "top20_r_ids": sorted(r_top20),
        "top20_go_only_reason_counts": top20_reason_counts_go_only,
        "top20_r_only_reason_counts": top20_reason_counts_r_only,
        "top_r_only": only_r[:30],
        "top_go_only": only_go[:30],
        "details": details,
    }

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    fieldnames = [
        "ID",
        "category",
        "reason",
        "set_size_total",
        "set_size_overlap_ranked",
        "go_nes",
        "r_nes",
        "go_pvalue",
        "r_pvalue",
        "go_padj",
        "r_padj",
        "go_unfiltered_padj",
        "nes_abs_diff",
        "padj_rel_diff",
    ]
    with open(out_tsv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        w.writeheader()
        for d in details:
            w.writerow(d)

    print(
        f"[DIAG] go_rows={len(go_rows)} r_rows={len(r_rows)} shared={len(shared)} "
        f"go_only={len(only_go)} r_only={len(only_r)}"
    )
    print(f"[DIAG] wrote {out_json}")
    print(f"[DIAG] wrote {out_tsv}")


if __name__ == "__main__":
    main()
