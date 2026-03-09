#!/usr/bin/env python3
import json
import math
import os
import sys
from typing import Dict, List, Tuple


def read_tsv(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f if line.strip()]
    if not lines:
        return rows
    header = lines[0].split("\t")
    for line in lines[1:]:
        parts = line.split("\t")
        row = {}
        for i, col in enumerate(header):
            row[col] = parts[i] if i < len(parts) else ""
        rows.append(row)
    return rows


def to_float(s: str):
    if s is None or s == "" or s == "NA":
        return None
    try:
        return float(s)
    except Exception:
        return None


def normalize_id(pathway_id: str) -> str:
    if pathway_id.startswith("path:"):
        return pathway_id[5:]
    return pathway_id


def rank_rows(rows: List[Dict[str, str]], analysis: str) -> List[Dict[str, str]]:
    def key(row):
        padj = to_float(row.get("PAdjust", row.get("p.adjust", "")))
        pval = to_float(row.get("PValue", row.get("pvalue", "")))
        qval = to_float(row.get("QValue", row.get("qvalue", "")))
        nes = to_float(row.get("NES", ""))
        if padj is None:
            padj = math.inf
        if pval is None:
            pval = math.inf
        if qval is None:
            qval = math.inf
        if nes is None:
            nes = 0.0
        pid = normalize_id(row.get("ID", ""))
        # Keep ORA ranking stable on significance.
        if analysis != "gsea":
            return (padj, pval, qval, pid)
        # For GSEA, align with clusterProfiler reporting preference:
        # significance first, then effect size magnitude.
        return (padj, pval, -abs(nes), pid)

    return sorted(rows, key=key)


def topk_overlap(go_rows: List[Dict[str, str]], r_rows: List[Dict[str, str]], k: int) -> float:
    go_top = [normalize_id(r.get("ID", "")) for r in go_rows[:k]]
    r_top = [normalize_id(r.get("ID", "")) for r in r_rows[:k]]
    if not go_top and not r_top:
        return 1.0
    denom = max(1, min(k, max(len(go_top), len(r_top))))
    inter = len(set(go_top).intersection(set(r_top)))
    return inter / float(denom)


def significant_set(rows: List[Dict[str, str]]) -> set:
    sig = set()
    for row in rows:
        qv = to_float(row.get("QValue", row.get("qvalue", "")))
        padj = to_float(row.get("PAdjust", row.get("p.adjust", "")))
        val = qv if qv is not None else padj
        if val is not None and val <= 0.05:
            sig.add(normalize_id(row.get("ID", "")))
    return sig


def median(xs: List[float]):
    if not xs:
        return None
    xs2 = sorted(xs)
    n = len(xs2)
    if n % 2 == 1:
        return xs2[n // 2]
    return (xs2[n // 2 - 1] + xs2[n // 2]) / 2.0


def compare_pair(go_path: str, r_path: str, analysis: str, db: str) -> Dict:
    go_rows_raw = read_tsv(go_path)
    r_rows_raw = read_tsv(r_path)
    go_rows = rank_rows(go_rows_raw, analysis)
    r_rows = rank_rows(r_rows_raw, analysis)

    go_by_id = {normalize_id(r.get("ID", "")): r for r in go_rows}
    r_by_id = {normalize_id(r.get("ID", "")): r for r in r_rows}
    shared_ids = sorted(set(go_by_id.keys()).intersection(set(r_by_id.keys())))

    rel_errs = []
    nes_errs = []
    for sid in shared_ids:
        g = go_by_id[sid]
        rr = r_by_id[sid]
        g_p = to_float(g.get("PAdjust", g.get("p.adjust", "")))
        r_p = to_float(rr.get("PAdjust", rr.get("p.adjust", "")))
        if g_p is not None and r_p is not None:
            rel_errs.append(abs(g_p - r_p) / max(abs(r_p), 1e-12))
        if analysis == "gsea":
            g_nes = to_float(g.get("NES", ""))
            r_nes = to_float(rr.get("NES", ""))
            if g_nes is not None and r_nes is not None:
                nes_errs.append(abs(g_nes - r_nes))

    go_sig = significant_set(go_rows)
    r_sig = significant_set(r_rows)
    union = go_sig.union(r_sig)
    inter = go_sig.intersection(r_sig)
    jacc = 1.0 if not union else len(inter) / float(len(union))

    return {
        "analysis": analysis,
        "db": db,
        "go_rows": len(go_rows),
        "r_rows": len(r_rows),
        "shared_rows": len(shared_ids),
        "top10_overlap": topk_overlap(go_rows, r_rows, 10),
        "top20_overlap": topk_overlap(go_rows, r_rows, 20),
        "sig_jaccard": jacc,
        "padjust_rel_err_median": median(rel_errs),
        "nes_abs_err_median": median(nes_errs),
    }


def main():
    if len(sys.argv) != 2:
        print("Usage: compare_results.py <output_dir>")
        sys.exit(1)

    out_dir = sys.argv[1]
    pairs: List[Tuple[str, str, str, str]] = [
        ("go_ora_kegg.tsv", "r_ora_kegg.tsv", "ora", "kegg"),
        ("go_ora_go.tsv", "r_ora_go.tsv", "ora", "go"),
        ("go_gsea_kegg.tsv", "r_gsea_kegg.tsv", "gsea", "kegg"),
        ("go_gsea_go.tsv", "r_gsea_go.tsv", "gsea", "go"),
    ]
    optional_pairs: List[Tuple[str, str, str, str]] = [
        ("go_ora_reactome.tsv", "r_ora_reactome.tsv", "ora", "reactome"),
        ("go_gsea_reactome.tsv", "r_gsea_reactome.tsv", "gsea", "reactome"),
        ("go_ora_msigdb.tsv", "r_ora_msigdb.tsv", "ora", "msigdb"),
        ("go_gsea_msigdb.tsv", "r_gsea_msigdb.tsv", "gsea", "msigdb"),
    ]
    for go_file, r_file, analysis, db in optional_pairs:
        if os.path.exists(os.path.join(out_dir, go_file)) or os.path.exists(os.path.join(out_dir, r_file)):
            pairs.append((go_file, r_file, analysis, db))

    summary = []
    for go_file, r_file, analysis, db in pairs:
        summary.append(compare_pair(
            os.path.join(out_dir, go_file),
            os.path.join(out_dir, r_file),
            analysis,
            db,
        ))

    lines = ["analysis\tdb\tgo_rows\tr_rows\tshared_rows\ttop10_overlap\ttop20_overlap\tsig_jaccard\tpadjust_rel_err_median\tnes_abs_err_median"]
    for s in summary:
        lines.append(
            "{analysis}\t{db}\t{go_rows}\t{r_rows}\t{shared_rows}\t{top10_overlap:.4f}\t{top20_overlap:.4f}\t{sig_jaccard:.4f}\t{padjust_rel_err_median}\t{nes_abs_err_median}".format(
                analysis=s["analysis"],
                db=s["db"],
                go_rows=s["go_rows"],
                r_rows=s["r_rows"],
                shared_rows=s["shared_rows"],
                top10_overlap=s["top10_overlap"],
                top20_overlap=s["top20_overlap"],
                sig_jaccard=s["sig_jaccard"],
                padjust_rel_err_median="" if s["padjust_rel_err_median"] is None else f"{s['padjust_rel_err_median']:.6f}",
                nes_abs_err_median="" if s["nes_abs_err_median"] is None else f"{s['nes_abs_err_median']:.6f}",
            )
        )

    with open(os.path.join(out_dir, "comparison_summary.tsv"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    with open(os.path.join(out_dir, "comparison_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


if __name__ == "__main__":
    main()
