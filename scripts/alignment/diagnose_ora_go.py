#!/usr/bin/env python3
import csv
import json
import os
import sys


def read_tsv(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter='\t')
        for r in reader:
            rows.append(r)
    return rows


def norm_id(s):
    if s.startswith("path:"):
        return s[5:]
    return s


def parse_ratio(s):
    if not s or '/' not in s:
        return (None, None)
    a, b = s.split('/', 1)
    try:
        return int(a), int(b)
    except Exception:
        return (None, None)


def get_float(x, key):
    v = x.get(key, "")
    try:
        return float(v)
    except Exception:
        return None


def main():
    if len(sys.argv) != 4:
        print("Usage: diagnose_ora_go.py <go_ora_go.tsv> <r_ora_go.tsv> <out_json>")
        sys.exit(1)

    go_path, r_path, out_json = sys.argv[1], sys.argv[2], sys.argv[3]
    go_rows = read_tsv(go_path)
    r_rows = read_tsv(r_path)

    go_by = {norm_id(r.get("ID", "")): r for r in go_rows}
    r_by = {norm_id(r.get("ID", "")): r for r in r_rows}

    shared = sorted(set(go_by.keys()) & set(r_by.keys()))

    detail = []
    for tid in shared:
        g = go_by[tid]
        r = r_by[tid]
        gk, gn = parse_ratio(g.get("GeneRatio", ""))
        rk, rn = parse_ratio(r.get("GeneRatio", ""))
        gK, gN = parse_ratio(g.get("BgRatio", ""))
        rK, rN = parse_ratio(r.get("BgRatio", ""))
        detail.append({
            "ID": tid,
            "go_desc": g.get("Description", g.get("Name", "")),
            "r_desc": r.get("Description", ""),
            "go_k": gk, "go_n": gn, "go_K": gK, "go_N": gN,
            "r_k": rk, "r_n": rn, "r_K": rK, "r_N": rN,
            "go_padj": get_float(g, "PAdjust") if "PAdjust" in g else get_float(g, "p.adjust"),
            "r_padj": get_float(r, "p.adjust") if "p.adjust" in r else get_float(r, "PAdjust"),
        })

    # Identify first-order bias points
    n_go = sorted({d["go_n"] for d in detail if d["go_n"] is not None})
    n_r = sorted({d["r_n"] for d in detail if d["r_n"] is not None})
    N_go = sorted({d["go_N"] for d in detail if d["go_N"] is not None})
    N_r = sorted({d["r_N"] for d in detail if d["r_N"] is not None})

    bias = []
    if n_go != n_r:
        bias.append({
            "type": "input_gene_count_n_mismatch",
            "go_n_values": n_go,
            "r_n_values": n_r,
            "note": "ORA 输入基因数 n 不一致，会直接影响超几何检验。"
        })
    if N_go != N_r:
        bias.append({
            "type": "universe_size_N_mismatch",
            "go_N_values": N_go,
            "r_N_values": N_r,
            "note": "背景基因数 N 不一致，说明 universe 口径不同。"
        })

    # p.adjust deviation summary
    rel_errs = []
    for d in detail:
        gp = d["go_padj"]
        rp = d["r_padj"]
        if gp is None or rp is None:
            continue
        rel = abs(gp - rp) / max(abs(rp), 1e-12)
        rel_errs.append((rel, d["ID"], gp, rp))
    rel_errs.sort(reverse=True)

    result = {
        "go_rows": len(go_rows),
        "r_rows": len(r_rows),
        "shared_rows": len(shared),
        "bias_points": bias,
        "top_padjust_relative_errors": [
            {"ID": x[1], "relative_error": x[0], "go_padj": x[2], "r_padj": x[3]}
            for x in rel_errs[:10]
        ],
        "shared_detail": detail,
    }

    os.makedirs(os.path.dirname(out_json), exist_ok=True)
    with open(out_json, "w", encoding="utf-8") as out_f:
        json.dump(result, out_f, indent=2)

    print(f"[DIAG] go_rows={len(go_rows)} r_rows={len(r_rows)} shared={len(shared)}")
    for b in bias:
        print(f"[DIAG] bias={b['type']}")


if __name__ == "__main__":
    main()
