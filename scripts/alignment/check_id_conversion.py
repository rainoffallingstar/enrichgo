#!/usr/bin/env python3
import csv
import json
import os
import sys


def usage():
    print("Usage: check_id_conversion.py <input_csv> <idmap_tsv> <threshold> <output_json>")
    sys.exit(1)


def load_deg(path):
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError("empty csv header")
        gene_col = reader.fieldnames[0] if reader.fieldnames[0] else ""
        rows = []
        for r in reader:
            gene = (r.get(gene_col, "") or "").strip()
            if not gene:
                continue
            rows.append(r)
        return reader.fieldnames, rows


def load_idmap(path):
    sym2entrez = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            entrez = parts[0].strip()
            symbol = parts[-1].strip().upper()
            if symbol and symbol not in sym2entrez:
                sym2entrez[symbol] = entrez
    return sym2entrez


def mapped_rate(genes, sym2entrez):
    uniq = sorted(set(g for g in genes if g))
    if not uniq:
        return 0.0, 0, 0
    mapped = sum(1 for g in uniq if g.upper() in sym2entrez)
    return mapped / float(len(uniq)), mapped, len(uniq)


def main():
    if len(sys.argv) != 5:
        usage()

    input_csv = sys.argv[1]
    idmap_tsv = sys.argv[2]
    threshold = float(sys.argv[3])
    out_json = sys.argv[4]

    headers, rows = load_deg(input_csv)
    gene_col = headers[0] if headers and headers[0] else ""
    fdr_col = "FDR" if "FDR" in headers else None

    all_genes = [(r.get(gene_col, "") or "").strip() for r in rows]
    sig_genes = all_genes
    if fdr_col:
        sig_genes = []
        for r in rows:
            try:
                fdr = float((r.get(fdr_col, "") or "").strip())
            except Exception:
                continue
            if fdr <= 0.05:
                g = (r.get(gene_col, "") or "").strip()
                if g:
                    sig_genes.append(g)

    sym2entrez = load_idmap(idmap_tsv)
    all_rate, all_mapped, all_total = mapped_rate(all_genes, sym2entrez)
    sig_rate, sig_mapped, sig_total = mapped_rate(sig_genes, sym2entrez)

    result = {
        "input_csv": os.path.abspath(input_csv),
        "idmap_tsv": os.path.abspath(idmap_tsv),
        "threshold": threshold,
        "all": {
            "mapped": all_mapped,
            "total": all_total,
            "rate": all_rate,
        },
        "significant": {
            "mapped": sig_mapped,
            "total": sig_total,
            "rate": sig_rate,
        },
        "pass": sig_rate >= threshold,
    }

    os.makedirs(os.path.dirname(out_json), exist_ok=True)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"[ID-CHECK] all: {all_mapped}/{all_total} ({all_rate:.4f})")
    print(f"[ID-CHECK] significant: {sig_mapped}/{sig_total} ({sig_rate:.4f})")
    if not result["pass"]:
        print(f"[ID-CHECK] FAIL: significant mapping rate {sig_rate:.4f} < threshold {threshold:.4f}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
