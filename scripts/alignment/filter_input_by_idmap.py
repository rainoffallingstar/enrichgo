#!/usr/bin/env python3
import csv
import sys


def usage():
    print("Usage: filter_input_by_idmap.py <input_csv> <idmap_tsv> <output_csv>")
    sys.exit(1)


def load_symbols(idmap_tsv):
    syms = set()
    with open(idmap_tsv, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            sym = parts[-1].strip().upper()
            if sym:
                syms.add(sym)
    return syms


def main():
    if len(sys.argv) != 4:
        usage()
    input_csv, idmap_tsv, output_csv = sys.argv[1], sys.argv[2], sys.argv[3]

    syms = load_symbols(idmap_tsv)

    with open(input_csv, "r", encoding="utf-8") as fr:
        reader = csv.DictReader(fr)
        if not reader.fieldnames:
            raise RuntimeError("empty csv")
        gene_col = reader.fieldnames[0]
        if gene_col == "":
            gene_col = reader.fieldnames[0]

        rows = list(reader)

    kept = []
    for r in rows:
        gene = (r.get(gene_col, "") or "").strip().upper()
        if gene in syms:
            kept.append(r)

    with open(output_csv, "w", encoding="utf-8", newline="") as fw:
        writer = csv.DictWriter(fw, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(kept)

    print(f"[FILTER] kept {len(kept)}/{len(rows)} rows into {output_csv}")


if __name__ == "__main__":
    main()
