#!/usr/bin/env python3
import argparse
import csv
import json
import os
import subprocess
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional


@dataclass
class DatasetResult:
    dataset: str
    input_csv: str
    smoke_dir: str
    formal_dir: str
    smoke_ok: bool
    formal_ok: bool
    smoke_error: str
    formal_error: str
    smoke_summary: str
    formal_summary: str


def run_cmd(cmd: List[str], env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def gate(
    root: Path,
    summary_tsv: Path,
    kegg_top20_min: float,
    go_top20_min: float,
    enforce_l2: int,
) -> subprocess.CompletedProcess:
    cmd = [
        "python3",
        str(root / "scripts/alignment/check_alignment_thresholds.py"),
        str(summary_tsv),
        "--kegg-gsea-top20-min",
        str(kegg_top20_min),
        "--go-gsea-top20-min",
        str(go_top20_min),
        "--enforce-l2",
        str(enforce_l2),
    ]
    return run_cmd(cmd, os.environ.copy())


def run_alignment(
    root: Path,
    input_csv: Path,
    out_dir: Path,
    data_dir: Path,
    nperm: int,
    skip_download: int,
    skip_kegg: int,
    gsea_pvalue_method: str,
    gsea_pvalue_method_msigdb: str,
    gsea_max_perm: int,
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["ALIGN_SKIP_DOWNLOAD"] = str(skip_download)
    env["ALIGN_SKIP_KEGG"] = str(skip_kegg)
    env["ALIGN_DEBUG_GO_GSEA"] = "0"
    env["ALIGN_NPERM"] = str(nperm)
    env["ALIGN_GSEA_PVALUE_METHOD"] = gsea_pvalue_method
    env["ALIGN_GSEA_PVALUE_METHOD_MSIGDB"] = gsea_pvalue_method_msigdb
    env["ALIGN_GSEA_MAX_PERM"] = str(gsea_max_perm)
    cmd = [
        str(root / "scripts/alignment/run_alignment.sh"),
        str(input_csv),
        str(out_dir),
        str(data_dir),
    ]
    return run_cmd(cmd, env)


def write_results(out_base: Path, results: List[DatasetResult]) -> None:
    json_path = out_base / "batch_summary.json"
    tsv_path = out_base / "batch_summary.tsv"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in results], f, indent=2)

    fieldnames = list(asdict(results[0]).keys()) if results else [
        "dataset",
        "input_csv",
        "smoke_dir",
        "formal_dir",
        "smoke_ok",
        "formal_ok",
        "smoke_error",
        "formal_error",
        "smoke_summary",
        "formal_summary",
    ]
    with open(tsv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        w.writeheader()
        for r in results:
            w.writerow(asdict(r))


def collect_inputs(input_dir: Path, pattern: str) -> List[Path]:
    return sorted([p for p in input_dir.glob(pattern) if p.is_file()])


def short_name(path: Path) -> str:
    return path.stem.replace(" ", "_")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run alignment (smoke+formal) for multiple datasets.")
    parser.add_argument("input_dir", help="Directory containing dataset CSV files.")
    parser.add_argument("--pattern", default="*.csv", help="Glob pattern for dataset files. Default: *.csv")
    parser.add_argument("--out-base", default="/tmp/alignment_batch", help="Base output directory.")
    parser.add_argument("--data-dir", default="data", help="Data cache directory.")
    parser.add_argument("--smoke-nperm", type=int, default=100)
    parser.add_argument("--formal-nperm", type=int, default=1000)
    parser.add_argument("--skip-download", type=int, default=1, choices=[0, 1])
    parser.add_argument("--skip-kegg", type=int, default=0, choices=[0, 1])
    parser.add_argument("--smoke-gsea-pvalue-method", default="simple", choices=["simple", "adaptive"])
    parser.add_argument("--formal-gsea-pvalue-method", default="adaptive", choices=["simple", "adaptive"])
    parser.add_argument("--formal-gsea-pvalue-method-msigdb", default="simple", choices=["simple", "adaptive"])
    parser.add_argument("--formal-gsea-max-perm", type=int, default=10000)
    parser.add_argument("--smoke-kegg-top20-min", type=float, default=0.50)
    parser.add_argument("--smoke-go-top20-min", type=float, default=0.40)
    parser.add_argument("--formal-kegg-top20-min", type=float, default=0.80)
    parser.add_argument("--formal-go-top20-min", type=float, default=0.60)
    parser.add_argument("--smoke-enforce-l2", type=int, default=0, choices=[0, 1])
    parser.add_argument("--formal-enforce-l2", type=int, default=1, choices=[0, 1])
    parser.add_argument("--continue-on-fail", action="store_true", help="Continue with next dataset if one fails.")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[2]
    input_dir = Path(args.input_dir).resolve()
    out_base = Path(args.out_base).resolve()
    data_dir = Path(args.data_dir).resolve()

    ensure_dir(out_base)
    if not input_dir.exists():
        print(f"[FAIL] input_dir not found: {input_dir}", file=sys.stderr)
        return 1
    if not data_dir.exists():
        print(f"[FAIL] data_dir not found: {data_dir}", file=sys.stderr)
        return 1

    inputs = collect_inputs(input_dir, args.pattern)
    if not inputs:
        print(f"[FAIL] no files matched pattern '{args.pattern}' under {input_dir}", file=sys.stderr)
        return 1

    print(f"[BATCH] datasets={len(inputs)} input_dir={input_dir} out_base={out_base}")
    results: List[DatasetResult] = []
    any_fail = False

    for input_csv in inputs:
        dataset = short_name(input_csv)
        ds_dir = out_base / dataset
        smoke_dir = ds_dir / "smoke"
        formal_dir = ds_dir / "formal"
        ensure_dir(smoke_dir)
        ensure_dir(formal_dir)

        print(f"[BATCH] dataset={dataset} smoke nPerm={args.smoke_nperm}")
        smoke_run = run_alignment(
            root, input_csv, smoke_dir, data_dir, args.smoke_nperm, args.skip_download, args.skip_kegg,
            args.smoke_gsea_pvalue_method, args.formal_gsea_pvalue_method_msigdb, args.formal_gsea_max_perm,
        )
        smoke_summary = smoke_dir / "comparison_summary.tsv"
        smoke_gate_ok = False
        smoke_err = ""
        if smoke_run.returncode != 0:
            smoke_err = smoke_run.stdout[-4000:]
        elif not smoke_summary.exists():
            smoke_err = f"missing smoke summary: {smoke_summary}"
        else:
            g = gate(
                root,
                smoke_summary,
                args.smoke_kegg_top20_min,
                args.smoke_go_top20_min,
                args.smoke_enforce_l2,
            )
            smoke_gate_ok = g.returncode == 0
            if not smoke_gate_ok:
                smoke_err = g.stdout[-4000:]

        formal_gate_ok = False
        formal_err = ""
        formal_summary = formal_dir / "comparison_summary.tsv"
        if smoke_gate_ok:
            print(f"[BATCH] dataset={dataset} formal nPerm={args.formal_nperm}")
            formal_run = run_alignment(
                root, input_csv, formal_dir, data_dir, args.formal_nperm, args.skip_download, args.skip_kegg,
                args.formal_gsea_pvalue_method, args.formal_gsea_pvalue_method_msigdb, args.formal_gsea_max_perm,
            )
            if formal_run.returncode != 0:
                formal_err = formal_run.stdout[-4000:]
            elif not formal_summary.exists():
                formal_err = f"missing formal summary: {formal_summary}"
            else:
                g2 = gate(
                    root,
                    formal_summary,
                    args.formal_kegg_top20_min,
                    args.formal_go_top20_min,
                    args.formal_enforce_l2,
                )
                formal_gate_ok = g2.returncode == 0
                if not formal_gate_ok:
                    formal_err = g2.stdout[-4000:]
        else:
            formal_err = "skipped: smoke gate failed"

        row = DatasetResult(
            dataset=dataset,
            input_csv=str(input_csv),
            smoke_dir=str(smoke_dir),
            formal_dir=str(formal_dir),
            smoke_ok=smoke_gate_ok,
            formal_ok=formal_gate_ok,
            smoke_error=smoke_err.strip(),
            formal_error=formal_err.strip(),
            smoke_summary=str(smoke_summary),
            formal_summary=str(formal_summary),
        )
        results.append(row)

        if not smoke_gate_ok or not formal_gate_ok:
            any_fail = True
            print(f"[BATCH][FAIL] dataset={dataset} smoke_ok={smoke_gate_ok} formal_ok={formal_gate_ok}")
            if not args.continue_on_fail:
                write_results(out_base, results)
                print(f"[BATCH] wrote {out_base / 'batch_summary.tsv'}")
                print(f"[BATCH] wrote {out_base / 'batch_summary.json'}")
                return 1
        else:
            print(f"[BATCH][PASS] dataset={dataset}")

    write_results(out_base, results)
    print(f"[BATCH] wrote {out_base / 'batch_summary.tsv'}")
    print(f"[BATCH] wrote {out_base / 'batch_summary.json'}")
    return 1 if any_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
