#!/usr/bin/env python3
import json
import os
import sys


def load_json(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_tsv_rows(path):
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        lines = [x.rstrip("\n") for x in f if x.strip()]
    if len(lines) <= 1:
        return rows
    header = lines[0].split("\t")
    for line in lines[1:]:
        parts = line.split("\t")
        row = {header[i]: parts[i] if i < len(parts) else "" for i in range(len(header))}
        rows.append(row)
    return rows


def fmt_pct(x):
    try:
        return f"{float(x)*100:.2f}%"
    except Exception:
        return "-"


def md_table(rows):
    if not rows:
        return "（无数据）"
    hdr = ["analysis", "db", "go_rows", "r_rows", "shared_rows", "top10_overlap", "top20_overlap", "sig_jaccard", "padjust_rel_err_median", "nes_abs_err_median"]
    out = []
    out.append("| 分析 | 数据库 | Go行数 | R行数 | 共享ID | Top10重叠 | Top20重叠 | 显著集Jaccard | p.adjust中位相对误差 | NES中位绝对误差 |")
    out.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        out.append(
            f"| {r.get('analysis','')} | {r.get('db','')} | {r.get('go_rows','')} | {r.get('r_rows','')} | {r.get('shared_rows','')} | {r.get('top10_overlap','')} | {r.get('top20_overlap','')} | {r.get('sig_jaccard','')} | {r.get('padjust_rel_err_median','')} | {r.get('nes_abs_err_median','')} |"
        )
    return "\n".join(out)


def main():
    if len(sys.argv) != 4:
        print("Usage: generate_alignment_report.py <smoke_dir> <formal_dir> <out_md>")
        sys.exit(1)

    smoke_dir, formal_dir, out_md = sys.argv[1], sys.argv[2], sys.argv[3]

    smoke_check = load_json(os.path.join(smoke_dir, "conversion_check.json"))
    formal_check = load_json(os.path.join(formal_dir, "conversion_check.json"))

    smoke_rows = load_tsv_rows(os.path.join(smoke_dir, "comparison_summary.tsv"))
    formal_rows = load_tsv_rows(os.path.join(formal_dir, "comparison_summary.tsv"))

    lines = []
    lines.append("# 对齐实验报告（V2）")
    lines.append("")
    lines.append("## 一、前置转换检查")
    lines.append("")
    if smoke_check:
        lines.append(f"- 烟测显著基因映射率：{smoke_check['significant']['mapped']}/{smoke_check['significant']['total']} ({fmt_pct(smoke_check['significant']['rate'])})，阈值 {fmt_pct(smoke_check['threshold'])}，结果：{'通过' if smoke_check['pass'] else '失败'}")
    if formal_check:
        lines.append(f"- 正式显著基因映射率：{formal_check['significant']['mapped']}/{formal_check['significant']['total']} ({fmt_pct(formal_check['significant']['rate'])})，阈值 {fmt_pct(formal_check['threshold'])}，结果：{'通过' if formal_check['pass'] else '失败'}")
    if not smoke_check and not formal_check:
        lines.append("- 未找到 conversion_check.json")

    lines.append("")
    lines.append("## 二、烟测结果（nPerm=100）")
    lines.append("")
    lines.append(md_table(smoke_rows))

    lines.append("")
    lines.append("## 三、正式结果（nPerm=1000）")
    lines.append("")
    lines.append(md_table(formal_rows))

    lines.append("")
    lines.append("## 四、结论")
    lines.append("")
    if formal_rows:
        lines.append("- 以正式结果为准。若 TopK 重叠与误差指标未达标，则判定当前版本与 clusterProfiler 未对齐。")
    else:
        lines.append("- 正式结果尚未完整产出，请先完成 formal 运行。")

    os.makedirs(os.path.dirname(out_md), exist_ok=True)
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
