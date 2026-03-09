# enrichgo

`enrichgo` 是一个用于 ORA/GSEA 的 Go CLI，并提供与 `clusterProfiler` 的对齐脚本。

详细对齐说明见：[docs/alignment.md](/home/fallingstar10/clusterProfiler/enrichgo/docs/alignment.md)

## 目录规范

- Go 源码：仓库根目录（`main.go`、`cmd_*.go`）与 `pkg/`
- 对齐与诊断脚本：`scripts/alignment/`
- 文档：`docs/`
- 测试数据与离线缓存：`test-data/`、`data/`
- 示例输入/输出：`examples/input/`、`examples/output/`
- 参考实现：`refer-code/`（本地保留，已加入 `.gitignore`）

## 默认参数

- GSEA 默认置换次数：`nPerm=1000`
  - CLI: `./enrichgo gsea ...`（未显式传 `-nPerm` 时默认 1000）
  - 对齐脚本: `scripts/alignment/run_alignment.sh` 默认 `ALIGN_NPERM=1000`

## Go/R 运行模式

- 默认：使用 Go 实现（`enrichgo enrich` / `enrichgo gsea`）。
- `--use-r`：改为调用 R 基线脚本（`clusterProfiler`）输出对应结果。
- `--benchmark`：同一次命令同时运行 Go 和 R 两侧，并输出 benchmark 报告（时间+内存）。
  - 可用 `--benchmark-out` 指定 benchmark 报告路径（TSV）。
  - R 侧依赖检查会在运行前执行；若系统没有 `Rscript` 或缺少 `clusterProfiler` / `org.Hs.eg.db` / `jsonlite`，命令会直接报错退出。

## 性能对比（优化前 vs 优化后）

以下为同一机器、同一数据、同一参数口径下的 Go 侧 GSEA 耗时对比（2026-03-08）：

- 优化前基线：`/tmp/perf_cmp/metrics.tsv`
- 优化后基线：`/tmp/perf_cmp/metrics_go_opt3.tsv`

| 数据库 | 优化前(s) | 优化后(s) | 加速比 |
|---|---:|---:|---:|
| KEGG | 103.751 | 1.799 | 57.67x |
| GO | 284.530 | 3.927 | 72.45x |
| Reactome | 423.450 | 6.560 | 64.55x |
| MSigDB | 461.643 | 11.898 | 38.80x |

说明：

- 提速主要来自三点：并行 worker、低分配采样器、ES 计算从 `O(N)` 改为按命中位点 `O(k)`。
- 内存峰值与优化前接近（仅小幅变化），速度提升显著。

## 最新 Go vs R 对齐与性能（对齐：2026-03-09；性能：2026-03-08）

口径：

- 数据集：`test-data/DE_results.csv`
- `nPerm=1000`
- Go: `pvalue-method=adaptive`（MSigDB 使用 `simple`）
- 覆盖库：KEGG + GO + Reactome + MSigDB(c7)

产物：

- 对齐汇总：`/tmp/alignment_latest_universe_fix/comparison_summary.tsv`
- 性能汇总：`/tmp/perf_cmp/metrics_latest.tsv`
- 合并总表：`/tmp/perf_cmp/latest_alignment_perf_compare.tsv`

### GSEA 对齐（Go vs R）

| DB | go_rows | r_rows | shared | shared/go | shared/r | sig_jaccard | top20_overlap | nes_abs_err_median |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| kegg | 31 | 33 | 30 | 0.968 | 0.909 | 0.8824 | 0.90 | 0.017134 |
| go | 45 | 41 | 40 | 0.889 | 0.976 | 0.8696 | 0.80 | 0.022662 |
| reactome | 57 | 54 | 53 | 0.930 | 0.981 | 0.9138 | 0.80 | 0.015152 |
| msigdb(c7) | 1209 | 1331 | 1191 | 0.985 | 0.895 | 0.8829 | 0.15 | 0.011145 |

结论：四库共享集合都较高；MSigDB 主要差异仍体现在排序（top20 overlap 低），不是检出覆盖。当前 L1 门禁剩余项为 `gsea/go` 的 `nes_abs_err_median=0.022662`（阈值 0.02）。

### 运行性能（最新 Go vs R）

| DB | Go时长(s) | R时长(s) | Go/R时长 | Go峰值RSS(KB) | R峰值RSS(KB) | Go/R内存 |
|---|---:|---:|---:|---:|---:|---:|
| kegg | 1.994 | 10.299 | 0.194x | 23200 | 531792 | 0.044x |
| go | 6.283 | 12.696 | 0.495x | 40712 | 574968 | 0.071x |
| reactome | 7.431 | 14.642 | 0.508x | 40744 | 593452 | 0.069x |
| msigdb | 13.453 | 49.327 | 0.273x | 128744 | 1061608 | 0.121x |

结论：最新版本下，Go 在四库均快于 R（约 2x~5x）且内存显著更低。

## 最新 ORA 对齐与性能（对齐：2026-03-09；性能：2026-03-08）

口径：

- 数据集：`test-data/DE_results.csv`
- 显著基因过滤：`FDR <= 0.05`
- 覆盖库：KEGG + GO + Reactome + MSigDB(c7)

产物：

- ORA 对齐汇总：`/tmp/alignment_latest_universe_fix/comparison_summary.tsv`
- ORA 性能汇总：`/tmp/perf_cmp/metrics_ora_latest.tsv`
- ORA 合并总表：`/tmp/perf_cmp/latest_ora_alignment_perf_compare.tsv`

### ORA 对齐（Go vs R）

| DB | go_rows | r_rows | shared | shared/go | shared/r | sig_jaccard | top20_overlap |
|---|---:|---:|---:|---:|---:|---:|---:|
| kegg | 15 | 11 | 11 | 0.733 | 1.000 | 0.7333 | 0.7333 |
| go | 34 | 34 | 34 | 1.000 | 1.000 | 1.0000 | 1.0000 |
| reactome | 9 | 8 | 8 | 0.889 | 1.000 | 0.8889 | 0.8889 |
| msigdb(c7) | 363 | 371 | 362 | 0.997 | 0.976 | 0.9731 | 0.9000 |

结论：GO 对齐最佳；`universe` 口径修复后，Reactome/MSigDB 的 ORA 覆盖差异明显收敛。

### ORA 运行性能（Go vs R）

| DB | Go时长(s) | R时长(s) | Go/R时长 | Go峰值RSS(KB) | R峰值RSS(KB) | Go/R内存 |
|---|---:|---:|---:|---:|---:|---:|
| kegg | 0.654 | 6.787 | 0.096x | 14964 | 534540 | 0.028x |
| go | 0.223 | 7.157 | 0.031x | 2432 | 553016 | 0.004x |
| reactome | 0.225 | 7.562 | 0.030x | 2304 | 565948 | 0.004x |
| msigdb | 0.436 | 18.803 | 0.023x | 56704 | 977840 | 0.058x |

结论：ORA 场景下 Go 在四库均显著快于 R，且内存占用显著更低。
