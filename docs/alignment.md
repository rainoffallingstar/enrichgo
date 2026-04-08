# 与 clusterProfiler 对齐验证（KEGG+GO, ORA+GSEA）

## 目标

用同一份 DEG 输入，分别跑：

- Go CLI (`enrichgo`) 的 ORA/GSEA
- R `clusterProfiler` 的 ORA/GSEA

并自动生成对齐报告，评估以下指标：

- `top10_overlap`
- `top20_overlap`
- `sig_jaccard`（显著结果集合 Jaccard）
- `padjust_rel_err_median`
- `nes_abs_err_median`（仅 GSEA）

## 运行方式

```bash
./scripts/alignment/run_alignment.sh \
  test-data/DE_results.csv \
  artifacts/alignment \
  data
```

可选环境变量：

- `ALIGN_NPERM`：覆盖 GSEA 置换次数（默认 `1000`；本地快速验收可设 `100`）
- `ALIGN_GSEA_PVALUE_METHOD`：Go 侧 GSEA p 值估计方法（`simple` 或 `adaptive`，默认 `simple`）
- `ALIGN_GSEA_PVALUE_METHOD_MSIGDB`：MSigDB GSEA 的 p 值估计方法（默认继承 `ALIGN_GSEA_PVALUE_METHOD`）
- `ALIGN_GSEA_MAX_PERM`：`adaptive` 方法下单通路最大置换数（默认 `20000`）
- `ALIGN_SKIP_DOWNLOAD=1`：跳过数据库下载，要求 `data/` 已有 `hsa.gmt` 与 `go_hsa_BP.gmt`
- `ALIGN_MIN_CONV_RATE`：SYMBOL->ENTREZ 显著基因映射率门槛（默认 `0.90`）
- `ALIGN_DEBUG_GO_GSEA=1`：额外输出 GO GSEA 排障工件（`go_gsea_go_unfiltered.tsv`、`go_gsea_go_ranked.tsv`，并生成 `gsea_go_diagnose.{json,tsv}`）
- `ALIGN_SKIP_KEGG=1`：跳过 KEGG（默认 `0`，建议保持关闭以产出 KEGG 对齐结果）
- `ALIGN_INCLUDE_REACTOME=1`：启用 Reactome ORA/GSEA 对齐（默认 `0`）
- `ALIGN_INCLUDE_MSIGDB=1`：启用 MSigDB ORA/GSEA 对齐（默认 `0`）
- `ALIGN_MSIGDB_COLLECTIONS`：MSigDB 集合，默认 `c1-c8`，支持如 `h` 或 `c2,c5`

参数：

1. 输入 DEG 表（默认 `test-data/DE_results.csv`）
2. 输出目录（默认 `artifacts/alignment`）
3. 数据缓存目录（默认 `data`）

## 输出文件

- Go 结果
  - `go_ora_kegg.tsv`
  - `go_ora_go.tsv`
  - `go_gsea_kegg.tsv`
  - `go_gsea_go.tsv`
- R 结果
  - `r_ora_kegg.tsv`
  - `r_ora_go.tsv`
  - `r_gsea_kegg.tsv`
  - `r_gsea_go.tsv`
  - （可选）`r_ora_reactome.tsv` / `r_gsea_reactome.tsv`
  - （可选）`r_ora_msigdb.tsv` / `r_gsea_msigdb.tsv`
- 报告
  - `comparison_summary.tsv`
  - `comparison_summary.json`
  - `gsea_go_diagnose.tsv`
  - `gsea_go_diagnose.json`
- 元数据
  - `go_meta.json`
  - `r_meta.json`

## 关键对齐设置

- 物种：`hsa`
- GO 本体：`BP`
- ORA 显著基因过滤：`FDR <= 0.05`
- GSEA 排名列：`logFC`
- `minGSSize=10`, `maxGSSize=500`
- `pAdjustMethod=BH`
- `seed=42`, `nPerm=1000`
- 前置门禁：`conversion_check.json` 中显著基因映射率需 `>= ALIGN_MIN_CONV_RATE`
- KEGG 对齐时，Go 侧会先生成 `input_kegg_mappable.csv`（仅保留可 SYMBOL->ENTREZ 映射的行）再执行 ORA/GSEA，避免严格转换模式下因少量不可映射基因提前失败。
- R 侧 KEGG 基线优先使用离线 `hsa.gmt` 与 `kegg_hsa_idmap.tsv`（由 `run_alignment.sh` 传入），提高可复现性并减少在线依赖。
- 若启用 Reactome/MSigDB，对齐同样走离线 GMT（Reactome: `reactome_hsa.gmt`；MSigDB: `msigdb_*.gmt` 合并后）。

## 回归门禁

门禁脚本：

```bash
python3 scripts/alignment/check_alignment_thresholds.py \
  /path/to/comparison_summary.tsv
```

一键执行 smoke+formal（含门禁）：

```bash
./scripts/alignment/run_alignment_ci.sh /tmp/alignment_ci data test-data/DE_results.csv
```

多数据集批量执行（目录下所有 `*.csv`）：

```bash
python3 scripts/alignment/run_alignment_batch.py \
  /abs/path/to/datasets \
  --out-base /tmp/alignment_batch \
  --data-dir data \
  --continue-on-fail
```

说明：`run_alignment_batch.py` 当前默认 `--formal-nperm 1000`，并在 formal 阶段默认使用 `--formal-gsea-pvalue-method adaptive`、`--formal-gsea-pvalue-method-msigdb simple`（与当前推荐口径一致）。

批量结果汇总：

- `/tmp/alignment_batch/batch_summary.tsv`
- `/tmp/alignment_batch/batch_summary.json`

默认阈值分层：

- `L1`（必过，稳定性）：
  - `ora/kegg`、`gsea/kegg`、`gsea/go` 要求 `go_rows/r_rows/shared_rows > 0`
  - `gsea/kegg`、`gsea/go` 要求 `nes_abs_err_median <= 0.02`
  - 若 summary 存在可选库：
    - `gsea/reactome` 要求 `nes_abs_err_median <= 0.05`
    - `gsea/msigdb` 要求 `sig_jaccard >= 0.75` 且 `nes_abs_err_median <= 0.05`
- `L2`（排序质量，可选）：
  - `top20_overlap` 阈值：`kegg>=0.80`、`go>=0.60`、`reactome>=0.40`、`msigdb>=0.10`
  - 默认仅告警；传 `--enforce-l2 1` 时转为失败

可通过参数覆盖阈值：

```bash
python3 scripts/alignment/check_alignment_thresholds.py \
  /path/to/comparison_summary.tsv \
  --kegg-gsea-top20-min 0.75 \
  --msigdb-gsea-sig-jaccard-min 0.80 \
  --enforce-l2 1
```

## 当前基线（2026-03-09）

正式推荐口径（2026-03-08）：`ALIGN_NPERM=1000`、`ALIGN_GSEA_PVALUE_METHOD=adaptive`、`ALIGN_GSEA_PVALUE_METHOD_MSIGDB=simple`、`ALIGN_GSEA_MAX_PERM=10000`。

参考结果（`/tmp/alignment_latest_universe_fix/comparison_summary.tsv`）：

- `ora/kegg`: `go_rows=15`, `r_rows=11`, `shared_rows=11`, `top20_overlap=0.7333`
- `ora/go`: `go_rows=34`, `r_rows=34`, `shared_rows=34`, `top20_overlap=1.0000`
- `ora/reactome`: `go_rows=9`, `r_rows=8`, `shared_rows=8`, `top20_overlap=0.8889`
- `ora/msigdb`: `go_rows=363`, `r_rows=371`, `shared_rows=362`, `top20_overlap=0.9000`
- `gsea/kegg`: `go_rows=31`, `r_rows=33`, `shared_rows=30`, `top20_overlap=0.9000`, `nes_abs_err_median=0.017134`
- `gsea/go`: `go_rows=45`, `r_rows=41`, `shared_rows=40`, `top20_overlap=0.8000`, `nes_abs_err_median=0.022662`
- `gsea/reactome`: `go_rows=57`, `r_rows=54`, `shared_rows=53`, `top20_overlap=0.8000`, `nes_abs_err_median=0.015152`
- `gsea/msigdb`: `go_rows=1209`, `r_rows=1331`, `shared_rows=1191`, `top20_overlap=0.1500`, `nes_abs_err_median=0.011145`

当前门禁状态（`scripts/alignment/check_alignment_thresholds.py` 默认阈值）：

- 未通过：`gsea/go nes_abs_err_median=0.022662 > 0.020000`
- 当前发布收口不以此项为阻塞；优先保证默认 SQLite / CLI 交付链路稳定，后续继续收敛算法对齐。

## 依赖

R 侧需要安装：

- `clusterProfiler`
- `org.Hs.eg.db`
- `jsonlite`

若缺依赖，可在 R 中安装：

```r
if (!requireNamespace("BiocManager", quietly = TRUE)) install.packages("BiocManager")
BiocManager::install(c("clusterProfiler", "org.Hs.eg.db"))
install.packages("jsonlite")
```

## 常见不一致原因

- KEGG/GO 数据版本不一致（下载时间不同）
- ID 映射命中率差异（SYMBOL -> ENTREZ）
- GSEA 统计口径差异（不同实现细节导致 NES/p 值偏差）
- 过滤阈值或排序规则不一致

## GO GSEA 排障建议

- 优先看 `comparison_summary.tsv` 的 `gsea/go` 行，确认 `top20_overlap` 与 `nes_abs_err_median`。
- 再看 `gsea_go_diagnose.json`：
  - `top20_go_only_reason_counts` / `top20_r_only_reason_counts`：Top20 不一致的主因分类
  - `reason_counts`：全量不一致主因
  - `shared_padj_rel_err_median` 与 `shared_nes_abs_err_median`：共享 term 的数值偏差
- 如需深入排查排序口径，开启 `ALIGN_DEBUG_GO_GSEA=1`，对照：
  - `go_gsea_go_ranked.tsv`（Go 侧实际使用的排名输入）
  - `go_gsea_go_unfiltered.tsv`（不过滤版结果）
