# enrichgo

`enrichgo` 是一个用于 ORA / GSEA 的 Go 命令行工具，并内置与 R `clusterProfiler` 的对齐与性能对比能力。

- 核心文档：[对齐流程与门禁说明](docs/alignment.md)
- 代码结构与算法设计：[docs/README.md](docs/README.md)

## 功能概览

- ORA：过度富集分析（`enrich`）
- GSEA：基因集富集分析（`gsea`）
- 数据库缓存：KEGG / GO / Reactome / MSigDB（`download`）
- 双实现模式：
  - 默认 Go 实现
  - `--use-r` 调用 R 基线（`clusterProfiler`）
  - `--benchmark` 同时跑 Go+R 并输出时间/内存报告

## 快速开始

### 1) 构建

```bash
go build -o enrichgo .
```

### 2) 下载离线数据库缓存

```bash
./enrichgo data sync -d kegg -s hsa -o data/
./enrichgo data sync -d go -s hsa -ont BP -o data/
```

也可以把“通路数据库 + ID 映射”打包到单个 SQLite 文件（便于离线分发与复用）：

```bash
# 打包所有通路库（KEGG/GO/Reactome/MSigDB）+ 离线 ID 映射到一个文件
./enrichgo data sync -d all -s hsa -ont ALL -c all --db data/enrichgo.db --db-only --idmaps --idmaps-level extended
```

说明：
- `--idmaps-level basic`：只用 KEGG list/link（更快、更小，但覆盖可能不如 extended）
- `--idmaps-level extended`：使用 NCBI + UniProt 官方映射（更全，但耗时/体积更大）
- `--idmaps-resume`：默认 `true`，extended 模式下按 source scope 跳过已完成回填，支持断点续跑
- `--idmaps-force-refresh`：默认 `false`，开启后忽略 resume，强制全量重拉并重写各个 idmap scope
- `--idmaps-local-dir`：basic 在线失败时，本地 `kegg_<species>_idmap.tsv` 兜底目录（默认 `data`）

### 3) 运行 ORA / GSEA

```bash
# ORA（Go 默认实现）
./enrichgo analyze ora \
  -i test-data/DE_results.csv \
  -d go -s hsa \
  --fdr-col FDR --fdr-threshold 0.05 --split-by-direction=false \
  -o /tmp/ora_go.tsv

# GSEA（Go 默认实现）
./enrichgo analyze gsea \
  -i test-data/DE_results.csv \
  -d kegg -s hsa \
  -rank-col logFC -nPerm 1000 \
  -o /tmp/gsea_kegg.tsv
```

默认发布二进制内置一个 SQLite 默认库（CI 构建 profile: `species=hsa`, `idmaps_level=basic`）。
不传 `--db` 时会自动使用内置库落盘副本（可用 `ENRICHGO_DEFAULT_DB_PATH` 指定默认落盘路径）。

内置库同时携带 `assets/default_enrichgo.db.manifest.json` 元数据（包含 `sha256` 和 `contract_profile`）；运行时会先校验 manifest 与嵌入 DB 哈希一致性。

```bash
# 直接使用内置 SQLite（无需 --db）
./enrichgo analyze ora -i test-data/DE_results.csv -d kegg -s hsa -o /tmp/ora_kegg.tsv
./enrichgo analyze gsea   -i test-data/DE_results.csv -d go   -s hsa -o /tmp/gsea_go.tsv
```

如果要显式指定 DB 文件，继续使用 `--db`：

```bash
./enrichgo analyze ora -i test-data/DE_results.csv -d kegg -s hsa --db data/enrichgo.db -o /tmp/ora_kegg.tsv
./enrichgo analyze gsea   -i test-data/DE_results.csv -d go   -s hsa --db data/enrichgo.db -o /tmp/gsea_go.tsv
```

如需在分析前刷新 SQLite 数据，可开启更新：

```bash
./enrichgo analyze ora -i test-data/DE_results.csv -d kegg -s hsa --update-db -o /tmp/ora_kegg.tsv
./enrichgo analyze gsea -i test-data/DE_results.csv -d go -s hsa --update-db --update-db-idmaps=true --update-db-idmaps-level basic -o /tmp/gsea_go.tsv
```

说明：
- 默认会自动检测运行时 SQLite 覆盖是否不足并自动扩容（`--auto-update-db=true`）
- 可通过 `--auto-update-db=false` 显式关闭自动扩容
- `--update-db` 不支持 `-d custom`
- `--update-db` 成功后，该 DB 会标记为用户管理，后续不会被内置默认库自动覆盖

## Go / R 运行模式

### 默认 Go 实现

```bash
./enrichgo analyze gsea -i test-data/DE_results.csv -d go -s hsa -rank-col logFC -o /tmp/go.tsv
```

### 使用 R 基线实现（clusterProfiler）

```bash
./enrichgo analyze gsea -i test-data/DE_results.csv -d go -s hsa -rank-col logFC --use-r -o /tmp/r.tsv
```

### 同时运行 Go + R 并输出 benchmark

```bash
./enrichgo analyze gsea \
  -i test-data/DE_results.csv \
  -d kegg -s hsa \
  -rank-col logFC -nPerm 100 \
  --benchmark \
  --benchmark-out /tmp/gsea_benchmark.tsv \
  -o /tmp/gsea_go.tsv
```

benchmark 输出示例列：

- `impl`：`go` / `r`
- `seconds`：运行时长（秒）
- `max_rss_kb`：峰值内存（KB）
- `output`：对应结果文件路径

### rs-reborn 依赖检查（自动）

在 `--use-r` 或 `--benchmark` 模式下，程序会先检查：

- `rvx`（来自 rs-reborn） 是否可用（可用环境变量 `ENRICHGO_RS_BIN` 指定二进制名/路径）

R 包安装与脚本依赖解析由 `rs-reborn` 负责（按脚本依赖自动管理）。缺失时会直接报错退出（fail-fast）。

## 常用参数

- 通用：
  - `-i` 输入文件
  - `-o` 输出文件
  - `-d` 数据库（`kegg/go/reactome/msigdb/custom`）
  - `--data-dir` 缓存目录（默认 `data`）
  - `--db` 指定 SQLite 离线包
  - `--use-embedded-db` 未提供 `--db` 时是否使用内置默认 SQLite（默认 `true`）
  - `--auto-update-db` 自动检测并扩容运行时 SQLite 覆盖（默认 `true`，可设为 `false` 关闭）
  - `--strict-mode` 一键切回严格策略（关闭自动扩容/在线回退/原始ID兜底，并将转换策略恢复到 threshold >= 0.90）
  - `--update-db` 分析前先更新目标 SQLite
  - `--update-db-idmaps` / `--update-db-idmaps-level` 控制更新时 ID 映射刷新（默认会刷新，`--update-db-idmaps=true`）
  - `--id-conversion-policy`（`strict` / `threshold` / `best-effort`，默认 `best-effort`）
  - `--min-conversion-rate`（threshold 策略最小转换率，默认 `0.50`）
  - `--enable-online-idmap-fallback` 缺少离线映射时允许在线回退（默认 `true`）
  - `--allow-id-fallback` 转换不完整时继续使用原始 ID（默认 `true`）
- db-audit：
  - `--profile` / `--strict-contract` 启用数据契约检查
  - `--expect-sha256` 校验目标 DB 文件哈希
  - `--expect-embedded-manifest` 读取内置 manifest 的 `sha256` + `contract_profile` 进行一致性校验
- GSEA：
  - `-rank-col` 排名列（默认 `logFC`）
  - `-nPerm` 置换次数（默认 `1000`）
  - `-pvalue-method`（`simple` / `adaptive`）
- ORA：
  - `--fdr-col`、`--fdr-threshold`
  - `--split-by-direction`
- 模式切换：
  - `--use-r`
  - `--benchmark`
  - `--benchmark-out`

## 与 clusterProfiler 对齐与性能（最新基线）

- 对齐日期：2026-03-09
- 性能日期：2026-03-08
- 数据集：`test-data/DE_results.csv`
- 口径：`nPerm=1000`，Go `adaptive`（MSigDB 用 `simple`），库覆盖 KEGG+GO+Reactome+MSigDB(c7)

关键产物：

- 对齐汇总：`/tmp/alignment_latest_universe_fix/comparison_summary.tsv`
- GSEA 性能汇总：`/tmp/perf_cmp/latest_alignment_perf_compare.tsv`
- ORA 性能汇总：`/tmp/perf_cmp/latest_ora_alignment_perf_compare.tsv`

### GSEA 对齐（Go vs R）

| DB | go_rows | r_rows | shared | shared/go | shared/r | top20_overlap | sig_jaccard | nes_abs_err_median |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| kegg | 31 | 33 | 30 | 0.968 | 0.909 | 0.90 | 0.8824 | 0.017134 |
| go | 45 | 41 | 40 | 0.889 | 0.976 | 0.80 | 0.8696 | 0.022662 |
| reactome | 57 | 54 | 53 | 0.930 | 0.981 | 0.80 | 0.9138 | 0.015152 |
| msigdb(c7) | 1209 | 1331 | 1191 | 0.985 | 0.895 | 0.15 | 0.8829 | 0.011145 |

当前门禁剩余项：`gsea/go` 的 `nes_abs_err_median=0.022662`（阈值 `0.02`）。

### GSEA 性能（Go vs R）

| DB | Go时长(s) | R时长(s) | Go/R时长 | Go峰值RSS(KB) | R峰值RSS(KB) | Go/R内存 |
|---|---:|---:|---:|---:|---:|---:|
| kegg | 1.994 | 10.299 | 0.194x | 23200 | 531792 | 0.044x |
| go | 6.283 | 12.696 | 0.495x | 40712 | 574968 | 0.071x |
| reactome | 7.431 | 14.642 | 0.508x | 40744 | 593452 | 0.069x |
| msigdb | 13.453 | 49.327 | 0.273x | 128744 | 1061608 | 0.121x |

### ORA 对齐（Go vs R）

| DB | go_rows | r_rows | shared | shared/go | shared/r | top20_overlap | sig_jaccard |
|---|---:|---:|---:|---:|---:|---:|---:|
| kegg | 15 | 11 | 11 | 0.733 | 1.000 | 0.7333 | 0.7333 |
| go | 34 | 34 | 34 | 1.000 | 1.000 | 1.0000 | 1.0000 |
| reactome | 9 | 8 | 8 | 0.889 | 1.000 | 0.8889 | 0.8889 |
| msigdb(c7) | 363 | 371 | 362 | 0.997 | 0.976 | 0.9000 | 0.9731 |

### ORA 性能（Go vs R）

| DB | Go时长(s) | R时长(s) | Go/R时长 | Go峰值RSS(KB) | R峰值RSS(KB) | Go/R内存 |
|---|---:|---:|---:|---:|---:|---:|
| kegg | 0.654 | 6.787 | 0.096x | 14964 | 534540 | 0.028x |
| go | 0.223 | 7.157 | 0.031x | 2432 | 553016 | 0.004x |
| reactome | 0.225 | 7.562 | 0.030x | 2304 | 565948 | 0.004x |
| msigdb | 0.436 | 18.803 | 0.023x | 56704 | 977840 | 0.058x |

## CI 工作流

- 对齐回归：`.github/workflows/alignment.yml`
  - 运行 smoke + formal 对齐
  - 执行阈值门禁
- 性能对比：`.github/workflows/performance.yml`
  - 运行 Go vs R benchmark（时间+内存）
  - 上传 benchmark 工件

## 仓库目录

- Go 源码：`main.go`、`cmd_*.go`、`pkg/`
- 脚本：`scripts/alignment/`
- 文档：`docs/`
- 数据与测试数据：`data/`、`test-data/`
- 示例：`examples/input/`、`examples/output/`
- 参考实现（本地保留，不纳入版本控制）：`refer-code/`

## 相关文档

- [docs/alignment.md](docs/alignment.md)：完整对齐流程、参数、门禁阈值
- [docs/README.md](docs/README.md)：文档导航
