# enrichgo 文档

## 目录

- [设计文档](design.md) - 系统架构、核心模块设计
- [进度报告](progress.md) - 已完成功能、构建测试
- [技术债务](tech-debt.md) - 待解决问题、已修复问题
- [对齐验证](alignment.md) - 与 clusterProfiler 的 ORA/GSEA 对齐流程
  - 包含 `run_alignment_ci.sh`（smoke+formal+门禁）与阈值校验脚本说明
  - GitHub Actions:
    - `.github/workflows/alignment.yml`：对齐回归（smoke+formal+门禁）
    - `.github/workflows/performance.yml`：Go vs R 基准对比（时间+内存）

## 快速链接

### 核心功能
- [ORA 算法](design.md#3-ora-算法-analysisora)
- [GSEA 算法](design.md#4-gsea-算法-analysisgsea)
- [ID 转换](design.md#2-id-转换-annotationbitr)

### 数据库
- [KEGG](design.md#kegg)
- [GO](design.md#go)
- [Reactome](design.md#reactome)
- [MSigDB](design.md#msigdb)

### 使用示例
```bash
# ORA 分析
./enrichgo analyze ora -i test-data/DE_results.csv -d kegg -s hsa --fdr-col FDR --fdr-threshold 0.05

# GSEA 分析
./enrichgo analyze gsea -i test-data/DE_results.csv -d msigdb -c c1-c8 -s hsa -seed 42

# 调用 R 基线实现
./enrichgo analyze gsea -i test-data/DE_results.csv -d go -s hsa --use-r

# 同时运行 Go+R 并输出 benchmark（TSV）
./enrichgo analyze gsea -i test-data/DE_results.csv -d go -s hsa -nPerm 100 --benchmark --benchmark-out /tmp/benchmark.tsv

# 下载数据库
./enrichgo data sync -d kegg -s hsa -o data/
```

### 离线优先说明

- `download -d kegg` 会同时缓存通路文件和 `kegg_<species>_idmap.tsv`（ID 映射）。
- 之后可通过 `--data-dir` 在离线环境复用缓存，避免运行时依赖网络 ID 转换。
- 默认发布二进制内置 SQLite 默认库（固定 profile：`species=hsa`，`idmaps_level=basic`），不传 `--db` 会自动使用。
- 内置库附带 `assets/default_enrichgo.db.manifest.json`（`sha256` + `contract_profile`），可通过 `enrichgo db audit --db <path> --expect-embedded-manifest` 做一致性校验。
- 也可用 `--db` 将通路库与 ID 映射打包到单个 SQLite 文件，运行时通过 `--db` 直接读取（更便于分发与复用）。

```bash
# 首次联网缓存
./enrichgo data sync -d kegg -s hsa -o data/

# 离线运行（使用本地缓存）
./enrichgo analyze ora -i test-data/DE_results.csv -d kegg -s hsa --data-dir data --fdr-col FDR --fdr-threshold 0.05

# 直接使用内置 SQLite（无需 --db）
./enrichgo analyze ora -i test-data/DE_results.csv -d kegg -s hsa -o /tmp/ora_kegg.tsv

# 打包并离线运行（SQLite 单文件）
./enrichgo data sync -d all -s hsa -ont ALL -c all --db data/enrichgo.db --db-only --idmaps --idmaps-level extended
./enrichgo analyze ora -i test-data/DE_results.csv -d kegg -s hsa --db data/enrichgo.db --fdr-col FDR --fdr-threshold 0.05

# extended 回填支持断点续跑（默认）
./enrichgo data sync -d all -s hsa --db data/enrichgo.db --db-only --idmaps --idmaps-level extended --idmaps-resume

# 强制全量刷新（忽略 resume）
./enrichgo data sync -d all -s hsa --db data/enrichgo.db --db-only --idmaps --idmaps-level extended --idmaps-force-refresh

# basic 在线失败时启用本地 TSV 兜底目录（kegg_<species>_idmap.tsv）
./enrichgo data sync -d kegg -s hsa --db data/enrichgo.db --db-only --idmaps --idmaps-level basic --idmaps-local-dir data

# 分析前刷新 SQLite（不适用于 -d custom）
./enrichgo analyze gsea -i test-data/DE_results.csv -d go -s hsa --update-db --update-db-idmaps=true --update-db-idmaps-level basic -o /tmp/gsea_go.tsv
```
