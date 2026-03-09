# 项目进度

## 已完成功能

### 核心算法
- [x] ORA 超几何检验实现
- [x] FDR (Benjamini-Hochberg) 校正
- [x] GSEA 算法实现
- [x] GSEA 随机种子支持

### ID 管理
- [x] ID 类型自动检测 (ENTREZ, SYMBOL, ENSEMBL, UNIPROT, KEGG, REFSEQ)
- [x] ID 批量检测 (多数投票算法)
- [x] KEGG list 本地映射优先的 ID 转换（支持离线缓存）
- [x] 内存缓存机制

### 数据库支持
- [x] KEGG 下载与解析
- [x] GO 下载与解析 (BP, MF, CC)
- [x] MSigDB 下载与本地缓存 (默认 c1-c8，可单集合)
- [x] Reactome 下载
- [x] 本地文件缓存

### CLI 命令
- [x] `enrich` - ORA 富集分析
- [x] `gsea` - GSEA 富集分析
- [x] `download` - 数据库下载
- [x] `--data-dir` 统一数据库目录
- [x] `--allow-id-fallback` 显式回退策略（默认转换失败即退出）

### 单元测试
- [x] 超几何检验测试
- [x] FDR 校正测试
- [x] ID 类型检测测试
- [x] ID 批量检测测试
- [x] ID 转换缓存测试
- [x] GO GAF 解析测试（使用 Symbol）
- [x] MSigDB 本地缓存/多集合合并测试
- [x] Reactome 物种前缀映射测试

---

## 当前状态（事实）

- `test-data/DE_results.csv` 可直接作为 `enrich`/`gsea` 输入。
- ORA/GSEA 结果可输出 `tsv/csv/json`。
- 默认 MSigDB 范围为 `c1-c8`，可用 `-c c2` 或 `-c c1,c2` 覆盖。
- ID 转换在网络不可用时会默认报错退出，避免“静默0结果”。
- `download -d kegg` 会额外缓存 `kegg_<species>_idmap.tsv`，供离线 ID 转换复用。

---

## 实现细节

### ORA 算法
- 输入: 基因列表 + 基因集
- 输出: 富集通路列表 (p-value, p-adjust, q-value, gene ratio)
- 统计: 超几何检验 + BH 校正

### GSEA 算法
- 输入: 排序基因列表 + 基因集
- 输出: 富集通路列表 (ES, NES, p-value, p-adjust)
- 置换次数: 默认 1000

### ID 转换
- 自动检测输入 ID 类型
- 调用 KEGG REST API 转换
- 内存缓存结果

---

## 构建与测试

### 构建
```bash
cd enrichgo
go build -o enrichgo
```

### 运行测试
```bash
go test ./...
```

### 测试覆盖
```
pkg/analysis:    ORA, FDR 算法测试
pkg/annotation:  ID 检测, ID 转换测试
```

---

## 版本历史

### v0.1.0 (当前)
- 初始版本
- ORA, GSEA 分析
- KEGG, GO, MSigDB, Reactome 数据库支持
- ID 自动检测与转换
