# enrichgo 设计文档

## 项目概述

enrichgo 是 clusterProfiler 的 Go 语言 CLI 实现，用于基因集富集分析 (Gene Set Enrichment Analysis)。

### 核心功能

- **ORA (Over-Representation Analysis)**: 超几何检验
- **GSEA (Gene Set Enrichment Analysis)**: 基因集富集分析
- **ID 类型自动检测与转换**: 支持 ENTREZ, SYMBOL, ENSEMBL, UNIPROT, KEGG, REFSEQ

### 支持的数据库

- KEGG (KEGG PATHWAY)
- GO (Gene Ontology)
- Reactome
- MSigDB (c1-c8)
- 自定义 GMT 文件

---

## 系统架构

```
enrichgo/
├── main.go              # 入口文件
├── cmd_enrich.go        # ORA 命令
├── cmd_gsea.go          # GSEA 命令
├── cmd_download.go      # 数据库下载命令
│
└── pkg/
    ├── annotation/      # ID 检测与转换
    │   ├── bitr.go      # ID 类型检测、转换接口
    │   └── bitr_test.go
    │
    ├── analysis/        # 分析算法
    │   ├── ora.go       # ORA 超几何检验
    │   ├── gsea.go     # GSEA 算法
    │   ├── fdr.go      # FDR 校正
    │   └── ora_test.go
    │
    ├── database/        # 数据库模块
    │   ├── kegg.go     # KEGG 下载/解析
    │   ├── go.go       # GO 下载/解析
    │   ├── msigdb.go   # MSigDB 下载
    │   └── reactome.go # Reactome 下载
    │
    ├── io/              # 输入输出
    │   └── io.go        # 文件解析/写入
    │
    └── types/          # 共享类型
        └── geneset.go   # GeneSet, Pathway 等类型
```

---

## 核心模块设计

### 1. ID 类型检测 (annotation/bitr.go)

```go
type IDType string

const (
    IDUnknown   IDType = "unknown"
    IDEntrez    IDType = "ENTREZID"
    IDSymbol    IDType = "SYMBOL"
    IDEnsembl   IDType = "ENSEMBL"
    IDUniprot   IDType = "UNIPROT"
    IDRefSeq    IDType = "REFSEQ"
    IDKEGG      IDType = "KEGG"
)
```

**检测规则** (按优先级):
1. KEGG ID: `^[a-z]{2,4}:[a-z0-9]+$` (如 `hsa:10458`, `eco:b0001`)
2. ENSEMBL ID: `(?i)^ens[a-z]+\d{6,15}$` (如 `ENSG00000141510`, `ENSMUSG00000027387`)
3. UniProt ID: `^[A-NP-Q][0-9][A-Z0-9]{3}[0-9]$` (如 `P12345`)
4. RefSeq: `^N[MRP]_\d+$` (如 `NM_001`)
5. 纯数字: `^\d+$` → Entrez ID
6. 基因符号: `^[A-Z][a-zA-Z0-9]{1,15}$` (如 `TP53`)

**批量检测**: 统计每种 ID 类型数量，超过 50% 识别率则返回该类型

### 2. ID 转换 (annotation/bitr.go)

```go
type IDConverter interface {
    Convert(geneIDs []string, fromType, toType IDType, species string) (map[string][]string, error)
}

type KEGGIDConverter struct {
    cache map[string]map[string][]string
    mu    sync.RWMutex
}
```

- 使用 KEGG REST API 进行转换
- 内存缓存机制避免重复请求
- 缓存 key: `species:fromType:toType`

### 3. ORA 算法 (analysis/ora.go)

- 使用超几何检验计算 p-value
- Benjamini-Hochberg FDR 校正
- 支持基因集大小过滤

### 4. GSEA 算法 (analysis/gsea.go)

- 计算 enrichment score (ES)
- Permutation test 评估显著性
- 支持随机种子确保可复现性
- 计算 NES (Normalized Enrichment Score)

### 5. 数据库模块

#### KEGG
- 下载: `https://rest.kegg.jp/link/{species}/pathway`
- 解析: pathway → gene 映射

#### GO
- 下载: Gene Ontology OBO 文件
- 解析: GO term 层级结构
- 物种基因映射

#### Reactome
- 下载: `https://reactome.org/download/current/genesets/{species}.gmt`
- GMT 格式解析

#### MSigDB
- 下载: MSigDB GMT 文件
- 支持 c1-c8 所有基因集

---

## 使用示例

### ORA 分析

```bash
# 使用基因符号 (自动转换为 Entrez)
./enrichgo enrich -i genes.txt -d kegg -s hsa

# 指定 ID 类型
./enrichgo enrich -i genes.txt -d kegg -s hsa -id-type symbol

# 使用自定义基因集
./enrichgo enrich -i genes.txt -d custom -gmt my_genesets.gmt
```

### GSEA 分析

```bash
# 使用排序好的基因列表
./enrichgo gsea -i ranked_genes.txt -d kegg -s hsa

# 指定随机种子
./enrichgo gsea -i ranked_genes.txt -d kegg -s hsa -seed 42

# 1000 次置换
./enrichgo gsea -i ranked_genes.txt -d kegg -s hsa -nPerm 1000
```

### 下载数据库

```bash
./enrichgo download -d kegg -s hsa -o data/
./enrichgo download -d go -s mmu -ont BP -o data/
```
