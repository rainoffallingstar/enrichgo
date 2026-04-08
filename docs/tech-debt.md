# 技术债务

## 待解决问题

### 默认发布链路与门禁
**现状（已缓解）**:
- 默认内嵌 SQLite 已切回 `embedded-hsa-basic`，并补上 `no --db` 的 ORA/GSEA smoke 测试与 release precheck 覆盖。
- 默认落盘路径遇到旧 schema runtime DB 时会自动重装内嵌库，不再复用 schema v1。

**仍待解决**:
- 若要扩大默认内嵌库覆盖（Reactome / MSigDB / 更多 GO ontology），需要单独评估资产体积与构建时间。
- 保持本地 `tools/build_embedded_db` 与发布流程中的嵌入资产生成步骤一致。

---

### GSEA/GO 对齐收敛
**现状**:
- 当前基线仍有 `gsea/go nes_abs_err_median=0.022662 > 0.020000`，本次发布收口未将其作为阻塞项。

**仍待解决**:
- 继续收敛 GSEA/GO 的排序、归一化与 p 值估计差异，恢复 formal 门禁通过。

---

### 1. ID 转换缓存：持久化/可配置化 (优先级: 中)
**现状（已缓解）**:
- `KEGGIDConverter` 已使用按 `species:fromType:toType` 分桶的 LRU 缓存，并支持“部分命中 + 仅补齐缺失项”，避免重复请求/重复计算。
- 默认每个桶 `maxEntries=50000`（可通过 CLI `--kegg-id-cache-max-entries` 或环境变量 `ENRICHGO_KEGG_ID_CACHE_MAX_ENTRIES` 调整）。

**仍待解决**:
- 在对齐流程里记录命中率（当前可通过 `ENRICHGO_KEGG_ID_CACHE_METRICS_TSV` 导出缓存命中/未命中/淘汰统计；benchmark TSV 也已包含该指标）。
- 评估是否需要把 conv API 的结果落盘（与现有 `kegg_<species>_idmap.tsv` 的物种映射区分开）。

---

### 2. 错误处理完善 (优先级: 低)
**现状（已缓解）**:
- 已统一下载/拉取路径的 HTTP 超时与重试策略，减少偶发网络失败导致的“硬失败”。

**仍待解决**:
- 为关键下载添加更明确的错误上下文（URL/尝试次数/最终状态码）。
- 对部分大文件下载（NCBI/UniProt）补充断点续传或更细粒度的失败恢复（如需要）。

---

### 3. 并发优化 (优先级: 低)
**问题**: GSEA 置换计算为串行执行

**优化方向**:
```go
// 使用 goroutine 并行计算
var wg sync.WaitGroup
for i := 0; i < permutations; i++ {
    wg.Add(1)
    go func() {
        defer wg.Done()
        // 计算置换
    }()
}
wg.Wait()
```

---

### 4. 内存使用优化 (优先级: 低)
**问题**: 大基因集可能导致内存占用高

**优化方向**:
- 流式处理 GMT 文件
- 延迟加载基因集
- 增量计算

---

## 已修复问题

### ✅ 网络请求超时/重试（统一）
- **修复**: 引入统一 HTTP client（timeout + retry/backoff），并替换数据库下载与 KEGG 在线映射请求路径的直连 `http.Get/http.Post`。

### ✅ GO 数据库本地加载
- **修复**: 已实现 `LoadOrDownloadGO`，优先读取 `go_<species>_<ontology>.gmt`，缺失时再下载生成。

### ✅ Reactome 命令集成
- **问题**: `cmd_enrich.go` 和 `cmd_gsea.go` 中 Reactome case 返回 "not yet implemented"
- **修复**: 在两个命令中实现完整的 `LoadOrDownloadReactome` 集成（`cmd_enrich.go:167-185`，`cmd_gsea.go:183-201`）
- **状态**: 已完成

### ✅ GSEA 随机种子
- **问题**: 之前随机洗牌不是真随机
- **修复**: 使用 `math/rand` 配合随机种子
- **状态**: 已完成

### ✅ ID 检测正则表达式
- **问题**: ENSEMBL、KEGG ID 检测失败
- **修复**: 优化正则表达式支持更多格式
- **状态**: 已完成

### ✅ 本地缓存
- **问题**: 每次运行都重新下载数据库
- **修复**: 实现 `LoadOrDownload*` 函数
- **状态**: 已完成

---

## 长期改进

1. **性能优化**: 并行计算、内存优化
2. **错误恢复**: 网络重试、超时处理
3. **功能扩展**: 更多数据库支持
4. **测试完善**: 集成测试、基准测试
