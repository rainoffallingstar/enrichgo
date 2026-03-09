# 技术债务

## 待解决问题

### 1. ID 转换缓存优化 (优先级: 中)
**问题**: 当前缓存使用简单 map，未区分不同物种/ID类型组合

**现状**:
- `KEGGIDConverter.cache` 存储所有转换结果
- 缓存 key: `species:fromType:toType`
- 存在缓存未命中时的重复请求问题

**优化方向**:
- 考虑使用 `sync.Map` 提高并发性能
- 添加缓存大小限制 (LRU 淘汰)
- 持久化缓存到本地文件

---

### 2. GO 数据库本地加载 (优先级: 低)
**问题**: 每次运行都重新下载 GO 数据

**现状**: `DownloadGO` 只下载不检查本地缓存

**修复方案**:
```go
func LoadOrDownloadGO(species, ontology, dataDir string) (*GOData, error) {
    // 先检查本地文件
    localFile := filepath.Join(dataDir, fmt.Sprintf("go_%s_%s.obo", species, ontology))
    if _, err := os.Stat(localFile); err == nil {
        return LoadGO(localFile)
    }
    return DownloadGO(species, ontology, dataDir)
}
```

---

### 3. 错误处理完善 (优先级: 低)
**问题**: 部分错误未优雅处理

**需要改进**:
- 网络请求超时设置
- 重试机制
- 更好的错误提示

---

### 4. 并发优化 (优先级: 低)
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

### 5. 内存使用优化 (优先级: 低)
**问题**: 大基因集可能导致内存占用高

**优化方向**:
- 流式处理 GMT 文件
- 延迟加载基因集
- 增量计算

---

## 已修复问题

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
