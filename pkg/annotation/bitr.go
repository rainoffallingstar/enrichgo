package annotation

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"enrichgo/pkg/netutil"
)

// ID 类型
type IDType string

const (
	IDUnknown IDType = "unknown"
	IDEntrez  IDType = "ENTREZID"
	IDSymbol  IDType = "SYMBOL"
	IDEnsembl IDType = "ENSEMBL"
	IDUniprot IDType = "UNIPROT"
	IDRefSeq  IDType = "REFSEQ"
	IDKEGG    IDType = "KEGG"
)

// DetectIDType 自动检测基因 ID 类型
func DetectIDType(geneID string) IDType {
	// KEGG ID (如 hsa:10458, eco:b0001) - 需要先检测，因为带冒号
	if regexp.MustCompile(`^[a-z]{2,4}:[a-z0-9]+$`).MatchString(geneID) {
		return IDKEGG
	}

	// ENSEMBL ID (如 ENSG00000141510, ENSMUSG00000027387)
	if regexp.MustCompile(`(?i)^ens[a-z]+\d{6,15}$`).MatchString(geneID) {
		return IDEnsembl
	}

	// UniProt ID (如 P12345, Q9BQN5) - 必须在检测 Symbol 之前
	if regexp.MustCompile(`^[A-NP-Q][0-9][A-Z0-9]{3}[0-9]$`).MatchString(geneID) {
		return IDUniprot
	}

	// RefSeq (如 NM_001, NP_001, NR_024)
	if regexp.MustCompile(`^N[MRP]_\d+$`).MatchString(geneID) {
		return IDRefSeq
	}

	// 纯数字 (如 1234)
	if regexp.MustCompile(`^\d+$`).MatchString(geneID) {
		return IDEntrez
	}

	// 基因符号 (如 TP53, ACTB)
	if regexp.MustCompile(`^[A-Z][a-zA-Z0-9]{1,15}$`).MatchString(geneID) {
		return IDSymbol
	}

	return IDUnknown
}

// BatchDetectIDType 批量检测 ID 类型
func BatchDetectIDType(geneIDs []string) IDType {
	if len(geneIDs) == 0 {
		return IDUnknown
	}

	typeCounts := make(map[IDType]int)
	for _, id := range geneIDs {
		t := DetectIDType(id)
		typeCounts[t]++
	}

	var maxType IDType
	var maxCount int
	for t, count := range typeCounts {
		if t != IDUnknown && count > maxCount {
			maxType = t
			maxCount = count
		}
	}

	if float64(maxCount)/float64(len(geneIDs)) > 0.5 {
		return maxType
	}

	return IDUnknown
}

// IDConverter ID 转换接口
type IDConverter interface {
	Convert(geneIDs []string, fromType, toType IDType, species string) (map[string][]string, error)
}

type speciesGeneMap struct {
	symbolToEntrez map[string]string
	entrezToSymbol map[string]string
}

// KEGGIDConverter 基于 KEGG list 接口 + 本地缓存的 ID 转换器
type KEGGIDConverter struct {
	cache       map[string]*lruCache
	speciesMaps map[string]*speciesGeneMap
	dataDir     string
	maxEntries  int
	hits        uint64
	misses      uint64
	mu          sync.RWMutex
}

const defaultKEGGIDCacheMaxEntries = 50000

type KEGGIDCacheStats struct {
	Hits       uint64
	Misses     uint64
	Evictions  uint64
	Entries    int
	Buckets    int
	MaxEntries int
}

// NewKEGGIDConverter 创建 KEGG ID 转换器。
// 可选 dataDir 参数用于本地持久化缓存（kegg_<species>_idmap.tsv）。
func NewKEGGIDConverter(dataDir ...string) *KEGGIDConverter {
	dir := ""
	if len(dataDir) > 0 {
		dir = dataDir[0]
	}
	return &KEGGIDConverter{
		cache:       make(map[string]*lruCache),
		speciesMaps: make(map[string]*speciesGeneMap),
		dataDir:     dir,
		maxEntries:  defaultKEGGIDCacheMaxEntries,
	}
}

// getCacheKey 获取缓存 key
func (c *KEGGIDConverter) getCacheKey(species string, fromType, toType IDType) string {
	return fmt.Sprintf("%s:%s:%s", species, fromType, toType)
}

// SetMaxCacheEntries sets the per-(species,from,to) in-memory cache cap.
// max<=0 disables eviction (unbounded).
func (c *KEGGIDConverter) SetMaxCacheEntries(max int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxEntries = max
	for _, cc := range c.cache {
		if cc == nil {
			continue
		}
		cc.max = max
		cc.evictIfNeeded()
	}
}

func (c *KEGGIDConverter) Stats() KEGGIDCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	st := KEGGIDCacheStats{
		Hits:       c.hits,
		Misses:     c.misses,
		MaxEntries: c.maxEntries,
	}
	for _, cc := range c.cache {
		if cc == nil {
			continue
		}
		st.Buckets++
		st.Entries += cc.Len()
		st.Evictions += cc.Evicted()
	}
	return st
}

func (c *KEGGIDConverter) getOrCreateCacheLocked(key string) *lruCache {
	if c.cache == nil {
		c.cache = make(map[string]*lruCache)
	}
	cc := c.cache[key]
	if cc == nil {
		cc = newLRUCache(c.maxEntries)
		c.cache[key] = cc
	}
	return cc
}

// getCached 获取缓存（兼容旧测试）
func (c *KEGGIDConverter) getCached(key string, geneID string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cc := c.cache[key]
	if cc == nil {
		return "", false
	}
	ids, ok := cc.Get(geneID)
	if !ok || len(ids) == 0 {
		return "", false
	}
	return ids[0], true
}

// setCache 设置缓存
func (c *KEGGIDConverter) setCache(key string, mapping map[string][]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cc := c.getOrCreateCacheLocked(key)
	for k, v := range mapping {
		cc.Set(k, v)
	}
}

// Convert 转换 ID。优先本地映射，再尝试在线刷新映射。
func (c *KEGGIDConverter) Convert(geneIDs []string, fromType, toType IDType, species string) (map[string][]string, error) {
	if len(geneIDs) == 0 {
		return make(map[string][]string), nil
	}

	cacheKey := c.getCacheKey(species, fromType, toType)
	result := make(map[string][]string, len(geneIDs))
	missing := make([]string, 0)
	c.mu.Lock()
	cc := c.getOrCreateCacheLocked(cacheKey)
	for _, id := range geneIDs {
		if ids, ok := cc.Get(id); ok {
			result[id] = ids
			c.hits++
			continue
		}
		c.misses++
		missing = append(missing, id)
	}
	c.mu.Unlock()
	if len(missing) == 0 {
		return result, nil
	}

	if fromType == toType {
		newMap := make(map[string][]string, len(missing))
		for _, id := range missing {
			v := []string{id}
			result[id] = v
			newMap[id] = v
		}
		c.setCache(cacheKey, newMap)
		return result, nil
	}

	if supportsSpeciesMapConversion(fromType, toType) {
		spMap, err := c.loadSpeciesGeneMap(species)
		if err != nil {
			return nil, err
		}

		newMap := make(map[string][]string, len(missing))
		for _, id := range missing {
			var v []string
			if converted, ok := convertWithSpeciesMap(id, fromType, toType, species, spMap); ok {
				v = converted
			} else {
				v = []string{id}
			}
			result[id] = v
			newMap[id] = v
		}
		c.setCache(cacheKey, newMap)
		return result, nil
	}

	// 对于 KEGG list 无法覆盖的类型，保留旧的 conv 端点兜底。
	convMap, err := c.convertByKEGGConvAPI(missing, fromType, toType, species)
	if err != nil {
		return nil, err
	}
	for k, v := range convMap {
		result[k] = v
	}
	c.setCache(cacheKey, convMap)
	return result, nil
}

func supportsSpeciesMapConversion(fromType, toType IDType) bool {
	valid := func(t IDType) bool {
		return t == IDSymbol || t == IDEntrez || t == IDKEGG
	}
	return valid(fromType) && valid(toType)
}

func convertWithSpeciesMap(id string, fromType, toType IDType, species string, spMap *speciesGeneMap) ([]string, bool) {
	var entrez string
	switch fromType {
	case IDSymbol:
		sym := normalizeSymbol(id)
		if sym == "" {
			return nil, false
		}
		v, ok := spMap.symbolToEntrez[sym]
		if !ok {
			return nil, false
		}
		entrez = v
	case IDEntrez:
		entrez = normalizeEntrez(id, species)
		if entrez == "" {
			return nil, false
		}
	case IDKEGG:
		entrez = normalizeKEGGGeneID(id, species)
		if entrez == "" {
			return nil, false
		}
	default:
		return nil, false
	}

	switch toType {
	case IDEntrez:
		return []string{entrez}, true
	case IDKEGG:
		return []string{species + ":" + entrez}, true
	case IDSymbol:
		if sym, ok := spMap.entrezToSymbol[entrez]; ok && sym != "" {
			return []string{sym}, true
		}
		return nil, false
	default:
		return nil, false
	}
}

func normalizeSymbol(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	return strings.ToUpper(s)
}

func normalizeEntrez(s, species string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, species+":")
	s = strings.TrimPrefix(s, "ncbi-geneid:")
	return s
}

func normalizeKEGGGeneID(s, species string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, species+":")
	return s
}

func (c *KEGGIDConverter) loadSpeciesGeneMap(species string) (*speciesGeneMap, error) {
	c.mu.RLock()
	if m, ok := c.speciesMaps[species]; ok {
		c.mu.RUnlock()
		return m, nil
	}
	c.mu.RUnlock()

	if m, err := c.loadSpeciesGeneMapFromFile(species); err == nil && m != nil {
		c.mu.Lock()
		c.speciesMaps[species] = m
		c.mu.Unlock()
		return m, nil
	}

	m, err := c.fetchSpeciesGeneMapFromKEGG(species)
	if err != nil {
		return nil, fmt.Errorf("failed to load ID mapping for %s: %w", species, err)
	}

	if err := c.saveSpeciesGeneMapToFile(species, m); err != nil {
		// 缓存写失败不阻断转换
		fmt.Fprintf(os.Stderr, "Warning: failed to save ID mapping cache: %v\n", err)
	}

	c.mu.Lock()
	c.speciesMaps[species] = m
	c.mu.Unlock()
	return m, nil
}

func (c *KEGGIDConverter) speciesMapFile(species string) string {
	if strings.TrimSpace(c.dataDir) == "" {
		return ""
	}
	return filepath.Join(c.dataDir, fmt.Sprintf("kegg_%s_idmap.tsv", species))
}

func (c *KEGGIDConverter) loadSpeciesGeneMapFromFile(species string) (*speciesGeneMap, error) {
	path := c.speciesMapFile(species)
	if path == "" {
		return nil, fmt.Errorf("no data directory configured")
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	m := &speciesGeneMap{
		symbolToEntrez: make(map[string]string),
		entrezToSymbol: make(map[string]string),
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 2 {
			continue
		}
		entrez := normalizeEntrez(fields[0], species)
		symbol := strings.TrimSpace(fields[len(fields)-1])
		if entrez == "" || symbol == "" {
			continue
		}
		if _, ok := m.entrezToSymbol[entrez]; !ok {
			m.entrezToSymbol[entrez] = symbol
		}
		m.symbolToEntrez[normalizeSymbol(symbol)] = entrez
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(m.entrezToSymbol) == 0 {
		return nil, fmt.Errorf("empty mapping file: %s", path)
	}
	return m, nil
}

func (c *KEGGIDConverter) saveSpeciesGeneMapToFile(species string, m *speciesGeneMap) error {
	path := c.speciesMapFile(species)
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	entrezIDs := make([]string, 0, len(m.entrezToSymbol))
	for entrez := range m.entrezToSymbol {
		entrezIDs = append(entrezIDs, entrez)
	}
	sort.Strings(entrezIDs)

	w := bufio.NewWriter(file)
	for _, entrez := range entrezIDs {
		fmt.Fprintf(w, "%s\t%s\n", entrez, m.entrezToSymbol[entrez])
	}
	return w.Flush()
}

func (c *KEGGIDConverter) fetchSpeciesGeneMapFromKEGG(species string) (*speciesGeneMap, error) {
	url := fmt.Sprintf("https://rest.kegg.jp/list/%s", species)
	client := netutil.NewClient(netutil.Options{Timeout: 30 * time.Second})
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("KEGG API error: %s", resp.Status)
	}

	m := &speciesGeneMap{
		symbolToEntrez: make(map[string]string),
		entrezToSymbol: make(map[string]string),
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "\t")
		if len(fields) < 2 {
			continue
		}

		entrez := normalizeKEGGGeneID(fields[0], species)
		if entrez == "" {
			continue
		}

		right := fields[len(fields)-1]
		genesPart := right
		if idx := strings.Index(right, ";"); idx >= 0 {
			genesPart = right[:idx]
		}
		aliases := strings.Split(genesPart, ",")
		if len(aliases) == 0 {
			continue
		}

		primary := strings.TrimSpace(aliases[0])
		if primary == "" {
			continue
		}
		if _, ok := m.entrezToSymbol[entrez]; !ok {
			m.entrezToSymbol[entrez] = primary
		}

		for _, alias := range aliases {
			a := normalizeSymbol(alias)
			if a == "" {
				continue
			}
			if _, exists := m.symbolToEntrez[a]; !exists {
				m.symbolToEntrez[a] = entrez
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(m.entrezToSymbol) == 0 {
		return nil, fmt.Errorf("empty mapping returned from KEGG list")
	}

	return m, nil
}

func (c *KEGGIDConverter) convertByKEGGConvAPI(geneIDs []string, fromType, toType IDType, species string) (map[string][]string, error) {
	fromStr := idTypeToKEGG(fromType)
	toStr := idTypeToKEGG(toType)
	url := fmt.Sprintf("https://rest.kegg.jp/conv/%s/%s/%s", toStr, species, fromStr)

	cleanIDs := make([]string, len(geneIDs))
	for i, id := range geneIDs {
		cleanIDs[i] = strings.TrimPrefix(id, species+":")
	}

	body := strings.NewReader(strings.Join(cleanIDs, "\n"))
	req, _ := http.NewRequest(http.MethodPost, url, body)
	req.Header.Set("Content-Type", "text/plain")
	resp, err := netutil.DefaultClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to call KEGG API: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("KEGG API error: %s", resp.Status)
	}

	result := make(map[string][]string)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "\t")
		if len(fields) < 2 {
			continue
		}
		sourceID := strings.TrimPrefix(fields[0], species+":")
		targetID := fields[1]
		result[sourceID] = append(result[sourceID], targetID)
	}

	finalResult := make(map[string][]string, len(geneIDs))
	for _, id := range geneIDs {
		cleanID := strings.TrimPrefix(id, species+":")
		if mapping, ok := result[cleanID]; ok {
			finalResult[id] = mapping
		} else {
			finalResult[id] = []string{id}
		}
	}

	return finalResult, nil
}

func idTypeToKEGG(idType IDType) string {
	switch idType {
	case IDEntrez:
		return "ncbi-geneid"
	case IDSymbol:
		return "genesymbol"
	case IDUniprot:
		return "uniprot"
	case IDKEGG:
		return "kegg"
	default:
		return string(idType)
	}
}

// ConvertGeneID 转换基因 ID
// 自动检测输入 ID 类型并转换为目标类型
func ConvertGeneID(geneIDs []string, targetType IDType, species string, converter IDConverter) ([]string, map[string][]string, error) {
	inputType := BatchDetectIDType(geneIDs)
	if inputType == IDUnknown {
		return nil, nil, fmt.Errorf("cannot detect input ID type")
	}

	if inputType == targetType {
		result := make(map[string][]string)
		for _, id := range geneIDs {
			result[id] = []string{id}
		}
		return geneIDs, result, nil
	}

	mapping, err := converter.Convert(geneIDs, inputType, targetType, species)
	if err != nil {
		return nil, nil, err
	}
	if err := validateConversionResult(geneIDs, mapping); err != nil {
		return nil, nil, err
	}

	var convertedIDs []string
	for _, ids := range mapping {
		convertedIDs = append(convertedIDs, ids...)
	}
	convertedIDs = uniqueStrings(convertedIDs)

	return convertedIDs, mapping, nil
}

func validateConversionResult(geneIDs []string, mapping map[string][]string) error {
	unmapped := 0
	for _, orig := range geneIDs {
		ids, ok := mapping[orig]
		if !ok || len(ids) == 0 {
			unmapped++
			continue
		}
		converted := false
		for _, id := range ids {
			if strings.TrimSpace(id) != "" && id != orig {
				converted = true
				break
			}
		}
		if !converted {
			unmapped++
		}
	}
	if unmapped > 0 {
		return fmt.Errorf("ID conversion incomplete: %d/%d genes were not converted", unmapped, len(geneIDs))
	}
	return nil
}

func uniqueStrings(ss []string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, s := range ss {
		s = strings.TrimSpace(s)
		if s != "" && !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}
	return result
}
