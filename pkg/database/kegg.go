package database

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"enrichgo/pkg/netutil"
)

// KEGGData KEGG 数据库
type KEGGData struct {
	Pathways map[string]*Pathway // pathway ID -> Pathway
	Species  string
}

// Pathway KEGG 通路
type Pathway struct {
	ID          string
	Name        string
	Genes       map[string]bool
	Category    string
	Description string
}

// DownloadKEGG 下载 KEGG 通路数据
func DownloadKEGG(species, outputDir string) (*KEGGData, error) {
	// KEGG REST API
	// 1. 获取物种列表
	// 2. 获取通路列表
	// 3. 获取通路基因映射

	// 示例: 获取人类 hsa 通路
	url := fmt.Sprintf("https://rest.kegg.jp/link/%s/pathway", species)

	client := netutil.DefaultClient()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch KEGG data: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("KEGG API error: %s", resp.Status)
	}

	data := &KEGGData{
		Pathways: make(map[string]*Pathway),
		Species:  species,
	}

	// 解析 KEGG link 数据
	// 格式: pathway:hsa00010	hsa:10327
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		pathwayID := strings.TrimPrefix(parts[0], "pathway:")
		geneID := parts[1]

		// 提取物种前缀后的基因 ID
		if idx := strings.Index(geneID, ":"); idx >= 0 {
			geneID = geneID[idx+1:]
		}

		// 创建或更新通路
		if data.Pathways[pathwayID] == nil {
			data.Pathways[pathwayID] = &Pathway{
				ID:    pathwayID,
				Genes: make(map[string]bool),
			}
		}
		data.Pathways[pathwayID].Genes[geneID] = true
	}

	// 获取通路描述
	if err := fetchKEGGPathwayInfo(data); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to fetch pathway info: %v\n", err)
	}

	// 下载并缓存 ID 映射，供离线 ID 转换使用
	if outputDir != "" {
		if err := fetchAndSaveKEGGIDMap(species, outputDir); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to cache KEGG ID mapping: %v\n", err)
		}
	}

	// 保存到文件
	if outputDir != "" {
		if err := saveKEGGData(data, outputDir); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// fetchKEGGPathwayInfo 获取通路描述信息
func fetchKEGGPathwayInfo(data *KEGGData) error {
	// 获取通路定义
	url := fmt.Sprintf("https://rest.kegg.jp/list/pathway/%s", data.Species)

	client := netutil.DefaultClient()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("KEGG API error: %s", resp.Status)
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) < 2 {
			continue
		}

		pathwayID := strings.TrimPrefix(parts[0], "pathway:")
		name := parts[1]

		if pw, ok := data.Pathways[pathwayID]; ok {
			pw.Name = name
		}
	}

	return nil
}

// LoadKEGG 加载本地 KEGG 数据
func LoadKEGG(species, dataDir string) (*KEGGData, error) {
	// 检查本地文件是否存在
	gmtFile := fmt.Sprintf("%s/%s.gmt", dataDir, species)

	if _, err := os.Stat(gmtFile); os.IsNotExist(err) {
		// 文件不存在，返回 nil，调用者会下载
		return nil, nil
	}

	// 加载 GMT 文件
	sets, err := LoadGMTFile(gmtFile)
	if err != nil {
		return nil, err
	}

	// 转换为 KEGGData
	data := &KEGGData{
		Pathways: make(map[string]*Pathway),
		Species:  species,
	}

	for _, gs := range sets {
		data.Pathways[gs.ID] = &Pathway{
			ID:          gs.ID,
			Name:        nameFromGMTGeneSet(gs),
			Genes:       gs.Genes,
			Description: gs.Description,
		}
	}

	return data, nil
}

// LoadOrDownloadKEGG 加载本地 KEGG 数据，如不存在则下载
func LoadOrDownloadKEGG(species, dataDir string) (*KEGGData, error) {
	// 尝试加载本地数据
	data, err := LoadKEGG(species, dataDir)
	if err != nil {
		return nil, err
	}

	if data != nil && len(data.Pathways) > 0 {
		return data, nil
	}

	// 本地数据不存在，下载
	return DownloadKEGG(species, dataDir)
}

// saveKEGGData 保存 KEGG 数据到文件
func saveKEGGData(data *KEGGData, outputDir string) error {
	// 创建 GMT 格式文件
	gmtFile := fmt.Sprintf("%s/%s.gmt", outputDir, data.Species)
	file, err := os.Create(gmtFile)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, pw := range data.Pathways {
		fmt.Fprintf(writer, "%s\t%s\t", pw.ID, pw.Name)
		genes := make([]string, 0, len(pw.Genes))
		for gene := range pw.Genes {
			genes = append(genes, gene)
		}
		fmt.Fprintln(writer, strings.Join(genes, "\t"))
	}
	writer.Flush()

	return nil
}

// GetSupportedKEGGSpecies 获取支持的 KEGG 物种
func GetSupportedKEGGSpecies() (map[string]string, error) {
	url := "https://rest.kegg.jp/list/organism"

	client := netutil.DefaultClient()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	species := make(map[string]string)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) < 3 {
			continue
		}
		// 格式: T_number   species_name   taxonomy_id
		orgCode := parts[0]
		orgName := parts[1]
		species[orgCode] = orgName
	}

	return species, nil
}

// fetchAndSaveKEGGIDMap 下载并保存 KEGG 物种 ID 映射（entrez -> symbol）。
func fetchAndSaveKEGGIDMap(species, outputDir string) error {
	url := fmt.Sprintf("https://rest.kegg.jp/list/%s", species)

	client := netutil.DefaultClient()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch KEGG gene list: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("KEGG API error: %s", resp.Status)
	}

	entrezToSymbol := make(map[string]string)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) < 2 {
			continue
		}
		entrez := strings.TrimPrefix(fields[0], species+":")
		if entrez == "" {
			continue
		}
		genesPart := fields[len(fields)-1]
		if idx := strings.Index(genesPart, ";"); idx >= 0 {
			genesPart = genesPart[:idx]
		}
		symbol := strings.TrimSpace(strings.Split(genesPart, ",")[0])
		if symbol == "" {
			continue
		}
		if _, ok := entrezToSymbol[entrez]; !ok {
			entrezToSymbol[entrez] = symbol
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(entrezToSymbol) == 0 {
		return fmt.Errorf("empty mapping returned from KEGG list")
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}
	path := filepath.Join(outputDir, fmt.Sprintf("kegg_%s_idmap.tsv", species))
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	entrezIDs := make([]string, 0, len(entrezToSymbol))
	for id := range entrezToSymbol {
		entrezIDs = append(entrezIDs, id)
	}
	sort.Strings(entrezIDs)

	w := bufio.NewWriter(file)
	for _, id := range entrezIDs {
		fmt.Fprintf(w, "%s\t%s\n", id, entrezToSymbol[id])
	}
	return w.Flush()
}
