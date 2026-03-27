package database

import (
	"archive/zip"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"enrichgo/pkg/netutil"
)

// ReactomeData Reactome 数据库
type ReactomeData struct {
	Pathways map[string]*Pathway
	Species  string
}

// ReactomeSpeciesMap Reactome 物种代码映射 (Reactome 使用全名作为 ID 前缀)
var ReactomeSpeciesMap = map[string]string{
	"hsa": "Homo sapiens",
	"mmu": "Mus musculus",
	"rno": "Rattus norvegicus",
	"dre": "Danio rerio",
	"cel": "Caenorhabditis elegans",
	"dme": "Drosophila melanogaster",
	"ath": "Arabidopsis thaliana",
	"sce": "Saccharomyces cerevisiae",
	"bta": "Bos taurus",
	"gga": "Gallus gallus",
}

var reactomeSpeciesPrefixMap = map[string]string{
	"hsa": "R-HSA",
	"mmu": "R-MMU",
	"rno": "R-RNO",
	"dre": "R-DRE",
	"cel": "R-CEL",
	"dme": "R-DME",
	"ath": "R-ATH",
	"sce": "R-SCE",
	"bta": "R-BTA",
	"gga": "R-GGA",
}

var reactomeHTTPClient = netutil.NewClient(netutil.Options{Timeout: 5 * time.Minute})

// DownloadReactome 下载 Reactome 通路数据
func DownloadReactome(species, outputDir string) (*ReactomeData, error) {
	// 下载 Reactome GMT zip 文件 (包含所有物种)
	url := "https://reactome.org/download/current/ReactomePathways.gmt.zip"

	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := reactomeHTTPClient.Do(req)
	if err != nil {
		return &ReactomeData{
			Pathways: make(map[string]*Pathway),
			Species:  species,
		}, fmt.Errorf("failed to download Reactome data: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return &ReactomeData{
			Pathways: make(map[string]*Pathway),
			Species:  species,
		}, fmt.Errorf("failed to download Reactome data: HTTP %d", resp.StatusCode)
	}

	// 读取 zip 内容
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, fmt.Errorf("failed to read zip: %v", err)
	}

	// 查找 GMT 文件
	var gmtReader io.ReadCloser
	for _, file := range zipReader.File {
		if strings.HasSuffix(file.Name, ".gmt") {
			rc, err := file.Open()
			if err != nil {
				continue
			}
			gmtReader = rc
			break
		}
	}

	if gmtReader == nil {
		return nil, fmt.Errorf("no GMT file found in zip")
	}
	defer gmtReader.Close()

	// 获取物种前缀 (Reactome pathway ID: R-HSA-xxxxx)
	speciesPrefix := reactomeSpeciesPrefixMap[strings.ToLower(species)]

	data := &ReactomeData{
		Pathways: make(map[string]*Pathway),
		Species:  species,
	}

	// 解析 GMT 文件，只保留指定物种
	scanner := bufio.NewScanner(gmtReader)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "\t")
		if len(fields) < 3 {
			continue
		}

		// Reactome GMT 格式: pathway_name\tR-HSA-xxxxx\tgene1\tgene2\t...
		// 与常见 GMT 不同：第一列是名称，第二列是稳定 ID。
		pathwayName := fields[0]
		pathwayID := fields[1]

		// 过滤物种
		if speciesPrefix != "" && !strings.HasPrefix(pathwayID, speciesPrefix) {
			continue
		}

		genes := make(map[string]bool)
		for i := 2; i < len(fields); i++ {
			if fields[i] != "" {
				genes[fields[i]] = true
			}
		}

		data.Pathways[pathwayID] = &Pathway{
			ID:          pathwayID,
			Name:        pathwayName,
			Genes:       genes,
			Description: pathwayName,
		}
	}

	// 保存到文件
	if outputDir != "" {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return data, err
		}
		gmtFile := filepath.Join(outputDir, fmt.Sprintf("reactome_%s.gmt", species))
		if err := SaveGMTFileFromPathways(data.Pathways, gmtFile); err != nil {
			return data, err
		}
	}

	return data, nil
}

// SaveGMTFileFromPathways 保存为 GMT 文件
func SaveGMTFileFromPathways(pathways map[string]*Pathway, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, pw := range pathways {
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

// LoadReactome 加载本地 Reactome 数据
func LoadReactome(species, dataDir string) (*ReactomeData, error) {
	// 检查本地文件
	gmtFile := filepath.Join(dataDir, fmt.Sprintf("reactome_%s.gmt", species))

	if _, err := os.Stat(gmtFile); os.IsNotExist(err) {
		return nil, nil
	}

	sets, err := LoadGMTFile(gmtFile)
	if err != nil {
		return nil, err
	}

	data := &ReactomeData{
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

// LoadOrDownloadReactome 加载本地或下载 Reactome 数据
func LoadOrDownloadReactome(species, dataDir string) (*ReactomeData, error) {
	data, err := LoadReactome(species, dataDir)
	if err != nil {
		return nil, err
	}

	if data != nil && len(data.Pathways) > 0 {
		return data, nil
	}

	return DownloadReactome(species, dataDir)
}
