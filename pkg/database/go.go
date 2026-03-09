package database

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// goSpeciesMap KEGG 物种代码到 GO 注释文件名的映射
var goSpeciesMap = map[string]string{
	"hsa": "goa_human",
	"mmu": "mgi",
	"rno": "rgd",
	"dre": "zfin",
	"dme": "fb",
	"cel": "wb",
	"sce": "sgd",
	"ath": "tair",
	"eco": "ecocyc",
	"bta": "goa_cow",
	"gga": "goa_chicken",
}

// GOData GO 数据库
type GOData struct {
	Terms      map[string]*GOTerm  // GO term ID -> Term
	Gene2Terms map[string][]string // 基因 -> GO terms
	Ontology   string              // BP, MF, CC
	Species    string
}

// GOTerm GO Term
type GOTerm struct {
	ID         string
	Name       string
	Ontology   string // BP, MF, CC
	Definition string
	Parents    []string // 父term
	Children   []string // 子term
}

// DownloadGO 下载 GO 注释数据
// 需要提供物种代码和数据库来源
func DownloadGO(species, ontology, outputDir string) (*GOData, error) {
	data := &GOData{
		Terms:      make(map[string]*GOTerm),
		Gene2Terms: make(map[string][]string),
		Ontology:   ontology,
		Species:    species,
	}

	// GO 数据可以从多个来源获取:
	// 1. Gene Ontology OBO 文件 (http://purl.obolibrary.org/obo/go/go-basic.obo)
	// 2. 物种注释 GAF 文件 (从AnnotationHub或GO官网)

	// 下载 GO 基础本体
	if err := fetchGOOntology(data); err != nil {
		return nil, fmt.Errorf("failed to fetch GO ontology: %v", err)
	}

	// 下载物种注释
	if err := fetchGOAnnotations(data, species); err != nil {
		return nil, fmt.Errorf("failed to fetch GO annotations: %v", err)
	}

	// 过滤特定本体
	if ontology != "" {
		filterByOntology(data, ontology)
	}

	// 保存到文件
	if outputDir != "" {
		if err := saveGOData(data, outputDir); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// fetchGOOntology 下载 GO 本体
func fetchGOOntology(data *GOData) error {
	url := "http://purl.obolibrary.org/obo/go/go-basic.obo"

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GO API error: %s", resp.Status)
	}

	return parseOBO(resp.Body, data)
}

// parseOBO 解析 OBO 格式文件
func parseOBO(r io.Reader, data *GOData) error {
	scanner := bufio.NewScanner(r)
	var currentTerm *GOTerm

	for scanner.Scan() {
		line := scanner.Text()

		// 跳过空行和注释
		if strings.HasPrefix(line, "!") || strings.TrimSpace(line) == "" {
			continue
		}

		// 检查新 term 开始
		if strings.HasPrefix(line, "[Term]") {
			if currentTerm != nil {
				data.Terms[currentTerm.ID] = currentTerm
			}
			currentTerm = &GOTerm{
				Parents:  make([]string, 0),
				Children: make([]string, 0),
			}
			continue
		}

		if currentTerm == nil {
			continue
		}

		// 解析 tag:value
		if idx := strings.Index(line, ": "); idx > 0 {
			tag := line[:idx]
			value := strings.TrimSpace(line[idx+2:])

			switch tag {
			case "id":
				currentTerm.ID = value
			case "name":
				currentTerm.Name = value
			case "namespace":
				currentTerm.Ontology = value
			case "def":
				currentTerm.Definition = value
			case "is_a":
				currentTerm.Parents = append(currentTerm.Parents, strings.Split(value, " ! ")[0])
			case "is_part_of":
				currentTerm.Parents = append(currentTerm.Parents, strings.Split(value, " ! ")[0])
			}
		}
	}

	// 保存最后一个 term
	if currentTerm != nil {
		data.Terms[currentTerm.ID] = currentTerm
	}

	return nil
}

// fetchGOAnnotations 下载物种 GO 注释
func fetchGOAnnotations(data *GOData, species string) error {
	// 将 KEGG 物种代码映射到 GO 注释文件名
	filename, ok := goSpeciesMap[species]
	if !ok {
		return fmt.Errorf("unsupported species for GO annotations: %s", species)
	}

	url := fmt.Sprintf("http://current.geneontology.org/annotations/%s.gaf.gz", filename)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to download GO annotations: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GO annotations API error: %s", resp.Status)
	}

	gr, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to decompress GO annotations: %v", err)
	}
	defer gr.Close()

	return parseGAF(gr, data)
}

// parseGAF 解析 GAF 格式文件
func parseGAF(r io.Reader, data *GOData) error {
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := scanner.Text()

		// 跳过注释和空行
		if strings.HasPrefix(line, "!") || strings.TrimSpace(line) == "" {
			continue
		}

		// GAF 格式字段 (以 tab 分隔)
		// 0: DB, 1: DB Object ID, 2: DB Object Symbol, 3: Qualifier, 4: GO ID, ...
		fields := strings.Split(line, "\t")
		if len(fields) < 5 {
			continue
		}

		// 使用 Gene Symbol 与常见 DEG 表输入保持一致；缺失时回退到 DB Object ID
		geneID := strings.TrimSpace(fields[2])
		if geneID == "" {
			geneID = strings.TrimSpace(fields[1])
		}
		goID := fields[4]      // GO ID
		qualifier := fields[3] // 限定符 (e.g., NOT)

		// 跳过 NOT 限定
		if strings.HasPrefix(qualifier, "NOT") {
			continue
		}

		// 添加注释
		data.Gene2Terms[geneID] = append(data.Gene2Terms[geneID], goID)
	}

	return nil
}

// filterByOntology 按本体过滤
func filterByOntology(data *GOData, ontology string) {
	// 确定 ontology 前缀
	ontMap := map[string]string{
		"BP": "biological_process",
		"MF": "molecular_function",
		"CC": "cellular_component",
	}
	ontFull, ok := ontMap[strings.ToUpper(ontology)]
	if !ok {
		ontFull = ontology
	}

	// 过滤基因到 term 的映射
	for gene, terms := range data.Gene2Terms {
		var filtered []string
		for _, term := range terms {
			if t, ok := data.Terms[term]; ok && t.Ontology == ontFull {
				filtered = append(filtered, term)
			}
		}
		data.Gene2Terms[gene] = filtered
	}
}

// saveGOData 保存 GO 数据
func saveGOData(data *GOData, outputDir string) error {
	// 保存为 GMT 格式 (GO term -> genes)
	gmtFile := fmt.Sprintf("%s/go_%s_%s.gmt", outputDir, data.Species, data.Ontology)
	file, err := os.Create(gmtFile)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for termID, term := range data.Terms {
		// 收集所有与此 term 相关的基因
		var genes []string
		for gene, terms := range data.Gene2Terms {
			for _, t := range terms {
				if t == termID {
					genes = append(genes, gene)
					break
				}
			}
		}

		if len(genes) > 0 {
			fmt.Fprintf(writer, "%s\t%s\t", termID, term.Name)
			fmt.Fprintln(writer, strings.Join(genes, "\t"))
		}
	}
	writer.Flush()

	return nil
}

// LoadGO 加载本地 GO 数据（GMT 格式）
func LoadGO(filePath string) (*GOData, error) {
	sets, err := LoadGMTFile(filePath)
	if err != nil {
		return nil, err
	}

	data := &GOData{
		Terms:      make(map[string]*GOTerm),
		Gene2Terms: make(map[string][]string),
	}

	for _, gs := range sets {
		data.Terms[gs.ID] = &GOTerm{
			ID:         gs.ID,
			Name:       gs.Name,
			Definition: gs.Description,
		}
		for gene := range gs.Genes {
			data.Gene2Terms[gene] = append(data.Gene2Terms[gene], gs.ID)
		}
	}

	return data, nil
}

// LoadOrDownloadGO 加载本地 GO 数据，如不存在则下载
func LoadOrDownloadGO(species, ontology, dataDir string) (*GOData, error) {
	gmtFile := fmt.Sprintf("%s/go_%s_%s.gmt", dataDir, species, ontology)

	if _, err := os.Stat(gmtFile); err == nil {
		data, err := LoadGO(gmtFile)
		if err != nil {
			return nil, err
		}
		if data != nil && len(data.Terms) > 0 {
			data.Species = species
			data.Ontology = ontology
			return data, nil
		}
	}

	return DownloadGO(species, ontology, dataDir)
}
