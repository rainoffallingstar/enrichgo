package io

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// GeneInput 基因输入数据
type GeneInput struct {
	Genes          []string           // 基因列表 (ORA 显著基因, 或 GSEA 全部基因)
	GeneValues     map[string]float64 // 基因 -> 值 (GSEA 排名)
	AllGenes       []string           // 所有被检测基因 (ORA 背景/Universe)
	GeneDirections map[string]string  // 基因 -> 方向 (如 "Up"/"Down")
}

// ParseDiffGeneTableOptions 列选择与过滤选项
type ParseDiffGeneTableOptions struct {
	GeneCol      string  // 基因名列名（空字符串表示第一列）
	ValueCol     string  // 数值列名（用于 GSEA 排名，默认 "logFC"）
	FilterCol    string  // 过滤列名（用于 ORA 显著性过滤，空字符串表示不过滤）
	FilterVal    string  // 字符串匹配过滤值（如 "TRUE"）
	FilterThresh float64 // 数值过滤阈值（如 0.05）
	FilterMode   string  // "string"（匹配 FilterVal）或 "numeric_lte"（<=阈值）
	DirCol       string  // 方向列名（如 "direction"），空字符串表示不提取
}

// ParseGeneListFile 解析基因列表文件 (每行一个基因)
func ParseGeneListFile(filePath string) (*GeneInput, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	input := &GeneInput{
		Genes:      make([]string, 0),
		GeneValues: make(map[string]float64),
	}

	scanner := newWordScanner(file)
	for scanner.Scan() {
		gene := strings.TrimSpace(scanner.Text())
		if gene != "" && !strings.HasPrefix(gene, "#") {
			input.Genes = append(input.Genes, gene)
			input.GeneValues[gene] = 1.0 // 默认值
		}
	}

	return input, scanner.Err()
}

// ParseDiffGeneTable 解析差异基因表格
// 支持格式: gene, log2FC, pvalue 或 gene, log2FC, pvalue, padj
func ParseDiffGeneTable(filePath string, hasHeader bool) (*GeneInput, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = detectDelimiter(filePath)
	reader.FieldsPerRecord = -1

	input := &GeneInput{
		Genes:      make([]string, 0),
		GeneValues: make(map[string]float64),
	}

	firstLine := true
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// 跳过表头
		if firstLine && hasHeader {
			// 检测是否为表头
			if isHeader(record) {
				firstLine = false
				continue
			}
		}
		firstLine = false

		if len(record) < 2 {
			continue
		}

		gene := strings.TrimSpace(record[0])
		if gene == "" {
			continue
		}

		// 解析 log2FC 或其他数值
		var value float64 = 1.0
		if len(record) >= 2 {
			if v, err := strconv.ParseFloat(strings.TrimSpace(record[1]), 64); err == nil {
				value = v
			}
		}

		input.Genes = append(input.Genes, gene)
		input.GeneValues[gene] = value
	}

	return input, nil
}

// ParseDiffGeneTableWithOptions 解析差异基因表格，支持按列名选择数值列和过滤显著基因
//
// opts.GeneCol:     基因名列名（空字符串 = 第一列）
// opts.ValueCol:    数值列名（空字符串 = 第二列）
// opts.FilterCol:   过滤列名（空字符串 = 不过滤，返回全部基因）
// opts.FilterMode:  "string" 匹配 FilterVal；"numeric_lte" 数值 <= FilterThresh
//
// 返回值:
//
//	input.AllGenes  = 表格中全部基因（ORA Universe）
//	input.Genes     = 通过过滤的基因（ORA 显著基因）；若 FilterCol="" 则等于 AllGenes
//	input.GeneValues= 全部基因的数值（ValueCol 列）
func ParseDiffGeneTableWithOptions(filePath string, opts *ParseDiffGeneTableOptions) (*GeneInput, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = detectDelimiter(filePath)
	reader.FieldsPerRecord = -1

	input := &GeneInput{
		Genes:          make([]string, 0),
		GeneValues:     make(map[string]float64),
		AllGenes:       make([]string, 0),
		GeneDirections: make(map[string]string),
	}

	// 读取表头
	header, err := reader.Read()
	if err == io.EOF {
		return input, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}

	// 建立列名 -> 索引映射
	colIndex := make(map[string]int)
	for i, h := range header {
		colIndex[strings.TrimSpace(h)] = i
	}

	// 确定各列索引
	geneColIdx := 0
	if opts.GeneCol != "" {
		if idx, ok := colIndex[opts.GeneCol]; ok {
			geneColIdx = idx
		} else {
			return nil, fmt.Errorf("gene column %q not found in header", opts.GeneCol)
		}
	}

	valueColIdx := -1
	if opts.ValueCol != "" {
		if idx, ok := colIndex[opts.ValueCol]; ok {
			valueColIdx = idx
		} else {
			return nil, fmt.Errorf("value column %q not found in header", opts.ValueCol)
		}
	} else if len(header) > 1 {
		valueColIdx = 1 // 默认第二列
	}

	filterColIdx := -1
	if opts.FilterCol != "" {
		if idx, ok := colIndex[opts.FilterCol]; ok {
			filterColIdx = idx
		} else {
			return nil, fmt.Errorf("filter column %q not found in header", opts.FilterCol)
		}
	}

	dirColIdx := -1
	if opts.DirCol != "" {
		if idx, ok := colIndex[opts.DirCol]; ok {
			dirColIdx = idx
		}
		// 列不存在时静默跳过；调用方通过 GeneDirections 是否为空来感知
	}

	// 读取数据行
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if geneColIdx >= len(record) {
			continue
		}

		gene := strings.TrimSpace(record[geneColIdx])
		if gene == "" {
			continue
		}

		// 提取数值
		var value float64 = 1.0
		if valueColIdx >= 0 && valueColIdx < len(record) {
			if v, parseErr := strconv.ParseFloat(strings.TrimSpace(record[valueColIdx]), 64); parseErr == nil {
				value = v
			}
		}

		input.AllGenes = append(input.AllGenes, gene)
		input.GeneValues[gene] = value

		// 提取方向
		if dirColIdx >= 0 && dirColIdx < len(record) {
			input.GeneDirections[gene] = strings.TrimSpace(record[dirColIdx])
		}

		// 过滤判断
		passes := filterColIdx < 0 // 无过滤列时默认通过
		if filterColIdx >= 0 && filterColIdx < len(record) {
			cellVal := strings.TrimSpace(record[filterColIdx])
			switch opts.FilterMode {
			case "string":
				passes = cellVal == opts.FilterVal
			case "numeric_lte":
				if v, parseErr := strconv.ParseFloat(cellVal, 64); parseErr == nil {
					passes = v <= opts.FilterThresh
				}
			case "numeric_gte":
				if v, parseErr := strconv.ParseFloat(cellVal, 64); parseErr == nil {
					passes = v >= opts.FilterThresh
				}
			default:
				passes = cellVal == opts.FilterVal // 默认字符串匹配
			}
		}

		if passes {
			input.Genes = append(input.Genes, gene)
		}
	}

	// 无过滤时 Genes 等于 AllGenes
	if filterColIdx < 0 {
		input.Genes = input.AllGenes
	}

	return input, nil
}

// ParseRankedGeneFile 解析排序基因文件 (GSEA 格式)
// 格式: gene \t rank 或 rank \t gene
func ParseRankedGeneFile(filePath string) (*GeneInput, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	input := &GeneInput{
		Genes:      make([]string, 0),
		GeneValues: make(map[string]float64),
	}

	reader := csv.NewReader(file)
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// 跳过空行和注释
		if len(record) == 0 || strings.HasPrefix(record[0], "#") {
			continue
		}

		var gene string
		var value float64

		if len(record) == 1 {
			// 只有基因名
			gene = strings.TrimSpace(record[0])
			value = 1.0
		} else if len(record) >= 2 {
			// 尝试解析为 rank 或 value
			gene = strings.TrimSpace(record[0])

			// 尝试第二列为数值
			if v, err := strconv.ParseFloat(strings.TrimSpace(record[1]), 64); err == nil {
				value = v
			} else {
				// 可能是 rank 值
				if v, err := strconv.ParseFloat(strings.TrimSpace(record[1]), 64); err == nil {
					value = v
				}
			}
		}

		if gene != "" {
			input.Genes = append(input.Genes, gene)
			input.GeneValues[gene] = value
		}
	}

	return input, nil
}

// isHeader 检测是否为表头
func isHeader(record []string) bool {
	for _, field := range record {
		lower := strings.ToLower(field)
		if strings.Contains(lower, "gene") ||
			strings.Contains(lower, "log") ||
			strings.Contains(lower, "fold") ||
			strings.Contains(lower, "pvalue") ||
			strings.Contains(lower, "padj") ||
			strings.Contains(lower, "fdr") {
			return true
		}
	}
	return false
}

// detectDelimiter 检测文件分隔符
func detectDelimiter(filePath string) rune {
	file, err := os.Open(filePath)
	if err != nil {
		return '\t'
	}
	defer file.Close()

	// 读取第一行
	reader := csv.NewReader(file)
	record, err := reader.Read()
	if err != nil {
		return '\t'
	}

	// 统计分隔符
	tabCount := strings.Count(record[0], "\t")
	commaCount := strings.Count(record[0], ",")

	if tabCount > commaCount {
		return '\t'
	}
	return ','
}

// wordScanner 简单的词扫描器
type wordScanner struct {
	scanner *bufio.Scanner
}

func newWordScanner(r *os.File) *wordScanner {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanLines)
	return &wordScanner{
		scanner: scanner,
	}
}

func (s *wordScanner) Scan() bool {
	return s.scanner.Scan()
}

func (s *wordScanner) Text() string {
	return s.scanner.Text()
}

func (s *wordScanner) Err() error {
	return s.scanner.Err()
}

// OutputFormat 输出格式
type OutputFormat string

const (
	FormatTSV  OutputFormat = "tsv"
	FormatCSV  OutputFormat = "csv"
	FormatJSON OutputFormat = "json"
)

// WriteEnrichmentResults 写入富集结果
func WriteEnrichmentResults(results []*EnrichmentResult, filePath string, format OutputFormat) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	if format == FormatJSON {
		if results == nil {
			results = make([]*EnrichmentResult, 0)
		}
		enc := json.NewEncoder(file)
		enc.SetIndent("", "  ")
		return enc.Encode(results)
	}

	writer := csv.NewWriter(file)

	// 设置分隔符
	if format == FormatTSV {
		writer.Comma = '\t'
	}

	// 写入表头
	header := []string{"Direction", "ID", "Name", "GeneRatio", "BgRatio", "PValue", "PAdjust", "QValue", "Genes", "Count", "Description"}
	if err := writer.Write(header); err != nil {
		return err
	}

	// 写入数据
	for _, r := range results {
		row := []string{
			r.Direction,
			r.ID,
			r.Name,
			r.GeneRatio,
			r.BgRatio,
			fmt.Sprintf("%.2e", r.PValue),
			fmt.Sprintf("%.2e", r.PAdjust),
			fmt.Sprintf("%.2e", r.QValue),
			strings.Join(r.Genes, "/"),
			fmt.Sprintf("%d", r.Count),
			r.Description,
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	writer.Flush()
	return writer.Error()
}

// EnrichmentResult 为了避免循环引用，定义在这里
// 这个结构体与 analysis 包中的 EnrichmentResult 相同
type EnrichmentResult struct {
	Direction   string
	ID          string
	Name        string
	GeneRatio   string
	BgRatio     string
	PValue      float64
	PAdjust     float64
	QValue      float64
	Genes       []string
	Count       int
	Description string
}

// WriteGSEAResults 写入 GSEA 结果
func WriteGSEAResults(results []*GSEAResult, filePath string, format OutputFormat) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	if format == FormatJSON {
		if results == nil {
			results = make([]*GSEAResult, 0)
		}
		enc := json.NewEncoder(file)
		enc.SetIndent("", "  ")
		return enc.Encode(results)
	}

	writer := csv.NewWriter(file)
	if format == FormatTSV {
		writer.Comma = '\t'
	}

	// 写入表头
	header := []string{"ID", "Name", "NES", "PValue", "PAdjust", "QValue", "EnrichmentScore", "LeadGenes", "Description"}
	if err := writer.Write(header); err != nil {
		return err
	}

	// 写入数据
	for _, r := range results {
		row := []string{
			r.ID,
			r.Name,
			fmt.Sprintf("%.4f", r.NES),
			fmt.Sprintf("%.2e", r.PValue),
			fmt.Sprintf("%.2e", r.PAdjust),
			fmt.Sprintf("%.2e", r.QValue),
			fmt.Sprintf("%.4f", r.EnrichmentScore),
			strings.Join(r.LeadGenes, "/"),
			r.Description,
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	writer.Flush()
	return writer.Error()
}

type GSEAResult struct {
	ID              string
	Name            string
	NES             float64
	PValue          float64
	PAdjust         float64
	QValue          float64
	EnrichmentScore float64
	LeadGenes       []string
	Description     string
}
