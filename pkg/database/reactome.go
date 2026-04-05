package database

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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

// ReactomeDownloadOptions controls retry behavior for remote Reactome download.
type ReactomeDownloadOptions struct {
	AutoRetry    bool
	MaxRetries   int
	RetryBackoff time.Duration
}

func defaultReactomeDownloadOptions() ReactomeDownloadOptions {
	return ReactomeDownloadOptions{
		AutoRetry:    true,
		MaxRetries:   2,
		RetryBackoff: 10 * time.Second,
	}
}

func normalizeReactomeDownloadOptions(opts *ReactomeDownloadOptions) ReactomeDownloadOptions {
	n := defaultReactomeDownloadOptions()
	if opts == nil {
		return n
	}
	n.AutoRetry = opts.AutoRetry
	n.MaxRetries = opts.MaxRetries
	n.RetryBackoff = opts.RetryBackoff
	if n.MaxRetries < 0 {
		n.MaxRetries = 0
	}
	if n.RetryBackoff < 0 {
		n.RetryBackoff = 0
	}
	if !n.AutoRetry {
		n.MaxRetries = 0
	}
	return n
}

func emptyReactomeData(species string) *ReactomeData {
	return &ReactomeData{
		Pathways: make(map[string]*Pathway),
		Species:  species,
	}
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

var reactomeHTTPClient HTTPClient = netutil.NewClient(netutil.Options{Timeout: 5 * time.Minute})
var reactomeDownloadURL = "https://reactome.org/download/current/ReactomePathways.gmt.zip"

// DownloadReactome 下载 Reactome 通路数据。
// 默认启用自动重试（可通过 DownloadReactomeWithOptions 覆盖）。
func DownloadReactome(species, outputDir string) (*ReactomeData, error) {
	return DownloadReactomeWithOptions(species, outputDir, nil)
}

// DownloadReactomeWithOptions 下载 Reactome 通路数据，并按 options 控制自动重试。
func DownloadReactomeWithOptions(species, outputDir string, opts *ReactomeDownloadOptions) (*ReactomeData, error) {
	opt := normalizeReactomeDownloadOptions(opts)
	totalAttempts := opt.MaxRetries + 1
	var lastErr error

	for attempt := 1; attempt <= totalAttempts; attempt++ {
		data, err := downloadReactomeOnce(species, outputDir)
		if err == nil {
			if attempt > 1 {
				fmt.Fprintf(os.Stderr, "Info: Reactome download succeeded on retry %d/%d\n", attempt, totalAttempts)
			}
			return data, nil
		}

		lastErr = err
		if attempt == totalAttempts || !isRetryableReactomeError(err) {
			break
		}
		if opt.RetryBackoff > 0 {
			fmt.Fprintf(os.Stderr, "Warning: Reactome download attempt %d/%d failed: %v\n", attempt, totalAttempts, err)
			fmt.Fprintf(os.Stderr, "Warning: retrying Reactome download in %s...\n", opt.RetryBackoff)
			time.Sleep(opt.RetryBackoff)
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("reactome download failed without explicit error")
	}
	return emptyReactomeData(species), lastErr
}

func downloadReactomeOnce(species, outputDir string) (*ReactomeData, error) {
	req, _ := http.NewRequest(http.MethodGet, reactomeDownloadURL, nil)
	resp, err := reactomeHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to download Reactome data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to download Reactome data: HTTP %d", resp.StatusCode)
	}

	// 读取 zip 内容
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, fmt.Errorf("failed to read zip: %w", err)
	}

	// 查找 GMT 文件
	var gmtReader io.ReadCloser
	for _, file := range zipReader.File {
		if strings.HasSuffix(file.Name, ".gmt") {
			rc, openErr := file.Open()
			if openErr != nil {
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

	data := emptyReactomeData(species)

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
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to parse GMT: %w", err)
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

func isRetryableReactomeError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"tls handshake timeout",
		"i/o timeout",
		"timeout",
		"connection reset",
		"connection refused",
		"temporary failure",
		"temporarily unavailable",
		"unexpected eof",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	for _, code := range []string{"http 408", "http 429", "http 500", "http 502", "http 503", "http 504"} {
		if strings.Contains(msg, code) {
			return true
		}
	}
	return false
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

// LoadOrDownloadReactome 加载本地或下载 Reactome 数据。
// 默认启用自动重试（可通过 LoadOrDownloadReactomeWithOptions 覆盖）。
func LoadOrDownloadReactome(species, dataDir string) (*ReactomeData, error) {
	return LoadOrDownloadReactomeWithOptions(species, dataDir, nil)
}

// LoadOrDownloadReactomeWithOptions loads local data or downloads with custom options.
func LoadOrDownloadReactomeWithOptions(species, dataDir string, opts *ReactomeDownloadOptions) (*ReactomeData, error) {
	data, err := LoadReactome(species, dataDir)
	if err != nil {
		return nil, err
	}

	if data != nil && len(data.Pathways) > 0 {
		return data, nil
	}

	return DownloadReactomeWithOptions(species, dataDir, opts)
}
