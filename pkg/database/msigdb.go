package database

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"enrichgo/pkg/netutil"
	"enrichgo/pkg/types"
)

// GeneSet 是 types.GeneSet 的别名
type GeneSet = types.GeneSet

// GeneSets 是 types.GeneSets 的别名
type GeneSets = types.GeneSets

// MSigDBCollection MSigDB 集合
type MSigDBCollection string

const (
	MSigDBH  MSigDBCollection = "h"  // Hallmark
	MSigDBC1 MSigDBCollection = "c1" // Positional
	MSigDBC2 MSigDBCollection = "c2" // Curated
	MSigDBC3 MSigDBCollection = "c3" // Motif
	MSigDBC4 MSigDBCollection = "c4" // Computational
	MSigDBC5 MSigDBCollection = "c5" // GO
	MSigDBC6 MSigDBCollection = "c6" // Oncogenic
	MSigDBC7 MSigDBCollection = "c7" // Immunologic
	MSigDBC8 MSigDBCollection = "c8" // Cell Type
)

var msigdbReleaseFallbacks = []string{"2024.1.Hs", "2023.2.Hs", "7.5.1"}
var msigdbHTTPClient = netutil.NewClient(netutil.Options{Timeout: 2 * time.Minute})

func msigdbURLCandidates(collection MSigDBCollection) []string {
	collectionName := string(collection)
	candidates := make([]string, 0, len(msigdbReleaseFallbacks))
	for _, release := range msigdbReleaseFallbacks {
		// Broad release naming: <collection>.all.v<release>.Hs.symbols.gmt
		candidates = append(candidates, fmt.Sprintf(
			"https://data.broadinstitute.org/gsea-msigdb/msigdb/release/%s/%s.all.v%s.symbols.gmt",
			release, collectionName, release,
		))
	}
	return candidates
}

// DownloadMSigDB 下载 MSigDB 数据
func DownloadMSigDB(collection MSigDBCollection, outputDir string) (GeneSets, error) {
	var body []byte
	var lastErr error
	var usedURL string
	for _, url := range msigdbURLCandidates(collection) {
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		resp, err := msigdbHTTPClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("fetch failed for %s: %v", url, err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("MSigDB API error for %s: %s", url, resp.Status)
			resp.Body.Close()
			continue
		}
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("failed to read MSigDB response from %s: %v", url, err)
			continue
		}
		usedURL = url
		lastErr = nil
		break
	}
	if lastErr != nil {
		return nil, lastErr
	}

	sets, err := parseGMT(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	if outputDir != "" {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return nil, err
		}
		cacheFile := msigdbFilePath(collection, outputDir)
		if writeErr := os.WriteFile(cacheFile, body, 0644); writeErr != nil {
			return nil, writeErr
		}
	}
	fmt.Fprintf(os.Stderr, "MSigDB %s downloaded from %s (%d sets)\n", collection, usedURL, len(sets))

	return sets, nil
}

// DownloadMSigDBAll 下载所有 MSigDB 集合
func DownloadMSigDBAll(outputDir string) (GeneSets, error) {
	collections := []MSigDBCollection{
		MSigDBH, MSigDBC1, MSigDBC2, MSigDBC3, MSigDBC4,
		MSigDBC5, MSigDBC6, MSigDBC7, MSigDBC8,
	}

	var allSets GeneSets
	for _, col := range collections {
		sets, err := DownloadMSigDB(col, outputDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to download %s: %v\n", col, err)
			continue
		}
		allSets = append(allSets, sets...)
	}

	return allSets, nil
}

// DefaultMSigDBCollections 默认 MSigDB 集合（c1-c8）
func DefaultMSigDBCollections() []MSigDBCollection {
	return []MSigDBCollection{
		MSigDBC1, MSigDBC2, MSigDBC3, MSigDBC4,
		MSigDBC5, MSigDBC6, MSigDBC7, MSigDBC8,
	}
}

func msigdbFilePath(collection MSigDBCollection, dataDir string) string {
	return filepath.Join(dataDir, fmt.Sprintf("msigdb_%s.gmt", collection))
}

// LoadMSigDB 从本地缓存加载 MSigDB 集合
func LoadMSigDB(collection MSigDBCollection, dataDir string) (GeneSets, error) {
	cacheFile := msigdbFilePath(collection, dataDir)
	if _, err := os.Stat(cacheFile); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return LoadGMTFile(cacheFile)
}

// LoadOrDownloadMSigDB 加载本地缓存，如不存在则下载
func LoadOrDownloadMSigDB(collection MSigDBCollection, dataDir string) (GeneSets, error) {
	sets, err := LoadMSigDB(collection, dataDir)
	if err != nil {
		return nil, err
	}
	if len(sets) > 0 {
		return sets, nil
	}
	return DownloadMSigDB(collection, dataDir)
}

// LoadOrDownloadMSigDBCollections 加载多个 MSigDB 集合并去重
func LoadOrDownloadMSigDBCollections(collections []MSigDBCollection, dataDir string) (GeneSets, error) {
	if len(collections) == 0 {
		return nil, fmt.Errorf("no MSigDB collections provided")
	}

	var merged GeneSets
	seen := make(map[string]bool)
	for _, col := range collections {
		sets, err := LoadOrDownloadMSigDB(col, dataDir)
		if err != nil {
			return nil, err
		}
		for _, gs := range sets {
			if seen[gs.ID] {
				continue
			}
			seen[gs.ID] = true
			merged = append(merged, gs)
		}
	}

	sort.Slice(merged, func(i, j int) bool { return merged[i].ID < merged[j].ID })
	return merged, nil
}

// parseGMT 解析 GMT 格式
func parseGMT(r io.Reader) (GeneSets, error) {
	var results GeneSets

	scanner := bufio.NewScanner(r)
	// GO/Reactome 等 GMT 可能包含超长行，放大 scanner 缓冲避免 token too long。
	const maxCapacity = 16 * 1024 * 1024 // 16MB per line
	scanner.Buffer(make([]byte, 64*1024), maxCapacity)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "\t")

		if len(fields) < 3 {
			continue
		}

		// GMT 格式: gene_set_name \t description \t gene1 \t gene2 \t ...
		// 或: gene_set_name \t NA \t gene1 \t gene2 \t ...
		gs := &GeneSet{
			ID:    fields[0],
			Name:  fields[0],
			Genes: make(map[string]bool),
		}

		// 描述可能在第二列，如果没有则跳过
		if len(fields) > 1 && fields[1] != "NA" {
			gs.Description = fields[1]
		}

		// 基因从第三列开始
		for i := 2; i < len(fields); i++ {
			if fields[i] != "" {
				gs.Genes[fields[i]] = true
			}
		}

		results = append(results, gs)
	}

	return results, scanner.Err()
}

// LoadGMTFile 加载 GMT 文件
func LoadGMTFile(filePath string) (GeneSets, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 检测是否 gzip 压缩
	reader := io.Reader(file)
	if strings.HasSuffix(filePath, ".gz") {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, err
		}
		defer gzReader.Close()
		reader = gzReader
	}

	return parseGMT(reader)
}

// SaveGMTFile 保存为 GMT 文件
func SaveGMTFile(sets GeneSets, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, gs := range sets {
		fmt.Fprintf(writer, "%s\t%s\t", gs.ID, gs.Name)
		genes := make([]string, 0, len(gs.Genes))
		for gene := range gs.Genes {
			genes = append(genes, gene)
		}
		fmt.Fprintln(writer, strings.Join(genes, "\t"))
	}
	writer.Flush()

	return nil
}
