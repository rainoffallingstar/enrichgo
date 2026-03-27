package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type rRunOptions struct {
	Command          string
	Database         string
	Species          string
	Ontology         string
	Collection       string
	InputFile        string
	OutputFile       string
	DataDir          string
	NPerm            int
	Format           string
	SigCol           string
	SigVal           string
	FDRCol           string
	FDRThreshold     float64
	RankCol          string
	SplitByDirection bool
	DirCol           string
	UpVal            string
	DownVal          string
	LogFCCol         string
	LogFCThreshold   float64
}

type benchMetrics struct {
	Seconds  float64
	MaxRSSKB int
}

func ensureRReady() error {
	if _, err := exec.LookPath("Rscript"); err != nil {
		return errors.New("Rscript not found in PATH; please install R first")
	}

	checkScript := `pkgs <- c("clusterProfiler","org.Hs.eg.db","jsonlite")
missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly=TRUE)]
if (length(missing) > 0) {
  cat(paste(missing, collapse=","))
  quit(status=2)
}`
	cmd := exec.Command("Rscript", "-e", checkScript)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	msg := strings.TrimSpace(string(out))
	if msg == "" {
		return fmt.Errorf("failed to validate R packages: %w", err)
	}
	return fmt.Errorf("missing required R packages: %s", msg)
}

func runRMode(opts *rRunOptions) error {
	if err := ensureRReady(); err != nil {
		return err
	}
	if strings.ToLower(opts.Format) != "tsv" {
		return fmt.Errorf("--use-r currently supports --fmt=tsv only (got %q)", opts.Format)
	}
	db := strings.ToLower(opts.Database)
	if db == "custom" {
		return errors.New("--use-r/--benchmark does not support -d custom yet")
	}

	tmpOut, err := os.MkdirTemp("", "enrichgo-r-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpOut)

	rootDir, err := os.Getwd()
	if err != nil {
		return err
	}
	scriptPath := filepath.Join(rootDir, "scripts", "alignment", "clusterprofiler_baseline.R")
	if _, err := os.Stat(scriptPath); err != nil {
		return fmt.Errorf("R baseline script not found: %s", scriptPath)
	}

	selectedFile, err := resolveRResultFile(opts.Command, db)
	if err != nil {
		return err
	}

	env := os.Environ()
	env = append(env, "ALIGN_NPERM="+strconv.Itoa(opts.NPerm))
	env = append(env, "ALIGN_ONLY_ORA="+boolEnv(opts.Command == "enrich"))
	env = append(env, "ALIGN_SKIP_KEGG="+boolEnv(db != "kegg"))
	env = append(env, "ALIGN_INCLUDE_REACTOME="+boolEnv(db == "reactome"))
	env = append(env, "ALIGN_INCLUDE_MSIGDB="+boolEnv(db == "msigdb"))
	env = append(env, "ALIGN_MSIGDB_COLLECTIONS="+opts.Collection)
	env = append(env, "ALIGN_SIG_COL="+opts.SigCol)
	env = append(env, "ALIGN_SIG_VAL="+opts.SigVal)
	env = append(env, "ALIGN_FDR_COL="+opts.FDRCol)
	env = append(env, "ALIGN_FDR_THRESHOLD="+strconv.FormatFloat(opts.FDRThreshold, 'g', -1, 64))
	env = append(env, "ALIGN_RANK_COL="+opts.RankCol)
	env = append(env, "ALIGN_SPLIT_BY_DIRECTION="+boolEnv(opts.SplitByDirection))
	env = append(env, "ALIGN_DIR_COL="+opts.DirCol)
	env = append(env, "ALIGN_UP_VAL="+opts.UpVal)
	env = append(env, "ALIGN_DOWN_VAL="+opts.DownVal)
	env = append(env, "ALIGN_LOGFC_COL="+opts.LogFCCol)
	env = append(env, "ALIGN_LOGFC_THRESHOLD="+strconv.FormatFloat(opts.LogFCThreshold, 'g', -1, 64))

	keggGMT := filepath.Join(opts.DataDir, fmt.Sprintf("%s.gmt", opts.Species))
	keggIDMap := filepath.Join(opts.DataDir, fmt.Sprintf("kegg_%s_idmap.tsv", opts.Species))
	goGMT := filepath.Join(opts.DataDir, fmt.Sprintf("go_%s_%s.gmt", opts.Species, strings.ToUpper(opts.Ontology)))
	reactomeGMT := filepath.Join(opts.DataDir, fmt.Sprintf("reactome_%s.gmt", opts.Species))

	if fileExists(keggGMT) {
		env = append(env, "ALIGN_R_KEGG_GMT_FILE="+keggGMT)
	}
	if fileExists(keggIDMap) {
		env = append(env, "ALIGN_KEGG_IDMAP_TSV="+keggIDMap)
	}
	if fileExists(goGMT) {
		env = append(env, "ALIGN_R_GO_GMT_FILE="+goGMT)
	}

	if db == "reactome" {
		if !fileExists(reactomeGMT) {
			return fmt.Errorf("required file not found for Reactome R run: %s", reactomeGMT)
		}
		env = append(env, "ALIGN_R_REACTOME_GMT_FILE="+reactomeGMT)
	}
	if db == "msigdb" {
		msigdbGMT, err := buildMergedMSigDBGMT(opts.DataDir, opts.Collection, tmpOut)
		if err != nil {
			return err
		}
		env = append(env, "ALIGN_R_MSIGDB_GMT_FILE="+msigdbGMT)
	}

	cmd := exec.Command("Rscript", scriptPath, opts.InputFile, tmpOut)
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("R baseline failed: %w", err)
	}

	src := filepath.Join(tmpOut, selectedFile)
	if !fileExists(src) {
		return fmt.Errorf("expected R output not found: %s", src)
	}
	if err := copyFile(src, opts.OutputFile); err != nil {
		return err
	}
	return nil
}

func resolveRResultFile(command, db string) (string, error) {
	switch command {
	case "enrich":
		switch db {
		case "kegg":
			return "r_ora_kegg.tsv", nil
		case "go":
			return "r_ora_go.tsv", nil
		case "reactome":
			return "r_ora_reactome.tsv", nil
		case "msigdb":
			return "r_ora_msigdb.tsv", nil
		}
	case "gsea":
		switch db {
		case "kegg":
			return "r_gsea_kegg.tsv", nil
		case "go":
			return "r_gsea_go.tsv", nil
		case "reactome":
			return "r_gsea_reactome.tsv", nil
		case "msigdb":
			return "r_gsea_msigdb.tsv", nil
		}
	}
	return "", fmt.Errorf("unsupported command/database for R mode: %s/%s", command, db)
}

func runBenchmarkMode(command, database, outputFile, benchmarkOut string) error {
	exePath, err := os.Executable()
	if err != nil {
		return err
	}
	if benchmarkOut == "" {
		benchmarkOut = deriveOutputPath(outputFile, ".benchmark")
	}
	rOutput := deriveOutputPath(outputFile, ".r")

	baseArgs := append([]string{}, os.Args[1:]...)
	goArgs := append(baseArgs, "--benchmark=false", "--use-r=false", "-o", outputFile)
	rArgs := append(baseArgs, "--benchmark=false", "--use-r=true", "-o", rOutput)

	tmpOut, err := os.MkdirTemp("", "enrichgo-bench-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpOut)
	goMetricsPath := filepath.Join(tmpOut, "go_kegg_id_cache_metrics.tsv")

	fmt.Printf("Benchmarking Go implementation (%s/%s)...\n", command, database)
	goMetrics, err := runCommandWithMetrics(exePath, goArgs, []string{envKEGGIDCacheMetricsTSV + "=" + goMetricsPath})
	if err != nil {
		return fmt.Errorf("Go benchmark run failed: %w", err)
	}
	fmt.Printf("Benchmarking R implementation (%s/%s)...\n", command, database)
	rMetrics, err := runCommandWithMetrics(exePath, rArgs, nil)
	if err != nil {
		return fmt.Errorf("R benchmark run failed: %w", err)
	}

	cacheSt, _ := readKEGGIDCacheMetricsTSV(goMetricsPath)
	content := "impl\tcommand\tdb\tseconds\tmax_rss_kb\tkegg_id_cache_hits\tkegg_id_cache_misses\tkegg_id_cache_evictions\tkegg_id_cache_entries\tkegg_id_cache_buckets\tkegg_id_cache_max_entries\toutput\n"
	content += fmt.Sprintf("go\t%s\t%s\t%.6f\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%s\n",
		command, database, goMetrics.Seconds, goMetrics.MaxRSSKB,
		cacheSt.Hits, cacheSt.Misses, cacheSt.Evictions, cacheSt.Entries, cacheSt.Buckets, cacheSt.MaxEntries,
		outputFile,
	)
	content += fmt.Sprintf("r\t%s\t%s\t%.6f\t%d\t0\t0\t0\t0\t0\t0\t%s\n", command, database, rMetrics.Seconds, rMetrics.MaxRSSKB, rOutput)
	if err := os.WriteFile(benchmarkOut, []byte(content), 0644); err != nil {
		return err
	}
	fmt.Printf("Benchmark report written to %s\n", benchmarkOut)
	return nil
}

func runCommandWithMetrics(binary string, args []string, extraEnv []string) (*benchMetrics, error) {
	start := time.Now()
	cmd := exec.Command(binary, args...)
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	metrics := &benchMetrics{
		Seconds:  time.Since(start).Seconds(),
		MaxRSSKB: -1,
	}
	if maxKB, ok := maxRSSKBFromSysUsage(cmd.ProcessState.SysUsage()); ok {
		metrics.MaxRSSKB = maxKB
	}
	return metrics, nil
}

type keggIDCacheMetrics struct {
	Hits       uint64
	Misses     uint64
	Evictions  uint64
	Entries    uint64
	Buckets    uint64
	MaxEntries int64
}

func readKEGGIDCacheMetricsTSV(path string) (keggIDCacheMetrics, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return keggIDCacheMetrics{}, err
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) < 2 {
		return keggIDCacheMetrics{}, fmt.Errorf("invalid metrics tsv (expected header+row)")
	}
	parts := strings.Split(lines[1], "\t")
	if len(parts) < 6 {
		return keggIDCacheMetrics{}, fmt.Errorf("invalid metrics tsv row (expected 6 cols)")
	}
	parse := func(s string) (uint64, error) {
		s = strings.TrimSpace(s)
		if s == "" {
			return 0, nil
		}
		return strconv.ParseUint(s, 10, 64)
	}
	h, err := parse(parts[0])
	if err != nil {
		return keggIDCacheMetrics{}, err
	}
	m, err := parse(parts[1])
	if err != nil {
		return keggIDCacheMetrics{}, err
	}
	e, err := parse(parts[2])
	if err != nil {
		return keggIDCacheMetrics{}, err
	}
	en, err := parse(parts[3])
	if err != nil {
		return keggIDCacheMetrics{}, err
	}
	bk, err := parse(parts[4])
	if err != nil {
		return keggIDCacheMetrics{}, err
	}
	mxRaw := strings.TrimSpace(parts[5])
	var mx int64
	if mxRaw != "" {
		mx, err = strconv.ParseInt(mxRaw, 10, 64)
		if err != nil {
			return keggIDCacheMetrics{}, err
		}
	}
	return keggIDCacheMetrics{
		Hits:       h,
		Misses:     m,
		Evictions:  e,
		Entries:    en,
		Buckets:    bk,
		MaxEntries: mx,
	}, nil
}

func maxRSSKBFromSysUsage(u any) (int, bool) {
	ru, ok := u.(*syscall.Rusage)
	if !ok || ru == nil {
		return 0, false
	}

	// syscall.Rusage differs across platforms; avoid direct field access (e.g. Windows lacks Maxrss).
	v := reflect.ValueOf(ru).Elem()
	f := v.FieldByName("Maxrss")
	if !f.IsValid() {
		return 0, false
	}

	var max int64
	switch f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		max = f.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		max = int64(f.Uint())
	default:
		return 0, false
	}
	if max <= 0 {
		return 0, false
	}

	// Normalize to KB.
	if runtime.GOOS == "darwin" {
		return int(max / 1024), true
	}
	if runtime.GOOS == "linux" {
		return int(max), true
	}

	// Fallback heuristic for other platforms.
	maxRSSKB := int(max)
	if maxRSSKB > 0 && maxRSSKB < 1024 {
		return maxRSSKB, true
	}
	if maxRSSKB > 1024*1024*64 {
		return maxRSSKB / 1024, true
	}
	return maxRSSKB, true
}

func deriveOutputPath(path, suffix string) string {
	ext := filepath.Ext(path)
	base := strings.TrimSuffix(path, ext)
	if ext == "" {
		return base + suffix + ".tsv"
	}
	return base + suffix + ext
}

func boolEnv(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func buildMergedMSigDBGMT(dataDir, rawCollections, outDir string) (string, error) {
	cols, err := parseMSigDBCollections(rawCollections)
	if err != nil {
		return "", err
	}
	mergedPath := filepath.Join(outDir, "msigdb_merged.gmt")
	seen := make(map[string]bool)
	var lines []string
	for _, col := range cols {
		path := filepath.Join(dataDir, fmt.Sprintf("msigdb_%s.gmt", string(col)))
		if !fileExists(path) {
			return "", fmt.Errorf("required MSigDB cache file not found: %s", path)
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		for _, line := range strings.Split(string(content), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			id := strings.SplitN(line, "\t", 2)[0]
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			lines = append(lines, line)
		}
	}
	if err := os.WriteFile(mergedPath, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
		return "", err
	}
	return mergedPath, nil
}
