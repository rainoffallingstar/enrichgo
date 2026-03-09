package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type rRunOptions struct {
	Command    string
	Database   string
	Species    string
	Ontology   string
	Collection string
	InputFile  string
	OutputFile string
	DataDir    string
	NPerm      int
	Format     string
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
	env = append(env, "ALIGN_FDR_COL=FDR")
	env = append(env, "ALIGN_RANK_COL=logFC")

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

	fmt.Printf("Benchmarking Go implementation (%s/%s)...\n", command, database)
	goMetrics, err := runCommandWithMetrics(exePath, goArgs)
	if err != nil {
		return fmt.Errorf("Go benchmark run failed: %w", err)
	}
	fmt.Printf("Benchmarking R implementation (%s/%s)...\n", command, database)
	rMetrics, err := runCommandWithMetrics(exePath, rArgs)
	if err != nil {
		return fmt.Errorf("R benchmark run failed: %w", err)
	}

	content := "impl\tcommand\tdb\tseconds\tmax_rss_kb\toutput\n"
	content += fmt.Sprintf("go\t%s\t%s\t%.6f\t%d\t%s\n", command, database, goMetrics.Seconds, goMetrics.MaxRSSKB, outputFile)
	content += fmt.Sprintf("r\t%s\t%s\t%.6f\t%d\t%s\n", command, database, rMetrics.Seconds, rMetrics.MaxRSSKB, rOutput)
	if err := os.WriteFile(benchmarkOut, []byte(content), 0644); err != nil {
		return err
	}
	fmt.Printf("Benchmark report written to %s\n", benchmarkOut)
	return nil
}

func runCommandWithMetrics(binary string, args []string) (*benchMetrics, error) {
	start := time.Now()
	cmd := exec.Command(binary, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	metrics := &benchMetrics{
		Seconds:  time.Since(start).Seconds(),
		MaxRSSKB: -1,
	}
	if usage, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
		maxRSSKB := int(usage.Maxrss)
		// On macOS Maxrss is bytes; Linux is KB. Heuristic keeps values consistent in KB.
		if maxRSSKB > 0 && maxRSSKB < 1024 {
			metrics.MaxRSSKB = maxRSSKB
		} else if maxRSSKB > 1024*1024*64 {
			metrics.MaxRSSKB = maxRSSKB / 1024
		} else {
			metrics.MaxRSSKB = maxRSSKB
		}
	}
	return metrics, nil
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
