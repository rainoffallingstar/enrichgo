package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"enrichgo/pkg/annotation"
	"enrichgo/pkg/database"
	"enrichgo/pkg/store"
)

func effectiveIDMapsResume(resume, forceRefresh bool) bool {
	if forceRefresh {
		return false
	}
	return resume
}

func writeIDMapsToSQLiteWithRetryConfig(
	st *store.SQLiteStore,
	species, level string,
	attemptTimeout time.Duration,
	retries int,
	backoff time.Duration,
	client database.HTTPClient,
	resume bool,
	localIDMapDir string,
) error {
	if st == nil {
		return fmt.Errorf("sqlite store is nil")
	}
	if attemptTimeout <= 0 {
		return fmt.Errorf("invalid idmaps timeout %s", attemptTimeout)
	}
	if retries < 0 {
		return fmt.Errorf("invalid idmaps retries %d", retries)
	}
	if backoff < 0 {
		return fmt.Errorf("invalid idmaps retry backoff %s", backoff)
	}

	level = strings.ToLower(strings.TrimSpace(level))
	if level == "" {
		level = "basic"
	}
	if level != "basic" && level != "extended" {
		return fmt.Errorf("unknown --idmaps-level %q (use basic or extended)", level)
	}

	totalAttempts := retries + 1
	var lastErr error
	for attempt := 1; attempt <= totalAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
		var err error
		switch level {
		case "basic":
			err = writeBasicIDMapsToSQLite(attemptCtx, st, species)
			if err != nil {
				fallbackErr := writeBasicIDMapsFromLocalTSV(attemptCtx, st, species, localIDMapDir)
				if fallbackErr == nil {
					fmt.Fprintf(os.Stderr, "Info: basic idmaps online failed (%v); local TSV fallback succeeded\n", err)
					err = nil
				} else {
					err = fmt.Errorf("online basic idmaps failed: %w; local fallback failed: %v", err, fallbackErr)
				}
			}
		case "extended":
			err = writeExtendedIDMapsToSQLiteWithResume(attemptCtx, st, species, client, resume)
		}
		cancel()

		if err == nil {
			if attempt > 1 {
				fmt.Fprintf(os.Stderr, "Info: idmaps sync succeeded on retry %d/%d\n", attempt, totalAttempts)
			}
			return nil
		}

		lastErr = err
		if attempt == totalAttempts || !isRetryableIDMapError(err) {
			break
		}
		fmt.Fprintf(os.Stderr, "Warning: idmaps sync attempt %d/%d failed: %v\n", attempt, totalAttempts, err)
		if backoff > 0 {
			fmt.Fprintf(os.Stderr, "Warning: retrying idmaps sync in %s...\n", backoff)
			time.Sleep(backoff)
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("idmaps sync failed without explicit error")
	}
	return lastErr
}

func writeExtendedIDMapsToSQLiteWithResume(ctx context.Context, st *store.SQLiteStore, species string, client database.HTTPClient, resume bool) error {
	taxID, err := database.TaxIDForSpecies(species)
	if err != nil {
		return err
	}

	for _, step := range buildExtendedIDMapSteps(species, taxID, client) {
		if err := runExtendedIDMapSource(ctx, st, species, step.source, step.fromType, step.toType, resume, step.produce); err != nil {
			return err
		}
		logExtendedIDMapStepStats(step)
	}

	if err := writeKEGGFallbackIDMapsBestEffort(ctx, species, 2, 5*time.Second, func(innerCtx context.Context, sp string) error {
		return writeBasicIDMapsToSQLite(innerCtx, st, sp)
	}); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to write KEGG fallback idmaps: %v\n", err)
	}
	return nil
}

func runExtendedIDMapSource(
	ctx context.Context,
	st *store.SQLiteStore,
	species, source, fromType, toType string,
	resume bool,
	produce func(emit store.IDMapEmit) error,
) error {
	if resume {
		n, err := st.CountIDMapScope(ctx, species, source, fromType, toType)
		if err != nil {
			return err
		}
		if n > 0 {
			fmt.Fprintf(os.Stderr, "Info: idmaps resume skip source=%s from=%s rows=%d\n", source, fromType, n)
			return nil
		}
	}
	return st.ReplaceIDMapStream(ctx, species, source, fromType, toType, produce)
}

func writeBasicIDMapsFromLocalTSV(ctx context.Context, st *store.SQLiteStore, species, preferredDir string) error {
	path, err := findLocalKEGGIDMapTSV(species, preferredDir, "data")
	if err != nil {
		return err
	}

	symbolToEntrez, entrezToSymbol, seen, dropped, err := parseKEGGIDMapTSV(path, species)
	if err != nil {
		return err
	}
	if len(symbolToEntrez) == 0 || len(entrezToSymbol) == 0 {
		return fmt.Errorf("empty idmap parsed from %s", path)
	}

	symbolRows := make([]store.IDMapRow, 0, len(symbolToEntrez))
	for sym, entrez := range symbolToEntrez {
		symbolRows = append(symbolRows, store.IDMapRow{From: sym, To: entrez})
	}
	if err := st.ReplaceIDMap(ctx, species, "kegg_list", string(annotation.IDSymbol), string(annotation.IDEntrez), symbolRows); err != nil {
		return err
	}

	entrezRows := make([]store.IDMapRow, 0, len(entrezToSymbol))
	for entrez, sym := range entrezToSymbol {
		entrezRows = append(entrezRows, store.IDMapRow{From: entrez, To: sym})
	}
	if err := st.ReplaceIDMap(ctx, species, "kegg_list", string(annotation.IDEntrez), string(annotation.IDSymbol), entrezRows); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "Info: imported local KEGG idmap from %s symbol_to_entrez=%d entrez_to_symbol=%d seen=%d dropped=%d\n",
		path, len(symbolRows), len(entrezRows), seen, dropped)
	return nil
}

func findLocalKEGGIDMapTSV(species string, dirs ...string) (string, error) {
	name := fmt.Sprintf("kegg_%s_idmap.tsv", strings.ToLower(strings.TrimSpace(species)))
	seen := make(map[string]struct{})
	for _, dir := range dirs {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			continue
		}
		abs, err := filepath.Abs(dir)
		if err != nil {
			continue
		}
		if _, ok := seen[abs]; ok {
			continue
		}
		seen[abs] = struct{}{}
		path := filepath.Join(abs, name)
		if st, err := os.Stat(path); err == nil && !st.IsDir() {
			return path, nil
		}
	}
	return "", fmt.Errorf("local KEGG idmap TSV not found for species=%s in dirs=%v", species, dirs)
}

func parseKEGGIDMapTSV(path, species string) (symbolToEntrez map[string]string, entrezToSymbol map[string]string, seen int64, dropped int64, err error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	defer file.Close()

	symbolToEntrez = make(map[string]string)
	entrezToSymbol = make(map[string]string)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		seen++
		fields := strings.Split(line, "\t")
		if len(fields) < 2 {
			dropped++
			continue
		}
		entrez := strings.TrimSpace(fields[0])
		symbol := strings.TrimSpace(fields[len(fields)-1])
		speciesKey := strings.ToLower(strings.TrimSpace(species))
		if speciesKey != "" && strings.HasPrefix(strings.ToLower(entrez), speciesKey+":") {
			entrez = strings.TrimSpace(entrez[len(speciesKey)+1:])
		} else if idx := strings.Index(entrez, ":"); idx >= 0 {
			entrez = strings.TrimSpace(entrez[idx+1:])
		}
		if strings.HasPrefix(strings.ToLower(entrez), "ncbi-geneid:") {
			entrez = strings.TrimSpace(entrez[len("ncbi-geneid:"):])
		}
		if entrez == "" || symbol == "" {
			dropped++
			continue
		}
		if _, ok := entrezToSymbol[entrez]; !ok {
			entrezToSymbol[entrez] = symbol
		}
		symKey := strings.ToUpper(symbol)
		if _, ok := symbolToEntrez[symKey]; !ok {
			symbolToEntrez[symKey] = entrez
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return nil, nil, seen, dropped, scanErr
	}
	if len(entrezToSymbol) == 0 {
		return nil, nil, seen, dropped, fmt.Errorf("empty mapping file: %s", path)
	}
	return symbolToEntrez, entrezToSymbol, seen, dropped, nil
}
