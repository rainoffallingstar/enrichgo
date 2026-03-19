package database

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var uniProtOrgByTaxID = map[int]string{
	9606:   "HUMAN",
	10090:  "MOUSE",
	10116:  "RAT",
	7955:   "DANRE",
	7227:   "DROME",
	6239:   "CAEEL",
	559292: "YEAST",
	3702:   "ARATH",
	9913:   "BOVIN",
	9031:   "CHICK",
	562:    "ECOLI",
}

// StreamUniProtIDMappingSelected emits UNIPROT->ENTREZ and ENTREZ->UNIPROT pairs using UniProt idmapping_selected.tab.gz.
// This file is large; it should be used only during download/preprocessing.
func StreamUniProtIDMappingSelected(taxID int, client HTTPClient, emitUniProtToEntrez func(uniprot, entrez string) error, emitEntrezToUniProt func(entrez, uniprot string) error) error {
	urls := []string{}
	if org, ok := uniProtOrgByTaxID[taxID]; ok && org != "" {
		urls = append(urls,
			fmt.Sprintf("https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/by_organism/%s_%d_idmapping_selected.tab.gz", org, taxID),
			fmt.Sprintf("https://ftp.uniprot.org/pub/databases/uniprot/knowledgebase/idmapping/by_organism/%s_%d_idmapping_selected.tab.gz", org, taxID),
		)
	}
	// Fallback to the global file (much larger).
	urls = append(urls,
		"https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/idmapping_selected.tab.gz",
		"https://ftp.uniprot.org/pub/databases/uniprot/knowledgebase/idmapping/idmapping_selected.tab.gz",
	)
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Minute}
	}

	var lastErr error
	for _, url := range urls {
		req, _ := http.NewRequest("GET", url, nil)
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("HTTP %s for %s", resp.Status, url)
			resp.Body.Close()
			continue
		}
		gr, err := gzip.NewReader(resp.Body)
		if err != nil {
			lastErr = err
			resp.Body.Close()
			continue
		}
		err = parseUniProtIDMappingSelected(gr, taxID, emitUniProtToEntrez, emitEntrezToUniProt)
		gr.Close()
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no UniProt mapping URL succeeded")
	}
	return lastErr
}

func parseUniProtIDMappingSelected(r io.Reader, taxID int, emitUniProtToEntrez func(uniprot, entrez string) error, emitEntrezToUniProt func(entrez, uniprot string) error) error {
	// Expected columns (no header), commonly:
	// 0 UniProtKB-AC, 1 UniProtKB-ID, 2 GeneID, ... , 12 NCBI-taxon, ... (varies by release).
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 3 {
			continue
		}
		ac := strings.TrimSpace(fields[0])
		geneID := strings.TrimSpace(fields[2])
		if ac == "" || geneID == "" {
			continue
		}
		// Some organism-specific files may omit the taxon column; if present, enforce taxID.
		if len(fields) >= 13 && strings.TrimSpace(fields[12]) != "" {
			taxon := strings.TrimSpace(fields[12])
			tax, err := strconv.Atoi(taxon)
			if err != nil || tax != taxID {
				continue
			}
		}
		// GeneID can be multiple; use all.
		for _, gid := range strings.Split(geneID, ";") {
			gid = strings.TrimSpace(gid)
			if gid == "" {
				continue
			}
			if err := emitUniProtToEntrez(ac, gid); err != nil {
				return err
			}
			if err := emitEntrezToUniProt(gid, ac); err != nil {
				return err
			}
		}
	}
	return sc.Err()
}
