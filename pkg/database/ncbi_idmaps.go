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

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

func defaultHTTPClient() *http.Client {
	return &http.Client{Timeout: 5 * time.Minute}
}

func openGzipURL(url string, client HTTPClient) (io.ReadCloser, *gzip.Reader, error) {
	if client == nil {
		client = defaultHTTPClient()
	}
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, nil, fmt.Errorf("HTTP %s for %s", resp.Status, url)
	}
	gr, err := gzip.NewReader(resp.Body)
	if err != nil {
		resp.Body.Close()
		return nil, nil, err
	}
	return resp.Body, gr, nil
}

var ncbiGeneInfoURLBySpecies = map[string]string{
	"hsa": "https://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz",
	"mmu": "https://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Mammalia/Mus_musculus.gene_info.gz",
	"rno": "https://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Mammalia/Rattus_norvegicus.gene_info.gz",
	"dre": "https://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Vertebrates_other/Danio_rerio.gene_info.gz",
	"dme": "https://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Invertebrates/Drosophila_melanogaster.gene_info.gz",
	"cel": "https://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Invertebrates/Caenorhabditis_elegans.gene_info.gz",
	"ath": "https://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Plants/Arabidopsis_thaliana.gene_info.gz",
}

// StreamNCBIGeneInfo emits SYMBOL->ENTREZ and ENTREZ->SYMBOL pairs using NCBI's gene_info.gz.
func StreamNCBIGeneInfo(taxID int, client HTTPClient, emit func(entrez, symbol string) error, emitSyn func(symbol, entrez string) error) error {
	const url = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_info.gz"
	body, gr, err := openGzipURL(url, client)
	if err != nil {
		return err
	}
	defer body.Close()
	defer gr.Close()
	return parseNCBIGeneInfo(gr, taxID, emit, emitSyn)
}

// StreamNCBIGeneInfoForSpecies uses a much smaller species-specific gene_info.gz when available; falls back to the full gene_info.gz otherwise.
func StreamNCBIGeneInfoForSpecies(species string, taxID int, client HTTPClient, emit func(entrez, symbol string) error, emitSyn func(symbol, entrez string) error) error {
	species = strings.ToLower(strings.TrimSpace(species))
	if url, ok := ncbiGeneInfoURLBySpecies[species]; ok && url != "" {
		body, gr, err := openGzipURL(url, client)
		if err == nil {
			defer body.Close()
			defer gr.Close()
			return parseNCBIGeneInfo(gr, taxID, emit, emitSyn)
		}
	}
	return StreamNCBIGeneInfo(taxID, client, emit, emitSyn)
}

func parseNCBIGeneInfo(r io.Reader, taxID int, emitEntrezToSymbol func(entrez, symbol string) error, emitSymbolToEntrez func(symbol, entrez string) error) error {
	sc := bufio.NewScanner(r)
	// Lines can be long due to synonyms.
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)

	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 6 {
			continue
		}
		tax, err := strconv.Atoi(fields[0])
		if err != nil || tax != taxID {
			continue
		}
		entrez := strings.TrimSpace(fields[1])
		symbol := strings.TrimSpace(fields[2])
		synonyms := strings.TrimSpace(fields[4]) // '|' separated, or '-'
		symbolAuth := ""
		if len(fields) >= 12 {
			symbolAuth = strings.TrimSpace(fields[11])
		}

		if entrez == "" {
			continue
		}
		if symbol != "" && symbol != "-" {
			if err := emitEntrezToSymbol(entrez, symbol); err != nil {
				return err
			}
			if err := emitSymbolToEntrez(strings.ToUpper(symbol), entrez); err != nil {
				return err
			}
		}
		if symbolAuth != "" && symbolAuth != "-" {
			if err := emitSymbolToEntrez(strings.ToUpper(symbolAuth), entrez); err != nil {
				return err
			}
		}
		if synonyms != "" && synonyms != "-" {
			for _, syn := range strings.Split(synonyms, "|") {
				syn = strings.ToUpper(strings.TrimSpace(syn))
				if syn == "" || syn == "-" {
					continue
				}
				if err := emitSymbolToEntrez(syn, entrez); err != nil {
					return err
				}
			}
		}
	}
	return sc.Err()
}

// StreamNCBIGene2Ensembl emits ENSEMBL->ENTREZ and ENTREZ->ENSEMBL pairs using NCBI gene2ensembl.gz.
func StreamNCBIGene2Ensembl(taxID int, client HTTPClient, emitEnsemblToEntrez func(ensembl, entrez string) error, emitEntrezToEnsembl func(entrez, ensembl string) error) error {
	const url = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2ensembl.gz"
	body, gr, err := openGzipURL(url, client)
	if err != nil {
		return err
	}
	defer body.Close()
	defer gr.Close()
	return parseNCBIGene2Ensembl(gr, taxID, emitEnsemblToEntrez, emitEntrezToEnsembl)
}

func parseNCBIGene2Ensembl(r io.Reader, taxID int, emitEnsemblToEntrez func(ensembl, entrez string) error, emitEntrezToEnsembl func(entrez, ensembl string) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)

	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 3 {
			continue
		}
		tax, err := strconv.Atoi(fields[0])
		if err != nil || tax != taxID {
			continue
		}
		entrez := strings.TrimSpace(fields[1])
		if entrez == "" {
			continue
		}

		// Columns typically:
		// 0 tax_id, 1 GeneID, 2 Ensembl_gene_identifier, 3 RNA, 4 protein.
		for i := 2; i < len(fields) && i <= 4; i++ {
			ens := strings.TrimSpace(fields[i])
			if ens == "" || ens == "-" {
				continue
			}
			if err := emitEnsemblToEntrez(strings.ToUpper(ens), entrez); err != nil {
				return err
			}
			if err := emitEntrezToEnsembl(entrez, strings.ToUpper(ens)); err != nil {
				return err
			}
		}
	}
	return sc.Err()
}

// StreamNCBIGene2RefSeq emits REFSEQ->ENTREZ and ENTREZ->REFSEQ pairs using NCBI gene2refseq.gz.
func StreamNCBIGene2RefSeq(taxID int, client HTTPClient, emitRefSeqToEntrez func(refseq, entrez string) error, emitEntrezToRefSeq func(entrez, refseq string) error) error {
	const url = "https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2refseq.gz"
	body, gr, err := openGzipURL(url, client)
	if err != nil {
		return err
	}
	defer body.Close()
	defer gr.Close()
	return parseNCBIGene2RefSeq(gr, taxID, emitRefSeqToEntrez, emitEntrezToRefSeq)
}

func stripAccVersion(acc string) string {
	acc = strings.TrimSpace(acc)
	if acc == "" || acc == "-" {
		return ""
	}
	if i := strings.Index(acc, "."); i > 0 {
		return acc[:i]
	}
	return acc
}

func parseNCBIGene2RefSeq(r io.Reader, taxID int, emitRefSeqToEntrez func(refseq, entrez string) error, emitEntrezToRefSeq func(entrez, refseq string) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 3 {
			continue
		}
		tax, err := strconv.Atoi(fields[0])
		if err != nil || tax != taxID {
			continue
		}
		entrez := strings.TrimSpace(fields[1])
		if entrez == "" {
			continue
		}
		// The file contains many accession columns; we extract common ones by position:
		// 3 RNA_nucleotide_accession.version, 5 protein_accession.version, 7 genomic_nucleotide_accession.version.
		idxs := []int{3, 5, 7}
		for _, idx := range idxs {
			if idx >= len(fields) {
				continue
			}
			acc := stripAccVersion(fields[idx])
			if acc == "" {
				continue
			}
			acc = strings.ToUpper(acc)
			if err := emitRefSeqToEntrez(acc, entrez); err != nil {
				return err
			}
			if err := emitEntrezToRefSeq(entrez, acc); err != nil {
				return err
			}
		}
	}
	return sc.Err()
}
