#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(clusterProfiler)
  library(jsonlite)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("Usage: clusterprofiler_baseline.R <input_csv> <output_dir>")
}

input_csv <- args[[1]]
out_dir <- args[[2]]
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

gene_col <- ""
fdr_col <- Sys.getenv("ALIGN_FDR_COL", "FDR")
rank_col <- Sys.getenv("ALIGN_RANK_COL", "logFC")
sig_cutoff <- 0.05
min_gs <- 10
max_gs <- 500
n_perm <- 1000
if (nzchar(Sys.getenv("ALIGN_NPERM"))) {
  n_perm <- as.integer(Sys.getenv("ALIGN_NPERM"))
}
only_ora <- Sys.getenv("ALIGN_ONLY_ORA", "0") == "1"
skip_kegg <- Sys.getenv("ALIGN_SKIP_KEGG", "0") == "1"
go_gmt_file <- Sys.getenv("ALIGN_R_GO_GMT_FILE", "")
go_universe_file <- Sys.getenv("ALIGN_R_GO_UNIVERSE_FILE", "")
kegg_gmt_file <- Sys.getenv("ALIGN_R_KEGG_GMT_FILE", "")
kegg_input_csv <- Sys.getenv("ALIGN_KEGG_INPUT_CSV", "")
kegg_idmap_tsv <- Sys.getenv("ALIGN_KEGG_IDMAP_TSV", "")
include_reactome <- Sys.getenv("ALIGN_INCLUDE_REACTOME", "0") == "1"
include_msigdb <- Sys.getenv("ALIGN_INCLUDE_MSIGDB", "0") == "1"
reactome_gmt_file <- Sys.getenv("ALIGN_R_REACTOME_GMT_FILE", "")
msigdb_gmt_file <- Sys.getenv("ALIGN_R_MSIGDB_GMT_FILE", "")
p_cutoff <- 0.05
q_cutoff <- 0.05
seed <- 42
use_custom_go <- nzchar(go_gmt_file) && file.exists(go_gmt_file)
use_custom_kegg <- nzchar(kegg_gmt_file) && file.exists(kegg_gmt_file)
use_custom_reactome <- include_reactome && nzchar(reactome_gmt_file) && file.exists(reactome_gmt_file)
use_custom_msigdb <- include_msigdb && nzchar(msigdb_gmt_file) && file.exists(msigdb_gmt_file)
bp_param <- NULL
if (requireNamespace("BiocParallel", quietly = TRUE)) {
  bp_param <- BiocParallel::SerialParam(progressbar = FALSE)
}

load_gmt_term2gene <- function(path) {
  lines <- readLines(path, warn = FALSE)
  term <- character()
  gene <- character()
  for (line in lines) {
    parts <- strsplit(line, "\t", fixed = TRUE)[[1]]
    if (length(parts) < 3) next
    tid <- parts[[1]]
    genes <- parts[3:length(parts)]
    genes <- genes[genes != ""]
    if (length(genes) == 0) next
    term <- c(term, rep(tid, length(genes)))
    gene <- c(gene, genes)
  }
  unique(data.frame(term = term, gene = gene, stringsAsFactors = FALSE))
}

load_idmap <- function(path) {
  if (!nzchar(path) || !file.exists(path)) return(NULL)
  m <- read.table(path, sep = "\t", header = FALSE, quote = "", comment.char = "", stringsAsFactors = FALSE)
  if (ncol(m) < 2) return(NULL)
  colnames(m)[1:2] <- c("ENTREZID", "SYMBOL")
  m <- m[!is.na(m$ENTREZID) & !is.na(m$SYMBOL) & m$ENTREZID != "" & m$SYMBOL != "", c("SYMBOL", "ENTREZID")]
  m <- m[!duplicated(m$SYMBOL), ]
  m
}

read_deg <- function(path) {
  df <- read.csv(path, check.names = FALSE)
  if (ncol(df) < 2) {
    stop("input table has fewer than 2 columns")
  }
  if (names(df)[1] == "") {
    names(df)[1] <- "gene"
  }
  if (!"gene" %in% names(df)) {
    names(df)[1] <- "gene"
  }
  df$gene <- as.character(df$gene)
  df <- df[df$gene != "" & !is.na(df$gene), ]
  df
}

standardize_result <- function(df, analysis) {
  if (is.null(df) || nrow(df) == 0) {
    if (analysis == "gsea") {
      return(data.frame(ID=character(), Description=character(), pvalue=numeric(), p.adjust=numeric(), qvalue=numeric(), NES=numeric(), stringsAsFactors = FALSE))
    }
    return(data.frame(ID=character(), Description=character(), pvalue=numeric(), p.adjust=numeric(), qvalue=numeric(), Count=numeric(), GeneRatio=character(), BgRatio=character(), stringsAsFactors = FALSE))
  }

  required <- c("ID", "Description", "pvalue", "p.adjust", "qvalue")
  for (col in required) {
    if (!col %in% names(df)) {
      df[[col]] <- NA
    }
  }

  if (analysis == "gsea") {
    if (!"NES" %in% names(df)) df$NES <- NA
    out <- df[, c("ID", "Description", "pvalue", "p.adjust", "qvalue", "NES")]
  } else {
    if (!"Count" %in% names(df)) df$Count <- NA
    if (!"GeneRatio" %in% names(df)) df$GeneRatio <- NA
    if (!"BgRatio" %in% names(df)) df$BgRatio <- NA
    out <- df[, c("ID", "Description", "pvalue", "p.adjust", "qvalue", "Count", "GeneRatio", "BgRatio")]
  }

  out <- out[order(out$p.adjust, out$pvalue, na.last = TRUE), ]
  rownames(out) <- NULL
  out
}

df <- read_deg(input_csv)
df_kegg <- df
if (nzchar(kegg_input_csv) && file.exists(kegg_input_csv)) {
  df_kegg <- read_deg(kegg_input_csv)
}
if (!(fdr_col %in% names(df))) {
  stop(sprintf("missing FDR column: %s", fdr_col))
}
if (!(rank_col %in% names(df))) {
  stop(sprintf("missing rank column: %s", rank_col))
}
if (!(fdr_col %in% names(df_kegg))) {
  stop(sprintf("missing FDR column in KEGG input: %s", fdr_col))
}
if (!(rank_col %in% names(df_kegg))) {
  stop(sprintf("missing rank column in KEGG input: %s", rank_col))
}

sig_df <- df[!is.na(df[[fdr_col]]) & df[[fdr_col]] <= sig_cutoff, ]
if (nrow(sig_df) == 0) {
  stop("no significant genes after FDR filter")
}
sig_df_kegg <- df_kegg[!is.na(df_kegg[[fdr_col]]) & df_kegg[[fdr_col]] <= sig_cutoff, ]
if (nrow(sig_df_kegg) == 0) {
  stop("no significant genes after FDR filter for KEGG input")
}

all_symbols <- unique(df$gene)
symbol2entrez <- load_idmap(kegg_idmap_tsv)
if (is.null(symbol2entrez) || nrow(symbol2entrez) == 0) {
  suppressPackageStartupMessages(library(org.Hs.eg.db))
  symbol2entrez <- bitr(
    all_symbols,
    fromType = "SYMBOL",
    toType = "ENTREZID",
    OrgDb = org.Hs.eg.db
  )
  if (is.null(symbol2entrez) || nrow(symbol2entrez) == 0) {
    stop("SYMBOL->ENTREZ conversion returned empty")
  }
  colnames(symbol2entrez) <- c("SYMBOL", "ENTREZID")
}

# Use first hit when SYMBOL maps to multiple ENTREZ IDs (stable rule)
symbol2entrez <- symbol2entrez[!duplicated(symbol2entrez$SYMBOL), ]
entrez_map <- setNames(symbol2entrez$ENTREZID, symbol2entrez$SYMBOL)

sig_symbols <- unique(sig_df$gene)
sig_symbols_kegg <- unique(sig_df_kegg$gene)
sig_entrez <- unname(entrez_map[sig_symbols_kegg])
sig_entrez <- unique(sig_entrez[!is.na(sig_entrez)])
if (length(sig_entrez) == 0) {
  stop("no significant genes converted to ENTREZ for KEGG")
}

rank_vals <- as.numeric(df[[rank_col]])
names(rank_vals) <- df$gene
rank_vals <- rank_vals[!is.na(rank_vals)]
rank_vals <- sort(rank_vals, decreasing = TRUE)

# GO GSEA uses SYMBOL directly
geneList_go <- rank_vals

# KEGG GSEA uses ENTREZ mapped list from KEGG-specific input
rank_vals_kegg <- as.numeric(df_kegg[[rank_col]])
names(rank_vals_kegg) <- df_kegg$gene
rank_vals_kegg <- rank_vals_kegg[!is.na(rank_vals_kegg)]
rank_vals_kegg <- sort(rank_vals_kegg, decreasing = TRUE)
geneList_kegg <- rank_vals_kegg[names(rank_vals_kegg) %in% names(entrez_map)]
names(geneList_kegg) <- unname(entrez_map[names(geneList_kegg)])
# Collapse duplicated ENTREZ by max absolute ranking metric
geneList_kegg_split <- split(geneList_kegg, names(geneList_kegg))
geneList_kegg <- vapply(geneList_kegg_split, function(x) x[which.max(abs(x))], numeric(1))
geneList_kegg <- sort(geneList_kegg, decreasing = TRUE)

set.seed(seed)

if (!skip_kegg) {
  if (use_custom_kegg) {
    kegg_term2gene <- load_gmt_term2gene(kegg_gmt_file)
    kegg_universe <- as.character(unique(unname(entrez_map[unique(df_kegg$gene)])))
    kegg_universe <- unique(kegg_universe[!is.na(kegg_universe) & kegg_universe != ""])
    ora_kegg <- enricher(
      gene = sig_entrez,
      universe = kegg_universe,
      TERM2GENE = kegg_term2gene,
      pvalueCutoff = p_cutoff,
      pAdjustMethod = "BH",
      qvalueCutoff = q_cutoff,
      minGSSize = min_gs,
      maxGSSize = max_gs
    )
  } else {
    ora_kegg <- enrichKEGG(
      gene = sig_entrez,
      organism = "hsa",
      keyType = "ncbi-geneid",
      pvalueCutoff = p_cutoff,
      pAdjustMethod = "BH",
      qvalueCutoff = q_cutoff,
      minGSSize = min_gs,
      maxGSSize = max_gs
    )
  }
} else {
  ora_kegg <- NULL
}

if (use_custom_go) {
  term2gene <- load_gmt_term2gene(go_gmt_file)
  go_universe <- NULL
  if (nzchar(go_universe_file) && file.exists(go_universe_file)) {
    go_universe <- readLines(go_universe_file, warn = FALSE)
    go_universe <- unique(go_universe[go_universe != "" & !is.na(go_universe)])
  }
  ora_go <- enricher(
    gene = sig_symbols,
    universe = go_universe,
    TERM2GENE = term2gene,
    pvalueCutoff = p_cutoff,
    pAdjustMethod = "BH",
    qvalueCutoff = q_cutoff,
    minGSSize = min_gs,
    maxGSSize = max_gs
  )
} else {
  suppressPackageStartupMessages(library(org.Hs.eg.db))
  ora_go <- enrichGO(
    gene = sig_symbols,
    OrgDb = org.Hs.eg.db,
    keyType = "SYMBOL",
    ont = "BP",
    pvalueCutoff = p_cutoff,
    pAdjustMethod = "BH",
    qvalueCutoff = q_cutoff,
    minGSSize = min_gs,
    maxGSSize = max_gs,
    readable = FALSE
  )
}

if (!only_ora && !skip_kegg) {
  if (use_custom_kegg) {
    kegg_term2gene <- load_gmt_term2gene(kegg_gmt_file)
    gsea_kegg_args <- list(
      geneList = geneList_kegg,
      TERM2GENE = kegg_term2gene,
      exponent = 1,
      minGSSize = min_gs,
      maxGSSize = max_gs,
      pvalueCutoff = p_cutoff,
      pAdjustMethod = "BH",
      verbose = FALSE,
      seed = TRUE,
      nPermSimple = n_perm
    )
    if (!is.null(bp_param)) {
      gsea_kegg_args$BPPARAM <- bp_param
    }
    gsea_kegg <- do.call(GSEA, gsea_kegg_args)
  } else {
    gsea_kegg <- gseKEGG(
      geneList = geneList_kegg,
      organism = "hsa",
      keyType = "ncbi-geneid",
      exponent = 1,
      minGSSize = min_gs,
      maxGSSize = max_gs,
      pvalueCutoff = p_cutoff,
      pAdjustMethod = "BH",
      verbose = FALSE,
      nPerm = n_perm
    )
  }

} else {
  gsea_kegg <- NULL
}

if (use_custom_reactome) {
  reactome_term2gene <- load_gmt_term2gene(reactome_gmt_file)
  ora_reactome <- enricher(
    gene = sig_symbols,
    universe = all_symbols,
    TERM2GENE = reactome_term2gene,
    pvalueCutoff = p_cutoff,
    pAdjustMethod = "BH",
    qvalueCutoff = q_cutoff,
    minGSSize = min_gs,
    maxGSSize = max_gs
  )
} else {
  ora_reactome <- NULL
}

if (use_custom_msigdb) {
  msigdb_term2gene <- load_gmt_term2gene(msigdb_gmt_file)
  ora_msigdb <- enricher(
    gene = sig_symbols,
    universe = all_symbols,
    TERM2GENE = msigdb_term2gene,
    pvalueCutoff = p_cutoff,
    pAdjustMethod = "BH",
    qvalueCutoff = q_cutoff,
    minGSSize = min_gs,
    maxGSSize = max_gs
  )
} else {
  ora_msigdb <- NULL
}

if (!only_ora) {
  if (use_custom_go) {
    term2gene <- load_gmt_term2gene(go_gmt_file)
    gsea_args <- list(
      geneList = geneList_go,
      TERM2GENE = term2gene,
      exponent = 1,
      minGSSize = min_gs,
      maxGSSize = max_gs,
      pvalueCutoff = p_cutoff,
      pAdjustMethod = "BH",
      verbose = FALSE,
      seed = TRUE,
      nPermSimple = n_perm
    )
    if (!is.null(bp_param)) {
      gsea_args$BPPARAM <- bp_param
    }
    gsea_go <- do.call(GSEA, gsea_args)
  } else {
    suppressPackageStartupMessages(library(org.Hs.eg.db))
    gsea_go <- gseGO(
      geneList = geneList_go,
      OrgDb = org.Hs.eg.db,
      keyType = "SYMBOL",
      ont = "BP",
      exponent = 1,
      minGSSize = min_gs,
      maxGSSize = max_gs,
      pvalueCutoff = p_cutoff,
      pAdjustMethod = "BH",
      verbose = FALSE,
      nPerm = n_perm
    )
  }
} else {
  gsea_go <- NULL
}

if (!only_ora && use_custom_reactome) {
  reactome_term2gene <- load_gmt_term2gene(reactome_gmt_file)
  gsea_reactome_args <- list(
    geneList = geneList_go,
    TERM2GENE = reactome_term2gene,
    exponent = 1,
    minGSSize = min_gs,
    maxGSSize = max_gs,
    pvalueCutoff = p_cutoff,
    pAdjustMethod = "BH",
    verbose = FALSE,
    seed = TRUE,
    nPermSimple = n_perm
  )
  if (!is.null(bp_param)) {
    gsea_reactome_args$BPPARAM <- bp_param
  }
  gsea_reactome <- do.call(GSEA, gsea_reactome_args)
} else {
  gsea_reactome <- NULL
}

if (!only_ora && use_custom_msigdb) {
  msigdb_term2gene <- load_gmt_term2gene(msigdb_gmt_file)
  gsea_msigdb_args <- list(
    geneList = geneList_go,
    TERM2GENE = msigdb_term2gene,
    exponent = 1,
    minGSSize = min_gs,
    maxGSSize = max_gs,
    pvalueCutoff = p_cutoff,
    pAdjustMethod = "BH",
    verbose = FALSE,
    seed = TRUE,
    nPermSimple = n_perm
  )
  if (!is.null(bp_param)) {
    gsea_msigdb_args$BPPARAM <- bp_param
  }
  gsea_msigdb <- do.call(GSEA, gsea_msigdb_args)
} else {
  gsea_msigdb <- NULL
}

ora_kegg_df <- standardize_result(as.data.frame(ora_kegg), "ora")
ora_go_df <- standardize_result(as.data.frame(ora_go), "ora")
gsea_kegg_df <- standardize_result(as.data.frame(gsea_kegg), "gsea")
gsea_go_df <- standardize_result(as.data.frame(gsea_go), "gsea")
ora_reactome_df <- standardize_result(as.data.frame(ora_reactome), "ora")
gsea_reactome_df <- standardize_result(as.data.frame(gsea_reactome), "gsea")
ora_msigdb_df <- standardize_result(as.data.frame(ora_msigdb), "ora")
gsea_msigdb_df <- standardize_result(as.data.frame(gsea_msigdb), "gsea")

write.table(ora_kegg_df, file.path(out_dir, "r_ora_kegg.tsv"), sep = "\t", quote = FALSE, row.names = FALSE)
write.table(ora_go_df, file.path(out_dir, "r_ora_go.tsv"), sep = "\t", quote = FALSE, row.names = FALSE)
write.table(gsea_kegg_df, file.path(out_dir, "r_gsea_kegg.tsv"), sep = "\t", quote = FALSE, row.names = FALSE)
write.table(gsea_go_df, file.path(out_dir, "r_gsea_go.tsv"), sep = "\t", quote = FALSE, row.names = FALSE)
if (use_custom_reactome) {
  write.table(ora_reactome_df, file.path(out_dir, "r_ora_reactome.tsv"), sep = "\t", quote = FALSE, row.names = FALSE)
  write.table(gsea_reactome_df, file.path(out_dir, "r_gsea_reactome.tsv"), sep = "\t", quote = FALSE, row.names = FALSE)
}
if (use_custom_msigdb) {
  write.table(ora_msigdb_df, file.path(out_dir, "r_ora_msigdb.tsv"), sep = "\t", quote = FALSE, row.names = FALSE)
  write.table(gsea_msigdb_df, file.path(out_dir, "r_gsea_msigdb.tsv"), sep = "\t", quote = FALSE, row.names = FALSE)
}

meta <- list(
  input = normalizePath(input_csv),
  input_kegg = if (nzchar(kegg_input_csv) && file.exists(kegg_input_csv)) normalizePath(kegg_input_csv) else normalizePath(input_csv),
  n_genes_total = nrow(df),
  n_genes_kegg_total = nrow(df_kegg),
  n_genes_sig = nrow(sig_df),
  n_genes_kegg_sig = nrow(sig_df_kegg),
  n_sig_entrez = length(sig_entrez),
  n_rank_go = length(geneList_go),
  n_rank_kegg = length(geneList_kegg),
  params = list(
    fdr_col = fdr_col,
    rank_col = rank_col,
    sig_cutoff = sig_cutoff,
    minGSSize = min_gs,
    maxGSSize = max_gs,
    pvalueCutoff = p_cutoff,
    qvalueCutoff = q_cutoff,
    nPerm = n_perm,
    seed = seed,
    use_custom_go = use_custom_go,
    use_custom_kegg = use_custom_kegg,
    use_custom_reactome = use_custom_reactome,
    use_custom_msigdb = use_custom_msigdb
  ),
  session = capture.output(sessionInfo())
)
write(toJSON(meta, auto_unbox = TRUE, pretty = TRUE), file.path(out_dir, "r_meta.json"))
