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
sig_col <- Sys.getenv("ALIGN_SIG_COL", "significant")
sig_val <- Sys.getenv("ALIGN_SIG_VAL", "TRUE")
fdr_col <- Sys.getenv("ALIGN_FDR_COL", "")
fdr_threshold <- 0.05
if (nzchar(Sys.getenv("ALIGN_FDR_THRESHOLD"))) {
  fdr_threshold <- as.numeric(Sys.getenv("ALIGN_FDR_THRESHOLD"))
}
rank_col <- Sys.getenv("ALIGN_RANK_COL", "logFC")
if (!nzchar(rank_col)) {
  rank_col <- "logFC"
}
dir_col <- Sys.getenv("ALIGN_DIR_COL", "direction")
up_val <- Sys.getenv("ALIGN_UP_VAL", "Up")
down_val <- Sys.getenv("ALIGN_DOWN_VAL", "Down")
logfc_col <- Sys.getenv("ALIGN_LOGFC_COL", "")
logfc_threshold <- 0
if (nzchar(Sys.getenv("ALIGN_LOGFC_THRESHOLD"))) {
  logfc_threshold <- as.numeric(Sys.getenv("ALIGN_LOGFC_THRESHOLD"))
}
split_by_direction <- Sys.getenv("ALIGN_SPLIT_BY_DIRECTION", "0") == "1"
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

load_gmt_mappings <- function(path) {
  lines <- readLines(path, warn = FALSE)
  term <- character()
  gene <- character()
  name_term <- character()
  name_value <- character()
  for (line in lines) {
    parts <- strsplit(line, "	", fixed = TRUE)[[1]]
    if (length(parts) < 3) next
    tid <- parts[[1]]
    tname <- tid
    if (length(parts) >= 2) {
      raw_name <- parts[[2]]
      if (!is.na(raw_name) && raw_name != "" && raw_name != "NA") {
        tname <- raw_name
      }
    }
    genes <- parts[3:length(parts)]
    genes <- genes[genes != ""]
    if (length(genes) == 0) next
    term <- c(term, rep(tid, length(genes)))
    gene <- c(gene, genes)
    name_term <- c(name_term, tid)
    name_value <- c(name_value, tname)
  }
  list(
    term2gene = unique(data.frame(term = term, gene = gene, stringsAsFactors = FALSE)),
    term2name = unique(data.frame(term = name_term, name = name_value, stringsAsFactors = FALSE))
  )
}

load_idmap <- function(path) {
  if (!nzchar(path) || !file.exists(path)) return(NULL)
  m <- read.table(path, sep = "	", header = FALSE, quote = "", comment.char = "", stringsAsFactors = FALSE)
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
  if (!("gene" %in% names(df))) {
    names(df)[1] <- "gene"
  }
  df$gene <- as.character(df$gene)
  df <- df[df$gene != "" & !is.na(df$gene), ]
  df
}

resolve_value_column <- function(df, preferred = "") {
  if (nzchar(preferred)) {
    if (!(preferred %in% names(df))) {
      stop(sprintf("missing value column: %s", preferred))
    }
    return(preferred)
  }
  if (ncol(df) < 2) {
    stop("input table has fewer than 2 columns")
  }
  names(df)[[2]]
}

filter_significant <- function(df, fdr_col, fdr_threshold, sig_col, sig_val) {
  if (nzchar(fdr_col)) {
    if (!(fdr_col %in% names(df))) {
      stop(sprintf("missing FDR column: %s", fdr_col))
    }
    vals <- suppressWarnings(as.numeric(df[[fdr_col]]))
    return(!is.na(vals) & vals <= fdr_threshold)
  }
  if (nzchar(sig_col)) {
    if (!(sig_col %in% names(df))) {
      stop(sprintf("missing significance column: %s", sig_col))
    }
    vals <- trimws(as.character(df[[sig_col]]))
    vals[is.na(vals)] <- ""
    return(vals == sig_val)
  }
  rep(TRUE, nrow(df))
}

prepare_direction_groups <- function(df, split_enabled, dir_col, up_val, down_val, logfc_col, logfc_threshold) {
  combined <- unique(df$gene)
  out <- list(use_split = FALSE, combined = combined, groups = list())
  if (!split_enabled) {
    return(out)
  }

  directions <- rep("", nrow(df))
  has_direction_data <- FALSE
  if (nzchar(dir_col) && dir_col %in% names(df)) {
    directions <- trimws(as.character(df[[dir_col]]))
    directions[is.na(directions)] <- ""
    has_direction_data <- any(directions != "")
  }

  if (!has_direction_data) {
    value_col <- NULL
    if (nzchar(logfc_col)) {
      value_col <- resolve_value_column(df, logfc_col)
    } else if (ncol(df) >= 2) {
      value_col <- names(df)[[2]]
    }
    if (!is.null(value_col)) {
      vals <- suppressWarnings(as.numeric(df[[value_col]]))
      has_real_logfc <- any(!is.na(vals) & vals != 1)
      if (has_real_logfc) {
        directions <- rep("", length(vals))
        directions[!is.na(vals) & vals > logfc_threshold] <- up_val
        directions[!is.na(vals) & vals < -logfc_threshold] <- down_val
        has_direction_data <- any(directions != "")
      }
    }
  }

  if (!has_direction_data) {
    return(out)
  }

  out$use_split <- TRUE
  out$groups[[up_val]] <- unique(df$gene[directions == up_val])
  out$groups[[down_val]] <- unique(df$gene[directions == down_val])
  out
}

map_symbols_to_entrez <- function(symbols, entrez_map) {
  ids <- unname(entrez_map[unique(symbols)])
  ids <- ids[!is.na(ids) & ids != ""]
  unique(ids)
}

standardize_result <- function(df, analysis, direction = "") {
  if (analysis == "gsea") {
    empty <- data.frame(
      ID = character(),
      Name = character(),
      NES = numeric(),
      PValue = numeric(),
      PAdjust = numeric(),
      QValue = numeric(),
      EnrichmentScore = numeric(),
      LeadGenes = character(),
      Description = character(),
      stringsAsFactors = FALSE
    )
    if (is.null(df) || nrow(df) == 0) {
      return(empty)
    }
  } else {
    empty <- data.frame(
      Direction = character(),
      ID = character(),
      Name = character(),
      GeneRatio = character(),
      BgRatio = character(),
      PValue = numeric(),
      PAdjust = numeric(),
      QValue = numeric(),
      Genes = character(),
      Count = numeric(),
      Description = character(),
      stringsAsFactors = FALSE
    )
    if (is.null(df) || nrow(df) == 0) {
      return(empty)
    }
  }

  required <- c("ID", "Description", "pvalue", "p.adjust", "qvalue")
  for (col in required) {
    if (!(col %in% names(df))) {
      df[[col]] <- NA
    }
  }
  if (!("Name" %in% names(df))) {
    df$Name <- df$Description
  }

  df$ID <- as.character(df$ID)
  df$Name <- as.character(df$Name)
  df$Description <- as.character(df$Description)
  missing_name <- is.na(df$Name) | df$Name == ""
  df$Name[missing_name] <- df$Description[missing_name]
  missing_name <- is.na(df$Name) | df$Name == ""
  df$Name[missing_name] <- df$ID[missing_name]
  missing_desc <- is.na(df$Description) | df$Description == ""
  df$Description[missing_desc] <- df$Name[missing_desc]

  if (analysis == "gsea") {
    if (!("NES" %in% names(df))) df$NES <- NA
    if (!("enrichmentScore" %in% names(df))) df$enrichmentScore <- NA
    if (!("core_enrichment" %in% names(df))) df$core_enrichment <- NA
    out <- data.frame(
      ID = df$ID,
      Name = df$Name,
      NES = df$NES,
      PValue = df$pvalue,
      PAdjust = df$p.adjust,
      QValue = df$qvalue,
      EnrichmentScore = df$enrichmentScore,
      LeadGenes = as.character(df$core_enrichment),
      Description = df$Description,
      stringsAsFactors = FALSE
    )
  } else {
    if (!("Count" %in% names(df))) df$Count <- NA
    if (!("GeneRatio" %in% names(df))) df$GeneRatio <- NA
    if (!("BgRatio" %in% names(df))) df$BgRatio <- NA
    if (!("geneID" %in% names(df))) df$geneID <- NA
    out <- data.frame(
      Direction = rep(direction, nrow(df)),
      ID = df$ID,
      Name = df$Name,
      GeneRatio = as.character(df$GeneRatio),
      BgRatio = as.character(df$BgRatio),
      PValue = df$pvalue,
      PAdjust = df$p.adjust,
      QValue = df$qvalue,
      Genes = as.character(df$geneID),
      Count = df$Count,
      Description = df$Description,
      stringsAsFactors = FALSE
    )
  }

  out <- out[order(out$PAdjust, out$PValue, na.last = TRUE), ]
  rownames(out) <- NULL
  out
}

run_directional_ora <- function(direction_info, map_genes, run_single) {
  if (!direction_info$use_split) {
    genes <- map_genes(direction_info$combined)
    return(standardize_result(as.data.frame(run_single(genes)), "ora", ""))
  }

  outputs <- list()
  for (direction in c(up_val, down_val)) {
    genes <- map_genes(direction_info$groups[[direction]])
    if (length(genes) == 0) next
    outputs[[length(outputs) + 1]] <- standardize_result(as.data.frame(run_single(genes)), "ora", direction)
  }
  if (length(outputs) == 0) {
    return(standardize_result(NULL, "ora"))
  }
  out <- do.call(rbind, outputs)
  rownames(out) <- NULL
  out
}

df <- read_deg(input_csv)
df_kegg <- df
if (nzchar(kegg_input_csv) && file.exists(kegg_input_csv)) {
  df_kegg <- read_deg(kegg_input_csv)
}
if (!only_ora) {
  if (!(rank_col %in% names(df))) {
    stop(sprintf("missing rank column: %s", rank_col))
  }
  if (!(rank_col %in% names(df_kegg))) {
    stop(sprintf("missing rank column in KEGG input: %s", rank_col))
  }
}

sig_mask <- filter_significant(df, fdr_col, fdr_threshold, sig_col, sig_val)
sig_df <- df[sig_mask, , drop = FALSE]
if (nrow(sig_df) == 0) {
  stop("no significant genes after configured filter")
}
sig_mask_kegg <- filter_significant(df_kegg, fdr_col, fdr_threshold, sig_col, sig_val)
sig_df_kegg <- df_kegg[sig_mask_kegg, , drop = FALSE]
if (nrow(sig_df_kegg) == 0) {
  stop("no significant genes after configured filter for KEGG input")
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

symbol2entrez <- symbol2entrez[!duplicated(symbol2entrez$SYMBOL), ]
entrez_map <- setNames(symbol2entrez$ENTREZID, symbol2entrez$SYMBOL)

sig_symbols <- unique(sig_df$gene)
sig_symbols_kegg <- unique(sig_df_kegg$gene)
sig_entrez <- map_symbols_to_entrez(sig_symbols_kegg, entrez_map)
if (length(sig_entrez) == 0) {
  stop("no significant genes converted to ENTREZ for KEGG")
}

go_direction_info <- prepare_direction_groups(sig_df, split_by_direction, dir_col, up_val, down_val, logfc_col, logfc_threshold)
kegg_direction_info <- prepare_direction_groups(sig_df_kegg, split_by_direction, dir_col, up_val, down_val, logfc_col, logfc_threshold)

geneList_go <- numeric()
geneList_kegg <- numeric()
if (!only_ora) {
  rank_vals <- as.numeric(df[[rank_col]])
  names(rank_vals) <- df$gene
  rank_vals <- rank_vals[!is.na(rank_vals)]
  rank_vals <- sort(rank_vals, decreasing = TRUE)

  geneList_go <- rank_vals

  rank_vals_kegg <- as.numeric(df_kegg[[rank_col]])
  names(rank_vals_kegg) <- df_kegg$gene
  rank_vals_kegg <- rank_vals_kegg[!is.na(rank_vals_kegg)]
  rank_vals_kegg <- sort(rank_vals_kegg, decreasing = TRUE)
  geneList_kegg <- rank_vals_kegg[names(rank_vals_kegg) %in% names(entrez_map)]
  names(geneList_kegg) <- unname(entrez_map[names(geneList_kegg)])
  geneList_kegg_split <- split(geneList_kegg, names(geneList_kegg))
  geneList_kegg <- vapply(geneList_kegg_split, function(x) x[which.max(abs(x))], numeric(1))
  geneList_kegg <- sort(geneList_kegg, decreasing = TRUE)
}

set.seed(seed)

kegg_term2gene <- NULL
kegg_term2name <- NULL
if (use_custom_kegg) {
  kegg_mappings <- load_gmt_mappings(kegg_gmt_file)
  kegg_term2gene <- kegg_mappings$term2gene
  kegg_term2name <- kegg_mappings$term2name
}
go_term2gene <- NULL
go_term2name <- NULL
if (use_custom_go) {
  go_mappings <- load_gmt_mappings(go_gmt_file)
  go_term2gene <- go_mappings$term2gene
  go_term2name <- go_mappings$term2name
}
reactome_term2gene <- NULL
reactome_term2name <- NULL
if (use_custom_reactome) {
  reactome_mappings <- load_gmt_mappings(reactome_gmt_file)
  reactome_term2gene <- reactome_mappings$term2gene
  reactome_term2name <- reactome_mappings$term2name
}
msigdb_term2gene <- NULL
msigdb_term2name <- NULL
if (use_custom_msigdb) {
  msigdb_mappings <- load_gmt_mappings(msigdb_gmt_file)
  msigdb_term2gene <- msigdb_mappings$term2gene
  msigdb_term2name <- msigdb_mappings$term2name
}

run_kegg_ora_single <- function(genes) {
  if (length(genes) == 0) return(NULL)
  if (use_custom_kegg) {
    kegg_universe <- map_symbols_to_entrez(unique(df_kegg$gene), entrez_map)
    return(enricher(
      gene = genes,
      universe = kegg_universe,
      TERM2GENE = kegg_term2gene,
      TERM2NAME = kegg_term2name,
      pvalueCutoff = p_cutoff,
      pAdjustMethod = "BH",
      qvalueCutoff = q_cutoff,
      minGSSize = min_gs,
      maxGSSize = max_gs
    ))
  }
  enrichKEGG(
    gene = genes,
    organism = "hsa",
    keyType = "ncbi-geneid",
    pvalueCutoff = p_cutoff,
    pAdjustMethod = "BH",
    qvalueCutoff = q_cutoff,
    minGSSize = min_gs,
    maxGSSize = max_gs
  )
}

run_go_ora_single <- function(genes) {
  if (length(genes) == 0) return(NULL)
  if (use_custom_go) {
    go_universe <- NULL
    if (nzchar(go_universe_file) && file.exists(go_universe_file)) {
      go_universe <- readLines(go_universe_file, warn = FALSE)
      go_universe <- unique(go_universe[go_universe != "" & !is.na(go_universe)])
    }
    return(enricher(
      gene = genes,
      universe = go_universe,
      TERM2GENE = go_term2gene,
      TERM2NAME = go_term2name,
      pvalueCutoff = p_cutoff,
      pAdjustMethod = "BH",
      qvalueCutoff = q_cutoff,
      minGSSize = min_gs,
      maxGSSize = max_gs
    ))
  }
  suppressPackageStartupMessages(library(org.Hs.eg.db))
  enrichGO(
    gene = genes,
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

run_reactome_ora_single <- function(genes) {
  if (!use_custom_reactome || length(genes) == 0) return(NULL)
  enricher(
    gene = genes,
    universe = all_symbols,
    TERM2GENE = reactome_term2gene,
    TERM2NAME = reactome_term2name,
    pvalueCutoff = p_cutoff,
    pAdjustMethod = "BH",
    qvalueCutoff = q_cutoff,
    minGSSize = min_gs,
    maxGSSize = max_gs
  )
}

run_msigdb_ora_single <- function(genes) {
  if (!use_custom_msigdb || length(genes) == 0) return(NULL)
  enricher(
    gene = genes,
    universe = all_symbols,
    TERM2GENE = msigdb_term2gene,
    TERM2NAME = msigdb_term2name,
    pvalueCutoff = p_cutoff,
    pAdjustMethod = "BH",
    qvalueCutoff = q_cutoff,
    minGSSize = min_gs,
    maxGSSize = max_gs
  )
}

if (!skip_kegg) {
  ora_kegg_df <- run_directional_ora(
    kegg_direction_info,
    function(symbols) map_symbols_to_entrez(symbols, entrez_map),
    run_kegg_ora_single
  )
} else {
  ora_kegg_df <- standardize_result(NULL, "ora")
}

ora_go_df <- run_directional_ora(
  go_direction_info,
  function(symbols) unique(symbols),
  run_go_ora_single
)

ora_reactome_df <- run_directional_ora(
  go_direction_info,
  function(symbols) unique(symbols),
  run_reactome_ora_single
)

ora_msigdb_df <- run_directional_ora(
  go_direction_info,
  function(symbols) unique(symbols),
  run_msigdb_ora_single
)

if (!only_ora && !skip_kegg) {
  if (use_custom_kegg) {
    gsea_kegg_args <- list(
      geneList = geneList_kegg,
      TERM2GENE = kegg_term2gene,
      TERM2NAME = kegg_term2name,
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

if (!only_ora) {
  if (use_custom_go) {
    gsea_args <- list(
      geneList = geneList_go,
      TERM2GENE = go_term2gene,
      TERM2NAME = go_term2name,
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
  gsea_reactome_args <- list(
    geneList = geneList_go,
    TERM2GENE = reactome_term2gene,
    TERM2NAME = reactome_term2name,
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
  gsea_msigdb_args <- list(
    geneList = geneList_go,
    TERM2GENE = msigdb_term2gene,
    TERM2NAME = msigdb_term2name,
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

gsea_kegg_df <- standardize_result(as.data.frame(gsea_kegg), "gsea")
gsea_go_df <- standardize_result(as.data.frame(gsea_go), "gsea")
gsea_reactome_df <- standardize_result(as.data.frame(gsea_reactome), "gsea")
gsea_msigdb_df <- standardize_result(as.data.frame(gsea_msigdb), "gsea")

write.table(ora_kegg_df, file.path(out_dir, "r_ora_kegg.tsv"), sep = "	", quote = FALSE, row.names = FALSE)
write.table(ora_go_df, file.path(out_dir, "r_ora_go.tsv"), sep = "	", quote = FALSE, row.names = FALSE)
write.table(gsea_kegg_df, file.path(out_dir, "r_gsea_kegg.tsv"), sep = "	", quote = FALSE, row.names = FALSE)
write.table(gsea_go_df, file.path(out_dir, "r_gsea_go.tsv"), sep = "	", quote = FALSE, row.names = FALSE)
if (use_custom_reactome) {
  write.table(ora_reactome_df, file.path(out_dir, "r_ora_reactome.tsv"), sep = "	", quote = FALSE, row.names = FALSE)
  write.table(gsea_reactome_df, file.path(out_dir, "r_gsea_reactome.tsv"), sep = "	", quote = FALSE, row.names = FALSE)
}
if (use_custom_msigdb) {
  write.table(ora_msigdb_df, file.path(out_dir, "r_ora_msigdb.tsv"), sep = "	", quote = FALSE, row.names = FALSE)
  write.table(gsea_msigdb_df, file.path(out_dir, "r_gsea_msigdb.tsv"), sep = "	", quote = FALSE, row.names = FALSE)
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
    sig_col = sig_col,
    sig_val = sig_val,
    fdr_col = fdr_col,
    fdr_threshold = fdr_threshold,
    rank_col = rank_col,
    split_by_direction = split_by_direction,
    dir_col = dir_col,
    up_val = up_val,
    down_val = down_val,
    logfc_col = logfc_col,
    logfc_threshold = logfc_threshold,
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
