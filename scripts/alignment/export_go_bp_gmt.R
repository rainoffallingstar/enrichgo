#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1) {
  stop("Usage: export_go_bp_gmt.R <output_gmt>")
}
out_file <- args[[1]]

db_path <- "/usr/lib/R/library/org.Hs.eg.db/extdata/org.Hs.eg.sqlite"
if (!file.exists(db_path)) {
  stop(sprintf("org.Hs.eg sqlite not found: %s", db_path))
}
con <- dbConnect(RSQLite::SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

bp_df <- dbGetQuery(
  con,
  "SELECT DISTINCT g.go_id AS GO, gi.symbol AS SYMBOL
   FROM go_bp_all g
   JOIN gene_info gi ON gi._id = g._id
   WHERE gi.symbol IS NOT NULL AND gi.symbol <> ''"
)
bp_df <- unique(bp_df)

split_genes <- split(bp_df$SYMBOL, bp_df$GO)

con <- file(out_file, open = "w")
on.exit(close(con))

n_terms <- 0
for (go_id in sort(names(split_genes))) {
  genes <- sort(unique(split_genes[[go_id]]))
  genes <- genes[!is.na(genes) & genes != ""]
  if (length(genes) == 0) next
  desc <- go_id
  line <- paste(c(go_id, desc, genes), collapse = "\t")
  writeLines(line, con)
  n_terms <- n_terms + 1
}

cat(sprintf("[GO-GMT] exported %d BP terms to %s\n", n_terms, out_file))
