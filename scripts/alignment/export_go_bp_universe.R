#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1) {
  stop("Usage: export_go_bp_universe.R <output_file>")
}
out_file <- args[[1]]

db_path <- "/usr/lib/R/library/org.Hs.eg.db/extdata/org.Hs.eg.sqlite"
if (!file.exists(db_path)) {
  stop(sprintf("org.Hs.eg sqlite not found: %s", db_path))
}
con <- dbConnect(RSQLite::SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

map_df <- dbGetQuery(
  con,
  "SELECT DISTINCT gi.symbol AS SYMBOL, g.go_id AS GO
   FROM go_bp_all g
   JOIN gene_info gi ON gi._id = g._id
   WHERE gi.symbol IS NOT NULL AND gi.symbol <> ''"
)

bp_symbols <- unique(map_df$SYMBOL[!is.na(map_df$GO) & !is.na(map_df$SYMBOL)])
bp_symbols <- sort(unique(bp_symbols))

writeLines(bp_symbols, out_file)
cat(sprintf("[GO-UNIVERSE] exported %d BP symbols to %s\n", length(bp_symbols), out_file))
