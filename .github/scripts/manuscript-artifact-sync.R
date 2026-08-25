#!/usr/bin/env Rscript
# Manuscript tables must agree with the artifacts that generated them.
#
# THE DEFECT THIS CLOSES. A rendered table and its backing artifact drift the
# moment one is regenerated and the other is not, and nothing about the
# rendered file looks wrong afterwards -- it is a table of plausible numbers.
# The reader has no way to tell, and neither does review. Elsewhere in this
# project an abstract and a manuscript were found reporting different headline
# values for the same slope, with different denominators and different Gini
# bases, discovered by eye.
#
# WHAT IT DOES NOT DO: grep numbers out of prose. A number scraped from a
# sentence has no estimand, no denominator and no units, so "the manuscript
# says 0.28 somewhere" cannot be checked against anything. This links NAMED
# tables to NAMED generated panels through docs/submission/tables/TABLE_INDEX.csv,
# which already records run_id, source_artifact and input_sha256 per panel.
#
# FAIL CLOSED. A missing table, a missing backing CSV, a missing run_id, a
# missing source artifact or a missing input hash is a FAILURE, not a skip.
# Otherwise deleting the provenance record becomes the cheapest way to make
# the check pass, and an unprovenanced table is exactly the thing this is for.
#
# Usage: manuscript-artifact-sync.R [--check]

arguments <- commandArgs(trailingOnly = TRUE)
enforce <- "--check" %in% arguments

tables_dir <- file.path("docs", "submission", "tables")
index_path <- file.path(tables_dir, "TABLE_INDEX.csv")

problems <- character(0)
note <- function(...) problems <<- c(problems, paste0(...))

if (!file.exists(index_path)) {
  cat(sprintf("::error::manuscript table index absent: %s\n", index_path))
  cat("::error::Without an index, no manuscript claim is linked to a generated quantity.\n")
  quit(status = 1L)
}

index <- utils::read.csv(index_path, stringsAsFactors = FALSE)
required_index_columns <- c(
  "table", "panel", "rows", "source", "run_id", "source_artifact",
  "status", "input_sha256"
)
missing_columns <- setdiff(required_index_columns, names(index))
if (length(missing_columns) > 0L) {
  cat(sprintf(
    "::error::TABLE_INDEX.csv is missing provenance column(s): %s\n",
    paste(missing_columns, collapse = ", ")
  ))
  quit(status = 1L)
}

# Cells are compared after normalising whitespace only. Values are NOT coerced
# to numeric: "1,207" and "1207" are different renderings and the manuscript
# ships the rendered form, so a change in rendering is a change the reader sees.
normalise <- function(x) {
  x <- gsub(" ", " ", as.character(x))
  trimws(gsub("[[:space:]]+", " ", x))
}

# Pull the data rows out of a GitHub-flavoured pipe table.
markdown_rows <- function(lines) {
  pipe_lines <- grep("^\\s*\\|", lines, value = TRUE)
  if (!length(pipe_lines)) return(list())
  cells <- lapply(pipe_lines, function(line) {
    line <- sub("^\\s*\\|", "", line)
    line <- sub("\\|\\s*$", "", line)
    normalise(strsplit(line, "|", fixed = TRUE)[[1]])
  })
  # Drop alignment separators (|:---|---:|).
  is_separator <- vapply(cells, function(row) {
    all(grepl("^:?-{2,}:?$", row))
  }, logical(1))
  cells[!is_separator]
}

cat("== manuscript / artifact synchronisation ==\n\n")

for (i in seq_len(nrow(index))) {
  table_name <- index$table[[i]]
  panel_name <- index$panel[[i]]
  label <- sprintf("%s [%s]", table_name, panel_name)

  # ---- provenance must be present, not merely well-formed ----
  for (field in c("run_id", "source_artifact", "input_sha256")) {
    value <- normalise(index[[field]][[i]])
    if (!nzchar(value) || is.na(index[[field]][[i]])) {
      note(sprintf("%s: %s is empty -- the table has no run identity", label, field))
    }
  }

  markdown_path <- file.path(tables_dir, paste0(table_name, ".md"))
  panel_path <- file.path(
    tables_dir, sprintf("%s__%s.csv", table_name, panel_name)
  )

  if (!file.exists(markdown_path)) {
    note(sprintf("%s: rendered table absent (%s)", label, markdown_path))
    next
  }
  if (!file.exists(panel_path)) {
    note(sprintf("%s: backing artifact absent (%s)", label, panel_path))
    next
  }

  panel <- utils::read.csv(panel_path, stringsAsFactors = FALSE,
                           check.names = FALSE, colClasses = "character")
  declared_rows <- suppressWarnings(as.integer(index$rows[[i]]))
  if (!is.na(declared_rows) && nrow(panel) != declared_rows) {
    note(sprintf(
      "%s: index declares %d row(s), backing artifact has %d",
      label, declared_rows, nrow(panel)
    ))
  }

  rendered <- markdown_rows(readLines(markdown_path, warn = FALSE))
  rendered_key <- vapply(
    rendered, function(row) paste(normalise(row), collapse = ""),
    character(1)
  )

  missing_rows <- 0L
  for (row_index in seq_len(nrow(panel))) {
    wanted <- normalise(unlist(panel[row_index, ], use.names = FALSE))
    key <- paste(wanted, collapse = "")
    if (!(key %in% rendered_key)) {
      missing_rows <- missing_rows + 1L
      if (missing_rows <= 2L) {
        note(sprintf(
          "%s: artifact row %d is not present in the rendered table -- %s",
          label, row_index,
          substr(paste(wanted, collapse = " | "), 1L, 110L)
        ))
      }
    }
  }
  if (missing_rows > 2L) {
    note(sprintf("%s: %d further artifact row(s) absent from the rendered table",
                 label, missing_rows - 2L))
  }

  status <- if (missing_rows == 0L) "IN SYNC" else "DRIFTED"
  cat(sprintf("  [%s] %-58s rows=%d\n", status, label, nrow(panel)))
}

cat(sprintf("\n  panels checked: %d\n", nrow(index)))

if (length(problems) > 0L) {
  cat("\n")
  for (problem in problems) cat(sprintf("::error::%s\n", problem))
  cat("::error::A rendered manuscript table disagrees with the artifact that generated it.\n")
  cat("::error::Regenerate with scripts/manuscript/build_tables.R rather than editing the rendered file.\n")
  if (enforce) quit(status = 1L)
} else {
  cat("  Every indexed panel matches its backing artifact, with provenance present.\n")
}
