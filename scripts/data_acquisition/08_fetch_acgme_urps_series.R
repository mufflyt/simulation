#!/usr/bin/env Rscript
# ACGME URPS Fellow Series ----
#
#   Rscript scripts/data_acquisition/08_fetch_acgme_urps_series.R
#
# WHY THIS EXISTS. The entrant model calibrated its match-to-certification
# conversion against NRMP alone, and that conversion came out at 0.75 -- read as
# "three quarters of matched fellows certify". It is not. Scoring successive
# windows shows the ratio RISING, and for 2021-2023 it exceeds 1.0:
#
#   certifications 2016-2019 / NRMP matched 2013-2016 = 0.755
#   certifications 2020-2023 / NRMP matched 2017-2020 = 0.935
#   certifications 2021-2023 / NRMP matched 2018-2020 = 1.197
#
# A ratio above one is impossible if NRMP counted every entrant, so it does not.
# The conversion was absorbing an undercounted DENOMINATOR and reporting it as
# fellowship completion.
#
# WHAT ACGME ADDS. ACGME accredits the programs and counts fellows ON DUTY,
# including everyone who entered outside the match. It also reports the
# subspecialty under BOTH parent specialties -- Obstetrics and Gynecology, and
# Urology -- which is the same two-pathway split the ABOG/ABU certification
# counts already carry, and which a single NRMP line cannot express.
#
# THE NAME CHANGED, AND THAT IS WHY THIS WAS MISSED. ACGME renamed the
# subspecialty from "Female pelvic medicine and reconstructive surgery" to
# "Urogynecology and reconstructive pelvic surgery". NRMP still printed the old
# name through the 2025 report. Searching either name alone finds one source and
# not the other; the extractor below matches both.
#
# WHICH TABLE. "Number of Active Residents by Specialty and Subspecialty,
# Position Type, and Resident Year" gives, per parent specialty, the total
# on-duty count followed by one column per resident year. Column 1 is the
# ENTERING COHORT, which is the quantity a forward projection needs; the total is
# a stock across all years and must not be used as a flow.
#
# HOW A ROW IS IDENTIFIED. Not by column position, which moves between editions.
# A candidate row is accepted only when its per-year numbers SUM TO ITS OWN
# TOTAL. The five-year trend table prints the same specialty label with a
# superficially similar row shape, and that arithmetic is what rejects it.
#
# Requires: pdftotext (poppler) on PATH or the pdftools package, and network.
# Output: data-raw/calibration/acgme_urps_fellows_series.csv

OUT <- "data-raw/calibration/acgme_urps_fellows_series.csv"
RETRIEVED <- Sys.getenv("ACGME_RETRIEVAL_DATE", unset = format(Sys.Date()))

# Resolved from the ACGME Data Resource Book index rather than guessed; the file
# naming scheme changes several times across the archive.
BASE <- "https://www.acgme.org/globalassets/pfassets/publicationsbooks/"
URLS <- c(
  "2013-2014" = paste0(BASE, "2013-2014_acgme_databook_document.pdf"),
  "2014-2015" = paste0(BASE, "2014-2015_acgme_databook_document.pdf"),
  "2015-2016" = paste0(BASE, "2015-2016_acgme_databook_document_locked.pdf"),
  "2016-2017" = paste0(BASE, "2016-2017_acgme_databook_document.pdf"),
  "2017-2018" = paste0(BASE, "2017-2018_acgme_databook_document.pdf"),
  "2018-2019" = paste0(BASE, "2018-2019_acgme_databook_document.pdf"),
  "2019-2020" = paste0(BASE, "2019-2020_acgme_databook_document.pdf"),
  "2020-2021" = paste0(BASE, "2020-2021_acgme__databook_document.pdf"),
  "2021-2022" = paste0(BASE, "2021-2022_acgme__databook_document.pdf"),
  "2022-2023" = paste0(BASE, "2022-2023_acgme_databook_document.pdf"),
  "2023-2024" = paste0(BASE, "dataresourcebook2023-2024.pdf"),
  "2024-2025" = paste0(BASE, "2024-2025_acgme_databook_document.pdf")
)

# Anchors verified by reading the source PDFs on 2026-08-06. GATE 2 checks
# against these; years absent here are still accepted, because GATE 1 (the
# per-year columns summing to the printed total) is an arithmetic check the
# extractor cannot satisfy by picking the wrong row.
EXPECTED <- data.frame(
  academic_year = c("2016-2017", "2016-2017", "2020-2021", "2020-2021",
                    "2024-2025", "2024-2025"),
  parent        = c("obgyn", "urology", "obgyn", "urology", "obgyn", "urology"),
  active_total  = c(139L, 29L, 146L, 37L, 175L, 40L),
  year_1        = c(46L, 6L, 49L, 14L, 57L, 17L),
  stringsAsFactors = FALSE
)

SPECIALTY <- "(Female [Pp]elvic [Mm]edicine|Urogynecolog)"
PARENTS <- c(obgyn = "Obstetrics and gynecology", urology = "Urology")
CAPTION <- "Position Type, and Resident Year"

if (nchar(Sys.which("pdftotext")) == 0 &&
    !requireNamespace("pdftools", quietly = TRUE)) {
  stop("Neither pdftotext (poppler) nor the pdftools package is available; ",
       "install one to run this fetcher.", call. = FALSE)
}

cache <- Sys.getenv("ACGME_CACHE_DIR", unset = tempfile("acgme"))
dir.create(cache, showWarnings = FALSE, recursive = TRUE)

read_book <- function(year) {
  pdf <- file.path(cache, sprintf("acgme_%s.pdf", year))
  if (!file.exists(pdf)) {
    ok <- tryCatch({
      utils::download.file(URLS[[year]], pdf, quiet = TRUE, mode = "wb"); TRUE
    }, error = function(e) FALSE)
    if (!ok) return(NULL)
  }
  if (!file.exists(pdf) || file.size(pdf) < 1e5) return(NULL)
  if (nchar(Sys.which("pdftotext")) > 0) {
    txt <- file.path(cache, sprintf("acgme_%s.txt", year))
    if (!file.exists(txt)) {
      system2("pdftotext", c("-layout", shQuote(pdf), shQuote(txt)),
              stdout = NULL, stderr = NULL)
    }
    readLines(txt, warn = FALSE)
  } else {
    unlist(strsplit(pdftools::pdf_text(pdf), "\n"))
  }
}

# The subspecialty label is IDENTICAL under both parents, so attribution walks
# back to the nearest un-indented specialty line. Subspecialty rows start "-".
parent_of <- function(lines, k) {
  for (j in seq(k - 1, max(1, k - 400))) {
    l <- trimws(lines[j])
    if (!nzchar(l) || grepl("^[-–]", l)) next
    for (nm in names(PARENTS)) {
      if (startsWith(tolower(l), tolower(PARENTS[[nm]]))) return(nm)
    }
  }
  NA_character_
}

extract_book <- function(year) {
  lines <- read_book(year)
  if (is.null(lines)) {
    return(list(year = year, status = "UNAVAILABLE",
                detail = "download failed or implausibly small file"))
  }
  caps <- grep(CAPTION, lines)
  if (!length(caps)) {
    return(list(year = year, status = "INCOMPATIBLE",
                detail = "resident-year table caption absent in this edition"))
  }
  rows <- list()
  for (k in grep(SPECIALTY, lines)) {
    before <- caps[caps < k]
    if (!length(before) || (k - max(before)) > 250) next   # not in this table
    nums <- suppressWarnings(as.numeric(gsub(",", "", unlist(
      regmatches(lines[k], gregexpr("[0-9][0-9,]*\\.?[0-9]*", lines[k]))))))
    nums <- nums[is.finite(nums)]
    if (length(nums) < 3) next
    total <- nums[1]; years <- nums[-1]
    # GATE 1, as the DISAMBIGUATOR: the only row shape that can be the
    # resident-year table is one whose year columns sum to its own total.
    if (!isTRUE(all.equal(sum(years), total))) next
    p <- parent_of(lines, k)
    if (is.na(p)) next
    rows[[length(rows) + 1L]] <- data.frame(
      academic_year = year, parent = p,
      active_total = as.integer(total),
      year_1 = as.integer(years[1]),
      year_2 = if (length(years) > 1) as.integer(years[2]) else NA_integer_,
      year_3 = if (length(years) > 2) as.integer(years[3]) else NA_integer_,
      stringsAsFactors = FALSE)
  }
  if (!length(rows)) {
    return(list(year = year, status = "AMBIGUOUS",
                detail = "no specialty row satisfies total = sum(resident years)"))
  }
  out <- unique(do.call(rbind, rows))
  if (anyDuplicated(out$parent)) {
    return(list(year = year, status = "AMBIGUOUS",
                detail = "more than one row per parent satisfies the arithmetic"))
  }
  list(year = year, status = "OK", rows = out)
}

res <- lapply(names(URLS), extract_book)
ok <- Filter(function(x) identical(x$status, "OK"), res)
bad <- Filter(function(x) !identical(x$status, "OK"), res)

if (length(bad)) {
  cat("=== BOOKS NOT ADDED (reported, never imputed) ===\n")
  for (b in bad) cat(sprintf("  %s  %-13s %s\n", b$year, b$status, b$detail))
  cat("\n")
}
if (!length(ok)) stop("No ACGME book could be extracted.", call. = FALSE)

rows <- do.call(rbind, lapply(ok, function(x) x$rows))

# GATE 2: anchors must match the documented read.
chk <- merge(rows, EXPECTED, by = c("academic_year", "parent"),
             suffixes = c("", "_exp"))
if (nrow(chk) < nrow(EXPECTED)) {
  stop(sprintf("Only %d of %d documented anchors were extracted; the rest are ",
               nrow(chk), nrow(EXPECTED)), "missing.", call. = FALSE)
}
for (f in c("active_total", "year_1")) {
  d <- chk[[f]] != chk[[paste0(f, "_exp")]]
  if (any(d)) {
    stop(sprintf("ACGME audit FAILED for %s in %s: got %s, expected %s", f,
                 paste(chk$academic_year[d], chk$parent[d], collapse = ", "),
                 paste(chk[[f]][d], collapse = ", "),
                 paste(chk[[paste0(f, "_exp")]][d], collapse = ", ")), call. = FALSE)
  }
}

out <- rows[order(rows$academic_year, rows$parent), ]
# The entering cohort is what a forward projection needs; the total is a STOCK
# across all resident years and must never be used as an annual flow.
out$entering_cohort <- out$year_1
out$entry_year <- as.integer(substr(out$academic_year, 1, 4))
out$retrieved_on <- RETRIEVED
out$source_url <- unname(URLS[out$academic_year])
out <- out[, c("academic_year", "entry_year", "parent", "active_total",
               "year_1", "year_2", "year_3", "entering_cohort",
               "retrieved_on", "source_url")]

dir.create(dirname(OUT), recursive = TRUE, showWarnings = FALSE)
utils::write.csv(out, OUT, row.names = FALSE)
cat("Wrote", OUT, "\n")
print(out[, c("academic_year", "parent", "active_total", "year_1", "year_2", "year_3")],
      row.names = FALSE)

comb <- stats::aggregate(cbind(active_total, entering_cohort) ~ entry_year, out, sum)
cat("\nCombined across both parent pathways:\n")
print(comb, row.names = FALSE)
cat(sprintf("\n%d book-rows from %d books; %d books rejected.\n",
            nrow(out), length(ok), length(bad)))
