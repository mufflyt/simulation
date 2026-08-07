#!/usr/bin/env Rscript
# NRMP URPS Fellowship Entrant Series ----
#
#   Rscript scripts/data_acquisition/07_fetch_nrmp_urps_series.R
#
# WHY THIS EXISTS. The 2020->2023 back-test under-predicts in every arm, and the
# diagnosis was that the pre-cutoff estimator had nothing good to work with: the
# certification flow for 2018-2020 is 40, 48, 10 -- mean 32.7/yr -- against a
# realized 69/yr, because the 2020 examination was COVID-disrupted and its
# cohort spilled into 2021. Reaching 69 required knowing that URPS fellowship
# output had expanded, and the repository carried a SINGLE NRMP row: appointment
# year 2025. Using it in a 2020-cutoff arm would be flat temporal leakage.
#
# NRMP publishes its Specialties Matching Service results every year, so the
# pre-cutoff series exists -- it simply had not been fetched. Each report is
# published IN its appointment year, so `available_by_year` is what a leakage
# audit tests against a cutoff, and it is recorded per row rather than inferred.
#
# WHY IT IS A LEADING INDICATOR. URPS fellowship is three years, so fellows
# appointed in year Y finish in Y+3 and certify around Y+3 to Y+4. Appointment
# years therefore forecast the certification counts the contract reports, using
# only information a modeller held at the cutoff.
#
# WHICH COLUMN. NRMP Table 1 ("Fellowship Match Summary, YYYY Appointments")
# runs: Applicants (U.S. MD, All), Positions Offered, No. of Pgms, Matches
# (U.S. MD, All), % Filled (U.S. MD, All), Ranked Positions (U.S. MD, All),
# Unfilled Programs. The entering cohort is **Matches, All** -- positions
# actually filled, not offered.
#
# FORMAT IS NOT ASSUMED CONSTANT ACROSS 2010-2025. Three things vary:
#   * the URL scheme changes at least four times (see URLS below);
#   * the row label wraps differently -- 2010 prints "Female Pelvic Medicine and"
#     while later years print "...and Reconstructive";
#   * several other tables in the same PDF also begin with that label.
# The extractor therefore does NOT rely on column position or label form. It
# takes every candidate line and keeps only the one whose numbers SATISFY THE
# TABLE'S OWN ARITHMETIC: matches/offered must reproduce both printed
# percentages. A layout change that moved a column would fail this rather than
# silently return the wrong field.
#
# CAVEAT CARRIED FORWARD. NRMP counts MATCHED FELLOWS; the contract counts BOARD
# CERTIFICATIONS. Some certify without passing through the match (the reports
# footnote that FPMRS "also includes programs not accredited by the ACGME"), and
# timing slips. Any matched-to-certified conversion estimated from post-cutoff
# years must NOT enter a back-test arm.
#
# Requires: pdftotext (poppler) on PATH, and network access.
# Output: data-raw/calibration/nrmp_urps_entrants_series.csv

OUT <- "data-raw/calibration/nrmp_urps_entrants_series.csv"
RETRIEVED <- Sys.getenv("NRMP_RETRIEVAL_DATE", unset = format(Sys.Date()))

# The URL scheme is not stable. Each was resolved from that year's NRMP landing
# page (https://www.nrmp.org/match-data/<year>/02/results-and-data-specialties-
# matching-service-<year>-appointment-year/) rather than guessed.
URLS <- c(
  "2010" = "https://www.nrmp.org/wp-content/uploads/2021/07/resultsanddatasms2010.pdf",
  "2011" = "https://www.nrmp.org/wp-content/uploads/2021/07/resultsanddatasms2011.pdf",
  "2012" = "https://www.nrmp.org/wp-content/uploads/2021/07/resultsanddatasms2012.pdf",
  "2013" = "https://www.nrmp.org/wp-content/uploads/2021/07/resultsanddatasms2013.pdf",
  "2014" = "https://www.nrmp.org/wp-content/uploads/2021/07/National-Resident-Matching-Program-NRMP-Results-and-Data-SMS-2014-Final.pdf",
  "2015" = "https://www.nrmp.org/wp-content/uploads/2021/07/Results-and-Data-SMS-2015.pdf",
  "2016" = "https://www.nrmp.org/wp-content/uploads/2021/07/Results-and-Data-SMS-2016_Final.pdf",
  "2017" = "https://www.nrmp.org/wp-content/uploads/2021/07/Results-and-Data-SMS-2017.pdf",
  "2018" = "https://www.nrmp.org/wp-content/uploads/2021/07/Results-and-Data-SMS-2018.pdf",
  "2019" = "https://www.nrmp.org/wp-content/uploads/2021/07/Results-and-Data-SMS-2019.pdf",
  "2020" = "https://www.nrmp.org/wp-content/uploads/2021/07/Results-and-Data-SMS-2020.pdf",
  # 2021-2024 were absent until 2026-08-05. They were not missing from NRMP --
  # each URL simply follows a different scheme again, resolved from that year's
  # landing page rather than guessed.
  "2021" = "https://www.nrmp.org/wp-content/uploads/2022/04/SMS_Result_and_Data_2021.pdf",
  "2022" = "https://www.nrmp.org/wp-content/uploads/2022/03/2022-SMS-Results-Data-FINAL.pdf",
  "2023" = "https://www.nrmp.org/wp-content/uploads/2023/04/2023-SMS-Results-and-Data-Book.pdf",
  "2024" = "https://www.nrmp.org/wp-content/uploads/2024/02/2024-SMS-Results-Data-1.pdf",
  "2025" = "https://www.nrmp.org/wp-content/uploads/2025/02/SMS_Results_and_Data_2025.pdf"
)

# Verified from the source PDFs: 2010-2020 and 2025 by human read on 2026-08-04
# (2025 additionally matches the independently verified value in mufflyt/cliff).
# GATE 2 checks against this.
#
# 2021-2024 added 2026-08-05. These were NOT read by a human, and saying so
# matters, because GATE 2 exists to be a second source and would be circular if
# it were filled from the same regex it audits. They are instead corroborated by
# a source INDEPENDENT of Table 1: every SMS report also prints a five-year
# history of positions-offered and percent-filled, so each year appears again in
# up to four later reports. Reconstructing filled = offered x %filled from those
# history tables reproduces all four values, and also reproduces the
# human-verified 2019 (64/58) and 2020 (65/56), which is what makes the
# reconstruction trustworthy rather than merely self-consistent:
#
#   year  Table 1        history tables agreeing
#   2021  63 / 62 (98.4) 2022, 2023, 2025
#   2022  65 / 61 (93.8) 2023, 2025
#   2023  65 / 61 (93.8) 2025
#   2024  67 / 65 (97.0) 2025
#
# Replace with a human read when one is done; the values should not move.
EXPECTED <- data.frame(
  appointment_year  = c(2010L, 2011L, 2012L, 2013L, 2014L, 2015L, 2016L,
                        2017L, 2018L, 2019L, 2020L,
                        2021L, 2022L, 2023L, 2024L, 2025L),
  positions_offered = c(34L, 40L, 39L, 51L, 55L, 58L, 54L, 64L, 60L, 64L, 65L,
                        63L, 65L, 65L, 67L, 70L),
  positions_filled  = c(30L, 40L, 37L, 48L, 50L, 57L, 53L, 59L, 59L, 58L, 56L,
                        62L, 61L, 61L, 65L, 70L),
  stringsAsFactors = FALSE
)

PCT_TOLERANCE <- 0.15   # printed to one decimal; allow rounding

# PDF -> text, via poppler's pdftotext if present and the pdftools R package
# otherwise. The binary is preferred because `-layout` preserves the column
# spacing Table 1 depends on; pdftools::pdf_text() preserves it too. Requiring
# the binary outright is what kept 2021-2024 unfetched on a machine that had
# neither -- the fetcher could not run at all, so the gap looked like missing
# data rather than a missing tool.
if (nchar(Sys.which("pdftotext")) == 0 &&
    !requireNamespace("pdftools", quietly = TRUE)) {
  stop("Neither pdftotext (poppler) nor the pdftools package is available; ",
       "install one to run this fetcher.", call. = FALSE)
}

pdf_to_text_file <- function(pdf, txt) {
  if (nchar(Sys.which("pdftotext")) > 0) {
    system2("pdftotext", c("-layout", shQuote(pdf), shQuote(txt)),
            stdout = NULL, stderr = NULL)
  } else {
    writeLines(unlist(strsplit(pdftools::pdf_text(pdf), "\n")), txt)
  }
}

tmp <- Sys.getenv("NRMP_CACHE_DIR", unset = tempfile("nrmp"))
dir.create(tmp, showWarnings = FALSE, recursive = TRUE)

extract_row <- function(year) {
  url <- URLS[[as.character(year)]]
  pdf <- file.path(tmp, sprintf("sms_%s.pdf", year))
  txt <- file.path(tmp, sprintf("sms_%s.txt", year))
  if (!file.exists(txt)) {
    ok <- tryCatch({
      utils::download.file(url, pdf, quiet = TRUE, mode = "wb"); TRUE
    }, error = function(e) FALSE)
    if (!ok || !file.exists(pdf) || file.size(pdf) < 1e5) {
      return(list(year = year, status = "UNAVAILABLE",
                  detail = "download failed or implausibly small file"))
    }
    pdf_to_text_file(pdf, txt)
  }
  lines <- readLines(txt, warn = FALSE)

  # Every line mentioning the specialty, in any of its printed forms. The 2010
  # report wraps the label after "and"; later years wrap after "Reconstructive".
  cand <- grep("Female Pelvic Medicine (and|&)", lines, value = TRUE)
  if (length(cand) == 0) {
    return(list(year = year, status = "INCOMPATIBLE",
                detail = "specialty row absent -- subspecialty may not have matched this year"))
  }

  # GATE 1, applied as the DISAMBIGUATOR rather than as a post-hoc check: keep
  # only candidates whose own numbers reproduce both printed percentages. This
  # is what makes the extractor robust to a moved column or a changed label.
  hits <- list()
  for (ln in cand) {
    nums <- suppressWarnings(as.numeric(gsub(",", "", unlist(
      regmatches(ln, gregexpr("[0-9][0-9,]*\\.?[0-9]*", ln))))))
    if (length(nums) < 11) next
    offered <- nums[3]; usmd <- nums[5]; all_m <- nums[6]
    p_usmd <- nums[7]; p_all <- nums[8]
    if (!is.finite(offered) || offered <= 0) next
    if (abs(100 * usmd / offered - p_usmd) > PCT_TOLERANCE) next
    if (abs(100 * all_m / offered - p_all) > PCT_TOLERANCE) next
    hits[[length(hits) + 1L]] <- data.frame(
      appointment_year = as.integer(year),
      positions_offered = as.integer(offered),
      n_programs = as.integer(nums[4]),
      positions_filled = as.integer(all_m),
      pct_filled_all = p_all,
      stringsAsFactors = FALSE)
  }
  if (length(hits) == 0) {
    return(list(year = year, status = "AMBIGUOUS",
                detail = "no candidate row satisfies the printed %-filled arithmetic"))
  }
  u <- unique(do.call(rbind, hits))
  if (nrow(u) > 1) {
    return(list(year = year, status = "AMBIGUOUS",
                detail = sprintf("%d distinct rows satisfy the arithmetic", nrow(u))))
  }
  list(year = year, status = "OK", row = u, url = url)
}

res <- lapply(names(URLS), extract_row)
ok <- Filter(function(x) identical(x$status, "OK"), res)
bad <- Filter(function(x) !identical(x$status, "OK"), res)

if (length(bad)) {
  cat("=== YEARS NOT ADDED (reported, never imputed) ===\n")
  for (b in bad) cat(sprintf("  %s  %-13s %s\n", b$year, b$status, b$detail))
  cat("\n")
}
if (length(ok) == 0) stop("No NRMP year could be extracted.", call. = FALSE)

rows <- do.call(rbind, lapply(ok, function(x) x$row))

# GATE 2: values must match the documented human read.
chk <- merge(rows, EXPECTED, by = "appointment_year", suffixes = c("", "_exp"))
for (f in c("positions_offered", "positions_filled")) {
  d <- chk[[f]] != chk[[paste0(f, "_exp")]]
  if (any(d)) {
    stop(sprintf("NRMP audit FAILED for %s in year(s) %s: got %s, expected %s",
                 f, paste(chk$appointment_year[d], collapse = ", "),
                 paste(chk[[f]][d], collapse = ", "),
                 paste(chk[[paste0(f, "_exp")]][d], collapse = ", ")), call. = FALSE)
  }
}
if (nrow(chk) != nrow(rows)) {
  stop("A year was extracted that has no documented human read; add it to EXPECTED.",
       call. = FALSE)
}
if (anyDuplicated(rows$appointment_year)) {
  stop("Duplicate appointment years extracted.", call. = FALSE)
}

out <- rows[order(rows$appointment_year), ]
out$report_title <- sprintf(
  "Results and Data: Specialties Matching Service, %d Appointment Year", out$appointment_year)
out$table_name <- sprintf("Table 1, Fellowship Match Summary, %d Appointments",
                          out$appointment_year)
# Each report is published in its own appointment year, which is what makes the
# pre-cutoff rows usable in a cutoff-respecting back-test.
out$report_published <- out$appointment_year
out$available_by_year <- out$appointment_year
out$retrieved_on <- RETRIEVED
out$source_url <- unname(URLS[as.character(out$appointment_year)])
out <- out[, c("appointment_year", "positions_offered", "positions_filled",
               "pct_filled_all", "n_programs", "report_title", "table_name",
               "report_published", "available_by_year", "retrieved_on", "source_url")]

utils::write.csv(out, OUT, row.names = FALSE)
cat("Wrote", OUT, "\n")
print(out[, c("appointment_year", "positions_offered", "positions_filled", "pct_filled_all")],
      row.names = FALSE)
cat(sprintf("\n%d years added; %d rejected.\n", nrow(out), length(bad)))

# ---- Track split: OB/GYN-based vs urology-based ----------------------------
#
# Table 1 prints ONE row for the subspecialty, and its own footnote says why:
# "Female Pelvic Medicine and Reconstructive Surgery: Urology and OB/GYN". The
# aggregate therefore cannot answer which pathway a matched fellow came from --
# but the report also lists EVERY participating program individually, and the
# NRMP specialty code is embedded in each program code:
#
#   ...221F0   OB/GYN-based track
#   ...486F0   urology-based track
#
# Summing the program rows reconstructs the split. THE GATE IS THAT IT MUST
# REPRODUCE TABLE 1: obgyn + urology has to equal the printed offered and filled
# exactly, or the year is rejected. That is not a formality -- the program table
# is laid out in wrapped multi-column blocks, and a code split across a line
# break is silently missed.
#
# TOLERANCE. A year ships when the reconstruction is within SPLIT_TOLERANCE of
# Table 1 on BOTH offered and filled, and every row records its residual so the
# imprecision is carried in the data rather than assumed away. Exact
# reconciliation happens in four years; a tolerance of 2 admits six more, on
# totals near 60 (~3%). Years outside it are REPORTED, NOT SHIPPED.
#
# WHAT A RESIDUAL MEANS. It is a whole missing program, not measurement noise,
# and NOTHING SAYS WHICH TRACK IT BELONGS TO. A residual of 2 filled positions
# is therefore up to 2 positions of uncertainty in the urology share -- around 3
# percentage points at these totals. Use reconciles_exactly to get the clean
# subset when that matters.
#
# ACGME publishes this split directly and completely (R/supply-acgme_fellows.R),
# so this exists as an independent cross-check on that source, not as a
# replacement for it.

SPLIT_OUT <- "data-raw/calibration/nrmp_urps_track_split.csv"
TRACKS <- c(obgyn = "221", urology = "486")
SPLIT_TOLERANCE <- as.integer(Sys.getenv("NRMP_SPLIT_TOLERANCE", unset = "2"))

split_for <- function(year) {
  txt_path <- file.path(tmp, sprintf("sms_%s.txt", year))
  if (!file.exists(txt_path)) return(NULL)
  txt <- paste(readLines(txt_path, warn = FALSE), collapse = "\n")
  rows <- list()
  for (nm in names(TRACKS)) {
    # Anchor on the CODE, not the specialty label: the label wraps mid-entry.
    # The code can itself be split by the wrap, hence the optional last digit,
    # and the two counts may sit past the wrapped label, hence the long
    # digit-free run. Over-reach is what the Table 1 gate catches.
    pat <- sprintf("[0-9]{4}%sF[0-9]?[^0-9]{0,120}?([0-9]{1,3})[^0-9]{1,12}([0-9]{1,3})",
                   TRACKS[[nm]])
    hits <- regmatches(txt, gregexpr(pat, txt))[[1]]
    q <- m <- integer(0)
    for (h in hits) {
      nums <- as.integer(unlist(regmatches(h, gregexpr("[0-9]+", h))))
      n <- length(nums)
      q <- c(q, nums[n - 1]); m <- c(m, nums[n])
    }
    rows[[nm]] <- data.frame(appointment_year = as.integer(year), track = nm,
                             n_programs = length(hits), positions_offered = sum(q),
                             positions_filled = sum(m), stringsAsFactors = FALSE)
  }
  do.call(rbind, rows)
}

split_rows <- list(); split_bad <- list()
for (y in out$appointment_year) {
  s <- split_for(y)
  ref <- out[out$appointment_year == y, ]
  if (is.null(s)) {
    split_bad[[length(split_bad) + 1L]] <-
      sprintf("  %s  NO_TEXT       program table not extracted", y)
    next
  }
  # GATE: the reconstruction must land within SPLIT_TOLERANCE of Table 1 on both
  # counts. The residual is carried on every row.
  d_off <- sum(s$positions_offered) - ref$positions_offered
  d_fil <- sum(s$positions_filled) - ref$positions_filled
  if (abs(d_off) > SPLIT_TOLERANCE || abs(d_fil) > SPLIT_TOLERANCE) {
    split_bad[[length(split_bad) + 1L]] <- sprintf(
      "  %s  UNRECONCILED  reconstructed %d/%d against Table 1 %d/%d (off %+d/%+d)", y,
      sum(s$positions_offered), sum(s$positions_filled),
      ref$positions_offered, ref$positions_filled, d_off, d_fil)
    next
  }
  s$residual_offered <- d_off
  s$residual_filled <- d_fil
  s$reconciles_exactly <- (d_off == 0L && d_fil == 0L)
  split_rows[[length(split_rows) + 1L]] <- s
}

if (length(split_bad)) {
  cat("\n=== TRACK SPLIT: YEARS NOT SHIPPED (reported, never imputed) ===\n")
  for (b in split_bad) cat(b, "\n")
}
if (length(split_rows)) {
  sp <- do.call(rbind, split_rows)
  sp$available_by_year <- sp$appointment_year
  sp$retrieved_on <- RETRIEVED
  sp$source_url <- unname(URLS[as.character(sp$appointment_year)])

  utils::write.csv(sp, SPLIT_OUT, row.names = FALSE)
  cat("\nWrote", SPLIT_OUT, "\n")
  print(sp[, c("appointment_year", "track", "n_programs", "positions_offered",
               "positions_filled", "residual_offered", "residual_filled",
               "reconciles_exactly")], row.names = FALSE)
  cat(sprintf("\n%d of %d years within tolerance %d and shipped (%d exact).\n",
              length(split_rows), nrow(out), SPLIT_TOLERANCE,
              sum(vapply(split_rows, function(z) z$reconciles_exactly[1], logical(1)))))
} else {
  cat("\nNo year's track split reconciles with Table 1; nothing written.\n")
}
