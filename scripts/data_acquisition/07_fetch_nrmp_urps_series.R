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
# published IN its appointment year, so the 2017-2019 reports were all in hand
# well before a 2020 cutoff. `available_by_year` records that explicitly so a
# leakage audit can check availability rather than infer it.
#
# WHY IT IS A LEADING INDICATOR. URPS fellowship is three years, so fellows
# appointed in year Y finish in Y+3 and certify around Y+3 to Y+4:
#
#   appointed 2017 -> certify 2020/21     appointed 2019 -> certify 2022/23
#   appointed 2018 -> certify 2021/22     appointed 2020 -> certify 2023/24
#
# The 2017-2019 appointment years therefore forecast exactly the 2021-2023
# certifications the back-test scores, using only information a modeller had at
# the 2020 cutoff.
#
# WHICH COLUMN. NRMP Table 1 runs: Applicants (U.S. MD, All), Positions Offered,
# No. of Pgms, Matches (U.S. MD, All), % Filled (U.S. MD, All), Ranked Positions
# (U.S. MD, All), Unfilled Programs. The entering cohort is **Matches, All** --
# positions actually filled, not positions offered. Verified two ways: the
# %-filled columns reproduce as Matches/Positions Offered to the tenth, and the
# 2025 value extracted by this column (70) equals the independently
# human-verified value in mufflyt/cliff's data/nrmp_fellowship_entrants.csv.
#
# CAVEAT CARRIED FORWARD. NRMP counts MATCHED FELLOWS, and the contract counts
# BOARD CERTIFICATIONS. They are not the same people: some certify without
# passing through the match (the 2017 report notes the subspecialty "also
# includes programs not accredited by the ACGME"), and timing slips. Over
# 2021-2023 certifications averaged 69/yr against 58.7/yr matched three years
# earlier -- a conversion of about 1.17. That ratio is estimated from
# POST-CUTOFF data and must NOT be used in a back-test arm; it belongs to the
# forward model only.
#
# Requires: pdftotext (poppler) on PATH, and network access.
# Output: data-raw/calibration/nrmp_urps_entrants_series.csv

OUT <- "data-raw/calibration/nrmp_urps_entrants_series.csv"

# Human-verified from the source PDFs on 2026-08-04. Any NRMP re-issue or
# layout change must fail loudly rather than silently shift a column.
EXPECTED <- data.frame(
  appointment_year = c(2017L, 2018L, 2019L, 2020L, 2025L),
  positions_offered = c(64L, 60L, 64L, 65L, 70L),
  positions_filled = c(59L, 59L, 58L, 56L, 70L),
  pct_filled_all = c(92.2, 98.3, 90.6, 86.2, 100.0),
  n_programs = c(59L, 57L, 59L, 61L, 66L),
  stringsAsFactors = FALSE
)
URLS <- c(
  "2017" = "https://www.nrmp.org/wp-content/uploads/2021/07/Results-and-Data-SMS-2017.pdf",
  "2018" = "https://www.nrmp.org/wp-content/uploads/2021/07/Results-and-Data-SMS-2018.pdf",
  "2019" = "https://www.nrmp.org/wp-content/uploads/2021/07/Results-and-Data-SMS-2019.pdf",
  "2020" = "https://www.nrmp.org/wp-content/uploads/2021/07/Results-and-Data-SMS-2020.pdf",
  "2025" = "https://www.nrmp.org/wp-content/uploads/2025/02/SMS_Results_and_Data_2025.pdf"
)

if (nchar(Sys.which("pdftotext")) == 0) {
  stop("pdftotext (poppler) not found on PATH; install poppler to run this fetcher.",
       call. = FALSE)
}

tmp <- tempfile("nrmp"); dir.create(tmp)
on.exit(unlink(tmp, recursive = TRUE), add = TRUE)

extract_row <- function(year) {
  pdf <- file.path(tmp, sprintf("sms_%s.pdf", year))
  txt <- file.path(tmp, sprintf("sms_%s.txt", year))
  utils::download.file(URLS[[as.character(year)]], pdf, quiet = TRUE, mode = "wb")
  system2("pdftotext", c("-layout", shQuote(pdf), shQuote(txt)), stdout = NULL, stderr = NULL)
  lines <- readLines(txt, warn = FALSE)

  # The Table 1 body row: the label followed by at least three numeric fields.
  # Other mentions (contents listing, narrative) carry no such run.
  hit <- grep("Female Pelvic Medicine and Reconstructive\\s{2,}\\d", lines, value = TRUE)
  if (length(hit) == 0) {
    stop(sprintf("NRMP %s: no Table 1 row found for Female Pelvic Medicine", year),
         call. = FALSE)
  }
  nums <- as.numeric(gsub(",", "", regmatches(hit[1], gregexpr("[0-9][0-9,]*\\.?[0-9]*", hit[1]))[[1]]))
  if (length(nums) < 11) {
    stop(sprintf("NRMP %s: expected >= 11 numeric fields, got %d", year, length(nums)),
         call. = FALSE)
  }
  # Applicants(2), Positions Offered, No. Pgms, Matches(2), %Filled(2), Ranked(2), Unfilled
  data.frame(appointment_year = as.integer(year),
             positions_offered = as.integer(nums[3]),
             n_programs = as.integer(nums[4]),
             positions_filled = as.integer(nums[6]),
             pct_filled_all = nums[8],
             stringsAsFactors = FALSE)
}

rows <- do.call(rbind, lapply(names(URLS), extract_row))

# GATE 1: the percentage columns must reproduce from the counts. This is what
# proves the column mapping, independently of any remembered value.
recomputed <- round(100 * rows$positions_filled / rows$positions_offered, 1)
bad <- abs(recomputed - rows$pct_filled_all) > 0.15
if (any(bad)) {
  stop(sprintf(paste("NRMP column mapping FAILED for appointment year(s) %s:",
                     "filled/offered does not reproduce the printed %% filled",
                     "(%s vs %s). The table layout has changed."),
               paste(rows$appointment_year[bad], collapse = ", "),
               paste(recomputed[bad], collapse = ", "),
               paste(rows$pct_filled_all[bad], collapse = ", ")), call. = FALSE)
}

# GATE 2: values must match what a human read off the PDFs.
chk <- merge(rows, EXPECTED, by = "appointment_year", suffixes = c("", "_exp"))
for (f in c("positions_offered", "positions_filled", "n_programs")) {
  d <- chk[[f]] != chk[[paste0(f, "_exp")]]
  if (any(d)) {
    stop(sprintf("NRMP audit FAILED for %s in year(s) %s: got %s, expected %s",
                 f, paste(chk$appointment_year[d], collapse = ", "),
                 paste(chk[[f]][d], collapse = ", "),
                 paste(chk[[paste0(f, "_exp")]][d], collapse = ", ")), call. = FALSE)
  }
}

out <- rows[order(rows$appointment_year), ]
# Each report is published in its own appointment year, which is what makes the
# pre-2020 rows usable in a 2020-cutoff back-test.
out$report_published <- out$appointment_year
out$available_by_year <- out$appointment_year
out$source_url <- unname(URLS[as.character(out$appointment_year)])
out <- out[, c("appointment_year", "positions_offered", "positions_filled",
               "pct_filled_all", "n_programs", "report_published",
               "available_by_year", "source_url")]

utils::write.csv(out, OUT, row.names = FALSE)
cat("Wrote", OUT, "\n"); print(out[, 1:5], row.names = FALSE)
cat("\nPre-2020 mean filled (2017-2019):",
    round(mean(out$positions_filled[out$appointment_year <= 2019]), 2), "/yr\n")
