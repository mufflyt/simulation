################################################################################
# analysis/validate_retirement_signal.R
#
# Does credentials.retirement_consensus actually measure RETIREMENT?
#
# build_urps_exit_hazard() fits a Gompertz to departures drawn from
# credentials.retirement_consensus (see urps_cliff_query()). That table is
# assembled from billing- and enrolment-cessation signals -- Medicare Part B/D,
# Part D, Open Payments, Medicaid rosters. Cessation of billing is not
# retirement: it also fires on a job change, a move, a shift to a
# non-Medicare population, or leaving fee-for-service.
#
# This script tests that directly, against an INDEPENDENT data stream: does a
# physician bill Medicare Part B in a year AFTER the year the consensus says
# they retired? main.medicare_part_b_retirement_detection carries per-NPI,
# per-year service counts and did not feed the consensus flag.
#
# RESULT AS OF 2026-08-13 (n = 37 URPS departures in the fitted cohort):
#
#   * 26% false-positive rate (6 of 23 testable). One claimed 2017 retirement
#     billed 3,548 services in 2023 -- six years later. Two "retired" at 46
#     and 47 and billed 4,397 / 5,596 services the following year.
#   * Raising min_confidence does not separate them: 26% at >= 0.60, 20% at
#     >= 0.75, still 16% at >= 0.90. Every contributing source shares the same
#     blind spot, so agreement between them is not evidence of correctness.
#   * Dropping the contradicted records swings male exit probability at 65 by
#     +124% and raises hazard_cv from 0.171 to 0.198 -- FURTHER above the 0.15
#     analogy fallback. The binding problem is sample size, not contamination.
#
# CONCLUSION: do not promote observed_hazard to the default retirement source.
# The fix is upstream -- retirement_consensus needs a persistence rule (no
# billing across ALL subsequent covered years, not merely a gap) plus a
# positive retirement signal (license lapse, ABMS certification expiry).
# credentials.retirement_signal_abms and retirement_signal_reactivation look
# like the relevant inputs; the latter's existence suggests someone already
# noticed that people come back.
#
# Usage:
#   Rscript analysis/validate_retirement_signal.R [/path/to/nber_my_duckdb.duckdb]
################################################################################

suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
})

# ---- configuration ---------------------------------------------------------

DB_DEFAULT <- "/Volumes/MufflySamsung 1 1/DuckDB/nber_my_duckdb.duckdb"
TEMP_DIR   <- "/Volumes/MufflySamsung 1 1/duckdb_temp"

# The claims table's coverage. A contradiction is only OBSERVABLE inside it:
# if someone "retired" in 2024 there is no later covered year to check, and
# counting them as uncontradicted would understate the error rate.
COVERAGE_MIN <- 2018L
COVERAGE_MAX <- 2023L

TAXONOMY_URPS <- "207VF0040X"   # ObGyn / Female Pelvic Medicine & Reconstructive Surgery
ROSTER_YEARS  <- 2013:2024
MIN_CONF      <- 0.60
AGE_RANGE     <- c(30L, 80L)

args   <- commandArgs(trailingOnly = TRUE)
db_path <- if (length(args) >= 1) args[[1]] else DB_DEFAULT

if (!file.exists(db_path)) {
  stop("DuckDB not found at: ", db_path,
       "\nPass the path as the first argument.", call. = FALSE)
}

# ---- pull the cohort with independent activity evidence --------------------

con <- dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE,
                 config = list(temp_directory = TEMP_DIR, threads = "4"))
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

tax_clause <- paste(sprintf("taxonomy_%d = '%s'", 1:14, TAXONOMY_URPS),
                    collapse = " OR ")
roster <- paste(sprintf(
  "SELECT npi, gender, %d AS yr FROM credentials.temporal_obgyn_only_%d WHERE %s",
  ROSTER_YEARS, ROSTER_YEARS, tax_clause), collapse = " UNION ")

d <- dbGetQuery(con, sprintf("
  WITH cohort AS (SELECT npi, MAX(gender) AS gender FROM (%s) GROUP BY npi),
       consensus AS (
         SELECT npi, retirement_year_final AS ry,
                retirement_confidence_final AS conf, sources
         FROM credentials.retirement_consensus
         WHERE is_retired_final AND retirement_year_final IS NOT NULL),
       birth AS (
         SELECT npi, MAX(year_of_birth) AS yob
         FROM main.doximity_2024_medical_school
         WHERE year_of_birth IS NOT NULL GROUP BY npi),
       activity AS (
         SELECT npi_char AS npi, MAX(data_year) AS last_active_yr,
                SUM(total_services) AS services
         FROM main.medicare_part_b_retirement_detection
         WHERE total_services > 0 GROUP BY npi_char)
  SELECT cohort.npi, cohort.gender, consensus.ry, consensus.conf,
         consensus.sources, consensus.ry - birth.yob AS age_at_ret,
         activity.last_active_yr, activity.services
  FROM cohort
  JOIN consensus USING (npi)
  JOIN birth USING (npi)
  LEFT JOIN activity USING (npi)
  WHERE cohort.gender IS NOT NULL
    AND consensus.conf >= %f
    AND consensus.ry - birth.yob BETWEEN %d AND %d",
  roster, MIN_CONF, AGE_RANGE[1], AGE_RANGE[2]))

# ---- classify --------------------------------------------------------------

d$billed_after <- !is.na(d$last_active_yr) & d$last_active_yr > d$ry
d$in_claims    <- !is.na(d$last_active_yr)
# Visible in claims AND with at least one covered year after the claimed exit.
d$testable     <- d$in_claims & d$ry < COVERAGE_MAX

n_test <- sum(d$testable)
n_bad  <- sum(d$testable & d$billed_after)

# ---- report ----------------------------------------------------------------

cat("\n=== Retirement-signal validation ===\n")
cat("DB           :", db_path, "\n")
cat("claims window:", COVERAGE_MIN, "-", COVERAGE_MAX, "\n")
cat("cohort       :", nrow(d), "URPS departures (taxonomy", TAXONOMY_URPS, ")\n")
cat("in claims    :", sum(d$in_claims), "\n")
cat("testable     :", n_test, " (in claims and claimed exit before", COVERAGE_MAX, ")\n")
cat("unverifiable :", nrow(d) - n_test, "\n\n")

cat(sprintf("FALSE-POSITIVE RATE: %.0f%% (%d/%d) billed Medicare AFTER the claimed exit\n",
            100 * n_bad / max(n_test, 1L), n_bad, n_test))
cat("This is a FLOOR, not an estimate: the unverifiable records could only raise it.\n\n")

cat("--- confidence does not separate signal from noise ---\n")
for (thr in c(0.60, 0.75, 0.90)) {
  s <- d[d$testable & d$conf >= thr, ]
  cat(sprintf("  conf >= %.2f : %2d testable, %d contradicted (%.0f%%)\n",
              thr, nrow(s), sum(s$billed_after),
              100 * mean(s$billed_after)))
}

cat("\n--- the contradicted records ---\n")
bad <- d[d$testable & d$billed_after,
         c("gender", "ry", "conf", "age_at_ret", "last_active_yr", "services", "sources")]
bad <- bad[order(bad$ry), ]
print(bad, row.names = FALSE, digits = 4)

cat("\n--- age at claimed exit ---\n")
t <- d[d$testable, ]
cat("  contradicted   median:", median(t$age_at_ret[t$billed_after]), "\n")
cat("  uncontradicted median:", median(t$age_at_ret[!t$billed_after]), "\n")

cat(sprintf("\nRecords surviving validation: %d (fitter needs >= 30 total, >= 10 per sex)\n",
            sum(!d$billed_after)))
print(table(d$gender[!d$billed_after]))

invisible(d)
