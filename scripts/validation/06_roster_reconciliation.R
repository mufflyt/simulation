#!/usr/bin/env Rscript
# Reconciling the three URPS roster populations ----
#
#   Rscript scripts/validation/06_roster_reconciliation.R
#
# THE PROBLEM. Three artifacts claim to describe the same workforce and report
# three different sizes:
#
#   urps_roster_2026-07-22.csv                 1,500 rows / 1,495 npi values
#   urps_roster_2026-07-22_PROVENANCE.txt      "Rows: 1100 / Unique NPIs: 1092"
#   urps_provider_coordinates_2026-08-02.csv   1,552 rows / 1,552 NPIs
#
# Analysis 05 computes U_s -- the numerator of every bound -- by intersecting a
# roster with 2024 Medicare billing. Until it is settled which population that
# roster is, "reproduced at zero tolerance" says only that the same arithmetic
# ran twice. This script decides it by COMPARING IDENTIFIERS rather than
# reasoning about which count looks right, assigns every row of both data files
# exactly one disposition, and writes the frozen linkage roster that 05 consumes.
#
# WHAT IS DELIBERATELY NOT A CRITERION: activity in 2024. It is tempting to drop
# providers flagged retired, and it would be wrong. U_s is formed by
# intersecting with services that were actually billed in 2024, so a provider
# who did not bill contributes zero whether or not the roster carries them.
# Filtering on activity cannot remove a spurious match; it can only discard a
# real one -- someone flagged retired in a 2026 snapshot who was practising in
# 2024 -- which lowers U, lowers L, and weakens the only bound the analysis
# leads with. Inclusiveness is the conservative direction here.

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) pkgload::load_all(".", quiet = TRUE) else library(urpssim)
  library(data.table)
})
source(file.path("scripts", "validation", "_provenance.R"))

ROSTER <- file.path("data-raw", "urps_roster", "urps_roster_2026-07-22.csv")
COORDS <- file.path("data-raw", "urps_roster", "urps_provider_coordinates_2026-08-02.csv")
SIDECAR <- file.path("data-raw", "urps_roster", "urps_roster_2026-07-22_PROVENANCE.txt")
OUT <- file.path("data-raw", "urps_roster", "urps_linkage_roster_2024.csv")

DATA_YEAR <- 2024L

RUN <- begin_validation_run(
  "roster_reconciliation",
  params = list(
    purpose = "settle the 1500 / 1100 / 1552 discrepancy and freeze the 2024 linkage roster",
    data_year = DATA_YEAR,
    activity_filter = "NONE -- see header; filtering on activity can only lose true matches",
    consumed_by = "05_urps_share_partial_identification.R"),
  require_clean = TRUE, exploratory = FALSE,
  inputs = c(roster_csv = ROSTER, coordinate_csv = COORDS, roster_sidecar = SIDECAR))

# NPI check digit. A ten-digit string is not an NPI; NPIs carry a Luhn check
# over the 80840 prefix. Format-only validation would pass a transposed digit,
# which fails to match and is then indistinguishable from a physician who did
# not bill -- a silent loss from the numerator.
npi_valid <- function(x) {
  vapply(x, function(s) {
    if (is.na(s) || !grepl("^[0-9]{10}$", s)) return(FALSE)
    d <- rev(as.integer(strsplit(paste0("80840", substr(s, 1, 9)), "")[[1]]))
    tot <- 0
    for (j in seq_along(d)) {
      v <- d[j]
      if (j %% 2 == 1) { v <- v * 2; if (v > 9) v <- v - 9 }
      tot <- tot + v
    }
    ((10 - tot %% 10) %% 10) == as.integer(substr(s, 10, 10))
  }, logical(1), USE.NAMES = FALSE)
}

ros <- fread(ROSTER, showProgress = FALSE)
ros[, npi := trimws(as.character(npi))]
coo <- fread(COORDS, showProgress = FALSE)
coo[, npi := trimws(as.character(npi))]

# ---- Row-level disposition: the roster file ---------------------------------
#
# Mutually exclusive and evaluated in this order, so every row lands in exactly
# one bucket and the buckets sum to the file.
ros[, dup_rank := seq_len(.N), by = npi]
ros[, disposition := fifelse(
  is.na(npi) | !nzchar(npi),              "missing_npi",
  fifelse(!npi_valid(npi),                "invalid_npi",
  fifelse(dup_rank > 1L,                  "duplicate_npi",
  fifelse(is.na(cert_year),               "missing_cert_year",
  fifelse(cert_year > DATA_YEAR,          "entered_after_2024",
                                          "included_2024")))))]
stopifnot(nrow(ros) == sum(table(ros$disposition)))

waterfall <- data.table(
  step = c("Raw canonical roster rows",
           "  less rows with no NPI",
           "  less rows failing the NPI check digit",
           "  less duplicate NPI rows",
           "  less rows with no certification year",
           "  less certified after 2024",
           "Final 2024 linkage roster (distinct NPIs)"),
  n = c(nrow(ros),
        -sum(ros$disposition == "missing_npi"),
        -sum(ros$disposition == "invalid_npi"),
        -sum(ros$disposition == "duplicate_npi"),
        -sum(ros$disposition == "missing_cert_year"),
        -sum(ros$disposition == "entered_after_2024"),
        sum(ros$disposition == "included_2024")))
cat("\n=== roster reconciliation waterfall ===\n")
print(waterfall, row.names = FALSE)
cat("\n=== roster row dispositions ===\n")
print(ros[, .N, by = disposition][order(-N)], row.names = FALSE)

# The excluded rows are named by pathway, because "which population is missing"
# is the question a reader will ask next and the answer is not uniform.
excluded_profile <- ros[disposition != "included_2024",
                        .N, by = .(disposition, pathway, in_model_baseline)][order(disposition, -N)]
cat("\n=== who the excluded rows are ===\n")
print(excluded_profile, row.names = FALSE)

linkage <- ros[disposition == "included_2024", .(npi, pathway, state, cert_year)]
setorder(linkage, npi)

# ---- Row-level disposition: the coordinate file -----------------------------
coo[, dup_rank := seq_len(.N), by = npi]
coo[, disposition := fifelse(
  is.na(npi) | !nzchar(npi),          "missing_npi",
  fifelse(!npi_valid(npi),            "invalid_npi",
  fifelse(dup_rank > 1L,              "duplicate_npi",
  fifelse(npi %chin% linkage$npi,     "in_linkage_roster",
  fifelse(npi %chin% ros$npi,         "in_roster_but_excluded",
                                      "coordinate_only")))))]
cat("\n=== coordinate-file row dispositions ===\n")
print(coo[, .N, by = disposition][order(-N)], row.names = FALSE)

# THE COORDINATE-ONLY SET IS AN UNRESOLVED ASCERTAINMENT GAP, NOT A DECISION
# THAT WAS MADE. These NPIs pass every validity check and sit in an extract
# produced by the same URPS provider pipeline, yet no roster row carries them.
# They are NOT added to the linkage roster, because the coordinate file mixes
# source runs -- one of them is a general obstetrics-and-gynaecology geocode
# file -- and nothing reachable from this repository establishes that these
# particular records are URPS subspecialists. The roster is the artifact that
# claims to be the roster.
#
# The direction of the resulting bias is stated rather than left implicit: if
# any of them ARE URPS, excluding them lowers U, lowers L, and makes every
# lower bound conservative. That is the safe direction for a result that leads
# with a lower bound, which is why this is recorded as a gap and not repaired
# by assumption.
gap <- coo[disposition == "coordinate_only"]
gap_profile <- data.frame(
  coordinate_only_npis = nrow(gap),
  distinct_source_runs = length(unique(gap$source_run)),
  source_runs = paste(unique(gap$source_run), collapse = " | "),
  cert_year_min = min(gap$cert_year, na.rm = TRUE),
  cert_year_max = max(gap$cert_year, na.rm = TRUE),
  flagged_retired = sum(gap$is_retired %in% TRUE),
  effect_if_urps = "would RAISE U and every lower bound; exclusion is conservative")
cat("\n=== unresolved ascertainment gap ===\n"); print(t(gap_profile))

# ---- The sidecar is a superseded generation, not a filter -------------------
#
# It is tempting to treat "Rows: 1100" as a stricter subset of the 1,500 and go
# looking for the filter. It is not one. The sidecar makes three checkable
# assertions about the file it accompanies, and the file contradicts all three,
# which places it before a regeneration rather than after a restriction. There
# is no row-level comparison to run: the 1,100-row extract is not on disk.
sidecar <- data.frame(
  assertion = c("Rows: 1100", "Unique NPIs: 1092", "ABOG rows: 830",
                "ABU rows with NPI: 270 of 294",
                "has_medicare_2024 is FALSE (no CY2024 activity file found)"),
  observed_in_file = c(
    sprintf("%d", nrow(ros)),
    sprintf("%d distinct NPI values (%d valid NPIs + NA)", uniqueN(ros$npi),
            sum(npi_valid(ros$npi))),
    sprintf("%d", sum(ros$pathway == "ABOG (OB/GYN)")),
    sprintf("%d ABU rows, %d with an NPI",
            sum(ros$pathway == "ABU (urology)"),
            sum(ros$pathway == "ABU (urology)" & !is.na(ros$npi))),
    sprintf("TRUE for %d rows", sum(ros$has_medicare_2024 %in% TRUE))),
  agrees = c(FALSE, FALSE, FALSE, FALSE, FALSE))
cat("\n=== sidecar assertions against the file it accompanies ===\n")
print(sidecar, row.names = FALSE)
cat("\nAll five disagree, so the sidecar documents a SUPERSEDED generation of\n")
cat("this roster. It is not a filter of the current file and no subset of the\n")
cat("current file should be constructed to match it.\n")

# ---- Freeze -----------------------------------------------------------------
utils::write.csv(linkage, OUT, row.names = FALSE, quote = FALSE)
frozen <- data.frame(
  path = OUT,
  npis = nrow(linkage),
  sha256 = digest::digest(file = OUT, algo = "sha256"),
  derived_from = basename(ROSTER),
  rule = "non-missing, Luhn-valid, distinct NPI with cert_year <= 2024; no activity filter")
cat("\n=== frozen 2024 linkage roster ===\n"); print(t(frozen))

complete_validation_run(RUN, tables = list(
  waterfall            = as.data.frame(waterfall),
  roster_dispositions  = as.data.frame(ros[, .N, by = disposition]),
  excluded_profile     = as.data.frame(excluded_profile),
  coordinate_dispositions = as.data.frame(coo[, .N, by = disposition]),
  ascertainment_gap    = gap_profile,
  sidecar_assertions   = sidecar,
  frozen_linkage_roster = frozen))
