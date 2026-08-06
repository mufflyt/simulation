#!/usr/bin/env Rscript
# =============================================================================
# SWAN (Study of Women's Health Across the Nation) — download via ICPSR
# =============================================================================
#
# PURPOSE:
#   SWAN is the intended longitudinal source for fitting the DMDM onset/remission
#   hazards (R/demand-dmdm_fit_transitions.R). It follows a multi-ethnic cohort of
#   mid-life women across annual visits with repeated urinary-incontinence
#   measures and the covariates the engine uses (age, BMI, menopause status,
#   comorbidity; parity is on the baseline/screener). This script pulls the
#   SWAN public-use datasets from ICPSR so they can be reshaped into a person-year
#   panel and handed to dmdm_transition_data() -> fit_dmdm_transitions().
#
# IMPORTANT — POP CAVEAT:
#   SWAN measures urinary incontinence well but does NOT carry POP-Q staging, so
#   it fits UI (and AI where asked) but not graded prolapse. For POP, keep the
#   cited literature transitions (R/supply-roster, dmdm_transitions_with_pop_literature())
#   or fit from a POP-Q cohort (MOAD / WHI). See docs/DEMAND_METHODS.md sec 4.
#
# DATA ACCESS:
#   ICPSR account required (free). The PUBLIC-USE SWAN files download with an
#   account via the `icpsrdata` package, which logs in with your credentials.
#     Sys.setenv(icpsr_email = "you@inst.edu", icpsr_password = "...")
#   RESTRICTED SWAN files (exact dates, geography, some biomarkers) require a
#   signed Data Use Agreement / secure enclave and CANNOT be auto-downloaded —
#   request them through ICPSR's restricted-data path and place them manually.
#
#   SWAN is ICPSR Series 253:
#     https://www.icpsr.umich.edu/web/ICPSR/series/253
#   Each wave is a separate ICPSR study number. VERIFY the exact numbers for the
#   waves you need on the series page and list them in `swan_study_ids` below —
#   only the baseline id is pre-filled (others intentionally left for you to
#   confirm rather than hard-code possibly-stale ids).
#
# OUTPUT:
#   data-raw/swan/<study_id>/...        (raw ICPSR downloads, per study)
#   data-raw/swan/swan_manifest.txt
#   (reshaping into the DMDM panel is a separate, documented step below)
# =============================================================================

suppressPackageStartupMessages({
  for (p in c("icpsrdata", "here")) {
    if (!requireNamespace(p, quietly = TRUE))
      stop("Package '", p, "' is required. install.packages('", p, "')", call. = FALSE)
  }
})

if (nchar(Sys.getenv("icpsr_email")) == 0 || nchar(Sys.getenv("icpsr_password")) == 0) {
  stop(
    "ICPSR credentials not set. Register (free) at https://www.icpsr.umich.edu\n",
    "  then: Sys.setenv(icpsr_email = 'you@inst.edu', icpsr_password = 'YOUR_PW')\n",
    "  (icpsrdata reads these env vars to log in.)", call. = FALSE)
}

out_dir <- here::here("data-raw", "swan")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# SWAN public-use study numbers (ICPSR series 253). VERIFY / EXTEND on the series
# page before running — waves are added over time and numbering is not sequential.
#   28762 = SWAN Baseline Dataset, 1996-1997 (parity, baseline covariates)
# Add the cross-sectional screener and the annual visit datasets you need, e.g.
#   swan_study_ids <- c(28762, <screener>, <visit01>, <visit02>, ... )
swan_study_ids <- c(
  28762L   # Baseline 1996-1997  (VERIFY additional visit ids on series/253)
)

message("Downloading ", length(swan_study_ids), " SWAN study(ies) from ICPSR into ", out_dir)
icpsrdata::icpsr_download(
  file_id  = swan_study_ids,
  download_dir = out_dir,
  msg = TRUE
)

# ---- Manifest ---------------------------------------------------------------
writeLines(c(
  "SWAN (ICPSR series 253) download manifest",
  paste("Generated:", Sys.time()),
  paste("Studies:", paste(swan_study_ids, collapse = ", ")),
  paste("Download dir:", out_dir),
  "",
  "NEXT — reshape to the DMDM person-year panel (schema of dmdm_transition_data):",
  "  columns: person_id, year, age, cumulative_vaginal_deliveries,",
  "           years_since_last_vaginal_birth, bmi, hysterectomy,",
  "           menopause_status, comorbidity, has_ui[, has_pop, has_ai]",
  "  * one row per woman per visit-year; derive has_ui from the SWAN",
  "    incontinence items; carry parity from baseline forward.",
  "  Then:",
  "    td  <- dmdm_transition_data(swan_panel, conditions = 'ui')",
  "    fit <- fit_dmdm_transitions(td, conditions = 'ui')   # status = 'fitted'",
  "  POP is NOT in SWAN — keep dmdm_transitions_with_pop_literature() for prolapse.",
  "",
  "Public-use: account only. Restricted files: signed DUA, place manually.",
  "Series: https://www.icpsr.umich.edu/web/ICPSR/series/253"
), file.path(out_dir, "swan_manifest.txt"))

message("Done. SWAN raw files downloaded; reshape to the DMDM panel per the manifest.")
