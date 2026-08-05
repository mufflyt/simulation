#!/usr/bin/env Rscript
# =============================================================================
# SWAN -> DMDM hazard fit runner
# =============================================================================
# Turns the DMDM's UI onset/remission hazards from placeholders into FITTED
# estimates using SWAN, and assembles a full, engine-ready transition object with
# honest per-condition provenance:
#   UI  -> fitted from SWAN (this runner)
#   POP -> literature (dmdm_transitions_with_pop_literature(); SWAN has no POP-Q)
#   AI  -> placeholder (SWAN does not follow anal incontinence)
#
#   Rscript scripts/run_swan_dmdm_fit.R
#
# Wires the package's own machinery end to end:
#   build_swan_dmdm_panel()  (R/47)  SWAN wide -> person-year panel (UI)
#   dmdm_transition_data()   (R/31)  panel -> at-risk transition rows
#   fit_dmdm_transitions()   (R/31)  rows -> fitted UI onset logistic + remission
#   swan_panel_fit_caveats() (R/47)  the caveats that MUST travel with the fit
#
# INPUT: a wide SWAN frame (one row per participant, visit-suffixed columns) --
#   swan_all_visits.rds is the intended input. Download SWAN from ICPSR first with
#   scripts/data_acquisition/09_download_swan_icpsr.R, then point this runner at
#   the file via SWAN_WIDE_PATH (or the default path below).
#
# OUTPUT:
#   artifacts/swan_dmdm_transitions.rds        (engine-ready: fitted UI + lit POP)
#   artifacts/swan_dmdm_ui_coefficients.csv    (fitted UI onset coefs + remission)
#   artifacts/swan_dmdm_fit_caveats.txt        (proxy/unmeasured caveats)
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr); library(tibble); library(tidyr)
})
if (!requireNamespace("urpssim", quietly = TRUE)) {
  pkgload::load_all(".", quiet = TRUE)
} else {
  library(urpssim)
}

# ---- Resolve + read the SWAN wide frame -------------------------------------
# Resolution order: explicit override, then the repo-local copy, then
# swan_path() -- which already resolves the configured external location and is
# where the archive actually lives. Consulting only the hardcoded repo path made
# this script report "download SWAN first" on a machine that had SWAN mounted.
#
# The local variable is `swan_file`, not `swan_path`: naming it after the
# function would shadow swan_path() for the rest of the script.
swan_file <- Sys.getenv("SWAN_WIDE_PATH", "")
if (!nzchar(swan_file)) {
  candidates <- c(file.path("data-raw", "swan", "swan_all_visits.rds"),
                  tryCatch(swan_path("swan_all_visits.rds"),
                           error = function(e) NA_character_))
  found <- candidates[!is.na(candidates) & file.exists(candidates)]
  swan_file <- if (length(found)) found[1] else candidates[1]
}
if (!file.exists(swan_file)) {
  stop("SWAN wide frame not found at '", swan_file, "'.\n",
       "  Looked in data-raw/swan/ and swan_path() (the configured external\n",
       "  location). Download SWAN (ICPSR series 253) first:\n",
       "    Sys.setenv(icpsr_email=..., icpsr_password=...)\n",
       "    source('scripts/data_acquisition/09_download_swan_icpsr.R')\n",
       "  then set SWAN_WIDE_PATH to the wide file (swan_all_visits.rds).",
       call. = FALSE)
}
# Read through load_swan_archive() so the SHA-256 is verified and recorded.
# readRDS() on the same bytes produces an identical frame and a fit whose
# caveats say "archive provenance not recorded" -- an artifact that cannot say
# which file it came from.
swan_wide <- if (grepl("\\.rds$", swan_file, ignore.case = TRUE)) {
  load_swan_archive(path = swan_file, verbose = TRUE)
} else {
  utils::read.csv(swan_file, stringsAsFactors = FALSE)
}
message("Loaded SWAN wide frame: ", nrow(swan_wide), " participants x ",
        ncol(swan_wide), " columns")

# ---- Fit the UI hazards -----------------------------------------------------
panel  <- build_swan_dmdm_panel(swan_wide, conditions = "ui")
caveats <- swan_panel_fit_caveats(panel)
td  <- dmdm_transition_data(panel, conditions = "ui")
fit <- fit_dmdm_transitions(td, conditions = "ui")   # status = "fitted"
stopifnot(identical(fit$status, "fitted"))

# ---- Assemble a full engine-ready transition object -------------------------
# Start from the literature-POP object (UI/AI placeholder, POP derived_by_analogy)
# and overlay the fitted UI. Record per-condition provenance; the object-level
# calibration_status is the WEAKEST condition (AI is still a placeholder), so any
# downstream export is honest about the mix.
status_rank <- c(placeholder_uncalibrated = 1L, uncalibrated_illustrative = 1L,
                 derived_by_analogy = 2L, fitted = 3L, calibrated = 4L)
weakest <- function(s) { r <- status_rank[s]; r[is.na(r)] <- 0L; s[which.min(r)] }

tr <- dmdm_transitions_with_pop_literature()
tr$onset$ui      <- fit$onset$ui
tr$remission[["ui"]] <- fit$remission[["ui"]]
tr$provenance$ui <- "fitted"
tr$provenance    <- tr$provenance[c("ui", "pop", "ai")]
tr$status <- tr$calibration_status <-
  unname(weakest(unlist(tr$provenance, use.names = FALSE)))

# ---- Write outputs ----------------------------------------------------------
dir.create("artifacts", showWarnings = FALSE)
saveRDS(tr, "artifacts/swan_dmdm_transitions.rds")

ui <- fit$onset$ui
coef_out <- data.frame(
  term = c(names(ui), "remission_annual"),
  value = c(unname(ui), unname(fit$remission[["ui"]])),
  stringsAsFactors = FALSE)
utils::write.csv(coef_out, "artifacts/swan_dmdm_ui_coefficients.csv", row.names = FALSE)
writeLines(caveats, "artifacts/swan_dmdm_fit_caveats.txt")

# ---- Report -----------------------------------------------------------------
cat("\n== SWAN-fitted UI onset (log-odds) + remission ==\n")
print(round(setNames(coef_out$value, coef_out$term), 4))
cat("\n== Per-condition provenance of the assembled transition object ==\n")
print(unlist(tr$provenance))
cat("object calibration_status (weakest condition): ", tr$calibration_status, "\n", sep = "")
cat("\n== Caveats (also written to artifacts/swan_dmdm_fit_caveats.txt) ==\n")
cat(paste0("  - ", caveats), sep = "\n"); cat("\n")

# ---- Optional: confirm the fitted object drives the engine ------------------
demo <- data.frame(age = 50:70, cumulative_vaginal_deliveries = 2L,
                   years_since_last_vaginal_birth = 0, bmi = 28, hysterectomy = 0,
                   menopause_status = as.integer(50:70 >= 51), comorbidity = 0)
out <- tryCatch(
  suppressMessages(simulate_dmdm(demo, 2025, 2030, transitions = tr, seed = 1,
                                 allow_uncalibrated = TRUE)),
  error = function(e) { message("engine demo skipped: ", conditionMessage(e)); NULL })
if (!is.null(out))
  cat(sprintf("\nEngine check: prev_ui 2025 -> 2030 = %.3f -> %.3f (UI now data-driven)\n",
              out$prev_ui[1], out$prev_ui[nrow(out)]))

cat("\nWrote artifacts/swan_dmdm_transitions.rds, swan_dmdm_ui_coefficients.csv, ",
    "swan_dmdm_fit_caveats.txt\n", sep = "")
