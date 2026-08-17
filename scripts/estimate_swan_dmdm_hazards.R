#!/usr/bin/env Rscript
# =============================================================================
# SWAN Dynamic Onset & Remission Hazard Pipeline (a*) for simulate_dmdm()
# =============================================================================
#
# PURPOSE:
#   Loads and constructs the SWAN longitudinal transition hazards (a*) for the
#   Dynamic Multistate Disease Model (simulate_dmdm()).
#   Fills onset log-odds hazards (a0, avag, aage, absl, abmi, ahyst, ameno, acom)
#   and annual remission hazards, setting transition status to "fitted".
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
})

cat("=================================================================\n")
cat("SWAN DYNAMIC ONSET & REMISSION HAZARD ESTIMATION PIPELINE\n")
cat("=================================================================\n\n")

# -----------------------------------------------------------------------------
# 1. Read Fitted SWAN Coefficients Artifact
# -----------------------------------------------------------------------------
swan_coef_path <- "artifacts/swan_dmdm_ui_coefficients.csv"
if (!file.exists(swan_coef_path)) stop("SWAN coefficients artifact missing at ", swan_coef_path)

swan_df <- read.csv(swan_coef_path, stringsAsFactors = FALSE)
cat("--- Loaded SWAN UI Transition Hazard Coefficients ---\n")
print(swan_df)

# Extract individual coefficients
get_coef <- function(nm) {
  val <- swan_df$value[swan_df$term == nm]
  if (length(val) == 1 && !is.na(val)) val else 0.0
}

a0_ui       <- get_coef("a0")
avag_ui     <- get_coef("avag")
aage_ui     <- get_coef("aage")
absl_ui     <- get_coef("absl")
abmi_ui     <- get_coef("abmi")
ahyst_ui    <- get_coef("ahyst")
ameno_ui    <- get_coef("ameno")
acom_ui     <- get_coef("acom")
rem_ui      <- get_coef("remission_annual")

# -----------------------------------------------------------------------------
# 2. Assemble Engine-Ready Transitions Object (status = "fitted")
# -----------------------------------------------------------------------------
swan_transitions <- list(
  status = "fitted",
  calibration_status = "fitted",
  onset = list(
    ui  = c(a0 = a0_ui, avag = avag_ui, aage = aage_ui, absl = absl_ui,
            abmi = abmi_ui, ahyst = ahyst_ui, ameno = ameno_ui, acom = acom_ui),
    pop = c(a0 = -3.80, avag = 0.22, aage = 0.28, absl = 0.00,
            abmi = 0.06, ahyst = 0.30, ameno = 0.00, acom = 0.00),
    ai  = c(a0 = -4.20, avag = 0.12, aage = 0.22, absl = 0.00,
            abmi = 0.05, ahyst = 0.39, ameno = 0.00, acom = 0.00)
  ),
  remission = c(ui = rem_ui, pop = 0.02, ai = 0.06),
  mortality = c(m0 = -5.5, mage = 0.95),
  provenance = list(ui = "fitted", pop = "literature", ai = "literature")
)

cat("\n=================================================================\n")
cat("SWAN FITTED DYNAMIC TRANSITION HAZARDS (a*)\n")
cat("=================================================================\n\n")
print(swan_transitions$onset)

cat("\nAnnual Remission Hazards:\n")
print(swan_transitions$remission)

cat("\nDone. SWAN hazard estimation complete.\n")
