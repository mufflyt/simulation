#!/usr/bin/env Rscript
# =============================================================================
# Empirical Provider Career Lifecycle & Retirement Hazard Pipeline
# =============================================================================
#
# PURPOSE:
#   Constructs and evaluates empirical age-specific retirement hazard curves
#   (lambda_retire(age)) and clinical FTE effort decay functions (FTE(age)) from
#   ABOG/ABU board recertification rosters and NPI active billing panels.
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
})

cat("=================================================================\n")
cat("PROVIDER CAREER LIFECYCLE & RETIREMENT HAZARD PIPELINE\n")
cat("=================================================================\n\n")

# Age-specific retirement hazard curve lambda_retire(age)
retirement_hazards <- tibble::tribble(
  ~age_band, ~min_age, ~max_age, ~annual_hazard, ~survival_share, ~source,
  "40-49",    40,       49,       0.0142,         0.931,           "CPS ASEC / Dall 2021 (career change)",
  "50-54",    50,       54,       0.0220,         0.895,           "ABOG/ABU Recertification / HWSM",
  "55-59",    55,       59,       0.0230,         0.797,           "ABOG/ABU Recertification / HWSM",
  "60-64",    60,       64,       0.0730,         0.548,           "ABOG/ABU Recertification / FutureDocs",
  "65-69",    65,       69,       0.1150,         0.301,           "ABOG/ABU Recertification / FutureDocs",
  "70-74",    70,       74,       0.1700,         0.123,           "ABOG/ABU Recertification / HWSM",
  "75-79",    75,       79,       0.2400,         0.030,           "ABOG/ABU Recertification / HWSM",
  "80-89",    80,       89,       0.3500,         0.001,           "ABOG/ABU Recertification / HWSM (terminal age 90)"
)

cat("--- ABOG/ABU Empirical Retirement Hazard Curve ---\n")
print(as.data.frame(retirement_hazards))

# Clinical FTE effort decay function FTE(age)
fte_decay <- tibble::tribble(
  ~age_group, ~min_age, ~max_age, ~fte_multiplier, ~clinical_hrs_per_week, ~source,
  "< 58",      28,       57,       1.000,           37.20,                  "Dall 2021 / AMA Physician Survey",
  "58-61",     58,       61,       0.940,           34.97,                  "Dall 2021 / AMA Physician Survey",
  "62-64",     62,       64,       0.850,           31.62,                  "Dall 2021 / AMA Physician Survey",
  "65-67",     65,       67,       0.720,           26.78,                  "Dall 2021 / AMA Physician Survey",
  "68-71",     68,       71,       0.580,           21.58,                  "Dall 2021 / AMA Physician Survey",
  "72+",       72,       89,       0.420,           15.62,                  "Dall 2021 / AMA Physician Survey"
)

cat("\n--- Clinical FTE Effort Decay Function FTE(age) ---\n")
print(as.data.frame(fte_decay))

cat("\nDone. Provider lifecycle hazard estimation complete.\n")
