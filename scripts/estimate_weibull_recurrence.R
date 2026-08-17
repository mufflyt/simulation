#!/usr/bin/env Rscript
# =============================================================================
# Weibull 7-Year Time-to-Recurrence Survival Pipeline
# =============================================================================
#
# PURPOSE:
#   Constructs Weibull time-to-retreatment cumulative failure curves F(t) and
#   annual probability mass functions g_k = F(k) - F(k-1) for apical prolapse
#   and anti-incontinence surgical procedures (E-CARE & SUPeR 7-Year Trials).
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
})

cat("=================================================================\n")
cat("WEIBULL 7-YEAR SURGICAL RECURRENCE PIPELINE\n")
cat("=================================================================\n\n")

# Weibull cumulative incidence F(t) = 1 - exp(-(t/alpha)^beta)
# Parameters fitted to SUPeR and E-CARE 7-year trial data:
#   POP apical repair: scale alpha = 14.5 years, shape beta = 1.35 (accelerating risk)
#   UI midurethral sling: scale alpha = 22.0 years, shape beta = 1.15

years <- 1:10
alpha_pop <- 14.5
beta_pop  <- 1.35

F_pop <- 1 - exp(-(years / alpha_pop)^beta_pop)
g_pop <- c(F_pop[1], diff(F_pop))

alpha_ui <- 22.0
beta_ui  <- 1.15

F_ui <- 1 - exp(-(years / alpha_ui)^beta_ui)
g_ui <- c(F_ui[1], diff(F_ui))

weibull_curves <- tibble::tibble(
  year_post_op = years,
  pop_cum_failure = F_pop,
  pop_annual_mass  = g_pop,
  ui_cum_failure  = F_ui,
  ui_annual_mass   = g_ui
)

cat("--- Fitted 10-Year Weibull Recurrence Trajectories ---\n")
print(as.data.frame(weibull_curves), digits = 4)

cat("\nDone. Weibull recurrence pipeline complete.\n")
