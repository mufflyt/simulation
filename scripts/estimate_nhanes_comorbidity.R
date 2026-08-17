#!/usr/bin/env Rscript
# =============================================================================
# Estimate NHANES Comorbidity Log-Odds (bcomorb) for URPS Microsimulation
# =============================================================================
#
# PURPOSE:
#   Downloads DIQ (Diabetes), BPQ (Hypertension), and MCQ (Medical Conditions / CVD / Stroke / Arthritis)
#   modules from NHANES and merges them onto the UI (2017-2023) and AI (2005-2010) person-level microdata.
#   Fits survey-weighted logistic regressions to estimate bcomorb for UI and AI.
# =============================================================================

suppressPackageStartupMessages({
  library(nhanesA)
  library(survey)
  library(dplyr)
  library(tibble)
})

cat("=================================================================\n")
cat("NHANES COMORBIDITY LOG-ODDS ESTIMATION PIPELINE\n")
cat("=================================================================\n\n")

is_yes <- function(x) {
  if (is.null(x)) return(FALSE)
  if (is.logical(x)) return(x & !is.na(x))
  if (is.numeric(x)) return(!is.na(x) & x == 1)
  if (is.factor(x) || is.character(x)) {
    return(grepl("^Yes|^1", as.character(x), ignore.case = TRUE) & !is.na(x))
  }
  FALSE
}

# -----------------------------------------------------------------------------
# 1. Fetch Comorbidity Modules for UI Cohort (2017-2023)
# -----------------------------------------------------------------------------
ui_path <- "data-raw/nhanes/nhanes_ui_person_2017_2023.rds"
if (!file.exists(ui_path)) stop("UI microdata not found at ", ui_path)
ui <- readRDS(ui_path)

cat("Fetching 2017-2020 & 2021-2023 comorbidity modules...\n")
p_diq <- nhanes("P_DIQ")
p_bpq <- nhanes("P_BPQ")
p_mcq <- nhanes("P_MCQ")

ui_comorb <- p_diq |>
  dplyr::full_join(p_bpq, by = "SEQN") |>
  dplyr::full_join(p_mcq, by = "SEQN") |>
  dplyr::select(SEQN, dplyr::any_of(c("DIQ010", "BPQ020", "MCQ160A", "MCQ160B", "MCQ160C", "MCQ160E", "MCQ160F"))) |>
  dplyr::mutate(
    diabetes     = is_yes(DIQ010),
    hypertension = is_yes(BPQ020),
    cvd_arthritis= is_yes(MCQ160A) | is_yes(MCQ160B) | is_yes(MCQ160C) | is_yes(MCQ160E) | is_yes(MCQ160F),
    comorbidity  = as.numeric(diabetes | hypertension | cvd_arthritis)
  )

ui_merged <- ui |>
  dplyr::left_join(ui_comorb[, c("SEQN", "diabetes", "hypertension", "cvd_arthritis", "comorbidity")], by = "SEQN") |>
  dplyr::mutate(
    age_c              = (age - 50) / 10,
    bmi_c              = (bmi - 27) / 5,
    vaginal_deliveries = dplyr::coalesce(as.numeric(vaginal_deliveries), 0),
    hysterectomy       = dplyr::coalesce(hysterectomy, FALSE),
    comorbidity        = dplyr::coalesce(comorbidity, 0)
  )

des_ui <- survey::svydesign(ids = ~1, weights = ~WTMEC_pooled, data = ui_merged)

fit_ui_comorb <- survey::svyglm(
  ui_any ~ age_c + vaginal_deliveries + bmi_c + hysterectomy + comorbidity,
  design = des_ui,
  family = stats::quasibinomial()
)

ui_summary <- summary(fit_ui_comorb)$coefficients
ui_ci      <- stats::confint(fit_ui_comorb)
ui_or      <- exp(cbind(OR = stats::coef(fit_ui_comorb), ui_ci))

cat("\n--- UI Any Multivariable Model with Comorbidity (NHANES 2017-2023, n =", nrow(ui_merged), ") ---\n")
ui_table <- tibble::tibble(
  term       = names(stats::coef(fit_ui_comorb)),
  log_odds   = unname(stats::coef(fit_ui_comorb)),
  std_error  = unname(ui_summary[, "Std. Error"]),
  t_value    = unname(ui_summary[, "t value"]),
  p_value    = unname(ui_summary[, "Pr(>|t|)"]),
  odds_ratio = unname(ui_or[, "OR"]),
  ci_2.5     = unname(ui_or[, "2.5 %"]),
  ci_97.5    = unname(ui_or[, "97.5 %"])
)
print(as.data.frame(ui_table), digits = 4)

# -----------------------------------------------------------------------------
# 2. Fetch Comorbidity Modules for AI Cohort (2005-2010)
# -----------------------------------------------------------------------------
ai_path <- "data-raw/nhanes/nhanes_ai_person_2005_2010.rds"
if (!file.exists(ai_path)) stop("AI microdata not found at ", ai_path)
ai <- readRDS(ai_path)

cat("\nFetching 2005-2010 comorbidity modules (DIQ, BPQ, MCQ)...\n")
fetch_comorb_cycle <- function(letter) {
  diq <- nhanes(paste0("DIQ_", letter))
  bpq <- nhanes(paste0("BPQ_", letter))
  mcq <- nhanes(paste0("MCQ_", letter))
  
  diq |>
    dplyr::full_join(bpq, by = "SEQN") |>
    dplyr::full_join(mcq, by = "SEQN") |>
    dplyr::select(SEQN, dplyr::any_of(c("DIQ010", "BPQ020", "MCQ160A", "MCQ160B", "MCQ160C", "MCQ160E", "MCQ160F")))
}

ai_comorb <- dplyr::bind_rows(
  fetch_comorb_cycle("D"),
  fetch_comorb_cycle("E"),
  fetch_comorb_cycle("F")
) |>
  dplyr::mutate(
    diabetes     = is_yes(DIQ010),
    hypertension = is_yes(BPQ020),
    cvd_arthritis= is_yes(MCQ160A) | is_yes(MCQ160B) | is_yes(MCQ160C) | is_yes(MCQ160E) | is_yes(MCQ160F),
    comorbidity  = as.numeric(diabetes | hypertension | cvd_arthritis)
  )

ai_merged <- ai |>
  dplyr::left_join(ai_comorb[, c("SEQN", "diabetes", "hypertension", "cvd_arthritis", "comorbidity")], by = "SEQN") |>
  dplyr::mutate(
    age_c        = (age - 50) / 10,
    bmi_c        = (bmi - 27) / 5,
    live_births  = dplyr::coalesce(as.numeric(live_births), 0),
    hysterectomy = dplyr::coalesce(hysterectomy, FALSE),
    comorbidity  = dplyr::coalesce(comorbidity, 0)
  )

des_ai <- survey::svydesign(ids = ~1, weights = ~WTMEC_pooled, data = ai_merged)

fit_ai_comorb <- survey::svyglm(
  fi_wu ~ age_c + live_births + bmi_c + hysterectomy + comorbidity,
  design = des_ai,
  family = stats::quasibinomial()
)

ai_summary <- summary(fit_ai_comorb)$coefficients
ai_ci      <- stats::confint(fit_ai_comorb)
ai_or      <- exp(cbind(OR = stats::coef(fit_ai_comorb), ai_ci))

cat("\n--- AI (Wu 2014) Multivariable Model with Comorbidity (NHANES 2005-2010, n =", nrow(ai_merged), ") ---\n")
ai_table <- tibble::tibble(
  term       = names(stats::coef(fit_ai_comorb)),
  log_odds   = unname(stats::coef(fit_ai_comorb)),
  std_error  = unname(ai_summary[, "Std. Error"]),
  t_value    = unname(ai_summary[, "t value"]),
  p_value    = unname(ai_summary[, "Pr(>|t|)"]),
  odds_ratio = unname(ai_or[, "OR"]),
  ci_2.5     = unname(ai_or[, "2.5 %"]),
  ci_97.5    = unname(ai_or[, "97.5 %"])
)
print(as.data.frame(ai_table), digits = 4)

cat("\nDone. Comorbidity estimation complete.\n")
