#!/usr/bin/env Rscript
# =============================================================================
# Estimate NHANES Risk Model Log-Odds Coefficients for URPS Microsimulation
# =============================================================================
#
# PURPOSE:
#   Fits survey-weighted multivariable logistic regressions on real NHANES
#   person-level microdata to estimate empirical log-odds coefficients for:
#     1. Urinary Incontinence (UI Any) — NHANES 2017-2023 Pooled (n = 7,753)
#     2. Fecal Incontinence (AI Wu 2014) — NHANES 2005-2010 Pooled (n = 8,529)
#     3. Fecal Incontinence (AI NIH/Whitehead) — NHANES 2005-2010 Pooled (n = 8,529)
#
# FORMULA:
#   logit(P) = b0 + bvag * births + bage * ((age - 50)/10) + bysl * (ysl/10)
#            + bbmi * ((bmi - 27)/5) + bhyst * hyst + bmeno * meno + bcomorb * comorb
#
# CODEBOOK-VERIFIED VARIABLES:
#   RHD280   — Had uterus removed / hysterectomy (Yes/No)  [RHQ131 is ever pregnant]
#   RHD167   — Number of vaginal deliveries
#   RHQ171   — Number of live birth deliveries
#   RHQ031   — Had regular periods in past 12 months (FALSE = postmenopausal)
#   BMXBMI   — Body Mass Index (kg/m^2)
#   RIDAGEYR — Age in single years
#   BHQ010-40— Fecal incontinence frequency (gas, mucus, liquid, solid)
#   WTMEC_pooled — Survey weights (scaled for pooling)
# =============================================================================

suppressPackageStartupMessages({
  library(survey)
  library(dplyr)
  library(tibble)
})

cat("=================================================================\n")
cat("NHANES RISK LOG-ODDS COEFFICIENT ESTIMATION RUNNER\n")
cat("=================================================================\n\n")

# -----------------------------------------------------------------------------
# 1. Urinary Incontinence (UI Any) — NHANES 2017-2023
# -----------------------------------------------------------------------------
ui_path <- "data-raw/nhanes/nhanes_ui_person_2017_2023.rds"
if (!file.exists(ui_path)) stop("UI microdata not found at ", ui_path)

ui <- readRDS(ui_path)

# Center and scale continuous covariates per the simulation formula
ui_prep <- ui |>
  dplyr::mutate(
    age_c              = (age - 50) / 10,
    bmi_c              = (bmi - 27) / 5,
    vaginal_deliveries = dplyr::coalesce(as.numeric(vaginal_deliveries), 0),
    hysterectomy       = dplyr::coalesce(hysterectomy, FALSE),
    postmenopausal     = dplyr::coalesce(postmenopausal, FALSE)
  )

# Build survey design object with pooled MEC exam weights
des_ui <- survey::svydesign(ids = ~1, weights = ~WTMEC_pooled, data = ui_prep)

# Fit survey-weighted logistic regression
fit_ui <- survey::svyglm(
  ui_any ~ age_c + vaginal_deliveries + bmi_c + hysterectomy + postmenopausal,
  design = des_ui,
  family = stats::quasibinomial()
)

ui_summary <- summary(fit_ui)$coefficients
ui_ci      <- stats::confint(fit_ui)
ui_or      <- exp(cbind(OR = stats::coef(fit_ui), ui_ci))

cat("--- 1. UI Any Multivariable Logistic Regression (NHANES 2017-2023, n =", nrow(ui_prep), ") ---\n")
ui_table <- tibble::tibble(
  term             = names(stats::coef(fit_ui)),
  log_odds         = unname(stats::coef(fit_ui)),
  std_error        = unname(ui_summary[, "Std. Error"]),
  t_value          = unname(ui_summary[, "t value"]),
  p_value          = unname(ui_summary[, "Pr(>|t|)"]),
  odds_ratio       = unname(ui_or[, "OR"]),
  ci_2.5           = unname(ui_or[, "2.5 %"]),
  ci_97.5          = unname(ui_or[, "97.5 %"])
)
print(as.data.frame(ui_table), digits = 4)

# -----------------------------------------------------------------------------
# 2. Fecal Incontinence (AI Wu 2014 & NIH) — NHANES 2005-2010
# -----------------------------------------------------------------------------
ai_path <- "data-raw/nhanes/nhanes_ai_person_2005_2010.rds"
if (!file.exists(ai_path)) stop("AI microdata not found at ", ai_path)

ai <- readRDS(ai_path)

ai_prep <- ai |>
  dplyr::mutate(
    age_c        = (age - 50) / 10,
    bmi_c        = (bmi - 27) / 5,
    live_births  = dplyr::coalesce(as.numeric(live_births), 0),
    hysterectomy = dplyr::coalesce(hysterectomy, FALSE)
  )

des_ai <- survey::svydesign(ids = ~1, weights = ~WTMEC_pooled, data = ai_prep)

fit_ai_wu <- survey::svyglm(
  fi_wu ~ age_c + live_births + bmi_c + hysterectomy,
  design = des_ai,
  family = stats::quasibinomial()
)

ai_wu_summary <- summary(fit_ai_wu)$coefficients
ai_wu_ci      <- stats::confint(fit_ai_wu)
ai_wu_or      <- exp(cbind(OR = stats::coef(fit_ai_wu), ai_wu_ci))

cat("\n--- 2. AI (Wu 2014 Def: Mucus/Liquid/Solid, n =", nrow(ai_prep), ") ---\n")
ai_wu_table <- tibble::tibble(
  term       = names(stats::coef(fit_ai_wu)),
  log_odds   = unname(stats::coef(fit_ai_wu)),
  std_error  = unname(ai_wu_summary[, "Std. Error"]),
  t_value    = unname(ai_wu_summary[, "t value"]),
  p_value    = unname(ai_wu_summary[, "Pr(>|t|)"]),
  odds_ratio = unname(ai_wu_or[, "OR"]),
  ci_2.5     = unname(ai_wu_or[, "2.5 %"]),
  ci_97.5    = unname(ai_wu_or[, "97.5 %"])
)
print(as.data.frame(ai_wu_table), digits = 4)

fit_ai_nhs <- survey::svyglm(
  fi_nhs ~ age_c + live_births + bmi_c + hysterectomy,
  design = des_ai,
  family = stats::quasibinomial()
)

ai_nhs_summary <- summary(fit_ai_nhs)$coefficients
ai_nhs_ci      <- stats::confint(fit_ai_nhs)
ai_nhs_or      <- exp(cbind(OR = stats::coef(fit_ai_nhs), ai_nhs_ci))

cat("\n--- 3. AI (NIH/Whitehead Def: Liquid/Solid, n =", nrow(ai_prep), ") ---\n")
ai_nhs_table <- tibble::tibble(
  term       = names(stats::coef(fit_ai_nhs)),
  log_odds   = unname(stats::coef(fit_ai_nhs)),
  std_error  = unname(ai_nhs_summary[, "Std. Error"]),
  t_value    = unname(ai_nhs_summary[, "t value"]),
  p_value    = unname(ai_nhs_summary[, "Pr(>|t|)"]),
  odds_ratio = unname(ai_nhs_or[, "OR"]),
  ci_2.5     = unname(ai_nhs_or[, "2.5 %"]),
  ci_97.5    = unname(ai_nhs_or[, "97.5 %"])
)
print(as.data.frame(ai_nhs_table), digits = 4)

# -----------------------------------------------------------------------------
# 3. Construct Empirical Risk Vector for R/demand-transition_registry.R
# -----------------------------------------------------------------------------
cat("\n=================================================================\n")
cat("RECOMMENDED EMPIRICAL RISK COEFFICIENT TABLE (risk_wide)\n")
cat("=================================================================\n\n")

b0_ui   <- unname(stats::coef(fit_ui)["(Intercept)"])
bvag_ui <- unname(stats::coef(fit_ui)["vaginal_deliveries"])
bage_ui <- unname(stats::coef(fit_ui)["age_c"])
bbmi_ui <- unname(stats::coef(fit_ui)["bmi_c"])
bhyst_ui<- 0.0000  # p = 0.4676 (statistically null)
bmeno_ui<- 0.0000  # p = 0.3397 (statistically null)

b0_ai   <- unname(stats::coef(fit_ai_wu)["(Intercept)"])
bvag_ai <- unname(stats::coef(fit_ai_wu)["live_births"])
bage_ai <- unname(stats::coef(fit_ai_wu)["age_c"])
bbmi_ai <- unname(stats::coef(fit_ai_wu)["bmi_c"])
bhyst_ai<- unname(stats::coef(fit_ai_wu)["hysterectomyTRUE"]) # p < 0.00001 (OR = 1.956)

risk_wide_empirical <- tibble::tribble(
  ~condition,    ~b0,     ~bvag,   ~bage,   ~bysl,  ~bbmi,   ~bhyst,  ~bmeno,  ~bcomorb,
  "ui",       b0_ui,   bvag_ui, bage_ui,   0.00, bbmi_ui, bhyst_ui,  0.0000,    0.0000,
  "pop",     -5.0063,   0.3000, 0.1662,   0.00, 0.0800, 0.3365,   0.0000,    0.0000,
  "ai",       b0_ai,   bvag_ai, bage_ai,   0.00, bbmi_ai, bhyst_ai,  0.0000,    0.0000
)

print(as.data.frame(risk_wide_empirical), digits = 4)
cat("\nDone. Estimation complete.\n")
