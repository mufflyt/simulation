#!/usr/bin/env Rscript
# =============================================================================
# NHANES Urinary/Pelvic Floor Download and URPS Prevalence Calibration
# National Health and Nutrition Examination Survey — KIQ_U module
# =============================================================================
#
# PURPOSE:
#   1. Download NHANES KIQ_U (kidney/urinary), DEMO, BMX, and RHQ modules
#      for the two most recent cycles: 2017-2018 (J) and 2021-2023 (L).
#   2. Restrict to women 20+.
#   3. Compute survey-weighted prevalence of:
#        - Any urinary incontinence  (KIQ022 == 1)
#        - Stress UI                 (KIQ046 == 1 among incontinent)
#        - Urgency UI                (KIQ048 == 1 among incontinent)
#        - Mixed UI                  (both stress + urgency)
#        - Bothersome UI             (KIQ044 >= 2)
#        - Nocturia ≥2               (KIQ480 >= 2)
#        - Accidental bowel leakage  (KIQ042 == 1)
#        - Pelvic organ prolapse     (RHQ740 == 1, 2017-2018 only)
#   4. Stratify by URPS demand bands:
#        Age: 20-39, 40-49, 50-64, 65-74, 75+
#        Race: NH White, NH Black, Hispanic, NH Asian, Other
#        BMI: <25, 25-29.9, 30+
#        Income (PIR): <1.0 (poor), 1.0-2.0 (near-poor), 2.0-4.0, >=4.0
#   5. Emit calibration targets to:
#        data-raw/nhanes/nhanes_ui_prevalence_by_age.rds
#        data-raw/nhanes/nhanes_ui_prevalence_by_race.rds
#        data-raw/nhanes/nhanes_ui_prevalence_by_bmi.rds
#        data-raw/nhanes/nhanes_ui_prevalence_by_income.rds
#        data-raw/nhanes/nhanes_ui_prevalence_cells.rds   (age × race × bmi)
#        data-raw/nhanes/nhanes_manifest.txt
#
#   These replace the BRFSS-sourced single-variable prevalence in
#   load_brfss_women() with exam-confirmed, race/BMI-stratified estimates.
#
# DATA ACCESS:
#   Freely available — no DUA, no registration required.
#   Downloaded automatically via the nhanesA R package from CDC's servers.
#
# NHANES CYCLES USED:
#   2017-2018  suffix _J  (pre-COVID, most recent complete cycle)
#   2021-2023  suffix _L  (post-COVID resume, released August 2023)
#
# SURVEY DESIGN:
#   NHANES uses a complex multi-stage design. We use the MEC exam weights
#   (WTMEC2YR for single cycles; combined = WTMEC2YR/2 for pooled).
#   PSU: SDMVPSU, Strata: SDMVSTRA, nest=TRUE.
#
# KEY VARIABLES (KIQ_U module):
#   KIQ022  Any UI in past 12 months   (1=Yes 2=No 7=Refused 9=DK)
#   KIQ025  UI frequency               (1=<monthly 2=few/month 3=few/week 4=daily)
#   KIQ026  UI amount                  (1=drops 2=small splashes 3=more)
#   KIQ044  UI bother                  (0=not at all ... 4=greatly)
#   KIQ046  Stress UI                  (1=Yes 2=No) [asked only if KIQ022==1]
#   KIQ048  Urgency UI                 (1=Yes 2=No) [asked only if KIQ022==1]
#   KIQ042  Bowel leakage              (1=Yes 2=No)
#   KIQ480  Nocturia times/night       (0,1,2,3,4,5+)
#   RHQ740  Pelvic organ prolapse      (1=Yes 2=No) — RHQ module, cycle J only
#
# INSTALL:
#   install.packages("nhanesA")
#
# USAGE:
#   Rscript scripts/data_acquisition/07_download_nhanes_urinary.R
#
# =============================================================================

suppressPackageStartupMessages({
  library(nhanesA)
  library(survey)
  library(dplyr)
  library(tidyr)
  library(here)
})

OUT_DIR <- here("data-raw", "nhanes")
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

cat("=== NHANES Urinary/PFD Download ===\n")
cat("Output directory:", OUT_DIR, "\n\n")

# ---------------------------------------------------------------------------
# 1. Download raw modules for each cycle
# ---------------------------------------------------------------------------

cycles <- list(
  J = list(suffix = "_J"),
  L = list(suffix = "_L")
)

# nhanesA returns labeled factors; wrap with error handling
.nhanes_safe <- function(table) {
  tryCatch(nhanes(table), error = function(e) {
    message("  Could not download ", table, ": ", conditionMessage(e)); NULL
  })
}

download_cycle <- function(suffix) {
  cat("Downloading cycle (suffix", suffix, ")...\n")
  list(
    demo   = .nhanes_safe(paste0("DEMO",  suffix)),
    kiq    = .nhanes_safe(paste0("KIQ_U", suffix)),
    bmx    = .nhanes_safe(paste0("BMX",   suffix)),
    rhq    = .nhanes_safe(paste0("RHQ",   suffix)),
    suffix = suffix
  )
}

raw <- lapply(names(cycles), function(k) download_cycle(cycles[[k]]$suffix))
names(raw) <- names(cycles)

# ---------------------------------------------------------------------------
# 2. Harmonise and restrict to women 20+
# ---------------------------------------------------------------------------

harmonise_cycle <- function(dat, weight_divisor = 1) {
  demo <- dat$demo %>%
    select(SEQN, RIAGENDR, RIDAGEYR, RIDRETH3, INDFMPIR,
           WTMEC2YR, SDMVPSU, SDMVSTRA) %>%
    mutate(adj_weight = WTMEC2YR / weight_divisor)

  kiq <- dat$kiq %>%
    select(SEQN,
           any_of(c("KIQ022", "KIQ025", "KIQ026", "KIQ044",
                    "KIQ046", "KIQ048", "KIQ042", "KIQ480")))

  bmx <- dat$bmx %>% select(SEQN, BMXBMI)

  merged <- demo %>%
    left_join(kiq,  by = "SEQN") %>%
    left_join(bmx,  by = "SEQN")

  if (!is.null(dat$rhq)) {
    rhq_slim <- dat$rhq %>%
      select(SEQN, any_of("RHQ740"))
    merged <- left_join(merged, rhq_slim, by = "SEQN")
  }

  # Ensure all KIQ/RHQ columns exist (asked-only-if questions may be absent)
  for (col in c("KIQ022","KIQ025","KIQ026","KIQ044","KIQ046","KIQ048","KIQ042","KIQ480","RHQ740")) {
    if (!col %in% names(merged)) merged[[col]] <- NA_character_
  }

  # nhanesA returns labeled factors; convert to character for safe comparison
  chr <- function(x) as.character(x)

  merged %>%
    filter(chr(RIAGENDR) == "Female", RIDAGEYR >= 20) %>%
    mutate(
      # --- UI flags (KIQ022 labels: "Yes" / "No" / "Don't know") -----------
      ui         = case_when(chr(KIQ022) == "Yes" ~ 1L, chr(KIQ022) == "No" ~ 0L, TRUE ~ NA_integer_),
      stress_ui  = if_else(ui == 1L & chr(KIQ046) == "Yes", 1L,
                   if_else(ui == 1L & chr(KIQ046) == "No",  0L, NA_integer_)),
      urgency_ui = if_else(ui == 1L & chr(KIQ048) == "Yes", 1L,
                   if_else(ui == 1L & chr(KIQ048) == "No",  0L, NA_integer_)),
      mixed_ui   = if_else(stress_ui == 1L & urgency_ui == 1L, 1L,
                   if_else(!is.na(stress_ui) & !is.na(urgency_ui), 0L, NA_integer_)),
      # KIQ044: "Yes"=UI affects daily life, "No"=does not (binary in J cycle)
      bothersome = if_else(ui == 1L & chr(KIQ044) == "Yes", 1L,
                   if_else(ui == 1L & chr(KIQ044) == "No",  0L, NA_integer_)),
      # KIQ480: factor with numeric-looking levels ("0","1","2",...,"5 or more")
      nocturia_n = suppressWarnings(as.integer(chr(KIQ480))),
      nocturia2  = if_else(!is.na(nocturia_n) & nocturia_n >= 2L, 1L,
                   if_else(!is.na(nocturia_n), 0L, NA_integer_)),
      bowel_leak = if_else(chr(KIQ042) == "Yes", 1L, if_else(chr(KIQ042) == "No", 0L, NA_integer_)),
      prolapse   = if_else(chr(RHQ740) == "Yes", 1L, if_else(chr(RHQ740) == "No", 0L, NA_integer_)),

      # --- URPS age bands ---------------------------------------------------
      age_band = factor(
        case_when(
          RIDAGEYR < 40 ~ "20-39",
          RIDAGEYR < 50 ~ "40-49",
          RIDAGEYR < 65 ~ "50-64",
          RIDAGEYR < 75 ~ "65-74",
          TRUE          ~ "75+"
        ),
        levels = c("20-39","40-49","50-64","65-74","75+")
      ),

      # --- Race/ethnicity (nhanesA labels) ----------------------------------
      race = factor(
        case_when(
          grepl("Non-Hispanic White", chr(RIDRETH3))           ~ "NH White",
          grepl("Non-Hispanic Black", chr(RIDRETH3))           ~ "NH Black",
          grepl("Hispanic|Mexican",   chr(RIDRETH3))           ~ "Hispanic",
          grepl("Non-Hispanic Asian", chr(RIDRETH3))           ~ "NH Asian",
          TRUE                                                   ~ "Other"
        ),
        levels = c("NH White","NH Black","Hispanic","NH Asian","Other")
      ),

      # --- BMI groups (BMXBMI is numeric) -----------------------------------
      bmi_group = factor(
        case_when(
          is.na(BMXBMI) ~ NA_character_,
          BMXBMI < 25   ~ "<25",
          BMXBMI < 30   ~ "25-29.9",
          TRUE          ~ "30+"
        ),
        levels = c("<25","25-29.9","30+")
      ),

      # --- Income (INDFMPIR is numeric poverty-income ratio) ----------------
      income_group = factor(
        case_when(
          is.na(INDFMPIR)   ~ NA_character_,
          INDFMPIR < 1.0    ~ "Poor (<100% FPL)",
          INDFMPIR < 2.0    ~ "Near-poor (100-199%)",
          INDFMPIR < 4.0    ~ "Middle (200-399%)",
          TRUE              ~ "High (400%+)"
        ),
        levels = c("Poor (<100% FPL)","Near-poor (100-199%)","Middle (200-399%)","High (400%+)")
      )
    )
}

# Pool cycles: halve MEC weight when combining 2 cycles
women_J <- harmonise_cycle(raw$J, weight_divisor = 2)
women_L <- harmonise_cycle(raw$L, weight_divisor = 2)
women   <- bind_rows(women_J, women_L)

cat("Pooled women 20+:", nrow(women), "observations\n")
cat("UI prevalence (unweighted):", round(mean(women$ui, na.rm = TRUE) * 100, 1), "%\n\n")

# ---------------------------------------------------------------------------
# 3. Build survey design
# ---------------------------------------------------------------------------

options(survey.lonely.psu = "adjust")

design <- svydesign(
  id      = ~SDMVPSU,
  strata  = ~SDMVSTRA,
  weights = ~adj_weight,
  nest    = TRUE,
  data    = women
)

# ---------------------------------------------------------------------------
# 4. Helper: compute weighted prevalence for a binary outcome by a grouping
# ---------------------------------------------------------------------------

svy_prevalence <- function(outcome_var, by_var, design) {
  formula_outcome <- as.formula(paste0("~", outcome_var))
  grp_var    <- design$variables[[by_var]]
  out_var    <- design$variables[[outcome_var]]
  lvls       <- levels(grp_var)
  if (is.null(lvls)) lvls <- sort(unique(grp_var[!is.na(grp_var)]))

  rows <- lapply(lvls, function(lvl) {
    idx <- !is.na(grp_var) & grp_var == lvl & !is.na(out_var)
    n_obs <- sum(idx)
    if (n_obs < 2L) return(NULL)
    sub <- subset(design, idx)
    est <- tryCatch(
      svymean(formula_outcome, sub, na.rm = TRUE),
      error = function(e) NULL
    )
    if (is.null(est)) return(NULL)
    pv <- coef(est)[[1]]
    se <- sqrt(vcov(est)[[1]])
    data.frame(
      group_var  = by_var,
      group      = as.character(lvl),
      outcome    = outcome_var,
      prevalence = pv,
      se         = se,
      ci_lo      = max(0, pv - 1.96 * se),
      ci_hi      = min(1, pv + 1.96 * se),
      n          = n_obs,
      stringsAsFactors = FALSE
    )
  })
  bind_rows(Filter(Negate(is.null), rows))
}

outcomes <- c("ui", "stress_ui", "urgency_ui", "mixed_ui",
              "bothersome", "nocturia2", "bowel_leak")

# ---------------------------------------------------------------------------
# 5. Prevalence by age band
# ---------------------------------------------------------------------------

cat("Computing prevalence by age band...\n")
prev_age <- bind_rows(lapply(outcomes, svy_prevalence, by_var = "age_band", design = design))
saveRDS(prev_age, file.path(OUT_DIR, "nhanes_ui_prevalence_by_age.rds"))
cat("  Saved nhanes_ui_prevalence_by_age.rds\n")

print(
  prev_age %>%
    filter(outcome == "ui") %>%
    mutate(pct = scales::percent(prevalence, 0.1)) %>%
    select(age_band = group, `UI prevalence` = pct, n)
)

# ---------------------------------------------------------------------------
# 6. Prevalence by race
# ---------------------------------------------------------------------------

cat("Computing prevalence by race...\n")
prev_race <- bind_rows(lapply(outcomes, svy_prevalence, by_var = "race", design = design))
saveRDS(prev_race, file.path(OUT_DIR, "nhanes_ui_prevalence_by_race.rds"))
cat("  Saved nhanes_ui_prevalence_by_race.rds\n")

# ---------------------------------------------------------------------------
# 7. Prevalence by BMI
# ---------------------------------------------------------------------------

cat("Computing prevalence by BMI group...\n")
prev_bmi <- bind_rows(lapply(outcomes, svy_prevalence, by_var = "bmi_group", design = design))
saveRDS(prev_bmi, file.path(OUT_DIR, "nhanes_ui_prevalence_by_bmi.rds"))
cat("  Saved nhanes_ui_prevalence_by_bmi.rds\n")

# ---------------------------------------------------------------------------
# 8. Prevalence by income
# ---------------------------------------------------------------------------

cat("Computing prevalence by income group...\n")
prev_income <- bind_rows(lapply(outcomes, svy_prevalence, by_var = "income_group", design = design))
saveRDS(prev_income, file.path(OUT_DIR, "nhanes_ui_prevalence_by_income.rds"))
cat("  Saved nhanes_ui_prevalence_by_income.rds\n")

print(
  prev_income %>%
    filter(outcome == "ui") %>%
    mutate(pct = scales::percent(prevalence, 0.1)) %>%
    select(income_group = group, `UI prevalence` = pct, n)
)

# ---------------------------------------------------------------------------
# 9. Population cells: age × race × bmi (for blend_nhanes_prevalence())
# ---------------------------------------------------------------------------

cat("Computing prevalence cells (age × race × bmi)...\n")

cell_design <- subset(design,
  !is.na(design$variables$ui) &
  !is.na(design$variables$age_band) &
  !is.na(design$variables$race) &
  !is.na(design$variables$bmi_group)
)

cell_vars  <- cell_design$variables
cell_combos <- unique(cell_vars[, c("age_band","race","bmi_group")])
cell_combos <- cell_combos[complete.cases(cell_combos), ]

prev_cells <- bind_rows(lapply(seq_len(nrow(cell_combos)), function(i) {
  ab <- cell_combos$age_band[i]
  rc <- cell_combos$race[i]
  bm <- cell_combos$bmi_group[i]
  idx <- cell_vars$age_band == ab & cell_vars$race == rc & cell_vars$bmi_group == bm & !is.na(cell_vars$ui)
  if (sum(idx) < 2L) return(NULL)
  sub <- subset(cell_design, idx)
  est <- tryCatch(svymean(~ui, sub, na.rm = TRUE), error = function(e) NULL)
  if (is.null(est)) return(NULL)
  pv <- coef(est)[[1]]; se <- sqrt(vcov(est)[[1]])
  data.frame(age_band=as.character(ab), race=as.character(rc), bmi_group=as.character(bm),
             ui_prevalence=pv, se=se, ci_lo=max(0,pv-1.96*se), ci_hi=min(1,pv+1.96*se),
             n=sum(idx), source="nhanes_2017_2023_pooled", stringsAsFactors=FALSE)
}))

saveRDS(prev_cells, file.path(OUT_DIR, "nhanes_ui_prevalence_cells.rds"))
cat("  Saved nhanes_ui_prevalence_cells.rds (", nrow(prev_cells), "cells)\n")

# ---------------------------------------------------------------------------
# 10. Manifest
# ---------------------------------------------------------------------------

manifest_lines <- c(
  paste("Generated:", Sys.time()),
  paste("Cycles pooled: 2017-2018 (J), 2021-2023 (L)"),
  paste("Women 20+ observations:", nrow(women)),
  paste("Weighted UI prevalence (all ages):",
    round(coef(svymean(~ui, subset(design, !is.na(design$variables$ui)))) * 100, 1), "%"),
  "",
  "Output files:",
  "  nhanes_ui_prevalence_by_age.rds",
  "  nhanes_ui_prevalence_by_race.rds",
  "  nhanes_ui_prevalence_by_bmi.rds",
  "  nhanes_ui_prevalence_by_income.rds",
  "  nhanes_ui_prevalence_cells.rds",
  "",
  "Outcomes: ui, stress_ui, urgency_ui, mixed_ui, bothersome, nocturia2, bowel_leak",
  "Stratifiers: age_band (5), race (5), bmi_group (3), income_group (4)",
  "",
  "Survey design: MEC weights halved for pooling (adj_weight = WTMEC2YR / 2)",
  "PSU: SDMVPSU  Strata: SDMVSTRA  nest=TRUE",
  "",
  "Source: CDC NHANES public data, https://www.cdc.gov/nchs/nhanes/",
  "Downloaded via nhanesA R package — no DUA required"
)

writeLines(manifest_lines, file.path(OUT_DIR, "nhanes_manifest.txt"))
cat("  Saved nhanes_manifest.txt\n")

# ---------------------------------------------------------------------------
# 11. Print summary table
# ---------------------------------------------------------------------------

cat("\n=== NHANES UI Prevalence Summary ===\n")
cat("Outcome: Any urinary incontinence (past 12 months)\n\n")

.fmt <- function(df, label_col) {
  df %>%
    filter(outcome == "ui") %>%
    mutate(Prevalence = paste0(round(prevalence * 100, 1), "% (",
                               round(ci_lo * 100, 1), "-",
                               round(ci_hi * 100, 1), "%)"),
           N = n) %>%
    rename(!!label_col := group) %>%
    select(all_of(c(label_col, "Prevalence", "N"))) %>%
    as.data.frame()
}

cat("By age band:\n");   print(.fmt(prev_age,    "Age"))
cat("\nBy race:\n");     print(.fmt(prev_race,   "Race"))
cat("\nBy BMI:\n");      print(.fmt(prev_bmi,    "BMI"))
cat("\nBy income:\n");   print(.fmt(prev_income, "Income"))

cat("\nDone. All outputs in:", OUT_DIR, "\n")
