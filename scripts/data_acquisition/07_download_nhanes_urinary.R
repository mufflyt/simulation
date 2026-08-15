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
#        - PRIMARY: symptomatic UI, Incontinence Severity Index >= 3
#          ISI = frequency(KIQ005) x amount(KIQ010), Sandvik, range 0-12.
#          >= 3 is moderate-to-severe (Nygaard 2008 JAMA, nonpregnant women 20+).
#          The former "any leakage" definition (KIQ005 != Never) is RETIRED: it
#          counted women leaking less than once a month and reached 78% at 75+,
#          which is not a workforce-demand state.
#        - PHENOTYPES, which may overlap and are NOT subtypes of the above:
#            stress_leakage_12m  (KIQ042 == "Yes")
#            urgency_leakage_12m (KIQ044 == "Yes")
#            other_leakage_12m   (KIQ046 == "Yes")
#          KIQ042/044/046 ask whether leakage of that circumstance occurred at
#          all in 12 months; KIQ005/010 measure overall frequency and amount.
#          Different questions, so a phenotype may exceed primary prevalence.
#        - Mixed UI                  (both stress + urgency)
#        - Bothersome UI             (KIQ052: at least "Somewhat")
#        - Nocturia ≥2               (KIQ480/KIQ481 >= 2)
#        (KIQ_U carries no bowel-leakage item; bowel_leak is NA by design.)
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
#   KIQ005  Urinary leakage frequency (Never -> Every day/night)
#   KIQ010  Urine amount per leakage episode
#   KIQ042  Stress UI: leak with physical activity
#   KIQ044  Urgency UI: leak before reaching toilet
#   KIQ052  Activity impact of leakage
#   KIQ481  Nocturia times/night (KIQ480 in the earlier cycle)
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
#' Normalise a KIQ value label across NHANES vintages
#'
#' 2005-2006 stores question-text fragments ("never,", "small splashes, or",
#' "or more?"); later cycles store clean labels ("Never", "Small splashes").
#' Exact matching drops whole categories -- silently, and only in the middle.
.kiq_norm <- function(x) {
  x <- tolower(as.character(x))
  x <- gsub("^or\\s+", "", x)
  x <- gsub("[[:space:],]*\\bor\\b[[:space:]]*$", "", x)
  x <- gsub("[[:punct:]]+$", "", x)
  trimws(x)
}

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
           WTMEC2YR, SDMVPSU, SDMVSTRA, any_of("RIDEXPRG")) %>%
    mutate(adj_weight = WTMEC2YR / weight_divisor)

  kiq <- dat$kiq %>%
    select(SEQN,
           any_of(c("KIQ005", "KIQ010", "KIQ042", "KIQ044", "KIQ046", "KIQ052",
                    "KIQ480", "KIQ481")))

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
  for (col in c("KIQ005", "KIQ010", "KIQ042", "KIQ044", "KIQ046", "KIQ052",
                "KIQ480", "KIQ481", "RHQ740", "RIDEXPRG")) {
    if (!col %in% names(merged)) merged[[col]] <- NA_character_
  }

  # nhanesA returns labeled factors; convert to character for safe comparison
  chr <- function(x) as.character(x)

  merged %>%
    # Nonpregnant women 20+, following Nygaard 2008. Pregnancy UI is real but
    # transient; it belongs to the pregnancy component of the microsimulation,
    # not the chronic pelvic-floor prevalence anchor.
    filter(chr(RIAGENDR) == "Female", RIDAGEYR >= 20,
           is.na(RIDEXPRG) | chr(RIDEXPRG) != "Yes, positive lab pregnancy test") %>%
    mutate(
      # KIQ022 is kidney disease. KIQ005 is the NHANES urinary-leakage
      # frequency item; any response other than Never is urinary incontinence.
      # --- Incontinence Severity Index (Sandvik): frequency x amount --------
      # Labels are normalised before matching. NHANES 2005-2006 stores literal
      # question-text fragments and attaches the conjunction to the PENULTIMATE
      # option -- "drops," / "small splashes, or" / "more?" -- so exact matching
      # silently NAs the MIDDLE category of both items. That produced 5.7%
      # against Nygaard's 15.7% with nothing failing. Normalised, the same code
      # reproduces 15.7% exactly.
      isi_freq   = case_when(.kiq_norm(KIQ005) == "never"                  ~ 0L,
                             .kiq_norm(KIQ005) == "less than once a month" ~ 1L,
                             .kiq_norm(KIQ005) == "a few times a month"    ~ 2L,
                             .kiq_norm(KIQ005) == "a few times a week"     ~ 3L,
                             .kiq_norm(KIQ005) == "every day and/or night" ~ 4L,
                             TRUE ~ NA_integer_),
      isi_amount = case_when(.kiq_norm(KIQ010) == "drops"          ~ 1L,
                             .kiq_norm(KIQ010) == "small splashes" ~ 2L,
                             .kiq_norm(KIQ010) == "more"           ~ 3L,
                             TRUE ~ NA_integer_),
      # KIQ010 is skipped for "Never", so amount is legitimately NA there and
      # the ISI is 0 rather than missing.
      isi        = case_when(isi_freq == 0L                       ~ 0L,
                             !is.na(isi_freq) & !is.na(isi_amount) ~ isi_freq * isi_amount,
                             TRUE ~ NA_integer_),
      ui         = if_else(!is.na(isi), as.integer(isi >= 3L), NA_integer_),

      # --- PHENOTYPES. Not subtypes. May overlap; may exceed `ui`. ----------
      stress_leakage_12m  = if_else(chr(KIQ042) == "Yes", 1L,
                            if_else(chr(KIQ042) == "No",  0L, NA_integer_)),
      urgency_leakage_12m = if_else(chr(KIQ044) == "Yes", 1L,
                            if_else(chr(KIQ044) == "No",  0L, NA_integer_)),
      other_leakage_12m   = if_else(chr(KIQ046) == "Yes", 1L,
                            if_else(chr(KIQ046) == "No",  0L, NA_integer_)),

      # --- phenotype WITHIN the primary population --------------------------
      ui_phenotype = case_when(
        is.na(ui) | ui == 0L                                       ~ "no_moderate_severe_ui",
        stress_leakage_12m == 1L & urgency_leakage_12m == 1L        ~ "mixed_stress_urgency",
        stress_leakage_12m == 1L & urgency_leakage_12m %in% c(0L)   ~ "stress_predominant",
        urgency_leakage_12m == 1L & stress_leakage_12m %in% c(0L)   ~ "urgency_predominant",
        TRUE                                                        ~ "other_or_unclassified"),
      bothersome = if_else(chr(KIQ052) %in% c("Somewhat", "Very much", "Greatly"), 1L,
                   if_else(chr(KIQ052) %in% c("Not at all", "Only a little"), 0L, NA_integer_)),
      # KIQ481 replaced KIQ480 in the later cycle.
      nocturia_n = suppressWarnings(as.integer(if_else(!is.na(KIQ481), chr(KIQ481), chr(KIQ480)))),
      nocturia2  = if_else(!is.na(nocturia_n) & nocturia_n >= 2L, 1L,
                   if_else(!is.na(nocturia_n), 0L, NA_integer_)),
      # The urinary KIQ_U files do not contain a bowel-leakage item.
      bowel_leak = NA_integer_,
      prolapse   = if_else(chr(RHQ740) == "Yes", 1L, if_else(chr(RHQ740) == "No", 0L, NA_integer_)),

        # --- BRFSS-compatible age bands --------------------------------------
        # NHANES KIQ_U analysis begins at age 20. The 20-34 target is blended
        # into BRFSS's 18-34 cell with that small coverage mismatch declared.
      age_band = factor(
        case_when(
          RIDAGEYR < 35 ~ "20-34",
          RIDAGEYR < 45 ~ "35-44",
          RIDAGEYR < 65 ~ "45-64",
          RIDAGEYR < 75 ~ "65-74",
          TRUE          ~ "75+"
        ),
        levels = c("20-34","35-44","45-64","65-74","75+")
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

outcomes <- c("ui", "stress_leakage_12m", "urgency_leakage_12m", "other_leakage_12m",
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
  "Outcomes: ui (ISI>=3), stress/urgency/other_leakage_12m (phenotypes, may overlap), bothersome, nocturia2, bowel_leak",
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
