#!/usr/bin/env Rscript
# =============================================================================
# MEPS 2022 Download and URPS Care-Seeking Recalibration
# Medical Expenditure Panel Survey — HC-233 (2022 Full Year)
# =============================================================================
#
# PURPOSE:
#   1. Download the 2022 MEPS office-based visits, conditions, condition-event
#      link, and full-year consolidated files via the MEPS R package.
#   2. Identify office-based visits with a urinary incontinence (ICD-10 N39)
#      diagnosis by women 18+.
#   3. Compute survey-weighted care-seeking and visit rates by:
#        - Age group (URPS demand bands)
#        - Insurance type (private / public / uninsured)
#        - Income-to-poverty ratio quartile
#        - Provider specialty (OB/GYN, urology, primary care, other)
#   4. Emit calibration targets to:
#        data-raw/meps/meps_2023_ui_visit_rates.rds
#        data-raw/meps/meps_2023_income_care_seeking_multipliers.rds
#        data-raw/meps/meps_2023_manifest.txt
#
#   The income multipliers table is the direct replacement for
#   CARE_SEEKING_BY_INCOME in R/44-urps_population.R (currently sourced from
#   MEPS 2020, citation: "MEPS 2020 specialty visit rate by income quartile").
#
# DATA ACCESS:
#   Freely available. No DUA required. Downloaded automatically by the MEPS
#   R package from AHRQ's public FTP server.
#
#   Install the MEPS package once:
#     install.packages("remotes")
#     remotes::install_github("e-mitchell/meps_r_pkg/MEPS")
#
# OUTPUT:
#   data-raw/meps/meps_2023_ui_visit_rates.rds
#   data-raw/meps/meps_2023_income_care_seeking_multipliers.rds
#   data-raw/meps/meps_2023_insurance_care_seeking_multipliers.rds
#   data-raw/meps/meps_2023_specialty_distribution.rds
#   data-raw/meps/meps_2023_manifest.txt
#
# KEY ICD-10 CODES:
#   N39  — Disorders of urinary system (MEPS uses 3-char truncated codes)
#           includes N39.3 (SUI), N39.41 (urgency), N39.46 (mixed), N39.0 (UTI)
#   N81  — Pelvic organ prolapse
#   R32  — Unspecified urinary incontinence
#   R15  — Fecal incontinence
#
# SURVEY DESIGN:
#   id      = ~VARPSU
#   strata  = ~VARSTR
#   weights = ~PERWT22F   (note: weight suffix is year-specific)
#   nest    = TRUE
#
# MEPS FILE CODES (2022, HC-233 series):
#   FYC  — Full Year Consolidated (person-level demographics + insurance)
#   OB   — Office-Based Medical Provider Visits (event-level)
#   COND — Medical Conditions (ICD-10 diagnosis codes per condition record)
#   CLNK — Condition-Event Link (joins COND to OB/IP/ER/etc.)
# =============================================================================

if (!requireNamespace("MEPS", quietly = TRUE)) {
  stop(
    "MEPS package not installed.\n",
    "  remotes::install_github('e-mitchell/meps_r_pkg/MEPS')"
  )
}

library(MEPS)
library(dplyr)
library(survey)

options(survey.lonely.psu = "adjust")

meps_year <- 2023L

out_dir <- here::here("data-raw", "meps")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

message("MEPS ", meps_year, " — downloading files (this may take several minutes) ...")

# =============================================================================
# 1. Download raw files
# =============================================================================

message("[1/4] Office-Based Visits (OB) ...")
ob  <- read_MEPS(year = meps_year, type = "OB")
message("  OB rows: ", format(nrow(ob), big.mark = ","))

message("[2/4] Full-Year Consolidated (FYC) ...")
fyc <- read_MEPS(year = meps_year, type = "FYC")
message("  FYC rows: ", format(nrow(fyc), big.mark = ","))

message("[3/4] Medical Conditions (COND) ...")
cond <- read_MEPS(year = meps_year, type = "COND")
message("  COND rows: ", format(nrow(cond), big.mark = ","))

message("[4/4] Condition-Event Link (CLNK) ...")
clnk <- read_MEPS(year = meps_year, type = "CLNK")
message("  CLNK rows: ", format(nrow(clnk), big.mark = ","))

# =============================================================================
# 2. Identify UI (and broader PFD) conditions
# =============================================================================

# MEPS uses 3-character truncated ICD-10 codes to protect confidentiality.
pfd_icd10_3char <- c("N39", "N81", "R32", "R15")

ui_conditions <- cond %>%
  filter(ICD10CDX %in% pfd_icd10_3char)

message("\nUI/PFD condition records: ", format(nrow(ui_conditions), big.mark = ","))
message("ICD-10 code breakdown:")
print(table(ui_conditions$ICD10CDX))

# Link conditions -> events -> office visits
ui_event_links <- ui_conditions %>%
  inner_join(clnk, by = c("DUPERSID", "CONDIDX"))

ob_ui <- ui_event_links %>%
  inner_join(ob, by = c("DUPERSID", "EVNTIDX"))

message("Office-visit events linked to UI/PFD condition: ",
        format(nrow(ob_ui), big.mark = ","))

rm(clnk, cond)
invisible(gc())

# =============================================================================
# 3. Merge person-level file; restrict to women 18+
# =============================================================================

# FYC column names vary by release year; normalise to uppercase
names(fyc) <- toupper(names(fyc))
names(ob)  <- toupper(names(ob))
names(ob_ui) <- toupper(names(ob_ui))

# Year-specific person weight: PERWT22F for 2022
wt_col <- grep("^PERWT\\d{2}F$", names(fyc), value = TRUE)[1]
if (is.na(wt_col)) {
  wt_col <- grep("PERWT", names(fyc), value = TRUE)[1]
  message("  WARNING: using weight column: ", wt_col)
} else {
  message("  Person weight column: ", wt_col)
}

# PSU / stratum: VARPSU, VARSTR (or suffixed .x after join)
psu_col  <- grep("^VARPSU",  names(fyc), value = TRUE)[1]
strat_col <- grep("^VARSTR",  names(fyc), value = TRUE)[1]

person_vars <- c("DUPERSID", wt_col, psu_col, strat_col,
                 "SEX", "AGELAST",
                 # Insurance status at end of year (INSC1231: 1=private,2=public,3=uninsured)
                 "INSC1231",
                 # Income-to-poverty ratio (POVCAT22 or POVCAT: 1=poor,2=near-poor,3=low-income,4=mid,5=high)
                 grep("^POVCAT", names(fyc), value = TRUE)[1],
                 # Family income as pct of poverty (continuous)
                 grep("^TTLP", names(fyc), value = TRUE)[1])

person_vars <- unique(person_vars[!is.na(person_vars) & person_vars %in% names(fyc)])

# Pull only needed person-level columns and rename them before joining to avoid
# any collision with OB columns of the same name.
fyc_slim <- fyc[, person_vars]
rename_map <- setdiff(person_vars, "DUPERSID")
names(fyc_slim)[names(fyc_slim) %in% rename_map] <-
  paste0(names(fyc_slim)[names(fyc_slim) %in% rename_map], "_P")

ui_persons <- ob_ui %>%
  left_join(fyc_slim, by = "DUPERSID")

# All demographic vars now have _P suffix; use them directly.
sex_col_p   <- paste0("SEX",     "_P")
age_col_p   <- paste0("AGELAST", "_P")
insc_col_p  <- paste0("INSC1231","_P")
wt_col_p    <- paste0(wt_col,    "_P")
psu_col_p   <- paste0(psu_col,   "_P")
str_col_p   <- paste0(strat_col, "_P")

ui_persons$SEX_r    <- ui_persons[[sex_col_p]]
ui_persons$AGELAST_r <- ui_persons[[age_col_p]]
ui_persons$INSC_r   <- ui_persons[[insc_col_p]]
ui_persons$PERWT_r  <- ui_persons[[wt_col_p]]
ui_persons$VARPSU_r <- ui_persons[[psu_col_p]]
ui_persons$VARSTR_r <- ui_persons[[str_col_p]]

povcat_col <- grep("^POVCAT.*_P$", names(ui_persons), value = TRUE)[1]
ui_persons$POVCAT_r <- if (!is.na(povcat_col)) ui_persons[[povcat_col]] else NA_real_

# Final filter: women 18+
all_ui <- ui_persons %>%
  filter(SEX_r == 2, !is.na(AGELAST_r), AGELAST_r >= 18) %>%
  mutate(count = 1L)

# Remove pediatricians (DRSPLTY_M18 == 24)
drsplty_col <- grep("DRSPLTY", names(all_ui), value = TRUE)[1]
if (!is.na(drsplty_col)) {
  all_ui <- all_ui %>% filter(is.na(.data[[drsplty_col]]) | .data[[drsplty_col]] != 24)
}

message("\nFinal analytic sample (women 18+, UI/PFD OB visits): ",
        format(nrow(all_ui), big.mark = ","))

rm(ob, ob_ui, ui_event_links, ui_conditions, ui_persons)
invisible(gc())

# =============================================================================
# 4. Survey design
# =============================================================================

des <- tryCatch(
  svydesign(
    id      = ~VARPSU_r,
    strata  = ~VARSTR_r,
    weights = ~PERWT_r,
    data    = all_ui,
    nest    = TRUE
  ),
  error = function(e) {
    message("  svydesign failed: ", conditionMessage(e))
    message("  Falling back to simple random sample design for estimates.")
    svydesign(id = ~1, weights = ~PERWT_r, data = all_ui)
  }
)

# =============================================================================
# 5. Age-band visit rates
# =============================================================================

all_ui <- all_ui %>%
  mutate(
    age_band_urps = cut(AGELAST_r,
      breaks = c(17, 34, 44, 64, 74, Inf),
      labels = c("18-34", "35-44", "45-64", "65-74", "75+"),
      right  = TRUE)
  )

des <- update(des, age_band_urps = all_ui$age_band_urps)

age_visits <- svyby(~count, ~age_band_urps, des, svytotal, na.rm = TRUE)
age_visits$se   <- SE(age_visits)
age_visits$pct  <- age_visits$count / sum(age_visits$count) * 100
message("\nWeighted UI/PFD office visits by age band:")
print(age_visits[, c("age_band_urps", "count", "se", "pct")])

# =============================================================================
# 6. Insurance-type care-seeking multipliers
# =============================================================================

all_ui <- all_ui %>%
  mutate(
    insurance_cat = case_when(
      INSC_r == 1 ~ "Private",
      INSC_r == 2 ~ "Public",
      INSC_r == 3 ~ "Uninsured",
      INSC_r %in% c(-7L, -8L, -9L) | is.na(INSC_r) ~ "Unknown",
      TRUE ~ "Unknown"
    )
  )

des <- update(des, insurance_cat = all_ui$insurance_cat)

ins_visits <- svyby(~count, ~insurance_cat, des, svytotal, na.rm = TRUE)
ins_visits$se  <- SE(ins_visits)
ins_visits$pct <- ins_visits$count / sum(ins_visits$count) * 100

# Compute multipliers: Private = 1.0 reference
private_n <- ins_visits$count[ins_visits$insurance_cat == "Private"]
if (length(private_n) == 0 || private_n == 0) private_n <- max(ins_visits$count)

ins_visits$relative_rate <- ins_visits$count / private_n

message("\nInsurance care-seeking multipliers (Private = 1.0):")
print(ins_visits[, c("insurance_cat", "count", "pct", "relative_rate")])

# Structured multiplier table (matches CARE_SEEKING_BY_INSURANCE in R/44)
# Defaults (Richter 2007 / MEPS 2020) used when a category has zero visits.
defaults <- c(Private = 1.00, Public = 0.75, Uninsured = 0.58, Unknown = 0.80)
insurance_multipliers <- defaults
observed <- setNames(pmin(1.0, ins_visits$relative_rate), ins_visits$insurance_cat)
for (cat in names(observed)) {
  if (cat %in% names(insurance_multipliers)) {
    insurance_multipliers[[cat]] <- observed[[cat]]
  }
}
message("\nInsurance multipliers for CARE_SEEKING_BY_INSURANCE:")
print(round(insurance_multipliers, 3))

# =============================================================================
# 7. Income-tier care-seeking multipliers (key output for R/44 recalibration)
# =============================================================================

# POVCAT: 1=Poor (<100% FPL), 2=Near-poor (100-124%), 3=Low (125-199%),
#         4=Middle (200-399%), 5=High (400%+)
# Collapse to 4 URPS income tiers: LT25k, 25k_50k, 50k_100k, GT100k
# POVCAT 1-2 → LT25k, 3 → 25k_50k, 4 → 50k_100k, 5 → GT100k

all_ui <- all_ui %>%
  mutate(
    income_tier = case_when(
      POVCAT_r %in% 1:2 ~ "LT25k",
      POVCAT_r == 3     ~ "25k_50k",
      POVCAT_r == 4     ~ "50k_100k",
      POVCAT_r == 5     ~ "GT100k",
      TRUE              ~ NA_character_
    )
  )

des <- update(des, income_tier = all_ui$income_tier)

inc_sub <- all_ui[!is.na(all_ui$income_tier), ]
if (nrow(inc_sub) > 0) {
  des_inc <- svydesign(
    id      = ~VARPSU_r,
    strata  = ~VARSTR_r,
    weights = ~PERWT_r,
    data    = inc_sub,
    nest    = TRUE
  )

  inc_visits <- svyby(~count, ~income_tier, des_inc, svytotal, na.rm = TRUE)
  inc_visits$se  <- SE(inc_visits)
  inc_visits$pct <- inc_visits$count / sum(inc_visits$count) * 100

  gt100k_n <- inc_visits$count[inc_visits$income_tier == "GT100k"]
  if (length(gt100k_n) == 0 || gt100k_n == 0) gt100k_n <- max(inc_visits$count)
  inc_visits$relative_rate <- inc_visits$count / gt100k_n

  message("\nIncome-tier care-seeking multipliers (GT100k = 1.0):")
  print(inc_visits[, c("income_tier", "count", "pct", "relative_rate")])

  # Named vector matching CARE_SEEKING_BY_INCOME in R/44
  income_multipliers <- setNames(
    pmin(1.0, inc_visits$relative_rate),
    inc_visits$income_tier
  )
  income_multipliers["GT100k"] <- 1.00
  message("\nIncome multipliers for CARE_SEEKING_BY_INCOME (update R/44):")
  print(round(income_multipliers[c("LT25k", "25k_50k", "50k_100k", "GT100k")], 3))
} else {
  message("  WARNING: POVCAT not available; income multipliers not computed")
  income_multipliers <- c(LT25k = 0.72, "25k_50k" = 0.88, "50k_100k" = 0.97, GT100k = 1.00)
}

# =============================================================================
# 8. Provider specialty distribution
# =============================================================================

specialty_map <- c(
  "6"  = "Family_Practice",
  "8"  = "General_Practice",
  "11" = "OB_GYN",
  "14" = "Internal_Medicine",
  "20" = "Orthopedics",
  "28" = "Psychiatry",
  "33" = "Urology",
  "91" = "Other_Specialty"
)

if (!is.na(drsplty_col) && drsplty_col %in% names(all_ui)) {
  all_ui <- all_ui %>%
    mutate(
      specialty_raw = as.character(.data[[drsplty_col]]),
      specialty     = dplyr::recode(specialty_raw, !!!specialty_map, .default = "Unknown")
    )

  des <- update(des, specialty = all_ui$specialty)
  spec_visits <- svyby(~count, ~specialty, des, svytotal, na.rm = TRUE)
  spec_visits$se  <- SE(spec_visits)
  spec_visits$pct <- spec_visits$count / sum(spec_visits$count) * 100
  spec_visits <- spec_visits[order(-spec_visits$count), ]

  message("\nSpecialty distribution of UI/PFD office visits:")
  print(spec_visits[, c("specialty", "count", "pct")])
} else {
  spec_visits <- data.frame(specialty = character(0), count = numeric(0),
                             pct = numeric(0), stringsAsFactors = FALSE)
  message("  WARNING: provider specialty column not found")
}

# =============================================================================
# 9. Total national visit estimate → FTE demand
# =============================================================================

total_visits <- svytotal(~count, des)
n_visits <- as.numeric(total_visits)
se_visits <- as.numeric(SE(total_visits))
message(sprintf(
  "\nTotal weighted UI/PFD OB visits (women 18+): %s (SE %s)",
  format(round(n_visits), big.mark = ","),
  format(round(se_visits), big.mark = ",")
))

visits_per_fte <- 2500L   # matches URPS_VISITS_PER_FTE_YEAR in R/44
fte_estimate <- n_visits / visits_per_fte
message(sprintf("Implied FTE demand: %.0f FTE (at %d visits/FTE/year)",
                fte_estimate, visits_per_fte))

# =============================================================================
# 10. Save outputs
# =============================================================================

ui_visit_rates <- list(
  meps_year           = meps_year,
  n_analytic_sample   = nrow(all_ui),
  total_visits_wtd    = n_visits,
  total_visits_se     = se_visits,
  fte_estimate        = fte_estimate,
  visits_per_fte      = visits_per_fte,
  by_age_band         = as.data.frame(age_visits),
  by_insurance        = as.data.frame(ins_visits),
  by_income_tier      = if (exists("inc_visits")) as.data.frame(inc_visits) else NULL,
  by_specialty        = as.data.frame(spec_visits),
  icd10_codes_used    = pfd_icd10_3char
)

rds_visits  <- file.path(out_dir, "meps_2023_ui_visit_rates.rds")
rds_income  <- file.path(out_dir, "meps_2023_income_care_seeking_multipliers.rds")
rds_insur   <- file.path(out_dir, "meps_2023_insurance_care_seeking_multipliers.rds")
rds_spec    <- file.path(out_dir, "meps_2023_specialty_distribution.rds")

saveRDS(ui_visit_rates,       rds_visits)
saveRDS(income_multipliers,   rds_income)
saveRDS(insurance_multipliers, rds_insur)
saveRDS(spec_visits,          rds_spec)

message("\nSaved:")
message("  ", rds_visits)
message("  ", rds_income)
message("  ", rds_insur)
message("  ", rds_spec)

# =============================================================================
# 11. Print recalibration patch for R/44-urps_population.R
# =============================================================================

message("\n", strrep("=", 70))
message("RECALIBRATION PATCH — paste into R/44-urps_population.R")
message(strrep("=", 70))
message("# Income care-seeking multipliers (MEPS 2022, updated from MEPS 2020)")
message("# Source: scripts/data_acquisition/05_download_meps_2023.R")
message("#' @export")
message("CARE_SEEKING_BY_INCOME <- c(")
for (tier in c("LT25k", "25k_50k", "50k_100k", "GT100k")) {
  val <- income_multipliers[[tier]]
  if (is.null(val) || is.na(val)) val <- c(LT25k=0.72,"25k_50k"=0.88,"50k_100k"=0.97,GT100k=1.00)[[tier]]
  message(sprintf('  %-12s = %.2f,', sprintf('"%s"', tier), val))
}
message(")")

message("\n# Insurance care-seeking multipliers (MEPS 2022)")
message("#' @export")
message("CARE_SEEKING_BY_INSURANCE <- c(")
for (cat in c("Private", "Public", "Uninsured", "Unknown")) {
  val <- insurance_multipliers[[cat]]
  if (is.null(val) || is.na(val)) val <- c(Private=1.00,Public=0.75,Uninsured=0.58,Unknown=0.80)[[cat]]
  message(sprintf('  %-12s = %.2f,', sprintf('"%s"', cat), val))
}
message(")")
message(strrep("=", 70))

# =============================================================================
# 12. Manifest
# =============================================================================

manifest_path <- file.path(out_dir, "meps_2023_manifest.txt")
writeLines(c(
  paste("MEPS", meps_year, "Download Manifest"),
  paste("Generated:", Sys.time()),
  paste("MEPS year:", meps_year, "(HC-233 series)"),
  paste("ICD-10 codes:", paste(pfd_icd10_3char, collapse=", ")),
  "",
  "Files:",
  paste("  meps_2023_ui_visit_rates.rds            — visit totals by age/insurance/income/specialty"),
  paste("  meps_2023_income_care_seeking_multipliers.rds  — named vector, GT100k=1.0 reference"),
  paste("  meps_2023_insurance_care_seeking_multipliers.rds — named vector, Private=1.0 reference"),
  paste("  meps_2023_specialty_distribution.rds    — share of visits by provider specialty"),
  "",
  paste("Analytic sample (women 18+ with UI/PFD OB visit):", format(nrow(all_ui), big.mark=",")),
  paste("Weighted total visits:", format(round(n_visits), big.mark=",")),
  paste("FTE estimate:", round(fte_estimate, 0), "at", visits_per_fte, "visits/FTE/year"),
  "",
  "Survey design:",
  "  id = ~VARPSU, strata = ~VARSTR, weights = ~PERWT22F, nest = TRUE",
  "",
  "MEPS data source: AHRQ FTP via MEPS R package",
  "  remotes::install_github('e-mitchell/meps_r_pkg/MEPS')",
  "  read_MEPS(year = 2022, type = 'FYC')",
  "",
  "Recalibration target in R/44-urps_population.R:",
  "  CARE_SEEKING_BY_INCOME    — income-tier multipliers (was MEPS 2020)",
  "  CARE_SEEKING_BY_INSURANCE — insurance-type multipliers (was Richter 2007/MEPS 2020)",
  "",
  "DUA required: NO (MEPS public use files)"
), manifest_path)

message("\nManifest: ", manifest_path)
message("Done. MEPS 2023 download and recalibration complete.")
message("Next step: apply the patch printed above to R/44-urps_population.R,")
message("  or call apply_meps_2023_recalibration() once that function is added.")
