# URPS Population File (HWMM-style) ----
#
# Constructs the survey-weighted population cell table that the demand engine
# (R/demand-lifecourse.R, R/demand-utilization_models.R) operates on.  The
# design follows the IHS Markit / Dall Health Workforce Microsimulation Model
# (HWMM) population file architecture:
#
#   * Community-dwelling women 18+: ACS 5-year (demographics, income, insurance,
#     state, metro) cross-walked to BRFSS self-reported risk factors (BMI class,
#     smoking, approximate parity, UI/POP/FI where the state optional module
#     was administered).
#   * Match strata: age_group x sex x race_eth x insurance x income_tier.
#     Each stratum carries a population weight derived from ACS PUMS or, when
#     BRFSS is the only source, the BRFSS survey-design-adjusted cell weight.
#   * PFD prevalence imputation: when the BRFSS UI/POP/FI module is missing
#     (true for the 2023 core file -- optional module only), published age-band
#     prevalence (Nygaard 2008 JAMA / Wu 2014) is merged by age_group to give
#     a cell-level prevalence column without fabricating individual responses.
#
# References
#   Dall TM et al. (2013) The supply and demand for professional athletes.
#   Health Affairs 32(11):1993-2000. [HWMM architecture]
#   IHS Markit (2020) Complexities of physician supply and demand.
#   Nygaard I et al. (2008) Prevalence of symptomatic pelvic floor disorders.
#   JAMA 300(11):1311-1316.
#   Wu JM et al. (2014) Forecasting the prevalence of pelvic floor disorders.
#   Obstet Gynecol 123(4):697-703.

# ---- HWMM age bands (HDMM Exhibit 5) ----------------------------------------
#
# Five adult bands used throughout demand modules.  The upper two align with
# mufflyaccess::pfd_prevalence() contract bands (65-74, 75+).

#' Adult age bands used across the demand modules
#'
#' Five HWMM/HDMM adult bands (Exhibit 5). The upper two align exactly with the
#' `mufflyaccess::pfd_prevalence()` contract bands (65-74, 75+) so prevalence can
#' be joined without a wrong-grain error.
#'
#' @format Character vector of five band labels.
#' @family urps population
#' @concept data
#' @export
URPS_POP_AGE_BANDS <- c("18-34", "35-44", "45-64", "65-74", "75+")

# BRFSS _AGEG5YR codes -> URPS_POP_AGE_BANDS mapping
.AGEG5YR_TO_URPS_BAND <- c(
  "1"  = "18-34",   # 18-24
  "2"  = "18-34",   # 25-29
  "3"  = "18-34",   # 30-34
  "4"  = "35-44",   # 35-39
  "5"  = "35-44",   # 40-44
  "6"  = "45-64",   # 45-49
  "7"  = "45-64",   # 50-54
  "8"  = "45-64",   # 55-59
  "9"  = "45-64",   # 60-64
  "10" = "65-74",   # 65-69
  "11" = "65-74",   # 70-74
  "12" = "75+",     # 75-79
  "13" = "75+",     # 80-84
  "14" = "75+"      # 85+
)

# ---- Race/ethnicity (BRFSS _IMPRACE codes) -----------------------------------
.IMPRACE_LABELS <- c(
  "1" = "White_NH",
  "2" = "Black_NH",
  "3" = "AIAN_NH",
  "4" = "Asian_NH",
  "5" = "Hispanic",
  "6" = "Other_NH"
)

# ---- Insurance tier (BRFSS _HLTHPL1) -----------------------------------------
.HLTHPL_LABELS <- c(
  "1" = "Insured",
  "2" = "Uninsured",
  "9" = "Unknown"
)

# ---- Income tier (BRFSS INCOME3, 11-level -> 4-tier) -------------------------
#
# INCOME3 1-11 map to <$25k, $25-50k, $50-100k, $100k+ roughly.
# 77 = Don't know; 99 = Refused -> NA.
.income3_to_tier <- function(income3) {
  tier <- rep(NA_character_, length(income3))
  tier[income3 %in% 1:4]  <- "LT25k"
  tier[income3 %in% 5:6]  <- "25k_50k"
  tier[income3 %in% 7:8]  <- "50k_100k"   # 7=$50-75k, 8=$75-100k
  tier[income3 %in% 9:11] <- "GT100k"      # 9=$100-150k, 10=$150-200k, 11=$200k+
  tier
}

# ---- Care-seeking barrier multipliers by insurance and income ----------------
#
# Uninsured and low-income women access specialty urogynecology at reduced
# rates relative to insured women with higher incomes. Multipliers are applied
# per demand cell in project_urps_demand() before aggregation.
#
# Insurance multiplier: Richter 2007 AJOG; HCUP SASD specialty-access gap.
# Income multiplier: MEPS 2020 specialty visit rate by income quartile.

#' Care-seeking multiplier by insurance status
#'
#' Applied per demand cell in `project_urps_demand()` before aggregation.
#' Source: Richter 2007 AJOG; HCUP SASD specialty-access gap.
#'
#' @format Named numeric vector; 1.00 is the insured reference level.
#' @family urps population
#' @concept data
#' @export
CARE_SEEKING_BY_INSURANCE <- c(
  Insured   = 1.00,
  Uninsured = 0.58,
  Unknown   = 0.80
)

#' Care-seeking multiplier by income tier
#'
#' Applied per demand cell in `project_urps_demand()` before aggregation.
#' Source: MEPS 2020 specialty visit rate by income quartile.
#'
#' @format Named numeric vector; 1.00 is the highest-income reference level.
#' @family urps population
#' @concept data
#' @export
CARE_SEEKING_BY_INCOME <- c(
  LT25k     = 0.72,
  "25k_50k" = 0.88,
  "50k_100k"= 0.97,
  GT100k    = 1.00
)

#' Load MEPS-derived care-seeking multipliers and optionally apply them
#'
#' Reads the output of `scripts/data_acquisition/05_download_meps_2022.R` and
#' returns updated income and insurance care-seeking multiplier vectors.  Call
#' this at the top of an analysis script to replace the package defaults
#' (MEPS 2020 sourced) with the 2022 estimates without editing source code.
#'
#' @param year MEPS year to load (default 2022).
#' @param meps_dir Directory containing the MEPS RDS outputs.
#' @param verbose Logical.
#' @return Invisibly a named list with `income` and `insurance` multiplier
#'   vectors.  Prints a message with the values for confirmation.
#' @keywords internal
load_meps_care_seeking_multipliers <- function(year    = 2022L,
                                               meps_dir = NULL,
                                               verbose  = TRUE) {
  if (is.null(meps_dir)) {
    pkg_root <- system.file(package = "urpssim")
    meps_dir <- file.path(pkg_root, "..", "..", "data-raw", "meps")
    meps_dir <- normalizePath(meps_dir, mustWork = FALSE)
    if (!dir.exists(meps_dir))
      meps_dir <- file.path("data-raw", "meps")
  }

  inc_rds  <- file.path(meps_dir, sprintf("meps_%d_income_care_seeking_multipliers.rds",  year))
  ins_rds  <- file.path(meps_dir, sprintf("meps_%d_insurance_care_seeking_multipliers.rds", year))

  if (!file.exists(inc_rds) || !file.exists(ins_rds)) {
    stop(
      "MEPS ", year, " multiplier RDS not found.\n",
      "  Run: Rscript scripts/data_acquisition/05_download_meps_2022.R"
    )
  }

  income    <- readRDS(inc_rds)
  insurance <- readRDS(ins_rds)

  if (verbose) {
    message(sprintf("[load_meps_care_seeking_multipliers] MEPS %d", year))
    message("  Income multipliers (GT100k = 1.0 ref):")
    message("    ", paste(sprintf("%s=%.2f", names(income), income), collapse = "  "))
    message("  Insurance multipliers (Private = 1.0 ref):")
    message("    ", paste(sprintf("%s=%.2f", names(insurance), insurance), collapse = "  "))
  }

  invisible(list(income = income, insurance = insurance))
}

# ---- BMI class (BRFSS _BMI5CAT) ---------------------------------------------
.BMI5CAT_LABELS <- c(
  "1" = "Underweight",
  "2" = "Normal",
  "3" = "Overweight",
  "4" = "Obese"
)

# ---- Smoking status (_SMOKER3) -----------------------------------------------
.SMOKER3_LABELS <- c(
  "1" = "Current_Daily",
  "2" = "Current_Some",
  "3" = "Former",
  "4" = "Never",
  "9" = "Unknown"
)

# ---- Published PFD prevalence by URPS age band (imputed when BRFSS module absent)
#
# UI + POP combined "any PFD" from Nygaard 2008 / Wu 2014, women.
# Used ONLY when the BRFSS UI/POP/FI survey module columns are missing.

.PFD_PREVALENCE_BY_BAND <- c(
  "18-34" = 0.098,
  "35-44" = 0.217,
  "45-64" = 0.333,
  "65-74" = 0.368,
  "75+"   = 0.386
)

# ---- UI-specific prevalence (BRFSS optional module absent fallback) ----------
.UI_PREVALENCE_BY_BAND <- c(
  "18-34" = 0.064,
  "35-44" = 0.148,
  "45-64" = 0.263,
  "65-74" = 0.318,
  "75+"   = 0.356
)

# ---- POP-specific prevalence (symptomatic, Nygaard 2008) --------------------
.POP_PREVALENCE_BY_BAND <- c(
  "18-34" = 0.007,
  "35-44" = 0.025,
  "45-64" = 0.050,
  "65-74" = 0.067,
  "75+"   = 0.068
)

# ---- FI-specific prevalence (Whitehead 2009 / Bharucha 2005) ----------------
.FI_PREVALENCE_BY_BAND <- c(
  "18-34" = 0.018,
  "35-44" = 0.032,
  "45-64" = 0.058,
  "65-74" = 0.085,
  "75+"   = 0.104
)

# =============================================================================
# Public API
# =============================================================================

#' Load and harmonise the BRFSS women sub-file
#'
#' Reads the pre-processed RDS created by `scripts/data_acquisition/01_download_brfss.R`
#' and returns a minimal tidy tibble with strata columns, BRFSS survey weight,
#' and risk-factor flags needed by the demand cell builder.
#'
#' @param brfss_rds Path to the women-subset RDS. Defaults to the repo-local
#'   `data-raw/brfss/brfss_2023_women18plus.rds`.
#' @param verbose Logical; if TRUE prints a one-line summary.
#' @return A tibble with columns: `seqno`, `state_fips`, `survey_wt`,
#'   `age_group`, `race_eth`, `insurance`, `income_tier`, `metro`,
#'   `bmi_class`, `smoker`, `n_children`, `ui_flag`, `pop_flag`, `fi_flag`.
#' @family urps population
#' @concept data
#' @export
load_brfss_women <- function(brfss_rds = NULL, verbose = TRUE) {
  if (is.null(brfss_rds)) {
    pkg_root <- system.file(package = "urpssim")
    brfss_rds <- file.path(pkg_root, "..", "..", "data-raw", "brfss",
                           "brfss_2023_women18plus.rds")
    brfss_rds <- normalizePath(brfss_rds, mustWork = FALSE)
    if (!file.exists(brfss_rds)) {
      brfss_rds <- file.path("data-raw", "brfss", "brfss_2023_women18plus.rds")
    }
  }
  if (!file.exists(brfss_rds)) {
    stop(
      "BRFSS women RDS not found: ", brfss_rds,
      "\nRun: Rscript scripts/data_acquisition/01_download_brfss.R"
    )
  }
  raw <- readRDS(brfss_rds)

  # Rename backtick-column names to valid R identifiers
  nms <- names(raw)
  names(raw) <- sub("^_", "X_", nms)

  age_code <- as.character(raw[["X_AGEG5YR"]])
  race_code <- as.character(raw[["X_IMPRACE"]])
  ins_code  <- as.character(raw[["X_HLTHPL1"]])

  age_grp  <- .AGEG5YR_TO_URPS_BAND[age_code]
  race_eth <- .IMPRACE_LABELS[race_code]
  insurance <- .HLTHPL_LABELS[ins_code]
  income_tier <- .income3_to_tier(raw[["INCOME3"]])

  metro <- ifelse(raw[["X_METSTAT"]] == 1L, "Metro", "NonMetro")
  metro[is.na(raw[["X_METSTAT"]])] <- NA_character_

  bmi_class <- .BMI5CAT_LABELS[as.character(raw[["X_BMI5CAT"]])]
  smoker    <- .SMOKER3_LABELS[as.character(raw[["X_SMOKER3"]])]

  # Children ever born: CHILDREN = 88 means "none" (zero); 99 = refused (missing).
  n_ch <- raw[["CHILDREN"]]
  n_ch[n_ch == 88L] <- 0L
  n_ch[n_ch == 99L] <- NA_integer_

  # PFD self-report: optional BRFSS module -- absent in 2023 core
  ui_flag  <- .extract_pfd_flag(raw, c("BLADCON", "URINCON", "INCONTI"))
  pop_flag <- .extract_pfd_flag(raw, c("PROPLAP", "PELVORGAN"))
  fi_flag  <- .extract_pfd_flag(raw, c("BOWLLEA", "BOWLINC"))

  # Chronic condition flags
  chronic_flags <- lapply(.BRFSS_CHRONIC_COLS, function(spec) {
    .extract_chronic_flag(raw, spec$col,
                          yes_codes = spec$yes_codes,
                          no_codes  = spec$no_codes)
  })

  out <- tibble::tibble(
    seqno          = raw[["SEQNO"]],
    state_fips     = raw[["X_STATE"]],
    survey_wt      = raw[["X_LLCPWT"]],
    age_group      = factor(age_grp, levels = URPS_POP_AGE_BANDS),
    race_eth       = race_eth,
    insurance      = insurance,
    income_tier    = income_tier,
    metro          = metro,
    bmi_class      = bmi_class,
    smoker         = smoker,
    n_children     = n_ch,
    ui_flag        = ui_flag,
    pop_flag       = pop_flag,
    fi_flag        = fi_flag,
    # Chronic conditions (1 = has condition, 0 = does not, NA = unknown/refused)
    cc_hypertension = chronic_flags$hypertension,
    cc_hi_cholest   = chronic_flags$hi_cholest,
    cc_heart_attack = chronic_flags$heart_attack,
    cc_angina       = chronic_flags$angina,
    cc_stroke       = chronic_flags$stroke,
    cc_asthma       = chronic_flags$asthma,
    cc_copd         = chronic_flags$copd,
    cc_skin_cancer  = chronic_flags$skin_cancer,
    cc_other_cancer = chronic_flags$other_cancer,
    cc_diabetes     = chronic_flags$diabetes
  )
  out <- out[!is.na(out$age_group), ]

  if (verbose) {
    message(sprintf(
      "[load_brfss_women] n=%s rows, %s with survey weight > 0, survey year 2023",
      format(nrow(out), big.mark = ","),
      format(sum(!is.na(out$survey_wt) & out$survey_wt > 0), big.mark = ",")
    ))
  }
  out
}

.extract_pfd_flag <- function(raw, candidates) {
  found <- intersect(candidates, names(raw))
  if (length(found) == 0L) return(NA_integer_)
  vals <- raw[[found[1]]]
  flag <- rep(NA_integer_, length(vals))
  flag[vals == 1L] <- 1L
  flag[vals == 2L] <- 0L
  flag
}

# Extract a binary chronic-condition flag from a BRFSS column.
# yes_codes: values that mean "has condition" (default 1).
# no_codes:  values that mean "does not have" (default 2).
# All other values -> NA (don't know / refused / inapplicable).
.extract_chronic_flag <- function(raw, col, yes_codes = 1L, no_codes = 2L) {
  if (!col %in% names(raw)) return(NA_integer_)
  vals <- raw[[col]]
  flag <- rep(NA_integer_, length(vals))
  flag[vals %in% yes_codes] <- 1L
  flag[vals %in% no_codes]  <- 0L
  flag
}

# All 10 BRFSS chronic-condition columns wired to urpssim.
# BPHIGH6 needs yes_codes=1:2 (1=diagnosed, 2=borderline/pre-hypertensive).
.BRFSS_CHRONIC_COLS <- list(
  hypertension  = list(col = "BPHIGH6",  yes_codes = 1:2, no_codes = 3:4),
  hi_cholest    = list(col = "TOLDHI3",  yes_codes = 1L,  no_codes = 2L),
  heart_attack  = list(col = "CVDINFR4", yes_codes = 1L,  no_codes = 2L),
  angina        = list(col = "CVDCRHD4", yes_codes = 1L,  no_codes = 2L),
  stroke        = list(col = "CVDSTRK3", yes_codes = 1L,  no_codes = 2L),
  asthma        = list(col = "ASTHMA3",  yes_codes = 1L,  no_codes = 2L),
  copd          = list(col = "CHCCOPD3", yes_codes = 1L,  no_codes = 2L),
  skin_cancer   = list(col = "CHCSCNC1", yes_codes = 1L,  no_codes = 2L),
  other_cancer  = list(col = "CHCOCNC1", yes_codes = 1L,  no_codes = 2L),
  diabetes      = list(col = "DIABETE4", yes_codes = 1:2, no_codes = 3:4)
)

#' Build population cell table (HWMM Exhibit 5 architecture)
#'
#' Aggregates the BRFSS women microdata into a cell table where each row is a
#' unique combination of (age_group, race_eth, insurance, income_tier, metro,
#' bmi_class).  Each cell carries:
#'   - `n_respondents`: raw BRFSS count
#'   - `pop_weight`: sum of survey weights (proportional to US population)
#'   - `pct_smoker`, `mean_children`: risk-factor summaries
#'   - `ui_prevalence`, `pop_prevalence`, `fi_prevalence`: observed if the
#'     optional BRFSS module was present, imputed from published estimates
#'     otherwise (indicated by `pfd_source = "imputed"`).
#'
#' This cell table is the direct input to [project_urps_demand()] and to the
#' regression fitters in R/demand-utilization_models.R.
#'
#' @param brfss_women Tidy BRFSS women tibble from [load_brfss_women()].  If
#'   NULL, [load_brfss_women()] is called with default arguments.
#' @param verbose Logical.
#' @return A tibble with one row per stratum cell plus summary columns.
#' @family urps population
#' @concept data
#' @export
build_urps_population_cells <- function(brfss_women = NULL, verbose = TRUE) {
  if (is.null(brfss_women)) brfss_women <- load_brfss_women(verbose = verbose)

  pfd_observed <- !all(is.na(brfss_women$ui_flag))

  # Aggregate to cells via split-apply: aggregate()'s single per-column FUN can't
  # express "count rows for one column, weighted sums for the rest" in one pass,
  # so the cells are built explicitly below.
  grp_cols <- c("age_group", "race_eth", "insurance", "income_tier", "metro", "bmi_class")
  rows <- split(seq_len(nrow(brfss_women)), brfss_women[, grp_cols], drop = TRUE)

  cc_cols <- grep("^cc_", names(brfss_women), value = TRUE)

  cell_list <- lapply(rows, function(idx) {
    sub <- brfss_women[idx, ]
    wt  <- ifelse(is.na(sub$survey_wt), 0, sub$survey_wt)

    cc_prev <- setNames(
      vapply(cc_cols, function(cn) mean(sub[[cn]] == 1L, na.rm = TRUE), numeric(1)),
      cc_cols
    )

    base <- data.frame(
      n_respondents  = length(idx),
      pop_weight     = sum(wt),
      pct_smoker     = {
        # %in% never returns NA, so na.rm is inert and Unknown/NA smokers would
        # fold into the denominator as non-smokers. Exclude them explicitly.
        .known <- !is.na(sub$smoker) & sub$smoker != "Unknown"
        if (any(.known))
          mean(sub$smoker[.known] %in% c("Current_Daily", "Current_Some")) else NA_real_
      },
      mean_children  = mean(sub$n_children, na.rm = TRUE),
      n_ui_obs       = sum(sub$ui_flag == 1L, na.rm = TRUE),
      n_pop_obs      = sum(sub$pop_flag == 1L, na.rm = TRUE),
      n_fi_obs       = sum(sub$fi_flag == 1L, na.rm = TRUE),
      n_pfd_eligible = sum(!is.na(sub$ui_flag)),
      stringsAsFactors = FALSE
    )
    cbind(base, as.data.frame(t(cc_prev), stringsAsFactors = FALSE))
  })

  key_df <- do.call(rbind, lapply(names(rows), function(nm) {
    vals <- strsplit(nm, "\\.")[[1]]
    setNames(as.data.frame(t(vals), stringsAsFactors = FALSE), grp_cols)
  }))
  cell_df <- do.call(rbind, cell_list)
  out <- cbind(key_df, cell_df)
  rownames(out) <- NULL
  out <- tibble::as_tibble(out)
  out$age_group <- factor(out$age_group, levels = URPS_POP_AGE_BANDS)

  if (pfd_observed) {
    out$ui_prevalence  <- out$n_ui_obs  / pmax(out$n_pfd_eligible, 1)
    out$pop_prevalence <- out$n_pop_obs / pmax(out$n_pfd_eligible, 1)
    out$fi_prevalence  <- out$n_fi_obs  / pmax(out$n_pfd_eligible, 1)
    out$pfd_source     <- "brfss_observed"
    out$ui_source      <- "brfss_observed"
    out$pop_source     <- "brfss_observed"
    out$fi_source      <- "brfss_observed"
  } else {
    # Impute from published estimates merged by age_group
    band <- as.character(out$age_group)
    out$ui_prevalence  <- .UI_PREVALENCE_BY_BAND[band]
    out$pop_prevalence <- .POP_PREVALENCE_BY_BAND[band]
    out$fi_prevalence  <- .FI_PREVALENCE_BY_BAND[band]
    out$pfd_source     <- "imputed_nygaard_wu"
    out$ui_source      <- "imputed_nygaard_wu"
    out$pop_source     <- "imputed_nygaard_wu"
    out$fi_source      <- "imputed_nygaard_wu"
  }

  keep_cols <- c(grp_cols,
                 "n_respondents", "pop_weight",
                 "pct_smoker", "mean_children",
                 "ui_prevalence", "pop_prevalence", "fi_prevalence",
                 "pfd_source", "ui_source", "pop_source", "fi_source",
                 intersect(cc_cols, names(out)))
  out <- out[, keep_cols]

  if (verbose) {
    message(sprintf(
      "[build_urps_population_cells] %d cells, total pop_weight %.0f M, PFD source: %s",
      nrow(out),
      sum(out$pop_weight, na.rm = TRUE) / 1e6,
      unique(out$pfd_source)[1]
    ))
  }
  out
}

#' Blend nationally weighted NHANES UI prevalence into BRFSS population cells
#'
#' BRFSS 2023's core file has no urinary-incontinence module. NHANES has a
#' nationally representative urinary questionnaire, while BRFSS retains the
#' age, race, insurance, income, and geography strata used for the population
#' cell architecture. This function replaces only the UI prevalence component;
#' POP and FI remain on their separately documented sources.
#'
#' @param cells Population cells from [build_urps_population_cells()].
#' @param nhanes_age A tibble from `nhanes_ui_prevalence_by_age.rds`, or NULL to
#'   load `data-raw/nhanes/nhanes_ui_prevalence_by_age.rds`.
#' @param verbose Logical.
#' @return `cells` with nationally weighted UI prevalence by age, `ui_source =
#'   "nhanes_2017_2023_pooled"`, and an explicitly mixed `pfd_source`.
#' @details The youngest NHANES stratum is 20--34 because the KIQ_U analysis
#' starts at age 20. It is used for the BRFSS 18--34 stratum and that small
#' coverage mismatch is recorded in the source label rather than hidden.
#' @family urps population
#' @concept data
#' @export
blend_nhanes_ui_prevalence <- function(cells, nhanes_age = NULL, verbose = TRUE) {
  if (is.null(nhanes_age)) {
    path <- file.path("data-raw", "nhanes", "nhanes_ui_prevalence_by_age.rds")
    if (!file.exists(path)) {
      stop("NHANES UI prevalence file not found: ", path,
           "\nRun: Rscript scripts/data_acquisition/07_download_nhanes_urinary.R", call. = FALSE)
    }
    nhanes_age <- readRDS(path)
  }
  if ("outcome" %in% names(nhanes_age)) nhanes_age <- nhanes_age[nhanes_age$outcome == "ui", ]
  band_col <- intersect(c("age_band", "group"), names(nhanes_age))[1]
  if (is.na(band_col) || !"prevalence" %in% names(nhanes_age)) {
    stop("NHANES prevalence input needs age_band (or group) and prevalence columns", call. = FALSE)
  }
  targets <- c("18-34" = "20-34", "35-44" = "35-44", "45-64" = "45-64",
               "65-74" = "65-74", "75+" = "75+")
  observed <- stats::setNames(nhanes_age$prevalence, as.character(nhanes_age[[band_col]]))
  missing <- setdiff(unname(targets), names(observed))
  if (length(missing)) {
    stop("NHANES UI prevalence is missing age band(s): ", paste(missing, collapse = ", "),
         ". Re-run the NHANES acquisition script.", call. = FALSE)
  }
  if (any(!is.finite(observed[unname(targets)]) | observed[unname(targets)] < 0 |
          observed[unname(targets)] > 1)) {
    stop("NHANES UI prevalence must be finite proportions in [0, 1]", call. = FALSE)
  }
  for (band in names(targets)) {
    rows <- as.character(cells$age_group) == band
    cells$ui_prevalence[rows] <- observed[[targets[[band]]]]
    cells$ui_source[rows] <- "nhanes_2017_2023_pooled"
    cells$pfd_source[rows] <- "mixed_nhanes_ui_nygaard_wu"
  }
  if (verbose) message("[blend_nhanes_ui_prevalence] replaced UI prevalence in all age bands with pooled NHANES 2017-2018 + 2021-2023 estimates")
  cells
}

#' Project URPS demand from population cell table
#'
#' Multiplies each cell's effective-population count (pop_weight / total x
#' US female population) by age-band PFD prevalence, a care-seeking rate, and a
#' referral rate to get expected urogynecology visits per year.  Output is by
#' age_group so it can be compared directly to the supply FTE series.
#'
#' When `access_scenario` is not `"status_quo"`, insurance and/or income barrier
#' multipliers (`CARE_SEEKING_BY_INSURANCE`, `CARE_SEEKING_BY_INCOME`) are applied
#' per cell before aggregation, so the scenario lift is anchored to the actual
#' uninsured/low-income share in the demand cells rather than a population scalar.
#'
#' @param cells Population cell table from [build_urps_population_cells()].
#' @param us_female_pop Scalar; total US women 18+ (default: 2023 Census
#'   estimate, ~138 million).
#' @param care_seeking_rate Proportion of PFD-prevalent women who seek care
#'   in a given year.  HWMM / Nygaard 2008: ~0.25.
#' @param referral_rate Proportion of care-seeking women who reach a
#'   urogynecologist.  Default 0.50 (placeholder; calibrate to Kirby 2013).
#' @param access_scenario One of `"status_quo"` (default), `"insurance_equity"`,
#'   `"income_equity"`, or `"full_equity"`.  Controls whether insurance and/or
#'   income barriers are applied to per-cell care-seeking rates.
#' @param verbose Logical.
#' @return A tibble with columns: `age_group`, `pop_women`, `n_pfd`,
#'   `n_care_seeking`, `n_urgy_visits`, `demand_fte` (annualised FTE assuming
#'   the workload constant from R/supply-workload_to_fte.R).
#' @family urps population
#' @concept data
#' @export
project_urps_demand <- function(cells,
                                us_female_pop     = 138e6,
                                care_seeking_rate = 0.25,
                                referral_rate     = 0.50,
                                access_scenario   = c("status_quo", "insurance_equity",
                                                      "income_equity", "full_equity"),
                                verbose           = TRUE) {
  access_scenario <- match.arg(access_scenario)
  total_wt <- sum(cells$pop_weight, na.rm = TRUE)
  if (total_wt <= 0) stop("population cell table has zero total weight")

  cells$pop_women <- cells$pop_weight / total_wt * us_female_pop

  # Per-cell effective care-seeking rate: multiply base rate by insurance and
  # income barrier multipliers under status_quo; set barriers to 1.0 when a
  # scenario relaxes them.
  ins_mult <- rep(1.0, nrow(cells))
  inc_mult <- rep(1.0, nrow(cells))

  if (access_scenario %in% c("status_quo", "income_equity")) {
    ins_key <- cells$insurance
    ins_key[is.na(ins_key)] <- "Unknown"
    ins_mult <- ifelse(ins_key %in% names(CARE_SEEKING_BY_INSURANCE),
                       CARE_SEEKING_BY_INSURANCE[ins_key], 1.0)
  }
  if (access_scenario %in% c("status_quo", "insurance_equity")) {
    inc_key <- cells$income_tier
    inc_key[is.na(inc_key)] <- "Unknown"
    inc_mult <- ifelse(inc_key %in% names(CARE_SEEKING_BY_INCOME),
                       CARE_SEEKING_BY_INCOME[inc_key], 1.0)
  }

  eff_care_seeking <- care_seeking_rate * ins_mult * inc_mult

  cells$n_pfd          <- cells$pop_women * cells$ui_prevalence
  cells$n_care_seeking <- cells$n_pfd     * eff_care_seeking
  cells$n_urgy_visits  <- cells$n_care_seeking * referral_rate

  agg <- stats::aggregate(
    cbind(pop_women      = cells$pop_women,
          n_pfd          = cells$n_pfd,
          n_care_seeking = cells$n_care_seeking,
          n_urgy_visits  = cells$n_urgy_visits),
    by   = list(age_group = cells$age_group),
    FUN  = sum,
    na.rm = TRUE
  )
  agg <- tibble::as_tibble(agg)
  agg$age_group <- factor(agg$age_group, levels = URPS_POP_AGE_BANDS)
  agg <- agg[order(agg$age_group), ]

  URPS_VISITS_PER_FTE_YEAR <- 2500
  agg$demand_fte <- agg$n_urgy_visits / URPS_VISITS_PER_FTE_YEAR

  if (verbose) {
    tot_fte <- sum(agg$demand_fte, na.rm = TRUE)
    message(sprintf(
      "[project_urps_demand] %s -- total demand %.0f FTE (%.2f base care-seeking, %.2f referral)",
      access_scenario, tot_fte, care_seeking_rate, referral_rate
    ))
  }
  agg
}

#' Describe match-stratum coverage
#'
#' Returns the fraction of total survey weight captured by cells that have
#' all five strata columns non-missing.  Useful for diagnosing how much of the
#' population is lost to missing income/insurance/race data before matching.
#'
#' @param cells Cell table from [build_urps_population_cells()].
#' @return Named numeric vector: `complete_cell_share` (weight fraction),
#'   `n_complete_cells`, `n_total_cells`.
#' @family urps population
#' @concept data
#' @export
summarise_stratum_coverage <- function(cells) {
  strata_cols <- c("age_group", "race_eth", "insurance", "income_tier")
  complete <- rowSums(is.na(cells[, strata_cols])) == 0
  total_wt <- sum(cells$pop_weight, na.rm = TRUE)
  c(
    complete_cell_share = if (total_wt > 0)
      sum(cells$pop_weight[complete], na.rm = TRUE) / total_wt else NA_real_,
    n_complete_cells = sum(complete),
    n_total_cells    = nrow(cells)
  )
}

#' Compute the high-barrier prevalence from demand cells
#'
#' Returns the survey-weight-averaged fraction of the population that is
#' uninsured OR in the lowest income tier (LT25k) -- the "high_barrier" concept
#' used by [simulate_lifecourse_demand()] in R/demand-lifecourse.  Replaces the hardcoded 0.35
#' with a value anchored to actual BRFSS insurance/income data.
#'
#' @param cells Population cell table from [build_urps_population_cells()].
#' @param definition Character; `"uninsured_or_low_income"` (default) sets the
#'   barrier flag when insurance == "Uninsured" OR income_tier == "LT25k".
#'   `"uninsured_only"` uses insurance alone.
#' @return Named numeric scalar `barrier_prevalence`.
#' @keywords internal
brfss_barrier_prevalence <- function(cells,
                                     definition = c("uninsured_or_low_income",
                                                    "uninsured_only")) {
  definition <- match.arg(definition)
  wt <- ifelse(is.na(cells$pop_weight), 0, cells$pop_weight)
  if (definition == "uninsured_only") {
    flag <- !is.na(cells$insurance) & cells$insurance == "Uninsured"
  } else {
    flag <- (!is.na(cells$insurance) & cells$insurance == "Uninsured") |
            (!is.na(cells$income_tier) & cells$income_tier == "LT25k")
  }
  den <- sum(wt)
  if (den <= 0) return(c(barrier_prevalence = NA_real_))
  c(barrier_prevalence = sum(wt[flag]) / den)
}

# ---- Crosswalk to DEMAND_AGE_BANDS ------------------------------------------
#
# URPS_POP_AGE_BANDS ("18-34","35-44","45-64","65-74","75+") and
# DEMAND_AGE_BANDS   ("20-39","40-59","60-64","65-79","80+") do not align.
# The crosswalk uses year-width splits to apportion BRFSS cell weights:
#
#   DEMAND band   <- URPS bands contributing (fraction of 5-yr cells)
#   "20-39"       <- "18-34" (years 20-34 = 15/17) + tiny fraction ignored
#   "40-59"       <- "35-44" (years 40-44 = 5/10 = 0.5) +
#                   "45-64" (years 45-59 = 15/20 = 0.75)
#   "60-64"       <- "45-64" (years 60-64 = 5/20 = 0.25)
#   "65-79"       <- "65-74" (years 65-74 = 10/10 = 1.0, ignores 75-79)
#   "80+"         <- "75+"   (years 80+ ~ "75+" tail)
#
# Weights are approximate; the crosswalk is documented here, not hidden.

.URPS_TO_DEMAND_XWALK <- list(
  "20-39" = c("18-34" = 1.0),
  "40-59" = c("35-44" = 0.5, "45-64" = 0.75),
  "60-64" = c("45-64" = 0.25),
  "65-79" = c("65-74" = 1.0),
  "80+"   = c("75+"   = 1.0)
)

#' Aggregate BRFSS cell UI prevalence to DEMAND_AGE_BANDS
#'
#' Computes a survey-weight-averaged UI prevalence for each of the five
#' `DEMAND_AGE_BANDS` used by [compute_demand_denominators()], using the
#' approximate crosswalk from `URPS_POP_AGE_BANDS`. The result slots directly
#' into the `pfd_prevalence` argument of `compute_demand_denominators()` so
#' BRFSS-derived prevalence can replace the Nygaard 2008 constants without
#' restructuring the demand pipeline.
#'
#' @param cells Population cell table from [build_urps_population_cells()].
#' @param condition One of `"ui"` (default), `"pop"`, `"fi"`, or `"any_pfd"`
#'   (unweighted sum of the three rates, capped at 1).
#' @return Named numeric vector over `DEMAND_AGE_BANDS` (same structure as
#'   `pfd_prevalence_by_band()`).
#' @family urps population
#' @concept data
#' @export
brfss_pfd_prevalence_for_demand_bands <- function(cells,
                                                   condition = c("ui", "pop", "fi",
                                                                 "any_pfd")) {
  condition <- match.arg(condition)
  prev_col <- switch(condition,
    ui      = "ui_prevalence",
    pop     = "pop_prevalence",
    fi      = "fi_prevalence",
    any_pfd = NULL
  )

  if (is.null(prev_col)) {
    cells$any_pfd_prevalence <- pmin(
      cells$ui_prevalence + cells$pop_prevalence + cells$fi_prevalence, 1
    )
    prev_col <- "any_pfd_prevalence"
  }

  DEMAND_AGE_BANDS <- c("20-39", "40-59", "60-64", "65-79", "80+")
  out <- setNames(numeric(length(DEMAND_AGE_BANDS)), DEMAND_AGE_BANDS)

  for (dband in DEMAND_AGE_BANDS) {
    contributors <- .URPS_TO_DEMAND_XWALK[[dband]]
    wt_sum  <- 0
    prev_wt <- 0
    for (uband in names(contributors)) {
      frac  <- contributors[[uband]]
      sub   <- cells[!is.na(cells$age_group) & as.character(cells$age_group) == uband, ]
      if (nrow(sub) == 0) next
      w <- sub$pop_weight * frac
      wt_sum  <- wt_sum  + sum(w,                        na.rm = TRUE)
      prev_wt <- prev_wt + sum(w * sub[[prev_col]],      na.rm = TRUE)
    }
    out[[dband]] <- if (wt_sum > 0) prev_wt / wt_sum else NA_real_
  }

  missing <- is.na(out)
  if (any(missing)) {
    fallback <- .UI_PREVALENCE_BY_BAND
    fallback_demand <- c(
      "20-39" = fallback[["18-34"]],
      "40-59" = mean(c(fallback[["35-44"]], fallback[["45-64"]])),
      "60-64" = fallback[["45-64"]],
      "65-79" = fallback[["65-74"]],
      "80+"   = fallback[["75+"]]
    )
    out[missing] <- fallback_demand[missing]
  }
  out
}

# =============================================================================
# MCBS 2022 integration -- Medicare women 65+
# =============================================================================

#' Load and harmonise the MCBS 2022 women 65+ sub-file
#'
#' Reads the pre-processed RDS created during MCBS download and returns a
#' minimal tibble aligned to the URPS strata scheme so it can be used to
#' calibrate the 65-74 and 75+ UI prevalence cells via
#' [blend_mcbs_prevalence()].
#'
#' @param mcbs_rds Path to the women 65+ RDS.  Defaults to
#'   `data-raw/mcbs/mcbs_2022_women65plus.rds`.
#' @param verbose Logical.
#' @return Tibble with columns: `age_group`, `medicare_adv`, `private_medigap`,
#'   `ui_loss`, `ui_talked_dr`, `ui_had_surgery`, `had_stroke`, `had_cancer`,
#'   `had_depression`, `had_osteoporosis`, `income_cat`, `survey_wt`.
#' @keywords internal
load_mcbs_women65 <- function(mcbs_rds = NULL, verbose = TRUE) {
  if (is.null(mcbs_rds)) {
    pkg_root <- system.file(package = "urpssim")
    mcbs_rds <- file.path(pkg_root, "..", "..", "data-raw", "mcbs",
                          "mcbs_2022_women65plus.rds")
    mcbs_rds <- normalizePath(mcbs_rds, mustWork = FALSE)
    if (!file.exists(mcbs_rds))
      mcbs_rds <- file.path("data-raw", "mcbs", "mcbs_2022_women65plus.rds")
  }
  if (!file.exists(mcbs_rds)) {
    stop(
      "MCBS women 65+ RDS not found: ", mcbs_rds,
      "\nRun: Rscript scripts/data_acquisition/03_download_mcbs.R"
    )
  }
  raw <- readRDS(mcbs_rds)

  # MCBS uses "65-74", "75-84", "85+" -> collapse to URPS_POP_AGE_BANDS
  age_raw <- as.character(raw$age_group)
  age_urps <- ifelse(age_raw == "65-74", "65-74",
               ifelse(age_raw %in% c("75-84", "85+"), "75+", NA_character_))
  raw$age_group <- factor(age_urps, levels = URPS_POP_AGE_BANDS)

  weight_col <- intersect(c("PUFFWGT", "WTFA_SU"), names(raw))[1]

  out <- tibble::tibble(
    age_group      = raw$age_group,
    medicare_adv   = raw$medicare_adv,
    private_medigap = raw$private_medigap,
    ui_loss        = raw$ui_loss,
    ui_talked_dr   = raw$ui_talked_dr,
    ui_had_surgery = raw$ui_had_surgery,
    had_stroke     = raw$had_stroke,
    had_cancer     = raw$had_cancer,
    had_depression = raw$had_depression,
    had_osteoporosis = raw$had_osteoporosis,
    income_cat     = raw$income_cat,
    survey_wt      = if (!is.na(weight_col)) raw[[weight_col]] else 1
  )
  out <- out[!is.na(out$age_group), ]

  if (verbose) {
    n65 <- sum(out$age_group == "65-74", na.rm = TRUE)
    n75 <- sum(out$age_group == "75+",   na.rm = TRUE)
    ui_prev <- stats::weighted.mean(out$ui_loss == 1L,
                             w = ifelse(is.na(out$survey_wt), 1, out$survey_wt),
                             na.rm = TRUE)
    message(sprintf(
      "[load_mcbs_women65] n=%d (65-74: %d, 75+: %d) | UI prevalence %.1f%%",
      nrow(out), n65, n75, ui_prev * 100
    ))
  }
  out
}

#' Blend MCBS-calibrated UI prevalence into the 65+ population cells
#'
#' Replaces `ui_prevalence` in the "65-74" and "75+" age groups with
#' survey-weight-averaged estimates from the MCBS 2022 women 65+ file.
#' MCBS is the authoritative source for Medicare women: it captures the full
#' residential-care segment that BRFSS undercounts, and the UI module is in
#' the core (not optional) questionnaire.
#'
#' Cells outside "65-74" / "75+" are unchanged. Source fields identify MCBS
#' as the UI source and preserve the fact that POP/FI use separate inputs.
#'
#' @param cells Population cell table from [build_urps_population_cells()].
#' @param mcbs Optional MCBS tibble from [load_mcbs_women65()].  When NULL,
#'   [load_mcbs_women65()] is called with default arguments.
#' @param verbose Logical.
#' @return The `cells` tibble with `ui_prevalence` recalibrated for 65+ rows.
#' @family urps population
#' @concept data
#' @export
blend_mcbs_prevalence <- function(cells, mcbs = NULL, verbose = TRUE) {
  if (is.null(mcbs)) mcbs <- load_mcbs_women65(verbose = verbose)

  for (band in c("65-74", "75+")) {
    sub <- mcbs[!is.na(mcbs$age_group) & as.character(mcbs$age_group) == band, ]
    if (nrow(sub) == 0L) next
    wt   <- ifelse(is.na(sub$survey_wt), 1, sub$survey_wt)
    prev <- stats::weighted.mean(sub$ui_loss == 1L, w = wt, na.rm = TRUE)

    rows <- !is.na(cells$age_group) & as.character(cells$age_group) == band
    if (any(rows)) {
      cells$ui_prevalence[rows] <- prev
      cells$pfd_source[rows]    <- "mixed_mcbs_ui_nygaard_wu"
      if ("ui_source" %in% names(cells)) cells$ui_source[rows] <- "mcbs_2022_calibrated"
    }
    if (verbose) {
      message(sprintf(
        "[blend_mcbs_prevalence] %s: UI prevalence set to %.1f%% (MCBS 2022, n=%d)",
        band, prev * 100, nrow(sub)
      ))
    }
  }
  cells
}

#' Load all population data sources and return a fully calibrated cell table
#'
#' Convenience wrapper that calls [load_brfss_women()], [build_urps_population_cells()],
#' then [blend_nhanes_ui_prevalence()] when needed, and optionally
#' [blend_mcbs_prevalence()], returning a single calibrated table
#' ready for [project_urps_demand()] or [compute_brfss_demand_estimand()].
#'
#' @param use_mcbs Logical; if TRUE (default) and the MCBS RDS is present,
#'   blend MCBS 2022 prevalence into the 65+ cells.
#' @param use_nhanes Logical; if TRUE (default) and the NHANES RDS is present,
#'   blend national UI prevalence into all age cells. This is used when BRFSS
#'   lacks its optional UI module.
#' @param brfss_rds,mcbs_rds,nhanes_age_rds Paths to the respective RDS files.
#'   NULL = default.
#' @param verbose Logical.
#' @return Calibrated population cell table.
#' @family urps population
#' @concept data
#' @export
build_calibrated_population_cells <- function(use_mcbs  = TRUE,
                                               use_nhanes = TRUE,
                                               brfss_rds = NULL,
                                               mcbs_rds  = NULL,
                                               nhanes_age_rds = NULL,
                                               verbose   = TRUE) {
  brfss <- load_brfss_women(brfss_rds, verbose = verbose)
  cells <- build_urps_population_cells(brfss, verbose = verbose)

  if (isTRUE(use_nhanes)) {
    nhanes_path <- nhanes_age_rds %||% file.path("data-raw", "nhanes", "nhanes_ui_prevalence_by_age.rds")
    if (file.exists(nhanes_path)) {
      cells <- blend_nhanes_ui_prevalence(cells, readRDS(nhanes_path), verbose = verbose)
    } else if (verbose) {
      message("[build_calibrated_population_cells] NHANES UI prevalence not found; retaining published UI imputation")
    }
  }

  if (isTRUE(use_mcbs)) {
    mcbs_path <- mcbs_rds %||% file.path("data-raw", "mcbs", "mcbs_2022_women65plus.rds")
    if (file.exists(mcbs_path)) {
      mcbs <- load_mcbs_women65(mcbs_path, verbose = verbose)
      cells <- blend_mcbs_prevalence(cells, mcbs, verbose = verbose)
    } else if (verbose) {
      message("[build_calibrated_population_cells] MCBS RDS not found; 65+ cells retain their current UI calibration")
    }
  }
  cells
}
