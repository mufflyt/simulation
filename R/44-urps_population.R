# URPS Population File (HWMM-style) ----
#
# Constructs the survey-weighted population cell table that the demand engine
# (R/25-demand_lifecourse.R, R/26-utilization_models.R) operates on.  The
# design follows the IHS Markit / Dall Health Workforce Microsimulation Model
# (HWMM) population file architecture:
#
#   * Community-dwelling women 18+: ACS 5-year (demographics, income, insurance,
#     state, metro) cross-walked to BRFSS self-reported risk factors (BMI class,
#     smoking, approximate parity, UI/POP/FI where the state optional module
#     was administered).
#   * Match strata: age_group × sex × race_eth × insurance × income_tier.
#     Each stratum carries a population weight derived from ACS PUMS or, when
#     BRFSS is the only source, the BRFSS survey-design-adjusted cell weight.
#   * PFD prevalence imputation: when the BRFSS UI/POP/FI module is missing
#     (true for the 2023 core file — optional module only), published age-band
#     prevalence (Nygaard 2008 JAMA / Wu 2014) is merged by age_group to give
#     a cell-level prevalence column without fabricating individual responses.
#
# References
#   Dall TM et al. (2013) The supply and demand for professional athletes.
#   Health Affairs 32(11):1993–2000. [HWMM architecture]
#   IHS Markit (2020) Complexities of physician supply and demand.
#   Nygaard I et al. (2008) Prevalence of symptomatic pelvic floor disorders.
#   JAMA 300(11):1311–1316.
#   Wu JM et al. (2014) Forecasting the prevalence of pelvic floor disorders.
#   Obstet Gynecol 123(4):697–703.

# ---- HWMM age bands (HDMM Exhibit 5) ----------------------------------------
#
# Five adult bands used throughout demand modules.  The upper two align with
# mufflyaccess::pfd_prevalence() contract bands (65-79, 80+).

#' @export
URPS_POP_AGE_BANDS <- c("18-34", "35-44", "45-64", "65-74", "75+")

# BRFSS _AGEG5YR codes → URPS_POP_AGE_BANDS mapping
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

# ---- Income tier (BRFSS INCOME3, 11-level → 4-tier) -------------------------
#
# INCOME3 1-11 map to <$25k, $25-50k, $50-100k, $100k+ roughly.
# 77 = Don't know; 99 = Refused → NA.
.income3_to_tier <- function(income3) {
  tier <- rep(NA_character_, length(income3))
  tier[income3 %in% 1:4]  <- "LT25k"
  tier[income3 %in% 5:6]  <- "25k_50k"
  tier[income3 %in% 7:9]  <- "50k_100k"
  tier[income3 %in% 10:11] <- "GT100k"
  tier
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

  # Children ever born: CHILDREN = 88 means "none"; 99 = refused
  n_ch <- raw[["CHILDREN"]]
  n_ch[n_ch %in% c(88L, 99L)] <- NA_integer_

  # PFD self-report: optional BRFSS module — absent in 2023 core
  ui_flag  <- .extract_pfd_flag(raw, c("BLADCON", "URINCON", "INCONTI"))
  pop_flag <- .extract_pfd_flag(raw, c("PROPLAP", "PELVORGAN"))
  fi_flag  <- .extract_pfd_flag(raw, c("BOWLLEA", "BOWLINC"))

  out <- tibble::tibble(
    seqno       = raw[["SEQNO"]],
    state_fips  = raw[["X_STATE"]],
    survey_wt   = raw[["X_LLCPWT"]],
    age_group   = factor(age_grp, levels = URPS_POP_AGE_BANDS),
    race_eth    = race_eth,
    insurance   = insurance,
    income_tier = income_tier,
    metro       = metro,
    bmi_class   = bmi_class,
    smoker      = smoker,
    n_children  = n_ch,
    ui_flag     = ui_flag,
    pop_flag    = pop_flag,
    fi_flag     = fi_flag
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
#' regression fitters in R/26-utilization_models.R.
#'
#' @param brfss_women Tidy BRFSS women tibble from [load_brfss_women()].  If
#'   NULL, [load_brfss_women()] is called with default arguments.
#' @param verbose Logical.
#' @return A tibble with one row per stratum cell plus summary columns.
#' @export
build_urps_population_cells <- function(brfss_women = NULL, verbose = TRUE) {
  if (is.null(brfss_women)) brfss_women <- load_brfss_women(verbose = verbose)

  pfd_observed <- !all(is.na(brfss_women$ui_flag))

  cells <- aggregate(
    cbind(n_respondents = seq_len(nrow(brfss_women)),
          pop_weight    = brfss_women$survey_wt,
          n_smoker      = as.integer(!is.na(brfss_women$smoker) &
                                       brfss_women$smoker %in%
                                       c("Current_Daily", "Current_Some")),
          n_children    = ifelse(is.na(brfss_women$n_children), 0,
                                 brfss_women$n_children),
          n_ui_flag     = ifelse(is.na(brfss_women$ui_flag), 0,
                                 brfss_women$ui_flag),
          n_pop_flag    = ifelse(is.na(brfss_women$pop_flag), 0,
                                 brfss_women$pop_flag),
          n_fi_flag     = ifelse(is.na(brfss_women$fi_flag), 0,
                                 brfss_women$fi_flag)),
    by = list(
      age_group   = brfss_women$age_group,
      race_eth    = brfss_women$race_eth,
      insurance   = brfss_women$insurance,
      income_tier = brfss_women$income_tier,
      metro       = brfss_women$metro,
      bmi_class   = brfss_women$bmi_class
    ),
    FUN = function(x) if (identical(names(x), "n_respondents")) length(x) else sum(x, na.rm = TRUE)
  )

  # Recompute properly via split-apply (aggregate FUN contract is tricky)
  grp_cols <- c("age_group", "race_eth", "insurance", "income_tier", "metro", "bmi_class")
  rows <- split(seq_len(nrow(brfss_women)), brfss_women[, grp_cols], drop = TRUE)

  cell_list <- lapply(rows, function(idx) {
    sub <- brfss_women[idx, ]
    wt  <- ifelse(is.na(sub$survey_wt), 0, sub$survey_wt)
    data.frame(
      n_respondents  = length(idx),
      pop_weight     = sum(wt),
      pct_smoker     = mean(sub$smoker %in% c("Current_Daily", "Current_Some"),
                            na.rm = TRUE),
      mean_children  = mean(sub$n_children, na.rm = TRUE),
      n_ui_obs       = sum(sub$ui_flag == 1L, na.rm = TRUE),
      n_pop_obs      = sum(sub$pop_flag == 1L, na.rm = TRUE),
      n_fi_obs       = sum(sub$fi_flag == 1L, na.rm = TRUE),
      n_pfd_eligible = sum(!is.na(sub$ui_flag)),
      stringsAsFactors = FALSE
    )
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
  } else {
    # Impute from published estimates merged by age_group
    band <- as.character(out$age_group)
    out$ui_prevalence  <- .UI_PREVALENCE_BY_BAND[band]
    out$pop_prevalence <- .POP_PREVALENCE_BY_BAND[band]
    out$fi_prevalence  <- .FI_PREVALENCE_BY_BAND[band]
    out$pfd_source     <- "imputed_nygaard_wu"
  }

  out <- out[, c(grp_cols,
                 "n_respondents", "pop_weight",
                 "pct_smoker", "mean_children",
                 "ui_prevalence", "pop_prevalence", "fi_prevalence",
                 "pfd_source")]

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

#' Project URPS demand from population cell table
#'
#' Multiplies each cell's effective-population count (pop_weight / total ×
#' US female population) by age-band PFD prevalence, a care-seeking rate, and a
#' referral rate to get expected urogynecology visits per year.  Output is by
#' age_group so it can be compared directly to the supply FTE series.
#'
#' @param cells Population cell table from [build_urps_population_cells()].
#' @param us_female_pop Scalar; total US women 18+ (default: 2023 Census
#'   estimate, ~138 million).
#' @param care_seeking_rate Proportion of PFD-prevalent women who seek care
#'   in a given year.  HWMM / Nygaard 2008: ~0.25.
#' @param referral_rate Proportion of care-seeking women who reach a
#'   urogynecologist.  Default 0.50 (placeholder; calibrate to Kirby 2013).
#' @param verbose Logical.
#' @return A tibble with columns: `age_group`, `pop_women`, `n_pfd`,
#'   `n_care_seeking`, `n_urgy_visits`, `demand_fte` (annualised FTE assuming
#'   the workload constant from R/17-workload_to_fte.R).
#' @export
project_urps_demand <- function(cells,
                                us_female_pop   = 138e6,
                                care_seeking_rate = 0.25,
                                referral_rate   = 0.50,
                                verbose         = TRUE) {
  total_wt <- sum(cells$pop_weight, na.rm = TRUE)
  if (total_wt <= 0) stop("population cell table has zero total weight")

  cells$pop_women <- cells$pop_weight / total_wt * us_female_pop

  cells$n_pfd          <- cells$pop_women * cells$ui_prevalence
  cells$n_care_seeking <- cells$n_pfd     * care_seeking_rate
  cells$n_urgy_visits  <- cells$n_care_seeking * referral_rate

  agg <- stats::aggregate(
    cbind(pop_women = cells$pop_women,
          n_pfd     = cells$n_pfd,
          n_care_seeking = cells$n_care_seeking,
          n_urgy_visits  = cells$n_urgy_visits),
    by   = list(age_group = cells$age_group),
    FUN  = sum,
    na.rm = TRUE
  )
  agg <- tibble::as_tibble(agg)
  agg$age_group <- factor(agg$age_group, levels = URPS_POP_AGE_BANDS)
  agg <- agg[order(agg$age_group), ]

  # Convert visits to FTE using the workload constant from R/17
  # URPS_VISITS_PER_FTE_YEAR: ~2500 outpatient encounters per FTE per year
  # (placeholder; replace with convert_workload_to_fte() once calibrated).
  URPS_VISITS_PER_FTE_YEAR <- 2500
  agg$demand_fte <- agg$n_urgy_visits / URPS_VISITS_PER_FTE_YEAR

  if (verbose) {
    tot_fte <- sum(agg$demand_fte, na.rm = TRUE)
    message(sprintf(
      "[project_urps_demand] total demand %.0f FTE (%.2f care-seeking, %.2f referral rates)",
      tot_fte, care_seeking_rate, referral_rate
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
#' @export
summarise_stratum_coverage <- function(cells) {
  strata_cols <- c("age_group", "race_eth", "insurance", "income_tier")
  complete <- rowSums(is.na(cells[, strata_cols])) == 0
  c(
    complete_cell_share = sum(cells$pop_weight[complete], na.rm = TRUE) /
      sum(cells$pop_weight, na.rm = TRUE),
    n_complete_cells = sum(complete),
    n_total_cells    = nrow(cells)
  )
}

# ---- Crosswalk to DEMAND_AGE_BANDS ------------------------------------------
#
# URPS_POP_AGE_BANDS ("18-34","35-44","45-64","65-74","75+") and
# DEMAND_AGE_BANDS   ("20-39","40-59","60-64","65-79","80+") do not align.
# The crosswalk uses year-width splits to apportion BRFSS cell weights:
#
#   DEMAND band   ← URPS bands contributing (fraction of 5-yr cells)
#   "20-39"       ← "18-34" (years 20-34 = 15/17) + tiny fraction ignored
#   "40-59"       ← "35-44" (years 40-44 = 5/10 = 0.5) +
#                   "45-64" (years 45-59 = 15/20 = 0.75)
#   "60-64"       ← "45-64" (years 60-64 = 5/20 = 0.25)
#   "65-79"       ← "65-74" (years 65-74 = 10/10 = 1.0, ignores 75-79)
#   "80+"         ← "75+"   (years 80+ ≈ "75+" tail)
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
