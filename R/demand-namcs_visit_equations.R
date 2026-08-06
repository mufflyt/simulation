# NAMCS-based URPS Visit-Rate Equations (HWMM Gap #1 Partial) ----
#
# HWMM HDMM section 3 fits negative binomial prediction equations from MEPS
# (Medical Expenditure Panel Survey) person-level data: outcome = annual
# visits to URPS provider; covariates = age, sex, race/ethnicity, insurance,
# income, BMI, smoking, chronic conditions.
#
# MEPS person-level data are not available in this repo.  This module
# approximates that approach using two available survey sources:
#
#   * NAMCS 2019 (visit-level): 8,250 sampled office visits -> national totals
#     via PATWT patient weights.  Identifies URPS visits by ICD-10-CM code.
#   * BRFSS 2023 (person-level): 229,541 women 18+ -> population denominators
#     by demographic stratum (age x race/eth x insurance).
#
# Rate estimation: visits_weighted[stratum] / population_weighted[stratum]
# gives annual per-capita URPS visits.  A log-linear model on strata
# approximates the MEPS NB at the population level without requiring
# individual annual counts.
#
# The module produces `compute_namcs_demand_estimand()`, which returns the D5
# estimand (NAMCS-calibrated visit-based demand) for inclusion in the
# run_workforce_microsimulation() pipeline alongside D1-D4.
#
# References:
#   NCHS (2021) 2019 NAMCS Public Use File Documentation (doc2019-508.pdf).
#   IHS Markit (2020) Health Workforce Microsimulation Model v5.19.20, section HDMM.
#   Dall et al. (2013) Health Affairs 32(11):1993-2000.

# ---- URPS ICD-10-CM code prefixes -------------------------------------------

#' ICD-10-CM prefixes defining URPS conditions in NAMCS
#'
#' Covers stress, urgency, and mixed urinary incontinence; pelvic organ
#' prolapse; unspecified incontinence; and vault prolapse after hysterectomy.
#' N39.0 (UTI) is intentionally excluded; overactive bladder (N32.81) included.
#' @keywords internal
URPS_ICD10_PREFIXES <- c(
  "N393",  # Stress urinary incontinence
  "N394",  # Other specified urinary incontinence (urgency, mixed, overflow)
  "N81",   # Female genital prolapse (N81.0-N81.9)
  "N3281", # Overactive bladder
  "R32",   # Unspecified urinary incontinence
  "N993",  # Prolapse of vaginal vault after hysterectomy
  "N994"   # Postprocedural pelvic peritoneal adhesions
)

# ---- Age band mapping -------------------------------------------------------

#' Map continuous AGE (NAMCS) to URPS age bands
#' @param age Integer vector of patient ages.
#' @return Character vector of band labels.
.namcs_age_to_band <- function(age) {
  dplyr::case_when(
    age < 18              ~ NA_character_,
    age <= 34             ~ "18-34",
    age <= 44             ~ "35-44",
    age <= 64             ~ "45-64",
    age <= 74             ~ "65-74",
    !is.na(age)           ~ "75+",
    TRUE                  ~ NA_character_
  )
}

# ---- Insurance harmonisation -------------------------------------------------
#
# NAMCS PAYTYPER -> simplified insurance tier for stratum matching with BRFSS.

.namcs_insurance_tier <- function(paytyper) {
  dplyr::case_when(
    paytyper == 1            ~ "Private",
    paytyper == 2            ~ "Medicare",
    paytyper == 3            ~ "Medicaid",
    paytyper == 5            ~ "Uninsured",
    paytyper %in% c(4,6,7)  ~ "Other",
    TRUE                     ~ NA_character_
  )
}

# BRFSS _HLTHPL1: 1=Yes insured, 2=No.  Collapse to Private/Medicaid/Medicare
# is not possible from _HLTHPL1 alone; use a 2-tier Insured/Uninsured match.
.brfss_insurance_tier <- function(hlthpl1) {
  dplyr::case_when(
    hlthpl1 == 1  ~ "Insured",
    hlthpl1 == 2  ~ "Uninsured",
    TRUE          ~ NA_character_
  )
}

# Collapse NAMCS 5-tier to the 2-tier BRFSS coding for stratum joining.
.namcs_insurance_2tier <- function(paytyper) {
  dplyr::case_when(
    paytyper %in% c(1, 2, 3, 4, 6, 7) ~ "Insured",
    paytyper == 5                       ~ "Uninsured",
    TRUE                                ~ NA_character_
  )
}

# ---- Race/ethnicity harmonisation -------------------------------------------
#
# NAMCS RACERETH (1=NH-White, 2=NH-Black, 3=Hispanic, 4=NH-Other)
# BRFSS _IMPRACE (1=White-NH, 2=Black-NH, 3=AIAN-NH, 4=Asian-NH, 5=Hispanic, 6=Other-NH)

.namcs_racereth_label <- function(racereth) {
  dplyr::case_when(
    racereth == 1 ~ "White_NH",
    racereth == 2 ~ "Black_NH",
    racereth == 3 ~ "Hispanic",
    racereth == 4 ~ "Other_NH",
    TRUE          ~ NA_character_
  )
}

.brfss_imprace_label <- function(imprace) {
  dplyr::case_when(
    imprace == 1 ~ "White_NH",
    imprace == 2 ~ "Black_NH",
    imprace %in% c(3, 4, 6) ~ "Other_NH",
    imprace == 5 ~ "Hispanic",
    TRUE         ~ NA_character_
  )
}

# ---- Data loading -----------------------------------------------------------

#' Load the cleaned NAMCS 2019 fixed-width file
#'
#' Reads the RDS produced by `data-raw/namcs/01-namcs_acquire.R`.
#'
#' @param path Path to the cleaned RDS file.
#' @return Tibble with all retained NAMCS columns (8,250 rows).
#' @export
load_namcs_2019 <- function(path = "data-raw/namcs/namcs2019_clean.rds") {
  if (!file.exists(path)) {
    stop(
      "NAMCS 2019 cleaned file not found at '", path, "'.\n",
      "Run data-raw/namcs/01-namcs_acquire.R to create it."
    )
  }
  readRDS(path)
}

# ---- Visit filtering --------------------------------------------------------

#' Flag visits whose ICD-10 diagnoses match URPS condition prefixes
#'
#' A visit is flagged when ANY of DIAG1, DIAG2, or DIAG3 starts with a prefix
#' in [URPS_ICD10_PREFIXES].
#'
#' @param namcs Tibble from [load_namcs_2019()].
#' @param prefixes Character vector of ICD-10-CM prefixes.
#' @return `namcs` with logical column `is_urps`.
#' @export
flag_urps_visits <- function(namcs,
                             prefixes = URPS_ICD10_PREFIXES) {
  assertthat::assert_that(all(c("DIAG1", "DIAG2", "DIAG3") %in% names(namcs)))

  match_any <- function(code, pref) {
    vapply(code, function(x) any(startsWith(x, pref)), logical(1))
  }
  namcs |>
    dplyr::mutate(
      is_urps = match_any(.data$DIAG1, prefixes) |
                match_any(.data$DIAG2, prefixes) |
                match_any(.data$DIAG3, prefixes)
    )
}

# ---- Weighted stratum estimates ---------------------------------------------

#' Compute survey-weighted URPS visit totals by demographic stratum
#'
#' Applies PATWT patient weights to produce nationally representative visit
#' counts per stratum.  Strata are age_band x sex x race_eth x insurance_2tier.
#'
#' @param namcs Tibble from [flag_urps_visits()].
#' @return Tibble with columns: `age_band`, `sex`, `race_eth`,
#'   `insurance_2tier`, `n_visits_unweighted`, `visits_weighted`
#'   (national annual total), `visits_per_1000_weighted`.
#' @export
namcs_urps_stratum_visits <- function(namcs) {
  assertthat::assert_that(all(c("is_urps", "AGE", "SEX", "RACERETH",
                                "PAYTYPER", "PATWT") %in% names(namcs)))

  namcs |>
    dplyr::filter(.data$is_urps, !is.na(.data$PATWT)) |>
    dplyr::mutate(
      age_band       = .namcs_age_to_band(.data$AGE),
      # NAMCS CODES SEX 1 = FEMALE, 2 = MALE -- the OPPOSITE of Census, ACS,
      # BRFSS and MEPS, all of which use 2 = Female and all of which this
      # package also reads. This was inverted here, so every NAMCS-derived
      # "female" quantity was actually built from male visits: for a
      # female-predominant subspecialty that silently replaced the estimand with
      # its complement. Verified against sex-specific diagnoses in the 2019 PUF:
      # N40 benign prostatic hyperplasia 0/136 and C61 prostate cancer 0/68 fall
      # entirely in SEX = 2, while Z34 pregnancy supervision 108/0, N81 genital
      # prolapse 18/0 and C50 breast cancer 43/0 fall entirely in SEX = 1.
      sex            = dplyr::if_else(.data$SEX == 1L, "Female", "Male"),
      race_eth       = .namcs_racereth_label(.data$RACERETH),
      insurance_2tier = .namcs_insurance_2tier(.data$PAYTYPER)
    ) |>
    dplyr::filter(!is.na(.data$age_band),
                  !is.na(.data$race_eth),
                  !is.na(.data$insurance_2tier)) |>
    dplyr::group_by(.data$age_band, .data$sex, .data$race_eth, .data$insurance_2tier) |>
    dplyr::summarise(
      n_visits_unweighted = dplyr::n(),
      visits_weighted     = sum(.data$PATWT, na.rm = TRUE),
      .groups = "drop"
    )
}

# ---- BRFSS population denominators -----------------------------------------

#' Compute BRFSS population counts by stratum (denominators for visit rates)
#'
#' Uses BRFSS 2023 survey weights (`_LLCPWT`) to estimate the US civilian
#' non-institutionalized adult female population by demographic stratum,
#' matching the NAMCS strata produced by [namcs_urps_stratum_visits()].
#'
#' @param brfss Tibble from `data-raw/brfss/brfss_2023_women18plus.rds`.
#'   Must contain `_AGEG5YR`, `_IMPRACE`, `_HLTHPL1`, `_LLCPWT`.
#' @return Tibble with `age_band`, `sex`, `race_eth`, `insurance_2tier`,
#'   `pop_weighted` (BRFSS-estimated population).
#' @export
brfss_population_by_stratum <- function(brfss) {
  needed <- c("_AGEG5YR", "_IMPRACE", "_HLTHPL1", "_LLCPWT")
  assertthat::assert_that(all(needed %in% names(brfss)),
                          msg = paste("BRFSS missing:", paste(setdiff(needed, names(brfss)), collapse = ", ")))

  # BRFSS is women 18+ only in this dataset.
  ageg <- brfss[["_AGEG5YR"]]
  age_band <- dplyr::case_when(
    ageg %in% 1:3  ~ "18-34",
    ageg %in% 4:5  ~ "35-44",
    ageg %in% 6:9  ~ "45-64",
    ageg %in% 10:11 ~ "65-74",
    ageg %in% 12:14 ~ "75+",
    TRUE            ~ NA_character_
  )

  tibble::tibble(
    age_band        = age_band,
    sex             = "Female",
    race_eth        = .brfss_imprace_label(brfss[["_IMPRACE"]]),
    insurance_2tier = .brfss_insurance_tier(brfss[["_HLTHPL1"]]),
    wt              = brfss[["_LLCPWT"]]
  ) |>
    dplyr::filter(!is.na(.data$age_band),
                  !is.na(.data$race_eth),
                  !is.na(.data$insurance_2tier),
                  !is.na(.data$wt)) |>
    dplyr::group_by(.data$age_band, .data$sex, .data$race_eth, .data$insurance_2tier) |>
    dplyr::summarise(pop_weighted = sum(.data$wt, na.rm = TRUE), .groups = "drop")
}

# ---- Visit rate estimation --------------------------------------------------

#' Join NAMCS visit totals with BRFSS denominators -> per-capita visit rates
#'
#' @param stratum_visits Output of [namcs_urps_stratum_visits()].
#' @param stratum_pop   Output of [brfss_population_by_stratum()].
#' @return Tibble with stratum covariates plus:
#'   `visits_per_1000` -- NAMCS-estimated annual URPS visits per 1,000 persons.
#' @export
compute_urps_visit_rates <- function(stratum_visits, stratum_pop) {
  join_cols <- c("age_band", "sex", "race_eth", "insurance_2tier")
  assertthat::assert_that(all(join_cols %in% names(stratum_visits)))
  assertthat::assert_that(all(join_cols %in% names(stratum_pop)))

  dplyr::inner_join(stratum_visits, stratum_pop, by = join_cols) |>
    dplyr::mutate(
      visits_per_1000 = 1000 * .data$visits_weighted / .data$pop_weighted
    )
}

# ---- Log-linear visit rate model (approximates HWMM NB regression) ----------

#' Fit a log-linear model of URPS visit rates on demographic strata
#'
#' Regresses log(visits_per_1000) on age_band, race_eth, and insurance_2tier
#' using OLS weighted by n_visits_unweighted.  This is a stratum-level
#' approximation of the HWMM MEPS-based negative binomial; individual-level NB
#' requires person-level annual count data (MEPS), which are not available here.
#'
#' @param rate_table Output of [compute_urps_visit_rates()].
#' @return A `lm` object; coefficients are log visit-rate adjustments.
#' @export
fit_urps_visit_rate_model <- function(rate_table) {
  assertthat::assert_that(all(c("visits_per_1000", "age_band", "race_eth",
                                "insurance_2tier", "n_visits_unweighted") %in%
                               names(rate_table)))

  ok <- rate_table$visits_per_1000 > 0 & rate_table$n_visits_unweighted >= 1
  if (sum(ok) < 4) {
    stop("fit_urps_visit_rate_model: too few strata with positive rates (need >= 4, got ",
         sum(ok), ")", call. = FALSE)
  }

  d <- rate_table[ok, ]

  # Drop predictors with only one unique value to avoid contrast errors.
  candidate_preds <- c("age_band", "race_eth", "insurance_2tier")
  active_preds <- candidate_preds[vapply(candidate_preds,
    function(v) length(unique(d[[v]])) > 1L, logical(1))]

  if (length(active_preds) == 0) {
    stop("fit_urps_visit_rate_model: no predictor has >1 level in the rate table",
         call. = FALSE)
  }

  fml <- stats::as.formula(
    paste("log(visits_per_1000) ~", paste(active_preds, collapse = " + "))
  )
  stats::lm(fml, data = d, weights = d$n_visits_unweighted)
}

# ---- Prediction -------------------------------------------------------------

#' Predict annual URPS visits per 1,000 persons for a covariate profile
#'
#' @param model     Object from [fit_urps_visit_rate_model()].
#' @param newdata   Data frame with `age_band`, `race_eth`, `insurance_2tier`.
#' @param ci        If TRUE, return 95% prediction interval columns.
#' @return `newdata` with `predicted_visits_per_1000` and, when `ci = TRUE`,
#'   `predicted_lo` and `predicted_hi`.
#' @keywords internal
predict_urps_annual_visits <- function(model, newdata, ci = FALSE) {
  assertthat::assert_that(
    all(c("age_band", "race_eth", "insurance_2tier") %in% names(newdata))
  )

  # Replace levels unseen during training with NA so predict() doesn't error;
  # those rows will receive NA predictions and are excluded from demand sums.
  for (v in c("age_band", "race_eth", "insurance_2tier")) {
    known <- model$xlevels[[v]]
    if (!is.null(known)) {
      newdata[[v]] <- ifelse(newdata[[v]] %in% known, newdata[[v]], NA_character_)
    }
  }

  interval <- if (ci) "prediction" else "none"
  p <- stats::predict(model, newdata = newdata, interval = interval, na.action = na.pass)
  if (ci) {
    newdata$predicted_visits_per_1000 <- exp(p[, "fit"])
    newdata$predicted_lo              <- exp(p[, "lwr"])
    newdata$predicted_hi              <- exp(p[, "upr"])
  } else {
    newdata$predicted_visits_per_1000 <- exp(as.numeric(p))
  }
  newdata
}

# ---- D5 demand estimand producer --------------------------------------------

#' Compute the D5 NAMCS-calibrated demand estimand
#'
#' Applies the fitted visit-rate model to a projected population table to
#' produce annual URPS visit demand by year.  Dividing by visits-per-FTE
#' (or by a visits-to-provider conversion) yields required clinical FTE.
#'
#' The D5 estimand is a bottom-up visit demand approach anchored to the
#' nationally representative NAMCS 2019 visit survey; it complements the
#' top-down prevalence-based D1-D4 estimands in the pipeline.
#'
#' @param pop_projection Tibble with `year`, `age_band`, `race_eth`,
#'   `insurance_2tier`, `population` (projected women 18+).
#' @param model Object from [fit_urps_visit_rate_model()].
#' @param visits_per_fte Annual URPS visits one FTE can handle; defaults to
#'   2800 (4 visits/day x 700 provider-days/FTE).
#' @param fte_fraction  Fraction of visits attributable to FPMRS specialists
#'   (vs primary care or general OB/GYN).  Defaults to 0.35, consistent with
#'   the share of URPS visits in NAMCS with SPECCAT 2/3.
#' @return Tibble with `year`, `estimand = "D5"`, `demand_visits`,
#'   `demand_clinical_fte`.
#' @export
compute_namcs_demand_estimand <- function(pop_projection,
                                          model,
                                          visits_per_fte = 2800,
                                          fte_fraction   = 0.35) {
  assertthat::assert_that(
    is.data.frame(pop_projection),
    all(c("year", "age_band", "race_eth", "insurance_2tier", "population") %in%
          names(pop_projection))
  )
  assertthat::assert_that(inherits(model, "lm"))
  assertthat::assert_that(visits_per_fte > 0, fte_fraction > 0, fte_fraction <= 1)

  pred <- predict_urps_annual_visits(model, pop_projection)

  pred |>
    dplyr::mutate(
      visits_from_stratum = .data$predicted_visits_per_1000 / 1000 * .data$population
    ) |>
    dplyr::group_by(.data$year) |>
    dplyr::summarise(
      demand_visits       = sum(.data$visits_from_stratum, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      estimand            = "D5",
      demand_clinical_fte = .data$demand_visits * fte_fraction / visits_per_fte
    ) |>
    dplyr::select("year", "estimand", "demand_visits", "demand_clinical_fte")
}

# ---- Convenience wrapper: build D5 from raw sources -------------------------

#' Build the D5 demand estimand from raw NAMCS and BRFSS sources
#'
#' One-call wrapper that loads NAMCS, flags URPS visits, computes visit rates,
#' fits the log-linear model, and applies it to a population projection.
#'
#' @param pop_projection Tibble as described in [compute_namcs_demand_estimand()].
#' @param namcs_path Path to cleaned NAMCS 2019 RDS file.
#' @param brfss_path Path to BRFSS 2023 women 18+ RDS file.
#' @inheritParams compute_namcs_demand_estimand
#' @return List with `model`, `rate_table`, `estimand` (tibble).
#' @keywords internal
build_d5_namcs_estimand <- function(pop_projection,
                                    namcs_path    = "data-raw/namcs/namcs2019_clean.rds",
                                    brfss_path    = "data-raw/brfss/brfss_2023_women18plus.rds",
                                    visits_per_fte = 2800,
                                    fte_fraction   = 0.35) {
  namcs <- load_namcs_2019(namcs_path)
  namcs <- flag_urps_visits(namcs)

  stratum_visits <- namcs_urps_stratum_visits(namcs)
  brfss          <- readRDS(brfss_path)
  stratum_pop    <- brfss_population_by_stratum(brfss)
  rate_table     <- compute_urps_visit_rates(stratum_visits, stratum_pop)

  model    <- fit_urps_visit_rate_model(rate_table)
  estimand <- compute_namcs_demand_estimand(pop_projection, model,
                                            visits_per_fte = visits_per_fte,
                                            fte_fraction   = fte_fraction)

  list(model = model, rate_table = rate_table, estimand = estimand)
}
