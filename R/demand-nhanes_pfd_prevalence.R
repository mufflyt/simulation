# NHANES-Based Pelvic Floor Disorder Prevalence (Clinical Anchor) ----
#
# HWMM demand pipeline §HDMM uses clinical prevalence estimates as a primary
# demand anchor.  NHANES is the gold standard US prevalence source for urinary
# incontinence because it uses validated self-report questions (KIQ042/KIQ044)
# in a probability sample, allowing survey-weighted national estimates.
#
# This module (D6 estimand) computes:
#   1. UI prevalence by age × race/ethnicity × hysterectomy status from NHANES
#   2. Annual care-seeking proportion (from MCBS/BRFSS literature: ~25% of
#      women with UI seek specialist care)
#   3. Applies prevalence × population × care-seeking rate → visit demand → FTE
#
# UI types:
#   Stress UI (KIQ042): leaks with physical activity, coughing, sneezing
#   Urgency UI (KIQ044): sudden urge, couldn't get to toilet in time
#   Mixed/Any UI (ui_any): either of the above
#
# References:
#   Nygaard et al. (2008) JAMA 300(11):1311-1316 (NHANES UI prevalence).
#   Wu et al. (2014) Am J Obstet Gynecol (pelvic floor disorders in US women).
#   NCHS (2024) NHANES 2021-2023 Public Use Data.
#
# NOTE: this file uses dplyr:: and survey:: fully namespaced throughout; it must
# NOT call library() at file scope. Those calls run at package load time (a
# global-state side effect the source-safety gate forbids) and hard-fail the
# installed package when survey -- a Suggests, not an Import -- is absent, which
# is what broke R CMD check ("Error in library(survey): there is no package
# called 'survey'"). survey's functions are reached via survey:: with a
# requireNamespace() check at the call sites that need it.

# ---- Load and prepare NHANES data -------------------------------------------

#' Load the pooled NHANES PFD file
#'
#' Reads RDS produced by `data-raw/nhanes/01-nhanes_acquire.R`.
#'
#' @param path Path to `nhanes_pfd_pooled.rds`.
#' @return Tibble with KIQ_U + RHQ + DEMO variables for women 20+.
#' @export
load_nhanes_pfd <- function(path = "data-raw/nhanes/nhanes_pfd_pooled.rds") {
  if (!file.exists(path)) {
    stop(
      "NHANES pooled PFD file not found at '", path, "'.\n",
      "Run data-raw/nhanes/01-nhanes_acquire.R to download and create it.",
      call. = FALSE
    )
  }
  readRDS(path)
}

# ---- Age band and race harmonisation ----------------------------------------

.nhanes_age_to_band <- function(age) {
  dplyr::case_when(
    age < 20              ~ NA_character_,
    age <= 34             ~ "20-34",
    age <= 44             ~ "35-44",
    age <= 64             ~ "45-64",
    age <= 74             ~ "65-74",
    !is.na(age)           ~ "75+",
    TRUE                  ~ NA_character_
  )
}

.nhanes_race_label <- function(ridreth3) {
  dplyr::case_when(
    ridreth3 == 3L ~ "White_NH",
    ridreth3 == 4L ~ "Black_NH",
    ridreth3 %in% c(1L, 2L) ~ "Hispanic",
    ridreth3 %in% c(6L, 7L) ~ "Other_NH",
    TRUE           ~ NA_character_
  )
}

# ---- Survey-weighted prevalence by stratum ----------------------------------

#' Compute survey-weighted UI prevalence by demographic stratum
#'
#' Uses NHANES MEC weights and the complex survey design to produce nationally
#' representative UI prevalence estimates for women 20+.
#'
#' @param nhanes Tibble from [load_nhanes_pfd()].  Must contain `SDMVPSU`,
#'   `SDMVSTRA`, `WTMEC_pooled`, `ui_any`, `ui_stress`, `ui_urgency`,
#'   `RIDAGEYR`, `RIDRETH3`, `hysterectomy`.
#' @param ui_type One of `"any"`, `"stress"`, or `"urgency"`.
#' @return Tibble with `age_band`, `race_eth`, `hysterectomy`, `n_unweighted`,
#'   `prevalence` (weighted proportion with UI), `se` (standard error).
#' @export
nhanes_ui_prevalence_by_stratum <- function(nhanes, ui_type = "any") {
  if (!requireNamespace("survey", quietly = TRUE))
    stop("nhanes_ui_prevalence_by_stratum() needs the 'survey' package ",
         "(a Suggests) for the complex-sample design; install it to run this.",
         call. = FALSE)
  outcome_col <- switch(ui_type,
    "any"     = "ui_any",
    "stress"  = "ui_stress",
    "urgency" = "ui_urgency",
    stop("ui_type must be 'any', 'stress', or 'urgency'", call. = FALSE)
  )

  needed <- c("SDMVPSU", "SDMVSTRA", "WTMEC_pooled", outcome_col,
              "RIDAGEYR", "RIDRETH3")
  assertthat::assert_that(all(needed %in% names(nhanes)),
    msg = paste("NHANES missing:", paste(setdiff(needed, names(nhanes)), collapse = ", ")))

  df <- nhanes |>
    dplyr::mutate(
      age_band     = .nhanes_age_to_band(.data$RIDAGEYR),
      race_eth     = .nhanes_race_label(.data$RIDRETH3),
      hysterectomy = dplyr::if_else(
        dplyr::if_else(!is.na(.data$hysterectomy), .data$hysterectomy, FALSE),
        "Yes", "No"
      ),
      .outcome     = as.integer(.data[[outcome_col]])
    ) |>
    dplyr::filter(!is.na(.data$age_band), !is.na(.data$race_eth),
                  !is.na(.data$.outcome))

  # Survey design (NHANES complex sample)
  des <- survey::svydesign(
    ids     = ~SDMVPSU,
    strata  = ~SDMVSTRA,
    weights = ~WTMEC_pooled,
    data    = df,
    nest    = TRUE
  )

  # Prevalence by stratum using svyby
  prev <- survey::svyby(
    ~.outcome,
    by      = ~age_band + race_eth + hysterectomy,
    design  = des,
    FUN     = survey::svymean,
    na.rm   = TRUE,
    keep.names = FALSE
  )

  # Count unweighted observations per stratum
  n_tab <- df |>
    dplyr::count(.data$age_band, .data$race_eth, .data$hysterectomy,
                 name = "n_unweighted")

  prev |>
    dplyr::rename(prevalence = .outcome, se = `se(.outcome)`) |>
    dplyr::left_join(n_tab, by = c("age_band", "race_eth", "hysterectomy")) |>
    dplyr::mutate(ui_type = ui_type) |>
    dplyr::select("age_band", "race_eth", "hysterectomy", "ui_type",
                  "n_unweighted", "prevalence", "se") |>
    tibble::as_tibble()
}

# ---- D6 demand estimand -----------------------------------------------------

#' Compute the D6 NHANES-calibrated prevalence-based demand estimand
#'
#' Combines NHANES UI prevalence with population projections and a care-seeking
#' rate to produce annual URPS visit demand and required FTE.
#'
#' Formula:
#'   demand_visits[stratum, year] =
#'     prevalence[stratum] × population[stratum, year] × care_seeking_rate
#'
#'   demand_fte[year] = sum(demand_visits[year]) × fte_fraction / visits_per_fte
#'
#' @param pop_projection Tibble with `year`, `age_band`, `race_eth`,
#'   `population` (projected women 20+).  Columns `hysterectomy` is optional;
#'   if absent, prevalence is marginalized over hysterectomy status.
#' @param prevalence_table Output of [nhanes_ui_prevalence_by_stratum()].
#' @param care_seeking_rate Fraction of women with UI who seek specialist URPS
#'   care annually.  Defaults to 0.25 (per Sung et al. 2010, OB-GYN).
#' @param visits_per_fte Annual URPS visits one FTE provider handles.
#'   Defaults to 2800.
#' @param fte_fraction   Fraction of specialist visits attributable to FPMRS.
#'   Defaults to 0.35.
#' @return Tibble with `year`, `estimand = "D6"`, `demand_visits`,
#'   `demand_clinical_fte`.
#' @export
compute_nhanes_demand_estimand <- function(pop_projection,
                                           prevalence_table,
                                           care_seeking_rate = 0.25,
                                           visits_per_fte    = 2800,
                                           fte_fraction      = 0.35) {
  assertthat::assert_that(
    is.data.frame(pop_projection),
    all(c("year", "age_band", "race_eth", "population") %in% names(pop_projection))
  )
  assertthat::assert_that(
    is.data.frame(prevalence_table),
    all(c("age_band", "race_eth", "prevalence") %in% names(prevalence_table))
  )
  assertthat::assert_that(care_seeking_rate > 0, care_seeking_rate <= 1)

  # Marginalise over hysterectomy if not in projection
  if (!"hysterectomy" %in% names(pop_projection) &&
      "hysterectomy" %in% names(prevalence_table)) {
    prevalence_table <- prevalence_table |>
      dplyr::group_by(.data$age_band, .data$race_eth) |>
      dplyr::summarise(
        prevalence   = mean(.data$prevalence, na.rm = TRUE),
        n_unweighted = sum(.data$n_unweighted, na.rm = TRUE),
        .groups = "drop"
      )
  }

  join_cols <- intersect(c("age_band", "race_eth", "hysterectomy"),
                         names(prevalence_table))

  pop_projection |>
    dplyr::left_join(
      prevalence_table |>
        dplyr::select(dplyr::all_of(c(join_cols, "prevalence"))),
      by = join_cols
    ) |>
    dplyr::mutate(
      demand_from_stratum = .data$prevalence * .data$population * care_seeking_rate
    ) |>
    dplyr::group_by(.data$year) |>
    dplyr::summarise(
      demand_visits = sum(.data$demand_from_stratum, na.rm = TRUE),
      .groups       = "drop"
    ) |>
    dplyr::mutate(
      estimand            = "D6",
      demand_clinical_fte = .data$demand_visits * fte_fraction / visits_per_fte
    ) |>
    dplyr::select("year", "estimand", "demand_visits", "demand_clinical_fte")
}

# ---- Convenience wrapper ----------------------------------------------------

#' Build the D6 demand estimand from raw NHANES source
#'
#' One-call wrapper: loads NHANES, computes prevalence, applies to population
#' projection.
#'
#' @param pop_projection As in [compute_nhanes_demand_estimand()].
#' @param nhanes_path    Path to `nhanes_pfd_pooled.rds`.
#' @param ui_type        One of `"any"`, `"stress"`, `"urgency"`.
#' @inheritParams compute_nhanes_demand_estimand
#' @return List with `prevalence_table` and `estimand` (tibble).
#' @export
build_d6_nhanes_estimand <- function(pop_projection,
                                     nhanes_path       = "data-raw/nhanes/nhanes_pfd_pooled.rds",
                                     ui_type           = "any",
                                     care_seeking_rate = 0.25,
                                     visits_per_fte    = 2800,
                                     fte_fraction      = 0.35) {
  nhanes           <- load_nhanes_pfd(nhanes_path)
  prevalence_table <- nhanes_ui_prevalence_by_stratum(nhanes, ui_type = ui_type)

  estimand <- compute_nhanes_demand_estimand(
    pop_projection    = pop_projection,
    prevalence_table  = prevalence_table,
    care_seeking_rate = care_seeking_rate,
    visits_per_fte    = visits_per_fte,
    fte_fraction      = fte_fraction
  )

  list(prevalence_table = prevalence_table, estimand = estimand)
}
