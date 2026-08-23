# Informative priors for URPS calibration -------------------------------
#
# Scientific Note:
# While HRSA HWSM documents literature review, empirical calibration, and validation,
# this 10-parameter informative-prior framework is our Bayesian calibration extension
# built on top of the GP emulator / history matching engine.

URPS_PRIOR_SPEC_VERSION <- "1.0.0"


#' Convert a bounded prior mean into Beta shape parameters
#'
#' @param mean Prior mean on the original scale.
#' @param lower Lower support bound.
#' @param upper Upper support bound.
#' @param concentration Beta concentration alpha + beta.
#'
#' @return Named numeric vector with shape1 and shape2.
#' @keywords internal
.beta_shapes_from_mean <- function(
    mean,
    lower,
    upper,
    concentration) {

  if (!base::is.finite(mean) ||
      !base::is.finite(lower) ||
      !base::is.finite(upper) ||
      !base::is.finite(concentration)) {
    base::stop(
      "Beta prior inputs must be finite.",
      call. = FALSE
    )
  }

  if (upper <= lower) {
    base::stop(
      "Beta prior requires upper > lower.",
      call. = FALSE
    )
  }

  if (mean <= lower ||
      mean >= upper) {
    base::stop(
      "Beta prior mean must lie strictly inside its bounds.",
      call. = FALSE
    )
  }

  if (concentration <= 0) {
    base::stop(
      "Beta concentration must be positive.",
      call. = FALSE
    )
  }

  unit_mean <- (
    mean - lower
  ) / (
    upper - lower
  )

  c(
    shape1 =
      unit_mean * concentration,
    shape2 =
      (1 - unit_mean) * concentration
  )
}


#' Construct one bounded Beta prior specification row
#'
#' @keywords internal
.make_beta_prior <- function(
    parameter,
    lower,
    upper,
    mean,
    concentration,
    source,
    evidence_tier,
    rationale,
    identifiability = "nuisance_informative") {

  shapes <- .beta_shapes_from_mean(
    mean = mean,
    lower = lower,
    upper = upper,
    concentration = concentration
  )

  tibble::tibble(
    parameter = parameter,
    lower = lower,
    upper = upper,
    prior_type = "beta",
    prior_mean = mean,
    prior_sd = NA_real_,
    shape1 = shapes[["shape1"]],
    shape2 = shapes[["shape2"]],
    identifiability = identifiability,
    source = source,
    evidence_tier = evidence_tier,
    rationale = rationale
  )
}


#' Construct one LogNormal prior specification row
#'
#' @description
#' The existing Bayesian history-matching engine interprets
#' `prior_mean` as `meanlog` and `prior_sd` as `sdlog` for
#' lognormal priors. This helper follows that contract.
#'
#' @keywords internal
.make_lognormal_prior <- function(
    parameter,
    lower,
    upper,
    median,
    sdlog,
    source,
    evidence_tier,
    rationale,
    identifiability = "nuisance_informative") {

  if (median <= 0 ||
      sdlog <= 0) {
    base::stop(
      "LogNormal median and sdlog must be positive.",
      call. = FALSE
    )
  }

  tibble::tibble(
    parameter = parameter,
    lower = lower,
    upper = upper,
    prior_type = "lognormal",
    prior_mean = base::log(median),
    prior_sd = sdlog,
    shape1 = NA_real_,
    shape2 = NA_real_,
    identifiability = identifiability,
    source = source,
    evidence_tier = evidence_tier,
    rationale = rationale
  )
}


#' Construct one Normal prior specification row
#'
#' @keywords internal
.make_normal_prior <- function(
    parameter,
    lower,
    upper,
    mean,
    sd,
    source,
    evidence_tier,
    rationale) {

  if (sd <= 0) {
    base::stop(
      "Normal prior SD must be positive.",
      call. = FALSE
    )
  }

  tibble::tibble(
    parameter = parameter,
    lower = lower,
    upper = upper,
    prior_type = "normal",
    prior_mean = mean,
    prior_sd = sd,
    shape1 = NA_real_,
    shape2 = NA_real_,
    source = source,
    evidence_tier = evidence_tier,
    rationale = rationale
  )
}


#' Build informative prior specification for URPS calibration
#'
#' @description
#' Defines informative but deliberately non-dogmatic priors for
#' parameters that are weakly identified by aggregate workforce
#' calibration targets.
#'
#' Priors are classified by evidence strength. Parameters based on
#' internal scenario assumptions remain explicitly labeled as such and
#' are not presented as published HRSA coefficients.
#'
#' The returned column names are directly compatible with
#' [calibrate_bayesian_history_matching()].
#'
#' @return Tibble with one row per calibration parameter.
#' @family Bayesian calibration
#' @concept calibration
#' @export
build_urps_prior_specification <- function() {

  base::message(
    "[urps-priors] Building informative prior specification."
  )

  prior_rows <- base::list(

    # 1. Delegation capacity.
    .make_beta_prior(
      parameter =
        "app_delegation_capacity_factor",
      lower = 0.20,
      upper = 0.80,
      mean = 0.434,
      concentration = 18,
      source =
        "URPS delegation matrix / Forte analogy",
      evidence_tier =
        "derived_by_analogy",
      rationale =
        base::paste(
          "Centers the prior on the current delegation-capacity",
          "factor while allowing substantial uncertainty."
        )
    ),

    # 2. Medicaid demand realization.
    .make_lognormal_prior(
      parameter =
        "medicaid_demand_multiplier",
      lower = 0.30,
      upper = 1.20,
      median = 0.75,
      sdlog = 0.20,
      source =
        "URPS insurance sensitivity assumption",
      evidence_tier =
        "scenario_assumption",
      rationale =
        base::paste(
          "Represents lower realized specialty utilization",
          "under Medicaid access constraints."
        )
    ),

    # 3. Uninsured demand realization.
    .make_lognormal_prior(
      parameter =
        "uninsured_demand_multiplier",
      lower = 0.10,
      upper = 1.00,
      median = 0.45,
      sdlog = 0.25,
      source =
        "URPS insurance sensitivity assumption",
      evidence_tier =
        "scenario_assumption",
      rationale =
        base::paste(
          "Allows a broad reduction in realized specialty",
          "utilization among uninsured patients."
        )
    ),

    # 4. Commercial demand realization.
    .make_lognormal_prior(
      parameter =
        "commercial_demand_multiplier",
      lower = 0.75,
      upper = 1.60,
      median = 1.15,
      sdlog = 0.12,
      source =
        "URPS insurance sensitivity assumption",
      evidence_tier =
        "scenario_assumption",
      rationale =
        base::paste(
          "Represents relatively high realization of",
          "specialty care among commercially insured patients."
        )
    ),

    # 5. Medicare demand realization.
    .make_lognormal_prior(
      parameter =
        "medicare_demand_multiplier",
      lower = 0.80,
      upper = 1.80,
      median = 1.35,
      sdlog = 0.15,
      source =
        "URPS insurance sensitivity assumption",
      evidence_tier =
        "scenario_assumption",
      rationale =
        base::paste(
          "Represents high observed utilization in the",
          "older Medicare population."
        )
    ),

    # 6. Recurrence / retreatment.
    .make_lognormal_prior(
      parameter =
        "retreatment_hazard_multiplier",
      lower = 0.40,
      upper = 2.50,
      median = 1.00,
      sdlog = 0.25,
      source =
        "URPS recurrence evidence synthesis",
      evidence_tier =
        "literature_synthesis",
      rationale =
        base::paste(
          "Centers on the current recurrence kernel while",
          "allowing substantial uncertainty in retreatment."
        )
    ),

    # 7. ASC setting share.
    .make_beta_prior(
      parameter =
        "asc_procedure_share",
      lower = 0,
      upper = 1,
      mean = 0.5895,
      concentration = 40,
      source =
        "2024 CMS PSPS setting calibration",
      evidence_tier =
        "empirical_calibration",
      rationale =
        base::paste(
          "Centers between the calibrated sling ASC share",
          "(0.638) and prolapse ASC share (0.541)."
        )
    ),

    # 8. Younger-career occupational separation.
    .make_beta_prior(
      parameter =
        "career_change_hazard_under50",
      lower = 0,
      upper = 0.05,
      mean = 0.0142,
      concentration = 30,
      source =
        "CPS ASEC occupational separation evidence",
      evidence_tier =
        "literature_anchored",
      rationale =
        base::paste(
          "Centers on the existing 1.42% annual",
          "under-50 career-change hazard."
        )
    ),

    # 9. Fellowship-to-practice conversion.
    .make_beta_prior(
      parameter =
        "entrant_practice_conversion",
      lower = 0.70,
      upper = 1.00,
      mean = 0.90,
      concentration = 14,
      source =
        "workforce-entry sensitivity range",
      evidence_tier =
        "weakly_informed",
      rationale =
        base::paste(
          "Allows some fellowship graduates not to enter",
          "active US clinical practice immediately."
        )
    ),

    # 10. Overall clinical-hours scaling.
    .make_normal_prior(
      parameter =
        "clinical_hours_multiplier",
      lower = 0.80,
      upper = 1.20,
      mean = 1.00,
      sd = 0.05,
      source =
        "HRSA/Dall hours-worked model sensitivity",
      evidence_tier =
        "model_discrepancy",
      rationale =
        base::paste(
          "Represents residual specialty-specific deviation",
          "from the adopted demographic hours curve."
        )
    )
  )

  prior_tbl <- dplyr::bind_rows(
    prior_rows
  ) |>
    dplyr::mutate(
      prior_version =
        URPS_PRIOR_SPEC_VERSION,
      parameter_order =
        base::seq_len(
          dplyr::n()
        )
    ) |>
    dplyr::select(
      .data$parameter_order,
      .data$parameter,
      .data$lower,
      .data$upper,
      .data$prior_type,
      .data$prior_mean,
      .data$prior_sd,
      .data$shape1,
      .data$shape2,
      .data$identifiability,
      .data$source,
      .data$evidence_tier,
      .data$rationale,
      .data$prior_version
    )

  base::message(
    "[urps-priors] Priors specified: ",
    base::nrow(prior_tbl),
    "."
  )

  prior_tbl
}


#' Validate URPS informative priors
#'
#' @param prior_tbl Prior specification.
#'
#' @return Prior table invisibly.
#' @family Bayesian calibration
#' @concept calibration
#' @export
validate_urps_prior_specification <- function(
    prior_tbl) {

  required_cols <- c(
    "parameter",
    "lower",
    "upper",
    "prior_type",
    "prior_mean",
    "prior_sd",
    "shape1",
    "shape2"
  )

  missing_cols <- base::setdiff(
    required_cols,
    base::names(prior_tbl)
  )

  if (base::length(missing_cols) > 0L) {
    base::stop(
      "Prior specification is missing: ",
      base::paste(
        missing_cols,
        collapse = ", "
      ),
      call. = FALSE
    )
  }

  if (base::anyDuplicated(
    prior_tbl$parameter
  )) {
    base::stop(
      "Each parameter must have exactly one prior.",
      call. = FALSE
    )
  }

  if (base::any(
    !base::is.finite(prior_tbl$lower) |
      !base::is.finite(prior_tbl$upper)
  )) {
    base::stop(
      "Prior bounds must be finite.",
      call. = FALSE
    )
  }

  if (base::any(
    prior_tbl$upper <=
      prior_tbl$lower
  )) {
    base::stop(
      "Every prior must have upper > lower.",
      call. = FALSE
    )
  }

  allowed_types <- c(
    "beta",
    "lognormal",
    "normal",
    "gamma",
    "uniform"
  )

  unknown_type <- base::setdiff(
    base::unique(
      prior_tbl$prior_type
    ),
    allowed_types
  )

  if (base::length(unknown_type) > 0L) {
    base::stop(
      "Unsupported prior type(s): ",
      base::paste(
        unknown_type,
        collapse = ", "
      ),
      call. = FALSE
    )
  }

  beta_index <- prior_tbl$prior_type ==
    "beta"

  if (base::any(
    prior_tbl$shape1[beta_index] <= 0 |
      prior_tbl$shape2[beta_index] <= 0
  )) {
    base::stop(
      "Beta shape parameters must be positive.",
      call. = FALSE
    )
  }

  lognormal_index <-
    prior_tbl$prior_type == "lognormal"

  if (base::any(
    prior_tbl$prior_sd[
      lognormal_index
    ] <= 0
  )) {
    base::stop(
      "LogNormal sdlog must be positive.",
      call. = FALSE
    )
  }

  normal_index <-
    prior_tbl$prior_type == "normal"

  if (base::any(
    prior_tbl$prior_sd[
      normal_index
    ] <= 0
  )) {
    base::stop(
      "Normal prior SD must be positive.",
      call. = FALSE
    )
  }

  base::message(
    "[urps-priors] Prior specification validated."
  )

  base::invisible(
    prior_tbl
  )
}


#' Draw parameter vectors from URPS informative priors
#'
#' @param prior_tbl Prior specification.
#' @param n_draws Number of draws.
#' @param seed RNG seed.
#'
#' @return Wide tibble with one parameter vector per row.
#' @family Bayesian calibration
#' @concept calibration
#' @export
draw_urps_prior_parameters <- function(
    prior_tbl =
      build_urps_prior_specification(),
    n_draws = 1000L,
    seed = 20260820L) {

  validate_urps_prior_specification(
    prior_tbl
  )

  if (!base::is.numeric(n_draws) ||
      base::length(n_draws) != 1L ||
      n_draws < 1) {
    base::stop(
      "`n_draws` must be a positive integer.",
      call. = FALSE
    )
  }

  n_draws <- base::as.integer(
    n_draws
  )

  base::set.seed(seed)

  draw_list <- base::lapply(
    base::seq_len(
      base::nrow(prior_tbl)
    ),
    function(row_index) {

      prior_row <- prior_tbl[
        row_index,
        ,
        drop = FALSE
      ]

      prior_type <-
        prior_row$prior_type[[1]]

      lower <- prior_row$lower[[1]]
      upper <- prior_row$upper[[1]]

      if (prior_type == "beta") {

        unit_draw <- stats::rbeta(
          n = n_draws,
          shape1 =
            prior_row$shape1[[1]],
          shape2 =
            prior_row$shape2[[1]]
        )

        parameter_draw <- lower +
          unit_draw *
          (upper - lower)

      } else if (
        prior_type == "lognormal"
      ) {

        parameter_draw <- stats::rlnorm(
          n = n_draws,
          meanlog =
            prior_row$prior_mean[[1]],
          sdlog =
            prior_row$prior_sd[[1]]
        )

      } else if (
        prior_type == "normal"
      ) {

        parameter_draw <- stats::rnorm(
          n = n_draws,
          mean =
            prior_row$prior_mean[[1]],
          sd =
            prior_row$prior_sd[[1]]
        )

      } else if (
        prior_type == "uniform"
      ) {

        parameter_draw <- stats::runif(
          n = n_draws,
          min = lower,
          max = upper
        )

      } else {
        base::stop(
          "Prior drawing not implemented for ",
          prior_type,
          ".",
          call. = FALSE
        )
      }

      # The calibration search space is bounded.
      parameter_draw <- base::pmin(
        upper,
        base::pmax(
          lower,
          parameter_draw
        )
      )

      parameter_draw
    }
  )

  base::names(draw_list) <-
    prior_tbl$parameter

  prior_draw_tbl <- tibble::as_tibble(
    draw_list
  )

  prior_draw_tbl$draw_id <-
    base::seq_len(
      base::nrow(prior_draw_tbl)
    )

  prior_draw_tbl |>
    dplyr::relocate(
      .data$draw_id
    )
}
