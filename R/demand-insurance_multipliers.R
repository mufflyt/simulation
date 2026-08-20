# Insurance Coverage Micro-Demographic Demand Adjustment ----
#
# HRSA HWSM incorporates insurance coverage into individual-level
# utilization prediction equations. This module provides a transparent
# multiplicative approximation for the URPS microsimulation when the
# baseline demand quantity has NOT already been conditioned on insurance.
#
# IMPORTANT:
# Do not apply these multipliers to predictions from the existing
# MEPS care-seeking model when insurance is already included as a
# predictor. Doing so would count the insurance effect twice.

URPS_INSURANCE_DEMAND_MULTIPLIERS <- c(
  commercial = 1.15,
  medicare = 1.35,
  medicaid = 0.75,
  uninsured = 0.45
)

URPS_INSURANCE_MULTIPLIER_STATUS <-
  "scenario_assumption_not_hrsa_published_coefficient"


#' Insurance demand multiplier specification
#'
#' @description
#' Returns the insurance-demand assumptions used by the URPS
#' microsimulation.
#'
#' These values are scenario parameters rather than published HRSA
#' regression coefficients. HRSA itself models insurance through
#' utilization prediction equations estimated primarily from MEPS.
#'
#' @return Tibble containing coverage category, multiplier, status,
#'   and interpretation.
#' @family demand
#' @concept insurance
#' @export
insurance_demand_multiplier_spec <- function() {

  tibble::tribble(
    ~insurance_type, ~multiplier, ~interpretation,
    "commercial", 1.15,
    "Higher realized utilization relative to baseline",
    "medicare", 1.35,
    "Higher utilization in older Medicare population",
    "medicaid", 0.75,
    "Lower realized specialty utilization/access",
    "uninsured", 0.45,
    "Substantially reduced realized utilization"
  ) |>
    dplyr::mutate(
      calibration_status =
        URPS_INSURANCE_MULTIPLIER_STATUS
    )
}


#' Normalize insurance coverage categories
#'
#' @description
#' Maps common insurance labels onto the four categories used by the
#' URPS insurance-demand module.
#'
#' Medicare Advantage and traditional Medicare FFS are combined into
#' `medicare` unless a future calibration provides evidence that they
#' should be modeled separately.
#'
#' @param insurance Character insurance coverage labels.
#'
#' @return Character vector containing `commercial`, `medicare`,
#'   `medicaid`, `uninsured`, or `NA`.
#' @family demand
#' @concept insurance
#' @export
normalize_urps_insurance <- function(insurance) {

  insurance_chr <- base::tolower(
    stringr::str_squish(
      base::as.character(insurance)
    )
  )

  dplyr::case_when(
    insurance_chr %in% c(
      "commercial",
      "private",
      "private insurance",
      "employer",
      "employer sponsored",
      "employer-sponsored",
      "esi",
      "marketplace",
      "exchange"
    ) ~
      "commercial",

    insurance_chr %in% c(
      "medicare",
      "medicare ffs",
      "medicare fee for service",
      "medicare fee-for-service",
      "traditional medicare",
      "medicare advantage",
      "ma"
    ) ~
      "medicare",

    insurance_chr %in% c(
      "medicaid",
      "chip",
      "medicaid/chip",
      "public medicaid"
    ) ~
      "medicaid",

    insurance_chr %in% c(
      "uninsured",
      "none",
      "no insurance",
      "self pay",
      "self-pay"
    ) ~
      "uninsured",

    TRUE ~
      NA_character_
  )
}


#' Validate insurance demand multipliers
#'
#' @param multiplier_spec Named numeric vector or specification tibble.
#'
#' @return Named numeric multiplier vector, invisibly.
#' @family demand
#' @concept insurance
#' @export
validate_insurance_demand_multipliers <- function(
    multiplier_spec =
      URPS_INSURANCE_DEMAND_MULTIPLIERS) {

  if (base::is.data.frame(multiplier_spec)) {

    required_cols <- c(
      "insurance_type",
      "multiplier"
    )

    missing_cols <- base::setdiff(
      required_cols,
      base::names(multiplier_spec)
    )

    if (base::length(missing_cols) > 0L) {
      base::stop(
        "Insurance multiplier specification is missing: ",
        base::paste(
          missing_cols,
          collapse = ", "
        ),
        call. = FALSE
      )
    }

    multiplier_vector <- stats::setNames(
      base::as.numeric(
        multiplier_spec$multiplier
      ),
      base::as.character(
        multiplier_spec$insurance_type
      )
    )

  } else {

    multiplier_vector <- multiplier_spec
  }

  if (!base::is.numeric(multiplier_vector) ||
      base::is.null(base::names(multiplier_vector))) {
    base::stop(
      "Insurance multipliers must be a named numeric vector.",
      call. = FALSE
    )
  }

  required_names <- c(
    "commercial",
    "medicare",
    "medicaid",
    "uninsured"
  )

  missing_names <- base::setdiff(
    required_names,
    base::names(multiplier_vector)
  )

  if (base::length(missing_names) > 0L) {
    base::stop(
      "Insurance multiplier specification is missing: ",
      base::paste(
        missing_names,
        collapse = ", "
      ),
      call. = FALSE
    )
  }

  multiplier_vector <- multiplier_vector[
    required_names
  ]

  if (base::any(
    !base::is.finite(multiplier_vector)
  )) {
    base::stop(
      "Insurance multipliers must all be finite.",
      call. = FALSE
    )
  }

  if (base::any(
    multiplier_vector <= 0
  )) {
    base::stop(
      "Insurance multipliers must all be greater than zero.",
      call. = FALSE
    )
  }

  base::invisible(
    multiplier_vector
  )
}


#' Apply insurance micro-demographic demand multipliers
#'
#' @description
#' Adjusts individual-level baseline demand according to insurance
#' coverage:
#'
#' `adjusted_demand[i] = baseline_demand[i] * multiplier[insurance[i]]`
#'
#' The function should only be applied when insurance has NOT already
#' entered the model generating `baseline_demand`.
#'
#' @param population_tbl Individual-level or synthetic-person table.
#' @param demand_col Name of the baseline demand column.
#' @param insurance_col Name of the insurance coverage column.
#' @param multiplier_spec Named multiplier vector.
#' @param baseline_includes_insurance Logical indicating whether the
#'   upstream demand model already incorporated insurance.
#' @param unknown_action What to do with unknown insurance categories.
#'   `"error"` fails closed; `"baseline"` assigns multiplier 1.
#'
#' @return Original table with normalized insurance, multiplier,
#'   adjusted demand, and audit columns.
#' @family demand
#' @concept insurance
#' @export
apply_hrsa_insurance_demand_multipliers <- function(
    population_tbl,
    demand_col = "baseline_demand",
    insurance_col = "insurance",
    multiplier_spec =
      URPS_INSURANCE_DEMAND_MULTIPLIERS,
    baseline_includes_insurance = FALSE,
    unknown_action = c(
      "error",
      "baseline"
    )) {

  unknown_action <- base::match.arg(
    unknown_action
  )

  if (!base::is.data.frame(population_tbl)) {
    base::stop(
      "`population_tbl` must be a data frame or tibble.",
      call. = FALSE
    )
  }

  required_cols <- c(
    demand_col,
    insurance_col
  )

  missing_cols <- base::setdiff(
    required_cols,
    base::names(population_tbl)
  )

  if (base::length(missing_cols) > 0L) {
    base::stop(
      "population_tbl is missing: ",
      base::paste(
        missing_cols,
        collapse = ", "
      ),
      call. = FALSE
    )
  }

  if (base::isTRUE(
    baseline_includes_insurance
  )) {
    base::stop(
      "Insurance is already represented in baseline demand. ",
      "Applying an additional multiplier would double-count ",
      "the insurance effect.",
      call. = FALSE
    )
  }

  if ("insurance_adjustment_applied" %in%
      base::names(population_tbl)) {

    previously_adjusted <- base::any(
      population_tbl$insurance_adjustment_applied,
      na.rm = TRUE
    )

    if (previously_adjusted) {
      base::stop(
        "Insurance demand adjustment has already been applied.",
        call. = FALSE
      )
    }
  }

  validate_insurance_demand_multipliers(
    multiplier_spec
  )

  multiplier_vector <- if (
    base::is.data.frame(multiplier_spec)
  ) {
    stats::setNames(
      multiplier_spec$multiplier,
      multiplier_spec$insurance_type
    )
  } else {
    multiplier_spec
  }

  base::message(
    "[insurance-demand] Applying insurance demand adjustment."
  )

  base::message(
    "[insurance-demand] Rows: ",
    base::format(
      base::nrow(population_tbl),
      big.mark = ","
    )
  )

  baseline_vector <- base::as.numeric(
    population_tbl[[demand_col]]
  )

  if (base::any(
    baseline_vector < 0,
    na.rm = TRUE
  )) {
    base::stop(
      "Baseline demand cannot be negative.",
      call. = FALSE
    )
  }

  insurance_type <- normalize_urps_insurance(
    population_tbl[[insurance_col]]
  )

  unknown_index <- base::which(
    base::is.na(insurance_type) &
      !base::is.na(
        population_tbl[[insurance_col]]
      )
  )

  insurance_multiplier <- base::unname(
    multiplier_vector[
      insurance_type
    ]
  )

  if (base::length(unknown_index) > 0L) {

    unknown_values <- base::sort(
      base::unique(
        base::as.character(
          population_tbl[[insurance_col]][unknown_index]
        )
      )
    )

    if (unknown_action == "error") {
      base::stop(
        "Unrecognized insurance category or categories: ",
        base::paste(
          unknown_values,
          collapse = ", "
        ),
        ".",
        call. = FALSE
      )
    }

    insurance_multiplier[
      unknown_index
    ] <- 1

    base::warning(
      "Unknown insurance categories assigned multiplier 1.0: ",
      base::paste(
        unknown_values,
        collapse = ", "
      ),
      call. = FALSE
    )
  }

  missing_insurance_index <- base::which(
    base::is.na(
      population_tbl[[insurance_col]]
    )
  )

  if (base::length(
    missing_insurance_index
  ) > 0L) {

    if (unknown_action == "error") {
      base::stop(
        "Missing insurance coverage for ",
        base::format(
          base::length(
            missing_insurance_index
          ),
          big.mark = ","
        ),
        " person(s).",
        call. = FALSE
      )
    }

    insurance_multiplier[
      missing_insurance_index
    ] <- 1
  }

  adjusted_demand <- baseline_vector *
    insurance_multiplier

  adjusted_tbl <- population_tbl |>
    dplyr::mutate(
      insurance_type =
        insurance_type,
      insurance_demand_multiplier =
        insurance_multiplier,
      demand_before_insurance =
        baseline_vector,
      demand_after_insurance =
        adjusted_demand,
      insurance_adjustment_applied =
        TRUE
    )

  total_before <- base::sum(
    adjusted_tbl$demand_before_insurance,
    na.rm = TRUE
  )

  total_after <- base::sum(
    adjusted_tbl$demand_after_insurance,
    na.rm = TRUE
  )

  aggregate_multiplier <- if (
    total_before > 0
  ) {
    total_after / total_before
  } else {
    NA_real_
  }

  base::message(
    "[insurance-demand] Demand before adjustment: ",
    base::format(
      base::round(total_before, 1),
      big.mark = ","
    )
  )

  base::message(
    "[insurance-demand] Demand after adjustment: ",
    base::format(
      base::round(total_after, 1),
      big.mark = ","
    )
  )

  base::message(
    "[insurance-demand] Aggregate multiplier: ",
    base::format(
      aggregate_multiplier,
      digits = 4
    )
  )

  adjusted_tbl
}


#' Apply insurance multipliers to demographic cells
#'
#' @description
#' Computes a population-weighted insurance multiplier when demand is
#' represented by geographic or demographic cells rather than individual
#' synthetic persons.
#'
#' @param demand_tbl Baseline demand table.
#' @param insurance_share_tbl Long-format insurance shares.
#' @param by Character vector defining the demographic/geographic cell.
#' @param demand_col Baseline demand column.
#' @param insurance_col Insurance category column.
#' @param share_col Insurance population-share column.
#' @param multiplier_spec Insurance multiplier specification.
#'
#' @return Demand table with effective insurance multiplier and adjusted
#'   demand.
#' @family demand
#' @concept insurance
#' @export
apply_insurance_mix_demand_multiplier <- function(
    demand_tbl,
    insurance_share_tbl,
    by,
    demand_col = "baseline_demand",
    insurance_col = "insurance",
    share_col = "insurance_share",
    multiplier_spec =
      URPS_INSURANCE_DEMAND_MULTIPLIERS) {

  validate_insurance_demand_multipliers(
    multiplier_spec
  )

  multiplier_vector <- if (
    base::is.data.frame(multiplier_spec)
  ) {
    stats::setNames(
      multiplier_spec$multiplier,
      multiplier_spec$insurance_type
    )
  } else {
    multiplier_spec
  }

  required_share_cols <- c(
    by,
    insurance_col,
    share_col
  )

  missing_share_cols <- base::setdiff(
    required_share_cols,
    base::names(insurance_share_tbl)
  )

  if (base::length(missing_share_cols) > 0L) {
    base::stop(
      "insurance_share_tbl is missing: ",
      base::paste(
        missing_share_cols,
        collapse = ", "
      ),
      call. = FALSE
    )
  }

  if (!demand_col %in%
      base::names(demand_tbl)) {
    base::stop(
      "demand_tbl is missing `",
      demand_col,
      "`.",
      call. = FALSE
    )
  }

  share_work_tbl <- insurance_share_tbl |>
    dplyr::mutate(
      insurance_type =
        normalize_urps_insurance(
          .data[[insurance_col]]
        ),
      insurance_share =
        base::as.numeric(
          .data[[share_col]]
        )
    )

  if (base::any(
    base::is.na(
      share_work_tbl$insurance_type
    )
  )) {
    base::stop(
      "Every insurance-share row must map to a known ",
      "insurance category.",
      call. = FALSE
    )
  }

  if (base::any(
    share_work_tbl$insurance_share < 0 |
      share_work_tbl$insurance_share > 1,
    na.rm = TRUE
  )) {
    base::stop(
      "Insurance shares must lie between 0 and 1.",
      call. = FALSE
    )
  }

  share_work_tbl <- share_work_tbl |>
    dplyr::mutate(
      insurance_multiplier =
        base::unname(
          multiplier_vector[
            .data$insurance_type
          ]
        ),
      weighted_multiplier =
        .data$insurance_share *
        .data$insurance_multiplier
    )

  share_check_tbl <- share_work_tbl |>
    dplyr::group_by(
      dplyr::across(
        dplyr::all_of(by)
      )
    ) |>
    dplyr::summarise(
      share_sum = base::sum(
        .data$insurance_share,
        na.rm = TRUE
      ),
      effective_insurance_multiplier =
        base::sum(
          .data$weighted_multiplier,
          na.rm = TRUE
        ),
      .groups = "drop"
    )

  bad_share_tbl <- share_check_tbl |>
    dplyr::filter(
      base::abs(
        .data$share_sum - 1
      ) > 1e-6
    )

  if (base::nrow(bad_share_tbl) > 0L) {
    base::stop(
      "Insurance shares do not sum to 1 within ",
      base::nrow(bad_share_tbl),
      " demographic/geographic cell(s).",
      call. = FALSE
    )
  }

  adjusted_tbl <- demand_tbl |>
    dplyr::left_join(
      share_check_tbl |>
        dplyr::select(
          dplyr::all_of(by),
          .data$effective_insurance_multiplier
        ),
      by = by
    ) |>
    dplyr::mutate(
      demand_before_insurance =
        base::as.numeric(
          .data[[demand_col]]
        ),
      demand_after_insurance =
        .data$demand_before_insurance *
        .data$effective_insurance_multiplier,
      insurance_adjustment_applied =
        TRUE
    )

  if (base::any(
    base::is.na(
      adjusted_tbl$effective_insurance_multiplier
    )
  )) {
    base::stop(
      "Some demand cells had no corresponding insurance mix.",
      call. = FALSE
    )
  }

  base::message(
    "[insurance-demand] Cells adjusted: ",
    base::format(
      base::nrow(adjusted_tbl),
      big.mark = ","
    )
  )

  adjusted_tbl
}


#' Draw uncertain insurance-demand multipliers
#'
#' @description
#' Draws positive insurance multipliers from log-normal distributions.
#' The caller must supply the log-scale standard errors; the function
#' deliberately has no invented default uncertainty.
#'
#' @param multiplier_spec Named central multipliers.
#' @param log_sd Named log-scale standard deviations.
#' @param n_draws Number of Monte Carlo draws.
#' @param seed Optional RNG seed.
#'
#' @return Long-format multiplier-draw table.
#' @family demand
#' @concept insurance
#' @export
draw_insurance_demand_multipliers <- function(
    multiplier_spec =
      URPS_INSURANCE_DEMAND_MULTIPLIERS,
    log_sd,
    n_draws = 1000L,
    seed = NULL) {

  validate_insurance_demand_multipliers(
    multiplier_spec
  )

  if (base::missing(log_sd)) {
    base::stop(
      "`log_sd` must be empirically specified. ",
      "No uncertainty distribution is assumed by default.",
      call. = FALSE
    )
  }

  required_names <- base::names(
    URPS_INSURANCE_DEMAND_MULTIPLIERS
  )

  if (!base::all(
    required_names %in%
      base::names(log_sd)
  )) {
    base::stop(
      "`log_sd` must contain: ",
      base::paste(
        required_names,
        collapse = ", "
      ),
      ".",
      call. = FALSE
    )
  }

  if (base::any(
    !base::is.finite(log_sd) |
      log_sd < 0
  )) {
    base::stop(
      "`log_sd` must contain finite non-negative values.",
      call. = FALSE
    )
  }

  if (!base::is.null(seed)) {
    base::set.seed(seed)
  }

  insurance_types <- required_names

  draw_list <- base::lapply(
    insurance_types,
    function(insurance_type) {

      central_multiplier <-
        multiplier_spec[[insurance_type]]

      tibble::tibble(
        draw = base::seq_len(
          n_draws
        ),
        insurance_type =
          insurance_type,
        multiplier =
          stats::rlnorm(
            n = n_draws,
            meanlog = base::log(
              central_multiplier
            ),
            sdlog =
              log_sd[[insurance_type]]
          )
      )
    }
  )

  dplyr::bind_rows(
    draw_list
  )
}
