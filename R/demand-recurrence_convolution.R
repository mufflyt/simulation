# Recurrence as a treatment-cohort convolution ----
#
# THE DEFECT THIS REPLACES:
#
#     recurrences_t = procedures_t x annual_hazard
#
# i.e. THIS YEAR's operations as the entire risk set (350 x 0.12 = 42 per
# 1,000). Recurrences arise from the ACCUMULATED stock of everyone previously
# treated, so the old form exposed a single cohort-year and a year with no new
# procedures produced no recurrences at all.
#
# THE REPLACEMENT:
#
#     R_t = SUM_k  C_{t-k} * g_k
#
# g_k is the UNCONDITIONAL probability that FIRST recurrence occurs in year k
# after treatment. It is NOT automatically an annual hazard -- that conflation
# is exactly what produced 0.12, where a multi-year cumulative curve
# (SUPeR/E-CARE) was used to license an annual rate.
#
# GROUPING IS condition x index_treatment. SUPeR shows different long-term
# failure trajectories for two vaginal apical procedures, and E-CARE describes a
# different surgical population again, so one generic POP kernel is not
# defensible. group_cols defaults to "condition" only so existing single-limb
# callers keep working; real use supplies index_treatment as well.


#' Format an integer-like value for logging
#' @param x Numeric value.
#' @return Character scalar.
#' @keywords internal
.fmt_n <- function(x) {
  base::format(x, big.mark = ",", scientific = FALSE, trim = TRUE)
}

#' Assert that required columns are present
#' @param tbl A data frame or tibble.
#' @param required_cols Required column names.
#' @param object_name Name used in error messages.
#' @return `tbl`, invisibly.
#' @keywords internal
.assert_required_columns <- function(tbl, required_cols, object_name) {
  missing_cols <- base::setdiff(required_cols, base::names(tbl))
  if (base::length(missing_cols) > 0L) {
    base::stop(base::sprintf("%s is missing required columns: %s", object_name,
                             base::paste(missing_cols, collapse = ", ")),
               call. = FALSE)
  }
  base::invisible(tbl)
}

#' First-recurrence probability mass from a cumulative incidence curve
#'
#' `g_k = F(k) - F(k-1)`. Supplied SEPARATELY from the hazard converter
#' because a five-year cumulative recurrence proportion and an annual hazard
#' are different quantities, and treating one as the other is the specific
#' error recorded in docs/POP_RECURRENCE_ESTIMAND_AUDIT.md.
#'
#' @param cumulative_incidence Non-decreasing numeric vector in [0, 1].
#' @return Numeric vector of first-recurrence probability mass by year.
#' @family recurrence
#' @concept demand
#' @export
recurrence_mass_from_cumulative <- function(cumulative_incidence) {
  if (!base::length(cumulative_incidence) ||
      base::any(base::is.na(cumulative_incidence))) {
    base::stop("cumulative_incidence must be non-empty and free of NA.",
               call. = FALSE)
  }
  if (base::any(cumulative_incidence < 0 | cumulative_incidence > 1)) {
    base::stop("cumulative_incidence must lie in [0, 1].", call. = FALSE)
  }
  if (base::any(base::diff(cumulative_incidence) < -1e-10)) {
    base::stop(paste0("cumulative_incidence must be non-decreasing. A ",
                      "decreasing curve usually means an ANNUAL series was ",
                      "supplied where a CUMULATIVE one was expected."),
               call. = FALSE)
  }
  base::diff(base::c(0, cumulative_incidence))
}

#' Convert discrete recurrence hazards to first-recurrence probabilities
#'
#' `g_k = S_k * h_k`, where `S_k` is recurrence-free survival at the start of
#' interval `k`.
#'
#' @param recurrence_hazards Data frame with grouping columns,
#'   `years_since_treatment` and `recurrence_hazard`.
#' @param group_cols Columns defining separate recurrence processes.
#' @return A tibble with `survival_start`, `recurrence_prob`, `survival_end`.
#' @family recurrence
#' @concept demand
#' @export
build_recurrence_kernel <- function(recurrence_hazards,
                                    group_cols = "condition") {
  base::message("[build_recurrence_kernel] Starting.")
  base::message("[build_recurrence_kernel] Input rows: ",
                .fmt_n(base::nrow(recurrence_hazards)))
  base::message("[build_recurrence_kernel] Groups: ",
                base::paste(group_cols, collapse = ", "))

  required_cols <- base::c(group_cols, "years_since_treatment",
                           "recurrence_hazard")
  .assert_required_columns(recurrence_hazards, required_cols,
                           "recurrence_hazards")

  if (base::nrow(recurrence_hazards) == 0L) {
    base::stop("recurrence_hazards cannot be empty.", call. = FALSE)
  }
  if (!base::is.numeric(recurrence_hazards$years_since_treatment)) {
    base::stop("years_since_treatment must be numeric.", call. = FALSE)
  }
  if (!base::is.numeric(recurrence_hazards$recurrence_hazard)) {
    base::stop("recurrence_hazard must be numeric.", call. = FALSE)
  }
  if (base::any(!base::is.finite(recurrence_hazards$years_since_treatment))) {
    base::stop("years_since_treatment must be finite.", call. = FALSE)
  }
  if (base::any(!base::is.finite(recurrence_hazards$recurrence_hazard))) {
    base::stop("recurrence_hazard must be finite.", call. = FALSE)
  }

  integer_error <- base::abs(recurrence_hazards$years_since_treatment -
                               base::round(recurrence_hazards$years_since_treatment))
  if (base::any(integer_error > 1e-10)) {
    base::stop("years_since_treatment must contain whole years.", call. = FALSE)
  }
  if (base::any(recurrence_hazards$years_since_treatment < 1)) {
    base::stop(paste0("years_since_treatment must start at 1. Same-year ",
                      "recurrence is not supported."), call. = FALSE)
  }
  if (base::any(recurrence_hazards$recurrence_hazard < 0 |
                recurrence_hazards$recurrence_hazard > 1)) {
    base::stop("recurrence_hazard must be between 0 and 1.", call. = FALSE)
  }

  key_cols <- base::c(group_cols, "years_since_treatment")
  duplicate_keys <- recurrence_hazards |>
    dplyr::group_by(dplyr::across(dplyr::all_of(key_cols))) |>
    dplyr::summarise(key_n = dplyr::n(), .groups = "drop") |>
    dplyr::filter(.data$key_n > 1L)
  if (base::nrow(duplicate_keys) > 0L) {
    base::stop("recurrence_hazards contains duplicate group-by-year keys.",
               call. = FALSE)
  }

  lag_audit <- recurrence_hazards |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) |>
    dplyr::summarise(min_lag = base::min(.data$years_since_treatment),
                     max_lag = base::max(.data$years_since_treatment),
                     lag_n = dplyr::n_distinct(.data$years_since_treatment),
                     .groups = "drop") |>
    dplyr::mutate(contiguous = .data$min_lag == 1 & .data$lag_n == .data$max_lag)
  if (base::any(!lag_audit$contiguous)) {
    base::stop(paste0("Each recurrence kernel must contain contiguous years ",
                      "beginning at year 1."), call. = FALSE)
  }

  base::message("[build_recurrence_kernel] Validation complete.")

  recurrence_kernel <- recurrence_hazards |>
    dplyr::arrange(dplyr::across(dplyr::all_of(group_cols)),
                   .data$years_since_treatment) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) |>
    dplyr::mutate(
      survival_start = dplyr::lag(base::cumprod(1 - .data$recurrence_hazard),
                                  default = 1),
      recurrence_prob = .data$survival_start * .data$recurrence_hazard,
      survival_end = .data$survival_start * (1 - .data$recurrence_hazard)) |>
    dplyr::ungroup()

  mass_audit <- recurrence_kernel |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) |>
    dplyr::summarise(cumulative_recurrence_prob = base::sum(.data$recurrence_prob),
                     .groups = "drop")
  if (base::any(mass_audit$cumulative_recurrence_prob > 1 + 1e-10)) {
    base::stop(paste0("First-recurrence probability mass exceeds 1. Check the ",
                      "recurrence hazards."), call. = FALSE)
  }

  base::message("[build_recurrence_kernel] Kernel rows: ",
                .fmt_n(base::nrow(recurrence_kernel)))
  base::message("[build_recurrence_kernel] Finished.")
  recurrence_kernel
}


#' Compute recurrence from historical treatment cohorts
#'
#' Implements `R_t = sum_k C_(t-k) * g_k`.
#'
#' @details
#' `tail_policy = "error"` is the scientific default and is deliberately
#' inconvenient: if the evidence describes recurrence through five years and a
#' cohort treated eight years earlier is supplied, this REFUSES rather than
#' silently assuming years 6+ contribute nothing. Assuming zero is a claim about
#' the disease, not a default.
#'
#' The returned `contributions` table makes any predicted count traceable --
#' for a 2028 total you can see which treatment years produced it and in what
#' proportion, which `this_year_operations x 0.12` never permitted.
#'
#' @param treatment_cohorts Data frame with grouping columns, `treatment_year`,
#'   `treated_n`.
#' @param recurrence_kernel Data frame with grouping columns,
#'   `years_since_treatment`, `recurrence_prob`.
#' @param forecast_years Years for which recurrence should be computed.
#' @param group_cols Columns defining separate recurrence processes. Supply
#'   `c("condition", "index_treatment")` unless evidence shows treatments may
#'   share a kernel.
#' @param tail_policy `"error"` (default) or `"zero_after_kernel"`.
#' @return List with `annual` totals and per-cohort `contributions`.
#' @family recurrence
#' @concept demand
#' @export
compute_recurrence_convolution <- function(treatment_cohorts,
                                           recurrence_kernel,
                                           forecast_years,
                                           group_cols = "condition",
                                           tail_policy = base::c("error",
                                                                 "zero_after_kernel")) {
  tail_policy <- base::match.arg(tail_policy)
  base::message("[compute_recurrence_convolution] Starting.")
  base::message("[compute_recurrence_convolution] Treatment rows: ",
                .fmt_n(base::nrow(treatment_cohorts)))
  base::message("[compute_recurrence_convolution] Kernel rows: ",
                .fmt_n(base::nrow(recurrence_kernel)))

  cohort_required <- base::c(group_cols, "treatment_year", "treated_n")
  kernel_required <- base::c(group_cols, "years_since_treatment", "recurrence_prob")
  .assert_required_columns(treatment_cohorts, cohort_required, "treatment_cohorts")
  .assert_required_columns(recurrence_kernel, kernel_required, "recurrence_kernel")

  if (base::length(forecast_years) == 0L) {
    base::stop("forecast_years cannot be empty.", call. = FALSE)
  }
  if (!base::is.numeric(forecast_years)) {
    base::stop("forecast_years must be numeric.", call. = FALSE)
  }
  forecast_years <- base::sort(base::unique(forecast_years))
  if (base::any(base::abs(forecast_years - base::round(forecast_years)) > 1e-10)) {
    base::stop("forecast_years must contain whole years.", call. = FALSE)
  }
  if (!base::is.numeric(treatment_cohorts$treatment_year)) {
    base::stop("treatment_year must be numeric.", call. = FALSE)
  }
  if (!base::is.numeric(treatment_cohorts$treated_n)) {
    base::stop("treated_n must be numeric.", call. = FALSE)
  }
  if (base::any(!base::is.finite(treatment_cohorts$treated_n))) {
    base::stop("treated_n must be finite.", call. = FALSE)
  }
  if (base::any(treatment_cohorts$treated_n < 0)) {
    base::stop("treated_n cannot be negative.", call. = FALSE)
  }
  if (base::any(!base::is.finite(recurrence_kernel$recurrence_prob))) {
    base::stop("recurrence_prob must be finite.", call. = FALSE)
  }
  if (base::any(recurrence_kernel$recurrence_prob < 0 |
                recurrence_kernel$recurrence_prob > 1)) {
    base::stop("recurrence_prob must be between 0 and 1.", call. = FALSE)
  }

  cohort_key_cols <- base::c(group_cols, "treatment_year")
  duplicate_cohorts <- treatment_cohorts |>
    dplyr::group_by(dplyr::across(dplyr::all_of(cohort_key_cols))) |>
    dplyr::summarise(cohort_n = dplyr::n(), .groups = "drop") |>
    dplyr::filter(.data$cohort_n > 1L)
  if (base::nrow(duplicate_cohorts) > 0L) {
    base::stop(paste0("Treatment cohorts are not unique by group and ",
                      "treatment_year. Add the missing stratification columns ",
                      "to group_cols."), call. = FALSE)
  }

  cohort_groups <- treatment_cohorts |>
    dplyr::select(dplyr::all_of(group_cols)) |> dplyr::distinct()
  kernel_groups <- recurrence_kernel |>
    dplyr::select(dplyr::all_of(group_cols)) |> dplyr::distinct()
  missing_kernel_groups <- dplyr::anti_join(cohort_groups, kernel_groups,
                                            by = group_cols)
  if (base::nrow(missing_kernel_groups) > 0L) {
    base::stop("At least one treatment-cohort group has no recurrence kernel.",
               call. = FALSE)
  }

  mass_audit <- recurrence_kernel |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) |>
    dplyr::summarise(recurrence_mass = base::sum(.data$recurrence_prob),
                     .groups = "drop")
  if (base::any(mass_audit$recurrence_mass > 1 + 1e-10)) {
    base::stop(paste0("First-recurrence probability mass exceeds 1 for at ",
                      "least one group."), call. = FALSE)
  }

  kernel_horizon <- recurrence_kernel |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) |>
    dplyr::summarise(max_kernel_lag = base::max(.data$years_since_treatment),
                     .groups = "drop")

  year_tbl <- tibble::tibble(forecast_year = forecast_years, .cross_key = 1L)
  base::message("[compute_recurrence_convolution] Expanding historical ",
                "cohorts across forecast years.")

  expanded_cohorts <- treatment_cohorts |>
    dplyr::mutate(.cross_key = 1L) |>
    dplyr::inner_join(year_tbl, by = ".cross_key") |>
    dplyr::select(-".cross_key") |>
    dplyr::mutate(years_since_treatment = .data$forecast_year - .data$treatment_year) |>
    dplyr::filter(.data$years_since_treatment >= 1) |>
    dplyr::left_join(kernel_horizon, by = group_cols)

  beyond_kernel <- expanded_cohorts |>
    dplyr::filter(.data$years_since_treatment > .data$max_kernel_lag,
                  .data$treated_n > 0)

  if (tail_policy == "error" && base::nrow(beyond_kernel) > 0L) {
    base::stop(paste0("Treatment history extends beyond the recurrence ",
                      "kernel. Extend the kernel or explicitly use ",
                      "tail_policy = 'zero_after_kernel'."), call. = FALSE)
  }
  if (tail_policy == "zero_after_kernel" && base::nrow(beyond_kernel) > 0L) {
    base::message("[compute_recurrence_convolution] Explicitly treating ",
                  "recurrence probability after the kernel horizon as zero. ",
                  "Affected rows: ", .fmt_n(base::nrow(beyond_kernel)))
  }

  eligible_cohorts <- expanded_cohorts |>
    dplyr::filter(.data$years_since_treatment <= .data$max_kernel_lag) |>
    dplyr::select(-"max_kernel_lag")

  join_cols <- base::c(group_cols, "years_since_treatment")
  base::message("[compute_recurrence_convolution] Joining cohorts to ",
                "recurrence probability mass.")

  contributions <- eligible_cohorts |>
    dplyr::left_join(recurrence_kernel, by = join_cols)
  if (base::any(base::is.na(contributions$recurrence_prob))) {
    base::stop(paste0("A cohort-year has no matching recurrence probability. ",
                      "The recurrence kernel is incomplete."), call. = FALSE)
  }
  contributions <- contributions |>
    dplyr::mutate(recurrence_n = .data$treated_n * .data$recurrence_prob)

  annual_recurrence <- contributions |>
    dplyr::group_by(dplyr::across(dplyr::all_of(base::c(group_cols, "forecast_year")))) |>
    dplyr::summarise(recurrence_n = base::sum(.data$recurrence_n),
                     source_cohorts_n = dplyr::n_distinct(.data$treatment_year),
                     .groups = "drop")

  annual_grid <- cohort_groups |>
    dplyr::mutate(.cross_key = 1L) |>
    dplyr::left_join(year_tbl, by = ".cross_key") |>
    dplyr::select(-".cross_key")

  annual <- annual_grid |>
    dplyr::left_join(annual_recurrence, by = base::c(group_cols, "forecast_year")) |>
    dplyr::mutate(recurrence_n = dplyr::coalesce(.data$recurrence_n, 0),
                  source_cohorts_n = dplyr::coalesce(.data$source_cohorts_n, 0L)) |>
    dplyr::arrange(.data$forecast_year, dplyr::across(dplyr::all_of(group_cols)))

  base::message("[compute_recurrence_convolution] Annual rows: ",
                .fmt_n(base::nrow(annual)))
  base::message("[compute_recurrence_convolution] Contribution rows: ",
                .fmt_n(base::nrow(contributions)))
  base::message("[compute_recurrence_convolution] Finished.")

  base::list(annual = annual, contributions = contributions)
}

#' Status of the recurrence parameters
#'
#' @return A length-one character calibration tier.
#' @family recurrence
#' @concept demand
#' @export
recurrence_parameter_status <- function() {
  # 0.12 is documented as an ANNUAL hazard but justified by SUPeR/E-CARE
  # retreatment CURVES -- a multi-year cumulative observation licensing an
  # annual rate. 0.40 is not a recurrence rate at all but a conditional
  # reoperation share, and belongs downstream of recurrent care rather than
  # inside g_k. Neither may enter the convolution until sourced.
  # See docs/POP_RECURRENCE_ESTIMAND_AUDIT.md and
  # docs/RECURRENCE_ENDPOINT_CONTRACT.md.
  "unresolved_requires_source"
}

#' Recurrence evidence register
#'
#' @return Tibble of candidate recurrence parameters with their measure type
#'   and kernel compatibility.
#' @family recurrence
#' @concept demand
#' @export
recurrence_evidence_register <- function() {
  p <- system.file("extdata", "recurrence_evidence.csv", package = "urpssim")
  if (!nzchar(p) || !file.exists(p)) {
    root <- if (file.exists("config/recurrence_evidence.csv")) "." else "../.."
    p <- file.path(root, "config", "recurrence_evidence.csv")
  }
  if (!file.exists(p)) {
    base::stop("recurrence_evidence.csv not found.", call. = FALSE)
  }
  tibble::as_tibble(utils::read.csv(p, comment.char = "#", stringsAsFactors = FALSE))
}

#' Permitted measure types for recurrence evidence
#' @family recurrence
#' @concept demand
#' @export
RECURRENCE_MEASURE_TYPES <- c(
  "discrete_hazard",
  "cumulative_incidence",
  "first_recurrence_probability_mass",
  "repeat_treatment_rate",
  "unsupported_or_unknown"
)

#' Assert a recurrence parameter may enter the kernel
#'
#' @details
#' Compatibility is FALSE BY DEFAULT. `repeat_treatment_rate` has NO conversion
#' route to `g_k` -- a retreatment proportion is not recurrence probability
#' mass, and one vaginal-hysterectomy/USLS cohort reports roughly 20% recurrent
#' prolapse against 10% recurrent surgery, which is the size of the error that
#' substitution would introduce.
#'
#' @param condition,parameter Identify the register row.
#' @return Invisibly `TRUE` when the parameter may be converted to `g_k`.
#' @family recurrence
#' @concept demand
#' @export
assert_recurrence_kernel_compatible <- function(condition, parameter) {
  reg <- recurrence_evidence_register()
  row <- reg[reg$condition == condition & reg$parameter == parameter, , drop = FALSE]
  if (base::nrow(row) == 0L) {
    base::stop(base::sprintf(
      "No recurrence-evidence row for %s/%s. Every candidate parameter must be ",
      condition, parameter), "registered before it can enter the kernel.",
      call. = FALSE)
  }
  if (!row$measure_type[[1]] %in% RECURRENCE_MEASURE_TYPES) {
    base::stop(base::sprintf("Unknown measure_type '%s'. Permitted: %s",
                             row$measure_type[[1]],
                             base::paste(RECURRENCE_MEASURE_TYPES, collapse = ", ")),
               call. = FALSE)
  }
  if (!base::isTRUE(base::as.logical(row$kernel_compatible[[1]]))) {
    base::stop(base::sprintf(
      paste0("%s/%s is NOT kernel-compatible (measure_type = %s). %s ",
             "See docs/RECURRENCE_ENDPOINT_CONTRACT.md."),
      condition, parameter, row$measure_type[[1]],
      row$incompatibility_reason[[1]]), call. = FALSE)
  }
  base::invisible(TRUE)
}
