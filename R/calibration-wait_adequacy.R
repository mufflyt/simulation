# Wait-time evidence for base-year adequacy -----------------------------------
#
# Wait times are informative about capacity pressure but do not, by themselves,
# identify national supply / demand adequacy.
#
# This is the INVERSE counterpart to the forward, assumed bridge in
# R/calibration-access_response_bridge.R (adequacy_access_load() /
# simulate_access_for_adequacy()), which maps an adequacy ratio into a
# clear_access() queue via rho = 1 / adequacy. clear_access() uses:
#
#   rho  = demand / capacity
#   wait = wait_scale * rho / (1 - rho),   rho < 1
#
# and treats rho >= 1 as an unbounded / censored queue.
#
# Therefore a finite observed wait cannot be inverted into an adequacy below
# 1.0 using that response function. This module deliberately keeps the wait
# evidence separate from capacity_survey_adequacy() and does not promote it to a
# calibrated adequacy -- the same guardrail the Lizeth anchor
# (R/calibration-lizeth_access_anchor.R) states for its fielded observations.
#
# The purpose is to:
#   1. quantify capacity pressure supported by URPS wait observations;
#   2. propagate uncertainty in the wait-response function;
#   3. compare the implied range with REFERENCE_ADEQUACY_CALIBRATION;
#   4. refuse to promote wait evidence to "calibrated" adequacy.
#
# Base-R note: the package declares neither scales nor lubridate. To keep the
# dependency surface unchanged, thousands-formatting is done in base R.

.wait_comma <- function(x) base::format(x, big.mark = ",")


#' Invert the finite-wait branch of the URPS access model
#'
#' For the existing clear_access() equation:
#'
#'   wait = wait_scale * rho / (1 - rho)
#'
#' the inverse is:
#'
#'   rho = wait / (wait + wait_scale)
#'
#' and capacity / demand is 1 / rho.
#'
#' This inverse is valid only for the nonsaturated branch. Consequently its
#' implied adequacy is necessarily greater than 1 and MUST NOT be interpreted
#' as an estimate of a national workforce shortage.
#'
#' @param wait_business_days Positive observed wait in business days.
#' @param wait_scale Positive wait-response scale in business days.
#'
#' @return Tibble containing wait, utilization, and implied adequacy.
#' @seealso [simulate_access_for_adequacy()] for the forward direction.
#' @family baseline gap
#' @concept calibration
#' @export
invert_clear_access_wait <- function(
    wait_business_days,
    wait_scale) {

  base::message(
    "invert_clear_access_wait(): validating wait and wait_scale."
  )

  if (!is.numeric(wait_business_days) ||
      any(!is.finite(wait_business_days)) ||
      any(wait_business_days <= 0)) {
    base::stop(
      "`wait_business_days` must contain finite positive numbers.",
      call. = FALSE
    )
  }

  if (!is.numeric(wait_scale) ||
      length(wait_scale) != 1L ||
      !is.finite(wait_scale) ||
      wait_scale <= 0) {
    base::stop(
      "`wait_scale` must be one finite positive number.",
      call. = FALSE
    )
  }

  base::message(
    "invert_clear_access_wait(): inverting the finite-wait response."
  )

  utilization <- wait_business_days /
    (wait_business_days + wait_scale)

  implied_adequacy <- 1 / utilization

  inverse_tbl <- tibble::tibble(
    wait_business_days = wait_business_days,
    wait_scale = wait_scale,
    utilization = utilization,
    implied_adequacy = implied_adequacy
  )

  base::message(
    "invert_clear_access_wait(): complete; all implied adequacy values ",
    "must exceed 1 by construction."
  )

  inverse_tbl
}


#' Build URPS wait-time calibration targets
#'
#' Combines the registered Rabice observation with available Acosta/Lizeth
#' observations without silently promoting preliminary observations.
#'
#' @param observations URPS wait observation table.
#' @param include_preliminary Include preliminary Lizeth/Acosta observations.
#'
#' @return Tibble of wait-time targets.
#' @family baseline gap
#' @concept calibration
#' @export
urps_wait_calibration_targets <- function(
    observations = URPS_WAIT_OBSERVATIONS,
    include_preliminary = TRUE) {

  base::message(
    "urps_wait_calibration_targets(): validating observations."
  )

  if (!is.data.frame(observations)) {
    base::stop(
      "`observations` must be a data frame.",
      call. = FALSE
    )
  }

  required_columns <- c(
    "study",
    "data_year",
    "scenario",
    "insurance",
    "wait_business_days",
    "n_offices",
    "status",
    "citation"
  )

  missing_columns <- base::setdiff(
    required_columns,
    base::names(observations)
  )

  if (length(missing_columns) > 0L) {
    base::stop(
      "Missing columns: ",
      base::paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  target_tbl <- observations |>
    dplyr::filter(
      is.finite(.data$wait_business_days),
      .data$wait_business_days > 0
    )

  if (!isTRUE(include_preliminary)) {
    base::message(
      "urps_wait_calibration_targets(): excluding preliminary observations."
    )

    target_tbl <- target_tbl |>
      dplyr::filter(.data$status == "calibrated")
  }

  target_tbl <- target_tbl |>
    dplyr::mutate(
      evidence_weight = dplyr::if_else(
        .data$status == "calibrated",
        1,
        0.5
      )
    )

  base::message(
    "urps_wait_calibration_targets(): retained ",
    .wait_comma(nrow(target_tbl)),
    " registered wait targets."
  )

  target_tbl
}


#' Fit the wait-response scale conditional on an adequacy value
#'
#' This is intentionally a CONDITIONAL fit. Adequacy and wait_scale are not
#' jointly identified from a single cross-sectional wait observation.
#'
#' For an assumed adequacy A > 1:
#'
#'   rho = 1 / A
#'   k = wait * (1 - rho) / rho
#'
#' @param adequacy Assumed capacity / demand ratio, strictly greater than 1.
#' @param waits Observed waits in business days.
#' @param weights Optional nonnegative observation weights.
#'
#' @return Weighted estimate of wait_scale.
#' @family baseline gap
#' @concept calibration
#' @export
fit_wait_scale_given_adequacy <- function(
    adequacy,
    waits,
    weights = rep(1, length(waits))) {

  base::message(
    "fit_wait_scale_given_adequacy(): validating inputs."
  )

  if (!is.numeric(adequacy) ||
      length(adequacy) != 1L ||
      !is.finite(adequacy) ||
      adequacy <= 1) {
    base::stop(
      "`adequacy` must exceed 1 for the finite-wait branch.",
      call. = FALSE
    )
  }

  if (!is.numeric(waits) ||
      any(!is.finite(waits)) ||
      any(waits <= 0)) {
    base::stop(
      "`waits` must contain finite positive values.",
      call. = FALSE
    )
  }

  if (!is.numeric(weights) ||
      length(weights) != length(waits) ||
      any(!is.finite(weights)) ||
      any(weights < 0) ||
      sum(weights) <= 0) {
    base::stop(
      "`weights` must be finite, nonnegative, and have positive sum.",
      call. = FALSE
    )
  }

  utilization <- 1 / adequacy

  implied_scales <- waits *
    (1 - utilization) /
    utilization

  fitted_scale <- stats::weighted.mean(
    implied_scales,
    w = weights
  )

  base::message(
    "fit_wait_scale_given_adequacy(): fitted scale = ",
    format(round(fitted_scale, 2), nsmall = 2),
    " business days."
  )

  fitted_scale
}


#' Evaluate adequacy and wait-scale combinations against URPS waits
#'
#' This exposes the identification problem instead of hiding it. The resulting
#' surface shows which combinations of adequacy and wait-response scale produce
#' similar observed waits.
#'
#' @param targets Wait targets from urps_wait_calibration_targets().
#' @param adequacy_grid Candidate capacity / demand ratios.
#' @param wait_scale_grid Candidate wait-response scales in business days.
#'
#' @return Tibble containing the complete loss surface.
#' @family baseline gap
#' @concept calibration
#' @export
urps_wait_inverse_surface <- function(
    targets = urps_wait_calibration_targets(),
    adequacy_grid = seq(1.01, 1.50, by = 0.005),
    wait_scale_grid = seq(0.25, 30, by = 0.25)) {

  base::message(
    "urps_wait_inverse_surface(): building inverse-fit surface."
  )

  if (!is.data.frame(targets)) {
    base::stop(
      "`targets` must be a data frame.",
      call. = FALSE
    )
  }

  required_columns <- c(
    "wait_business_days",
    "evidence_weight"
  )

  missing_columns <- base::setdiff(
    required_columns,
    base::names(targets)
  )

  if (length(missing_columns) > 0L) {
    base::stop(
      "Missing target columns: ",
      base::paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  if (any(adequacy_grid <= 1)) {
    base::stop(
      paste(
        "`adequacy_grid` cannot contain values <= 1 because",
        "clear_access() maps those states to a saturated queue."
      ),
      call. = FALSE
    )
  }

  parameter_grid <- tidyr::crossing(
    adequacy = adequacy_grid,
    wait_scale = wait_scale_grid
  )

  observed_wait <- stats::weighted.mean(
    targets$wait_business_days,
    w = targets$evidence_weight
  )

  base::message(
    "urps_wait_inverse_surface(): weighted observed wait = ",
    format(round(observed_wait, 2), nsmall = 2),
    " business days."
  )

  inverse_surface <- parameter_grid |>
    dplyr::mutate(
      utilization = 1 / .data$adequacy,
      predicted_wait = .data$wait_scale *
        .data$utilization /
        (1 - .data$utilization),
      residual = .data$predicted_wait - observed_wait,
      squared_error = .data$residual^2
    ) |>
    dplyr::arrange(.data$squared_error)

  base::message(
    "urps_wait_inverse_surface(): evaluated ",
    .wait_comma(nrow(inverse_surface)),
    " adequacy / response-scale combinations."
  )

  inverse_surface
}


#' Test whether wait data identify the current base-year anchor
#'
#' The existing reference anchor is below 1.0. clear_access() places every
#' adequacy <= 1 state in the saturated branch, so finite wait observations
#' cannot discriminate 0.948 from 0.90, 0.80, or 0.70.
#'
#' @param reference_adequacy Current model calibration.
#'
#' @return Structured status object.
#' @family baseline gap
#' @concept calibration
#' @export
wait_adequacy_identification_status <- function(
    reference_adequacy = REFERENCE_ADEQUACY_CALIBRATION) {

  base::message(
    "wait_adequacy_identification_status(): checking identifiability."
  )

  if (!is.numeric(reference_adequacy) ||
      length(reference_adequacy) != 1L ||
      !is.finite(reference_adequacy) ||
      reference_adequacy <= 0) {
    base::stop(
      "`reference_adequacy` must be one positive finite number.",
      call. = FALSE
    )
  }

  reference_utilization <- 1 / reference_adequacy

  identified <- reference_utilization < 1

  status_text <- if (identified) {
    "finite_wait_branch"
  } else {
    "not_identified_saturated_branch"
  }

  base::message(
    "wait_adequacy_identification_status(): reference adequacy = ",
    format(round(reference_adequacy, 3), nsmall = 3),
    "; utilization = ",
    format(round(reference_utilization, 3), nsmall = 3),
    "; status = ",
    status_text,
    "."
  )

  list(
    reference_adequacy = reference_adequacy,
    reference_utilization = reference_utilization,
    identified_from_finite_wait = identified,
    status = status_text,
    calibration_status = "measured_input_unvalidated_response",
    reason = if (identified) {
      paste(
        "The reference lies on the finite-wait branch, but adequacy remains",
        "confounded with the unknown wait-response scale."
      )
    } else {
      paste(
        "The reference implies demand greater than capacity. Under",
        "clear_access(), every such value maps to an unbounded or censored",
        "queue, so finite mystery-caller waits cannot identify its magnitude."
      )
    }
  )
}


#' Create the URPS access evidence object for the baseline adequacy anchor
#'
#' Wires the registered Rabice and Acosta/Lizeth wait observations into the
#' baseline-gap evidence layer without incorrectly calling the capacity anchor
#' calibrated. Every row is filed as `not_identifiable_from_this_evidence`.
#'
#' This is the registered-summary counterpart to
#' [lizeth_adequacy_evidence()], which builds the same kind of ledger from the
#' raw Lizeth mystery-caller records; use whichever matches the input you hold.
#'
#' @param observations Registered URPS mystery-caller observations.
#'
#' @return urps_adequacy_evidence object.
#' @seealso [lizeth_adequacy_evidence()]
#' @family baseline gap
#' @concept calibration
#' @export
urps_wait_adequacy_evidence <- function(
    observations = URPS_WAIT_OBSERVATIONS) {

  base::message(
    "urps_wait_adequacy_evidence(): assembling URPS-specific evidence."
  )

  evidence_tbl <- observations |>
    dplyr::filter(
      is.finite(.data$wait_business_days),
      .data$wait_business_days > 0
    ) |>
    dplyr::mutate(
      evidence = paste(
        .data$study,
        .data$data_year,
        .data$scenario,
        .data$insurance,
        "appointment wait"
      ),
      model_implication = paste0(
        "Observed wait = ",
        format(
          round(.data$wait_business_days, 1),
          nsmall = 1
        ),
        " business days."
      ),
      observed = paste0(
        format(
          round(.data$wait_business_days, 1),
          nsmall = 1
        ),
        " business days"
      ),
      interpretation =
        "not_identifiable_from_this_evidence",
      evidence_type = "empirical_observation"
    )

  base::message(
    "urps_wait_adequacy_evidence(): creating evidence ledger."
  )

  adequacy_evidence_table(
    evidence = evidence_tbl$evidence,
    model_implication = evidence_tbl$model_implication,
    observed = evidence_tbl$observed,
    interpretation = evidence_tbl$interpretation,
    citation = evidence_tbl$citation,
    evidence_type = evidence_tbl$evidence_type
  )
}


#' Summarize what URPS wait evidence does and does not fix
#'
#' @param observations Registered URPS wait observations.
#' @param reference_adequacy Current baseline adequacy calibration.
#'
#' @return One-row tibble.
#' @family baseline gap
#' @concept calibration
#' @export
summarize_wait_adequacy_fix <- function(
    observations = URPS_WAIT_OBSERVATIONS,
    reference_adequacy = REFERENCE_ADEQUACY_CALIBRATION) {

  base::message(
    "summarize_wait_adequacy_fix(): summarizing evidence."
  )

  target_tbl <- urps_wait_calibration_targets(
    observations = observations,
    include_preliminary = TRUE
  )

  identification <- wait_adequacy_identification_status(
    reference_adequacy = reference_adequacy
  )

  wait_mean <- mean(target_tbl$wait_business_days)
  wait_sd <- stats::sd(target_tbl$wait_business_days)

  wait_quantiles <- stats::quantile(
    target_tbl$wait_business_days,
    probs = c(0.25, 0.50, 0.75),
    names = FALSE,
    na.rm = TRUE
  )

  summary_tbl <- tibble::tibble(
    n_wait_targets = nrow(target_tbl),
    mean_wait = wait_mean,
    sd_wait = wait_sd,
    p25_wait = wait_quantiles[[1]],
    median_wait = wait_quantiles[[2]],
    p75_wait = wait_quantiles[[3]],
    reference_adequacy = reference_adequacy,
    reference_utilization =
      identification$reference_utilization,
    identified = identification$identified_from_finite_wait,
    calibration_status =
      identification$calibration_status
  )

  base::message(
    "summarize_wait_adequacy_fix(): complete."
  )

  summary_tbl
}
