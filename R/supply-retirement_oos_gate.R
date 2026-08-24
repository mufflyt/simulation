# Retirement OOS calibration gate and uncertainty propagation ---------------
#
# A retirement model that has not been validated out of sample is not a
# validated model, however carefully its point estimates were constructed. The
# rules already established elsewhere in this work -- no future leakage,
# return-invalidates-exit, no pre-entry backfill of risk years -- are correct
# and currently live only inside the exit-hazard experiment. Isolated there
# they check nothing, because the geographic-access results are produced by a
# path that never consults them.
#
# validate_retirement_oos() exists to sit UPSTREAM of those results as a
# release gate. It is expected to be RED on the present model: measured
# coverage of an interval nominally 95% has been closer to 80%, which means
# the published uncertainty is too narrow by a wide margin. A gate that
# refuses to promote an under-dispersed model turns that from a sentence in
# the documentation into a condition somebody has to clear.

#' Gate a retirement model on out-of-sample calibration
#'
#' @description
#' Refuses to promote a retirement model whose intervals are not calibrated
#' out of sample. Under-dispersion is the failure mode that matters: an
#' interval narrower than the truth reads as precision and is reported as
#' confidence.
#'
#' @param metric_tbl Table with `horizon_years`, `coverage`, `bias`, `mae`,
#'   `rmse`, `n_origins`, `future_leakage_pass`. One row per horizon.
#' @param min_coverage Minimum empirical coverage of the nominal interval.
#' @param min_origins Minimum distinct origin years per horizon. One origin is
#'   an anecdote, not a back-test.
#' @param nominal_coverage The interval's advertised coverage, used only to
#'   describe the shortfall.
#'
#' @return Invisibly `TRUE`. Errors on any failed condition.
#' @family retirement contract
#' @concept supply
#' @export
validate_retirement_oos <- function(metric_tbl,
                                    min_coverage = 0.90,
                                    min_origins = 3L,
                                    nominal_coverage = 0.95) {
  base::message("[retirement] Validating out-of-sample calibration.")

  required_columns <- base::c(
    "horizon_years", "coverage", "bias", "mae", "rmse", "n_origins",
    "future_leakage_pass"
  )
  missing_columns <- base::setdiff(required_columns, base::names(metric_tbl))
  if (base::length(missing_columns) > 0L) {
    base::stop(
      "OOS metrics are missing: ",
      base::paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }
  if (base::nrow(metric_tbl) == 0L) {
    base::stop(
      "OOS metrics are empty. An unmeasured model is not a validated model; ",
      "an empty table must not read as a passing gate.",
      call. = FALSE
    )
  }

  # Leakage first. If the back-test saw the future, its coverage number is not
  # evidence about anything and the remaining checks would launder it.
  # `%in% TRUE` rather than `!x`: NA must count as a failure, not propagate.
  leakage_failures <- base::sum(!(metric_tbl$future_leakage_pass %in% TRUE))
  if (leakage_failures > 0L) {
    base::stop(
      leakage_failures, " horizon row(s) did not pass the future-leakage ",
      "check. Coverage measured with access to the future is not evidence.",
      call. = FALSE
    )
  }

  underpowered <- metric_tbl[
    base::is.na(metric_tbl$n_origins) | metric_tbl$n_origins < min_origins, ,
    drop = FALSE
  ]
  if (base::nrow(underpowered) > 0L) {
    base::stop(
      base::nrow(underpowered), " horizon row(s) rest on fewer than ",
      min_origins, " origin years. Coverage estimated from one or two ",
      "origins is not a calibration measurement.",
      call. = FALSE
    )
  }

  poor_coverage <- metric_tbl[
    base::is.na(metric_tbl$coverage) | metric_tbl$coverage < min_coverage, ,
    drop = FALSE
  ]
  if (base::nrow(poor_coverage) > 0L) {
    worst <- base::min(poor_coverage$coverage, na.rm = TRUE)
    base::stop(
      base::sprintf(
        base::paste(
          "Retirement uncertainty is under-calibrated: %d horizon row(s)",
          "below %.0f%% coverage (worst %.0f%%) against a nominal %.0f%%",
          "interval. An interval narrower than the truth is reported as",
          "precision, so this model must not be promoted."
        ),
        base::nrow(poor_coverage), 100 * min_coverage, 100 * worst,
        100 * nominal_coverage
      ),
      call. = FALSE
    )
  }

  base::message("[retirement] Out-of-sample calibration gate passed.")
  base::invisible(TRUE)
}

#' Propagate retirement uncertainty through an access measure
#'
#' @description
#' Draws each provider's active status from its `active_probability` and
#' recomputes access per draw, so the reported access interval carries the
#' uncertainty in WHO IS STILL PRACTISING rather than treating the retirement
#' panel as known.
#'
#' A point-estimate access measure computed from a probabilistic workforce
#' understates its own uncertainty, and does so in the direction that makes
#' shortage estimates look more certain than they are.
#'
#' @param provider_year_tbl Table with `provider_id` and `active_probability`,
#'   optionally `clinical_fte`.
#' @param access_function Function taking the sampled table and returning a
#'   table with `group_cols` and `access_value`.
#' @param group_cols Grouping columns of the access result.
#' @param n_draws Monte Carlo draws.
#' @param seed Seed, so an interval is reproducible.
#' @param probability_sampler Optional function of `(tbl, draw_id)` returning
#'   per-draw probabilities, for a second uncertainty level over the retirement
#'   model's own parameters.
#'
#' @return List with `draws` and `summary` (mean, sd, median, p25, p75,
#'   lower_95, upper_95 per group).
#' @family retirement contract
#' @concept supply
#' @export
propagate_retirement_uncertainty_to_access <- function(
    provider_year_tbl,
    access_function,
    group_cols = base::c("geography_id"),
    n_draws = 1000L,
    seed = 20260823L,
    probability_sampler = NULL) {
  base::message(
    "[retirement] Propagating retirement uncertainty into access (",
    base::format(n_draws, big.mark = ","), " draws)."
  )

  missing_columns <- base::setdiff(
    base::c("provider_id", "active_probability"),
    base::names(provider_year_tbl)
  )
  if (base::length(missing_columns) > 0L) {
    base::stop(
      "Provider uncertainty table is missing: ",
      base::paste(missing_columns, collapse = ", "),
      call. = FALSE
    )
  }

  probabilities <- provider_year_tbl$active_probability
  if (base::any(base::is.na(probabilities)) ||
      base::any(probabilities < 0 | probabilities > 1)) {
    base::stop(
      "active_probability must be non-missing and within [0, 1]. A missing ",
      "probability is not 1: an unknown provider is not a practising one.",
      call. = FALSE
    )
  }

  has_fte <- "clinical_fte" %in% base::names(provider_year_tbl)
  base::set.seed(seed)

  draw_parts <- base::lapply(base::seq_len(n_draws), function(draw_id) {
    draw_probabilities <- if (base::is.null(probability_sampler)) {
      probabilities
    } else {
      probability_sampler(provider_year_tbl, draw_id)
    }
    if (base::length(draw_probabilities) != base::nrow(provider_year_tbl)) {
      base::stop("probability_sampler returned the wrong length.", call. = FALSE)
    }

    active_draw <- stats::rbinom(
      n = base::nrow(provider_year_tbl), size = 1L, prob = draw_probabilities
    )
    sampled_tbl <- provider_year_tbl
    sampled_tbl$active_probability_draw <- draw_probabilities
    sampled_tbl$active_draw <- active_draw
    sampled_tbl$active_fte_draw <- if (has_fte) {
      active_draw * provider_year_tbl$clinical_fte
    } else {
      base::as.numeric(active_draw)
    }

    access_tbl <- access_function(sampled_tbl)
    missing_access <- base::setdiff(
      base::c(group_cols, "access_value"), base::names(access_tbl)
    )
    if (base::length(missing_access) > 0L) {
      base::stop(
        "access_function result is missing: ",
        base::paste(missing_access, collapse = ", "),
        call. = FALSE
      )
    }
    access_tbl$draw_id <- draw_id
    access_tbl
  })

  draw_tbl <- dplyr::bind_rows(draw_parts)

  summary_tbl <- draw_tbl |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) |>
    dplyr::summarise(
      mean = base::mean(.data$access_value, na.rm = TRUE),
      sd = stats::sd(.data$access_value, na.rm = TRUE),
      median = stats::median(.data$access_value, na.rm = TRUE),
      p25 = stats::quantile(.data$access_value, 0.25, na.rm = TRUE,
                            names = FALSE),
      p75 = stats::quantile(.data$access_value, 0.75, na.rm = TRUE,
                            names = FALSE),
      lower_95 = stats::quantile(.data$access_value, 0.025, na.rm = TRUE,
                                 names = FALSE),
      upper_95 = stats::quantile(.data$access_value, 0.975, na.rm = TRUE,
                                 names = FALSE),
      .groups = "drop"
    )

  base::list(draws = draw_tbl, summary = summary_tbl)
}
