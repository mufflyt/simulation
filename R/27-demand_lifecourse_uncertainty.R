# Parameter uncertainty for the life-course demand pathway ----
#
# The life-course demand generator (R/25) is deterministic given its coefficient
# tables. This module propagates uncertainty in those (placeholder) parameters by
# drawing the risk-model coefficients and care-pathway probabilities from
# specified distributions on each Monte Carlo iteration and re-running the
# trajectory, then reporting median + prediction intervals (improvement plan
# Part B / IP sec 5). The reported intervals combine parameter uncertainty with
# the individual-level sampling noise of the synthetic cohort.
#
# The uncertainty SDs are themselves placeholders (status =
# "placeholder_uncalibrated"): once the transition equations are fitted, pass
# their real standard errors / covariance instead.

#' Default parameter-uncertainty specification (placeholder)
#'
#' @return A list with `coef_sd` (SD on each risk log-odds coefficient) and
#'   `logit_sd` (SD on the logit of each care-pathway probability).
#' @export
lifecourse_param_uncertainty <- function() {
  list(status = "placeholder_uncalibrated", coef_sd = 0.10, logit_sd = 0.15)
}

# Draw one perturbed copy of the risk and pathway parameters. Internal.
.draw_lifecourse_params <- function(risk_params, pathway_params, unc) {
  rp <- risk_params
  for (cond in c("ui", "pop", "ai")) {
    rp[[cond]] <- lapply(risk_params[[cond]],
                         function(b) b + stats::rnorm(1L, 0, unc$coef_sd))
  }
  jlogit <- function(v) stats::plogis(stats::qlogis(v) + stats::rnorm(length(v), 0, unc$logit_sd))
  pp <- pathway_params
  for (nm in c("recognition", "p_seek", "p_referral", "p_treated")) {
    pp[[nm]] <- jlogit(pathway_params[[nm]])
  }
  list(risk = rp, pathway = pp)
}

#' Life-course demand trajectory with parameter-uncertainty intervals
#'
#' Runs [lifecourse_demand_trajectory()] `n_draws` times, each with the risk
#' coefficients and care-pathway probabilities drawn from `param_uncertainty`,
#' and returns the national demand summary with median and lower/upper prediction
#' bounds. The bounds combine parameter uncertainty with cohort sampling noise.
#'
#' @param pop_by_age_year Tibble with `year`, `age`, `population`.
#' @param years Years to run; default all years present.
#' @param scenario,n,... Passed to [lifecourse_demand_trajectory()].
#' @param n_draws Number of parameter draws.
#' @param probs Length-3 vector `c(lo, mid, hi)` of quantiles. Default 2.5/50/97.5.
#' @param seed Optional RNG seed (controls the whole draw sequence).
#' @param param_uncertainty Uncertainty spec from [lifecourse_param_uncertainty()].
#' @param risk_params,pathway_params Point-estimate parameter tables to perturb.
#' @return Tibble `year`, and for care-seeking and service units the median plus
#'   `_lo`/`_hi` columns.
#' @export
lifecourse_demand_trajectory_ci <- function(pop_by_age_year,
                                            years = sort(unique(pop_by_age_year$year)),
                                            scenario = "baseline", n = 1e5,
                                            n_draws = 200L, probs = c(0.025, 0.5, 0.975),
                                            seed = NULL,
                                            param_uncertainty = lifecourse_param_uncertainty(),
                                            risk_params = lifecourse_risk_params(),
                                            pathway_params = lifecourse_pathway_params(), ...) {
  stopifnot(length(probs) == 3L)
  if (!is.null(seed)) set.seed(seed)
  draws <- purrr::map(seq_len(n_draws), function(i) {
    dp <- .draw_lifecourse_params(risk_params, pathway_params, param_uncertainty)
    tr <- lifecourse_demand_trajectory(pop_by_age_year, years = years, scenario = scenario,
                                       n = n, seed = NULL,
                                       risk_params = dp$risk, pathway_params = dp$pathway, ...)
    tr$demand_summary
  })
  all_draws <- dplyr::bind_rows(draws)
  q <- function(x, p) stats::quantile(x, p, names = FALSE, na.rm = TRUE)
  dplyr::summarise(
    dplyr::group_by(all_draws, .data$year),
    care_seeking_national     = q(.data$care_seeking_national, probs[2]),
    care_seeking_national_lo  = q(.data$care_seeking_national, probs[1]),
    care_seeking_national_hi  = q(.data$care_seeking_national, probs[3]),
    service_units_national     = q(.data$service_units_national, probs[2]),
    service_units_national_lo  = q(.data$service_units_national, probs[1]),
    service_units_national_hi  = q(.data$service_units_national, probs[3]),
    .groups = "drop"
  )
}
