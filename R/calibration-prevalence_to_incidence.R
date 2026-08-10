# ---------------------------------------------------------------------------
# Prevalence-consistent onset inference, DisMod style (reproduced in R).
#
# Methodological reference: IHME dismod_at (actively maintained, 2026 releases,
# https://dismod-at.readthedocs.io) and its predecessor dismod_mr. We do NOT
# install either -- dismod_mr is Python 3.6 / PyMC 2.3.8 -- we reproduce the one
# component we need: inferring an age-specific ONSET (incidence) hazard that is
# epidemiologically consistent with observed age-band PREVALENCE, given an
# externally supplied remission and mortality assumptions.
#
# DISEASE EQUATION. DisMod represents women as susceptible (S) or with the
# condition (C); with incidence i(a), remission r(a), excess mortality e(a) and
# other mortality m(a). For UI and FI the condition does not materially alter
# mortality, so e(a)=0, and other-mortality is non-differential and cancels from
# the MARGINAL prevalence, leaving
#
#     dP/da = (1 - P(a)) i(a) - P(a) r(a).
#
# The annual recurrence the engine already uses,
#
#     p_{a+1} = p_a (1 - r) + (1 - p_a) i_a ,
#
# is the Euler discretization of that ODE, so the mathematics is unchanged. What
# this file adds is HOW the unknown i(a) is inferred:
#
#   1. INTERVAL-AWARE, POPULATION-WEIGHTED targets. Wu reports prevalence over age
#      intervals (60-69, 70-79, >=80). We integrate the modeled curve over each
#      interval weighted by the age distribution of US women, not a flat mean:
#          Phat_band = sum_{a in band} w_a P(a) / sum_{a in band} w_a .
#   2. OBSERVATION LIKELIHOOD with reported uncertainty. Instead of minimizing
#      MAPE (which treats every band as exact and equally important), we evaluate
#      P(Wu observations | i, r) using each band's standard error -- derived from
#      the reported CI when only a CI is given -- on the logit scale. A band with a
#      wide CI constrains the curve less, exactly as it should.
#   3. PARSIMONIOUS age model with a SMOOTHNESS prior. Wu gives only a few older-age
#      bands, nowhere near enough to justify a flexible spline. Onset is modeled on
#      the log-rate scale as a low-df hinge basis (level + slope + one knot), with a
#      Normal(0, tau^2) smoothness prior on the slope/curvature terms -- DisMod's
#      penalized-rate idea, kept interpretable.
#   4. IDENTIFIABILITY handled honestly. Prevalence alone cannot identify i and r
#      jointly (one trajectory, two rates). So r is supplied EXTERNALLY as a value
#      or a prior; conditional on each r we MAP-fit i(a); PSA returns the correlated
#      (r, i) pairs so downstream sampling never draws them independently.
#
# Status this earns is at most "fitted" on the disease_burden dimension (onset fit
# to observed prevalence), with the borrowed remission recorded as an assumption in
# the evidence layer -- never folded into the status string. POP is NOT modeled here
# (its natural history is staged, not two-state); use the stage-transition model.
# ---------------------------------------------------------------------------

#' Build a prevalence observation with a usable standard error (DisMod style).
#'
#' Standardizes a reported age-band prevalence to (age_start, age_end, prevalence,
#' se). If only a confidence interval is given, the SE is derived from it
#' (se = (ci_upper - ci_lower) / (2 * z)); if only a sample size is given, the
#' binomial SE is used. Failing loudly when neither uncertainty source is present
#' keeps an "exact" observation from silently dominating the likelihood.
#'
#' @param band Label for the age band.
#' @param age_start,age_end Inclusive integer ages of the interval.
#' @param prevalence observed prevalence in (0,1).
#' @param ci_lower,ci_upper reported CI bounds (optional).
#' @param se reported standard error (optional, takes precedence).
#' @param n sample size for a binomial SE (optional, last resort).
#' @param conf CI confidence level (default 0.95).
#' @return one-row data frame: band, age_start, age_end, prevalence, se.
#' @family prevalence-calibration
#' @export
prevalence_observation <- function(band, age_start, age_end, prevalence,
                                   ci_lower = NA_real_, ci_upper = NA_real_,
                                   se = NA_real_, n = NA_real_, conf = 0.95) {
  stopifnot(prevalence > 0, prevalence < 1, age_end >= age_start)
  if (is.na(se)) {
    if (!is.na(ci_lower) && !is.na(ci_upper)) {
      z <- stats::qnorm(1 - (1 - conf) / 2)
      se <- (ci_upper - ci_lower) / (2 * z)
    } else if (!is.na(n) && n > 0) {
      se <- sqrt(prevalence * (1 - prevalence) / n)
    } else {
      stop(sprintf("prevalence_observation('%s'): supply se, a CI, or n -- an ",
                   band), "observation with no uncertainty would dominate the likelihood.",
           call. = FALSE)
    }
  }
  if (!(se > 0)) stop(sprintf("prevalence_observation('%s'): se must be > 0.", band), call. = FALSE)
  data.frame(band = band, age_start = as.integer(age_start), age_end = as.integer(age_end),
             prevalence = prevalence, se = se, stringsAsFactors = FALSE)
}

#' Low-df log-incidence age basis (level + slope + interior-knot hinges).
#'
#' Parsimonious by design: Wu's few older-age bands cannot justify a flexible
#' spline. Modeled on the LOG-RATE scale so incidence is positive by construction.
#' @param ages integer ages; @param knots interior knot ages (default 60,70 --
#'   where Wu's older-age data live). 2 knots -> 4 columns (3 non-intercept df).
#' @return numeric matrix length(ages) x (2 + length(knots)).
#' @keywords internal
.log_incidence_basis <- function(ages, knots = c(60, 70)) {
  cols <- list(slope = (ages - 60) / 10)                       # per-decade slope, centered at 60
  for (k in knots) cols[[paste0("hinge", k)]] <- pmax((ages - k) / 10, 0)
  cbind(intercept = 1, do.call(cbind, cols))
}

#' Annual onset probability from a log-incidence rate (rate -> probability).
#' i_prob = 1 - exp(-exp(eta)); stays in (0,1), ~ exp(eta) for small rates.
#' @keywords internal
.incidence_prob <- function(eta) 1 - exp(-exp(eta))

#' Prevalence by age from an onset (incidence) hazard via the aging recurrence.
#'
#' The discrete (Euler) form of dP/da = (1-P) i - P r. `remission` is the annual
#' 1->0 probability (external). `prevalence_from_onset` is kept as an alias.
#' @param incidence numeric annual onset probability per age (same length as ages).
#' @param remission scalar annual remission probability.
#' @param p0 prevalence at the youngest age (default 0).
#' @family prevalence-calibration
#' @export
prevalence_from_incidence <- function(incidence, remission, p0 = 0) {
  # Every argument is a PROBABILITY, and the recurrence has no restoring force:
  # a remission above 1 drives prevalence negative (0.40 -> -0.20 -> 0.10 ...)
  # and an incidence above 1 drives it past 1, both in silence, and a negative
  # prevalence becomes a negative case count in every downstream demand total.
  # Same class as the negative provider counts (cycle 03) and the sum-to-one
  # validators missing a range check (cycle 04).
  chk <- function(v, nm) {
    if (!is.numeric(v) || any(!is.finite(v)) || any(v < 0) || any(v > 1)) {
      stop(sprintf(paste("prevalence_from_incidence: `%s` must be finite and in [0, 1];",
                         "it is a probability. The aging recurrence has no bound of its",
                         "own, so a value outside the unit interval yields prevalence",
                         "outside it too."), nm), call. = FALSE)
    }
  }
  chk(incidence, "incidence")
  chk(remission, "remission")
  chk(p0, "p0")
  if (length(remission) != 1L)
    stop("prevalence_from_incidence: `remission` must be a single probability.", call. = FALSE)
  n <- length(incidence)
  if (n == 0L) return(numeric(0))

  p <- numeric(n); p[1] <- p0
  # seq_len(n)[-1], not 2:n. At n = 1 the latter counts DOWN (2, 1), which grew
  # `p` to length 2 and then failed inside R with "replacement has length zero"
  # -- a legitimate single-age grid crashing on an index, not on its inputs.
  for (i in seq_len(n)[-1]) {
    p[i] <- p[i - 1] * (1 - remission) + (1 - p[i - 1]) * incidence[i - 1]
  }
  p
}
#' @rdname prevalence_from_incidence
#' @export
prevalence_from_onset <- prevalence_from_incidence

# population-weighted mean prevalence over an inclusive age interval
.band_prevalence <- function(p, ages, a0, a1, weights) {
  idx <- which(ages >= a0 & ages <= a1)
  w <- weights[idx]
  sum(w * p[idx]) / sum(w)
}

#' Fit an epidemiologically consistent onset hazard to age-band prevalence.
#'
#' DisMod-style inverse problem: given an EXTERNAL remission, find the low-df
#' age-specific onset that makes the disease recurrence reproduce the observed
#' age-band prevalence, scoring the fit by a CI-informed logit-Gaussian likelihood
#' with population-weighted interval integration and a Normal smoothness prior on
#' the log-rate shape (a penalized MAP estimate).
#'
#' @param observations data frame of [prevalence_observation()] rows
#'   (band, age_start, age_end, prevalence, se).
#' @param remission scalar external annual remission probability (held fixed).
#' @param ages integer age grid (default 18:100).
#' @param knots interior knots for the log-incidence basis (default c(60,70)).
#' @param p0 youngest-age prevalence (default 0).
#' @param age_weights named numeric (by age) population weights for interval
#'   integration; default uniform. Production use passes US-female age weights.
#' @param smooth_sd Normal prior SD (tau) on the non-intercept log-rate coefficients
#'   (the smoothness prior). Smaller = smoother. Default 1.0.
#' @return list: `beta`, `incidence` (by age), `fitted` (by band), `z` (per-band
#'   standardized residual), `compatible` (all |z|<2), `loglik`, `worst_z`,
#'   `plausible` (all incidence in (0,1)), `remission`.
#' @family prevalence-calibration
#' @export
fit_prevalence_consistent_transitions <- function(observations, remission,
                                                  ages = 18:100, knots = c(60, 70),
                                                  p0 = 0, age_weights = NULL,
                                                  smooth_sd = 1.0) {
  stopifnot(all(c("band", "age_start", "age_end", "prevalence", "se") %in% names(observations)))
  X <- .log_incidence_basis(ages, knots)
  if (is.null(age_weights)) {
    w <- rep(1, length(ages))
  } else {
    w <- age_weights[as.character(ages)]
    if (anyNA(w)) stop("fit_prevalence_consistent_transitions: age_weights missing some ages.", call. = FALSE)
  }
  obs_logit <- stats::qlogis(observations$prevalence)
  # delta-method SE on the logit scale: d logit/dp = 1/(p(1-p))
  obs_logit_sd <- observations$se / (observations$prevalence * (1 - observations$prevalence))

  band_pred <- function(beta) {
    incidence <- .incidence_prob(as.numeric(X %*% beta))
    p <- prevalence_from_incidence(incidence, remission, p0)
    vapply(seq_len(nrow(observations)), function(k)
      .band_prevalence(p, ages, observations$age_start[k], observations$age_end[k], w), numeric(1))
  }
  # negative log-posterior: CI-weighted logit-Gaussian data term + smoothness prior
  nlp <- function(beta) {
    fb <- band_pred(beta)
    fb <- pmin(pmax(fb, 1e-8), 1 - 1e-8)
    data  <- sum(((stats::qlogis(fb) - obs_logit) / obs_logit_sd)^2) / 2
    prior <- sum((beta[-1] / smooth_sd)^2) / 2            # Normal(0, tau^2) on shape terms
    data + prior
  }
  # equilibrium-based start for the intercept: onset* ~ p r / (1 - p) at median target
  pm <- stats::median(observations$prevalence)
  eta0 <- log(-log(1 - max(min(pm * remission / (1 - pm), 0.9), 1e-4)))   # invert .incidence_prob
  b0 <- c(eta0, rep(0, ncol(X) - 1))
  o  <- stats::optim(b0, nlp, method = "Nelder-Mead", control = list(maxit = 5000, reltol = 1e-12))
  o  <- stats::optim(o$par, nlp, method = "Nelder-Mead", control = list(maxit = 5000, reltol = 1e-12))
  beta <- o$par
  incidence <- .incidence_prob(as.numeric(X %*% beta))
  fitted <- band_pred(beta)
  z <- (fitted - observations$prevalence) / observations$se
  list(beta = beta, incidence = stats::setNames(incidence, ages),
       fitted = stats::setNames(fitted, observations$band),
       z = stats::setNames(z, observations$band),
       compatible = all(abs(z) < 2), worst_z = max(abs(z)),
       loglik = -o$value, plausible = all(incidence > 0 & incidence < 1),
       remission = remission)
}

#' Joint (remission, onset) posterior draws for PSA (DisMod / Bayesian style).
#'
#' Samples remission from its EXTERNAL prior and MAP-fits the onset conditional on
#' each draw, returning the correlated (r, onset) pairs PSA must sample together.
#' Prevalence identifies the PAIR, not each rate alone: to hold the same prevalence
#' a higher remission must be met by a higher onset, so the fitted onset level is
#' strongly positively correlated with r across the prior (the identifiability read).
#'
#' @param observations,ages,knots,p0,age_weights,smooth_sd as in
#'   [fit_prevalence_consistent_transitions()].
#' @param remission_prior function(n) returning n external remission draws.
#' @param n_draws number of PSA draws.
#' @param report_age age at which to read the onset level for the identifiability
#'   correlation (default 65).
#' @param seed RNG seed for the draws, so a reported identifiability correlation
#'   is reproducible.
#' @return list: `draws` (data frame: remission, onset_at_report_age, worst_z,
#'   compatible, plausible), `incidence_draws` (matrix ages x n_draws),
#'   `identifiability` (cor of remission vs onset@report_age).
#' @family prevalence-calibration
#' @export
fit_prevalence_consistent_psa <- function(observations, remission_prior, n_draws = 200L,
                                          ages = 18:100, knots = c(60, 70), p0 = 0,
                                          age_weights = NULL, smooth_sd = 1.0,
                                          report_age = 65, seed = 1L) {
  .preserve_rng_scope()
  set.seed(seed)
  rs <- remission_prior(n_draws)
  inc <- matrix(NA_real_, length(ages), n_draws, dimnames = list(ages, NULL))
  rows <- vector("list", n_draws)
  for (j in seq_len(n_draws)) {
    fit <- fit_prevalence_consistent_transitions(observations, rs[j], ages, knots, p0,
                                                 age_weights, smooth_sd)
    inc[, j] <- fit$incidence
    rows[[j]] <- data.frame(remission = rs[j],
                            onset_at_report_age = unname(fit$incidence[match(report_age, ages)]),
                            worst_z = fit$worst_z, compatible = fit$compatible,
                            plausible = fit$plausible)
  }
  draws <- do.call(rbind, rows)
  list(draws = draws, incidence_draws = inc,
       identifiability = stats::cor(rs, inc[match(report_age, ages), ]))
}
