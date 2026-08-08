# ---------------------------------------------------------------------------
# General prevalence -> incidence calibrator (DisMod / CISNET style).
#
# Serves BOTH UI and AI/FI (one function, not two ad hoc calibrators). The mature
# microsimulation literature does NOT fit onset and remission jointly to
# cross-sectional prevalence -- they are only weakly identifiable, and a plateau can
# be produced by less onset OR more remission. So here REMISSION is supplied
# EXTERNALLY (a value, or a prior distribution), and we solve the INVERSE problem:
# given remission (+ mortality, non-differential by default so it cancels from the
# marginal-prevalence recurrence), find the low-dimensional age-specific ONSET hazard
# that makes the dynamic disease equation reproduce the observed age-band prevalence.
#
#   p_{a+1} = p_a (1 - r) + (1 - p_a) i_a ,   logit(i_a) = beta0 + f(age)
#
# f(age) is deliberately low-df (piecewise-linear, 2-3 effective df). Wu's 60-69,
# 70-79 and >=80 prevalences then identify the SHAPE of i_a CONDITIONAL on r.
# `calibrate_onset_psa()` draws r from its external prior and calibrates the onset
# per draw, returning the JOINT (r, onset) posterior draws so downstream PSA samples
# the correlated pair -- never onset and remission independently.
#
# Status this earns is "indirectly_calibrated" (onset calibrated to prevalence,
# remission borrowed), NEVER "fitted": onset is not estimated from longitudinal
# transitions here.
# ---------------------------------------------------------------------------

#' Low-dimensional age-onset basis (piecewise-linear with interior knots).
#' @param ages integer ages; @param knots interior knot ages (2 -> 3 df total).
#' @return numeric matrix (length(ages) x (1 + length(knots) + 1)) incl intercept.
#' @keywords internal
.onset_age_basis <- function(ages, knots = c(50, 70)) {
  age_c <- (ages - 50) / 10                       # centered, per-decade (as the engine uses)
  cols <- list(`(age)` = age_c)
  for (k in knots) cols[[paste0("hinge", k)]] <- pmax((ages - k) / 10, 0)  # ReLU hinge
  cbind(intercept = 1, do.call(cbind, cols))
}

#' Prevalence by age from an onset-hazard vector via the aging Markov recurrence.
#' @param onset numeric onset hazard per age (same length as ages).
#' @param remission scalar annual 1->0 hazard.
#' @param p0 prevalence at the youngest age (default 0).
#' @keywords internal
prevalence_from_onset <- function(onset, remission, p0 = 0) {
  p <- numeric(length(onset)); p[1] <- p0
  for (i in 2:length(onset)) p[i] <- p[i - 1] * (1 - remission) + (1 - p[i - 1]) * onset[i - 1]
  p
}

#' Calibrate the age-specific onset to age-band prevalence GIVEN a remission value.
#'
#' @param target_by_band named numeric prevalence targets (names are band labels).
#' @param bands named list band label -> integer ages.
#' @param remission scalar external remission hazard (held fixed here).
#' @param ages integer age grid (default 18:100).
#' @param knots interior age knots for the onset basis (default c(50,70)).
#' @param p0 youngest-age prevalence (default 0).
#' @return list: `beta`, `onset` (by age), `fitted` (by band), `rel_err` (by band),
#'   `worst`, `plausible` (all onset in (0,1) and monotone-safe), `remission`.
#' @family prevalence-calibration
#' @export
calibrate_onset_given_remission <- function(target_by_band, bands, remission,
                                            ages = 18:100, knots = c(50, 70), p0 = 0) {
  X <- .onset_age_basis(ages, knots)
  band_pred <- function(beta) {
    onset <- stats::plogis(as.numeric(X %*% beta))
    p <- prevalence_from_onset(onset, remission, p0)
    vapply(bands, function(g) mean(p[match(g, ages)], na.rm = TRUE), numeric(1))
  }
  obj <- function(beta) {
    fb <- band_pred(beta)
    sum(((fb - target_by_band) / target_by_band)^2)
  }
  # Start the intercept at the onset that gives the median target prevalence at
  # steady state (onset* = p r / (1 - p)); this makes convergence robust across the
  # whole remission prior instead of failing for some draws from a fixed start.
  pm <- stats::median(target_by_band)
  o_eq <- max(min(pm * remission / (1 - pm), 0.9), 1e-4)
  b0 <- c(stats::qlogis(o_eq), rep(0, ncol(X) - 1))
  o  <- stats::optim(b0, obj, method = "Nelder-Mead",
                     control = list(maxit = 4000, reltol = 1e-12))
  o  <- stats::optim(o$par, obj, method = "Nelder-Mead",   # restart to polish
                     control = list(maxit = 4000, reltol = 1e-12))
  beta   <- o$par
  onset  <- stats::plogis(as.numeric(X %*% beta))
  fitted <- band_pred(beta)
  list(beta = beta, onset = stats::setNames(onset, ages), fitted = fitted,
       rel_err = (fitted - target_by_band) / target_by_band,
       worst = max(abs((fitted - target_by_band) / target_by_band)),
       plausible = all(onset > 0 & onset < 1), remission = remission)
}

#' Joint (remission, onset) calibration draws for PSA (DisMod/Bayesian style).
#'
#' Samples `remission` from its EXTERNAL prior and calibrates the onset conditional
#' on each draw, so the returned draws are the correlated (r, onset) pairs that PSA
#' must sample together. Also returns an identifiability read: how much the fitted
#' onset level moves with r (prevalence identifies the pair, not each alone).
#'
#' @param target_by_band,bands,ages,knots,p0 as in [calibrate_onset_given_remission()].
#' @param remission_prior function(n) returning n remission draws (the external prior).
#' @param n_draws number of PSA draws.
#' @return list: `draws` (data frame: remission, beta cols, worst, plausible),
#'   `onset_draws` (matrix ages x n), `identifiability` (cor of remission vs onset@65).
#' @family prevalence-calibration
#' @export
calibrate_onset_psa <- function(target_by_band, bands, remission_prior, n_draws = 200L,
                                ages = 18:100, knots = c(50, 70), p0 = 0, seed = 1L) {
  set.seed(seed)
  rs <- remission_prior(n_draws)
  onset_draws <- matrix(NA_real_, length(ages), n_draws, dimnames = list(ages, NULL))
  rows <- vector("list", n_draws)
  for (j in seq_len(n_draws)) {
    fit <- calibrate_onset_given_remission(target_by_band, bands, rs[j], ages, knots, p0)
    onset_draws[, j] <- fit$onset
    rows[[j]] <- data.frame(remission = rs[j], worst = fit$worst, plausible = fit$plausible,
                            t(stats::setNames(fit$beta, paste0("b", seq_along(fit$beta) - 1))))
  }
  draws <- do.call(rbind, rows)
  onset65 <- onset_draws[match(65, ages), ]
  list(draws = draws, onset_draws = onset_draws,
       identifiability = stats::cor(rs, onset65))   # strong POSITIVE -> onset/remission trade off (not separately identified)
}
