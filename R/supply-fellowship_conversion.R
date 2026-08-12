# Fellowship-to-practice conversion ------------------------------------------
#
# WHAT THIS IS AND IS NOT.
#
# There is no fellowship roster in this project -- no list of who began a URPS
# fellowship in which year. Individual-level linkage from a fellow to their
# certification and first billing is therefore IMPOSSIBLE with local data, and
# nothing here should be read as one. What follows is a COHORT-LEVEL
# deconvolution: it links the NRMP count of positions filled in appointment year
# y to the count of outcomes observed in later years, and recovers both the
# conversion fraction and the lag distribution that best reconcile the two
# series.
#
# The model is
#
#     outcome_t  =  theta * sum_k w_k * filled_{t-k} + e_t
#
# with theta the conversion fraction (outcomes per filled position), w a
# probability distribution over lags k (non-negative, sums to 1), and e
# residual. theta and w are estimated jointly.
#
# theta ABOVE 1.0 IS NOT CLIPPED. A conversion above one is impossible as a
# conversion and is the signature of a misalignment between the two series --
# wrong pathway, wrong geography, or a backlog year contaminating the outcome.
# `supply-acgme_fellows.R` already treats it that way and so does this. Clipping
# it at 1 would hide the diagnostic.
#
# WHY BOTH OUTCOMES ARE OFFERED. Certification counts are an exam-scheduling
# record, not a cohort flow: new ABOG URPS certifications run 166, 87, 26, 30,
# 34, 35, 3, 72, 42, 61 across 2014-2023, where 2014 is the subspecialty's
# founding backlog and 2020 is the cancelled exam. First observed Medicare
# billing is not exam-driven and is closer to "entered practice", but it is
# Medicare-only and roster-limited. Neither is authoritative; they are fitted
# separately so the disagreement stays visible.

#' Lags (years from fellowship appointment to outcome) searched by default
#'
#' A URPS fellowship is three years, so the earliest plausible appointment-to-
#' practice lag is 3. Certification additionally requires written and oral
#' examinations, pushing that outcome later. The support starts at 2 rather than
#' 3 so that a misspecified series shows up as mass piling on the boundary
#' instead of being silently excluded.
#' @family fellowship conversion
#' @concept supply
#' @export
FELLOWSHIP_DEFAULT_LAGS <- 2:8

#' Roster coverage of the nationally certified ABOG URPS population
#'
#' The roster carries 830 ABOG-pathway members against 1,027 nationally
#' certified in 2023. Outcome counts drawn from the roster are therefore a
#' fraction of the national flow, and a conversion computed against national
#' NRMP counts without this adjustment is biased DOWN by roughly a fifth.
#' @family fellowship conversion
#' @concept supply
#' @export
FELLOWSHIP_ROSTER_ABOG_COVERAGE <- 830 / 1027

.fc_stop <- function(...) stop("fellowship conversion: ", ..., call. = FALSE)

.fc_named_num <- function(x, arg) {
  if (!is.numeric(x) || is.null(names(x))) {
    .fc_stop(sprintf("`%s` must be a NAMED numeric vector keyed by year; got %s%s.",
                     arg, class(x)[1],
                     if (is.numeric(x)) " with no names" else ""))
  }
  yr <- suppressWarnings(as.integer(names(x)))
  if (anyNA(yr)) {
    bad <- names(x)[is.na(yr)]
    .fc_stop(sprintf("`%s` has %d name(s) that are not years, e.g. '%s'. Name the vector with calendar years.",
                     arg, length(bad), bad[1]))
  }
  if (any(!is.finite(x))) {
    .fc_stop(sprintf("`%s` has %d non-finite value(s) at year(s) %s.",
                     arg, sum(!is.finite(x)),
                     paste(names(x)[!is.finite(x)], collapse = ", ")))
  }
  stats::setNames(as.numeric(x), as.character(yr))
}

#' Count of roster providers by first observed Medicare Part B year
#'
#' The entry signal that does not depend on exam scheduling. A provider's first
#' observed billing year is taken as their entry to practice.
#'
#' LEFT CENSORING IS DROPPED, NOT MODELLED. The Part B panel begins in 2013, so
#' every provider already practising then registers a first-billing year of
#' 2013. That year is excluded rather than counted as a 2013 entry cohort.
#'
#' @param path Path to `provider_year_activity_long.csv`; found in the installed
#'   package when NULL.
#' @param pathway Board pathway to keep. The NRMP URPS match feeds the ABOG
#'   pathway; ABU members enter through urology and are NOT matched by it, so
#'   including them inflates the outcome against an NRMP denominator.
#' @param definition Activity column defining "billing" (see the D0-D4
#'   comparison). Defaults to any Part B activity.
#' @return Named numeric vector of entry counts keyed by year.
#' @family fellowship conversion
#' @concept supply
#' @export
fellowship_first_billing_series <- function(path = NULL,
                                            pathway = "ABOG",
                                            definition = "d1_any_partb") {
  if (is.null(path)) {
    path <- system.file("extdata", "provider_year",
                        "provider_year_activity_long.csv", package = "simulation")
  }
  if (!nzchar(path) || !file.exists(path)) {
    .fc_stop("provider_year_activity_long.csv not found",
             if (nzchar(path)) paste0(" at '", path, "'") else "",
             ". Build it with data-raw/provider_year_activity/build_provider_year_activity.R.")
  }
  d <- utils::read.csv(path, stringsAsFactors = FALSE)
  if (!definition %in% names(d)) {
    .fc_stop(sprintf("`definition` = '%s' is not a column of the panel. Available: %s.",
                     definition, paste(grep("^d[0-4]_", names(d), value = TRUE), collapse = ", ")))
  }
  if (!is.null(pathway)) {
    if (!pathway %in% unique(d$board_pathway)) {
      .fc_stop(sprintf("`pathway` = '%s' is not present; the panel has %s.",
                       pathway, paste(unique(d$board_pathway), collapse = ", ")))
    }
    d <- d[d$board_pathway == pathway, , drop = FALSE]
  }
  act <- d[which(d[[definition]] %in% TRUE), , drop = FALSE]
  if (!nrow(act)) .fc_stop("no provider-years satisfy '", definition, "'.")

  first <- tapply(act$year, act$npi, min)
  panel_start <- min(d$year)
  first <- first[first > panel_start]          # drop the left-censored pile-up
  tab <- table(first)
  stats::setNames(as.numeric(tab), names(tab))
}

#' New certifications per year from the contract series
#'
#' The cumulative certification series differenced into an annual flow.
#'
#' @param pathway Contract board pathway.
#' @param geography Contract geography.
#' @param series Optional pre-fetched contract series.
#' @return Named numeric vector of new certifications keyed by year.
#' @family fellowship conversion
#' @concept supply
#' @export
fellowship_certification_series <- function(pathway = "ABOG",
                                            geography = "national",
                                            series = NULL) {
  if (is.null(series)) {
    .require_mufflyaccess("The certification flow series")
    series <- mufflyaccess::urps_counts_long()
  }
  s <- series[series$geography == geography &
                series$measure == "board_certified_active" &
                series$board_pathway == pathway, , drop = FALSE]
  if (!nrow(s)) {
    .fc_stop(sprintf("no contract rows for pathway '%s', geography '%s'.",
                     pathway, geography))
  }
  s <- s[order(s$year), , drop = FALSE]
  # The series is cumulative and must never decrease; a negative difference means
  # strata were mixed, which is exactly the bug that made an earlier hand
  # calculation return conversions near 16.
  flow <- diff(s$n_active)
  if (any(flow < 0)) {
    .fc_stop(sprintf("differencing produced %d negative annual flow(s) (e.g. %+.0f at %d). The series is not a single monotone stratum.",
                     sum(flow < 0), flow[which(flow < 0)[1]], s$year[-1][which(flow < 0)[1]]))
  }
  stats::setNames(as.numeric(flow), as.character(s$year[-1]))
}

#' Steady-state conversion over a window, given a fixed lag
#'
#' The estimator that does not require identifying the lag distribution: over a
#' window, total outcomes divided by total positions filled `lag` years earlier.
#' Robust when the shape of `w` is weakly identified, which it usually is.
#'
#' @param filled,outcome Named numeric vectors keyed by year.
#' @param lag Integer lag in years.
#' @param coverage Fraction of the national outcome population the outcome
#'   series observes; counts are divided by it.
#' @param years Optional outcome years to restrict to.
#' @return One-row data frame with the conversion and the window used.
#' @family fellowship conversion
#' @concept supply
#' @export
fellowship_conversion_steady_state <- function(filled, outcome, lag,
                                               coverage = 1, years = NULL) {
  filled <- .fc_named_num(filled, "filled")
  outcome <- .fc_named_num(outcome, "outcome")
  if (!is.numeric(coverage) || length(coverage) != 1L ||
      !is.finite(coverage) || coverage <= 0 || coverage > 1) {
    .fc_stop(sprintf("`coverage` must be a single number in (0, 1]; got %s.",
                     paste(format(coverage), collapse = ", ")))
  }
  oy <- as.integer(names(outcome))
  if (!is.null(years)) oy <- intersect(oy, as.integer(years))
  usable <- oy[as.character(oy - lag) %in% names(filled)]
  if (length(usable) < 2L) {
    .fc_stop(sprintf("lag %d leaves %d usable outcome year(s); need at least 2. Outcome spans %s, filled spans %s.",
                     lag, length(usable),
                     paste(range(as.integer(names(outcome))), collapse = "-"),
                     paste(range(as.integer(names(filled))), collapse = "-")))
  }
  num <- sum(outcome[as.character(usable)]) / coverage
  den <- sum(filled[as.character(usable - lag)])
  data.frame(lag = lag, conversion = num / den,
             outcome_total = num, filled_total = den,
             n_years = length(usable),
             window_start = min(usable), window_end = max(usable))
}

#' Fit the conversion fraction and lag distribution jointly
#'
#' Constrained least squares on `outcome_t = theta * sum_k w_k * filled_{t-k}`.
#' `w` is carried on a softmax so it stays a probability vector without a
#' constrained optimiser, and `theta` on a log scale so it stays positive
#' WITHOUT being capped at 1 (see the file header).
#'
#' IDENTIFIABILITY IS REPORTED, NOT ASSUMED. With ten or so outcome years and a
#' seven-point lag support, `w` is weakly identified: many shapes fit almost
#' equally well. The returned object carries `n_obs`, `n_params` and the
#' steady-state comparison so a reader can see how much the shape is being
#' asked to do.
#'
#' @param filled Named numeric vector of NRMP positions filled, keyed by
#'   appointment year.
#' @param outcome Named numeric vector of outcomes, keyed by outcome year.
#' @param lags Integer lags to search.
#' @param coverage Fraction of the national outcome population observed.
#' @param exclude_years Outcome years to drop, for administrative artefacts that
#'   are not cohort flow -- the 2014 founding backlog of a subspecialty whose
#'   certification began in 2013, and the 2020 cancelled examination. Dropping
#'   them is a judgement about what the outcome series MEASURES, so it is an
#'   explicit argument, is recorded on the returned object, and is printed. It is
#'   never applied by default: a caller who does not ask keeps every year.
#' @param n_starts Random restarts for the optimiser.
#' @param seed RNG seed; the RNG state is restored on exit.
#' @return Object of class `fellowship_conversion`.
#' @family fellowship conversion
#' @concept supply
#' @export
fit_fellowship_conversion <- function(filled, outcome,
                                      lags = FELLOWSHIP_DEFAULT_LAGS,
                                      coverage = 1,
                                      exclude_years = NULL,
                                      n_starts = 24L,
                                      seed = 20260811L) {
  filled <- .fc_named_num(filled, "filled")
  outcome <- .fc_named_num(outcome, "outcome")
  lags <- sort(unique(as.integer(lags)))
  if (anyNA(lags) || any(lags < 0)) {
    .fc_stop("`lags` must be non-negative whole numbers.")
  }
  if (!is.numeric(coverage) || length(coverage) != 1L ||
      !is.finite(coverage) || coverage <= 0 || coverage > 1) {
    .fc_stop(sprintf("`coverage` must be a single number in (0, 1]; got %s.",
                     paste(format(coverage), collapse = ", ")))
  }

  dropped <- integer(0)
  if (!is.null(exclude_years)) {
    ex <- as.integer(exclude_years)
    if (anyNA(ex)) .fc_stop("`exclude_years` must be whole calendar years.")
    unknown <- setdiff(ex, as.integer(names(outcome)))
    if (length(unknown)) {
      .fc_stop(sprintf("`exclude_years` names %d year(s) not in the outcome series: %s. Outcome spans %s.",
                       length(unknown), paste(unknown, collapse = ", "),
                       paste(range(as.integer(names(outcome))), collapse = "-")))
    }
    dropped <- sort(ex)
    outcome <- outcome[!(as.integer(names(outcome)) %in% ex)]
    if (!length(outcome)) .fc_stop("`exclude_years` removed every outcome year.")
  }

  # An outcome year is usable only when EVERY lagged predictor exists, so the
  # design matrix is never silently ragged across candidate lags.
  oy <- as.integer(names(outcome))
  ok <- vapply(oy, function(t) all(as.character(t - lags) %in% names(filled)), logical(1))
  ty <- oy[ok]
  if (length(ty) < length(lags)) {
    .fc_stop(sprintf("only %d outcome year(s) have all %d lagged predictors available; the fit would be underdetermined. Outcome spans %s, filled spans %s. Narrow `lags`.",
                     length(ty), length(lags),
                     paste(range(oy), collapse = "-"),
                     paste(range(as.integer(names(filled))), collapse = "-")))
  }
  X <- outer(ty, lags, function(t, k) filled[as.character(t - k)])
  dim(X) <- c(length(ty), length(lags))
  y <- as.numeric(outcome[as.character(ty)]) / coverage

  sse <- function(par) {
    theta <- exp(par[1])
    w <- exp(c(0, par[-1])); w <- w / sum(w)
    sum((y - theta * as.vector(X %*% w))^2)
  }

  best <- NULL
  .preserve_rng_scope()
  set.seed(seed)
  starts <- c(list(c(log(max(sum(y) / sum(X[, 1]), 1e-6)), rep(0, length(lags) - 1L))),
              lapply(seq_len(n_starts), function(i)
                c(stats::rnorm(1, 0, 0.7), stats::rnorm(length(lags) - 1L, 0, 1.2))))
  for (p0 in starts) {
    fit <- tryCatch(stats::optim(p0, sse, method = "BFGS",
                                 control = list(maxit = 2000, reltol = 1e-12)),
                    error = function(e) NULL)
    if (!is.null(fit) && (is.null(best) || fit$value < best$value)) best <- fit
  }
  if (is.null(best)) .fc_stop("the optimiser failed from every start.")

  theta <- exp(best$par[1])
  w <- exp(c(0, best$par[-1])); w <- w / sum(w)
  fitted <- theta * as.vector(X %*% w)
  ss_tot <- sum((y - mean(y))^2)

  structure(list(
    conversion = theta,
    lag_weights = stats::setNames(w, lags),
    mean_lag = sum(lags * w),
    modal_lag = lags[which.max(w)],
    fitted = stats::setNames(fitted, ty),
    observed = stats::setNames(y, ty),
    residuals = stats::setNames(y - fitted, ty),
    sse = best$value,
    r_squared = if (ss_tot > 0) 1 - best$value / ss_tot else NA_real_,
    n_obs = length(ty),
    n_params = length(lags),          # theta + (K-1) free weights
    coverage = coverage,
    lags = lags,
    excluded_years = dropped,
    steady_state = do.call(rbind, lapply(lags, function(k)
      tryCatch(fellowship_conversion_steady_state(filled, outcome, k, coverage),
               error = function(e) NULL)))
  ), class = "fellowship_conversion")
}

#' @param x A `fellowship_conversion` object.
#' @param ... Unused.
#' @rdname fit_fellowship_conversion
#' @export
print.fellowship_conversion <- function(x, ...) {
  cat("Fellowship-to-practice conversion (cohort-level deconvolution)\n")
  cat(sprintf("  conversion (theta) : %.3f outcomes per filled position\n", x$conversion))
  cat(sprintf("  mean lag           : %.2f yr   modal lag: %d yr\n", x$mean_lag, x$modal_lag))
  cat(sprintf("  fit                : R2 = %.3f on %d outcome years, %d parameters\n",
              x$r_squared, x$n_obs, x$n_params))
  if (length(x$excluded_years)) {
    cat(sprintf("  excluded years     : %s (caller-specified, not automatic)\n",
                paste(x$excluded_years, collapse = ", ")))
  }
  if (identical(x$modal_lag, min(x$lags)) || identical(x$modal_lag, max(x$lags))) {
    cat(sprintf("  WARNING: lag mass sits on the %s boundary (%d yr) -- the true lag may lie\n",
                if (identical(x$modal_lag, min(x$lags))) "lower" else "upper", x$modal_lag))
    cat("           outside `lags`, or the two series are misaligned.\n")
  }
  if (x$n_obs <= x$n_params + 2L) {
    cat("  NOTE: n_obs is close to n_params; the lag SHAPE is weakly identified.\n")
    cat("        Prefer the conversion and mean lag over individual weights.\n")
  }
  if (x$conversion > 1) {
    cat("  WARNING: conversion above 1.0 is impossible as a conversion and\n")
    cat("           indicates the two series are misaligned (pathway/geography/backlog).\n")
  }
  cat("  lag weights:\n")
  for (i in seq_along(x$lag_weights)) {
    cat(sprintf("    %2d yr : %.3f\n", x$lags[i], x$lag_weights[i]))
  }
  invisible(x)
}
