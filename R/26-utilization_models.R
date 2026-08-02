# Corrected utilization models ----
#
# Statistically corrected fitters for the two utilization inputs the life-course
# demand generator (R/25) and the DPMM rely on, addressing Part A of
# HDMM_IMPROVEMENT_PLAN.md:
#
#   * fit_sling_rate_model(): a Poisson/quasipoisson RATE model on a population-
#     at-risk log-offset, replacing the degenerate cases-only logistic (which,
#     with every row a case, is perfectly separated and estimates nothing).
#   * fit_visit_model(): a survey-weighted outpatient-visit model via
#     survey::svyglm when the MEPS design is supplied, falling back to MASS
#     negative binomial or a weighted quasipoisson (with an explicit warning that
#     the SEs are then not design-based).
#   * predict_count_with_uncertainty(): parameter-uncertainty propagation via
#     base-R multivariate-normal coefficient draws (no MASS dependency).
#
# Base R in the core paths (stats::glm, chol); survey and MASS are optional
# (Suggests) and used only when installed.

# ---- surgery: Poisson rate model on population at risk ---------------------

#' Fit a surgery-rate model (Poisson with population-at-risk offset)
#'
#' Replaces the degenerate cases-only logistic. Expects aggregated cells with a
#' surgery count and the population at risk in each cell; the count is modelled
#' with a log link and `log(pop_at_risk)` as an offset, so coefficients are
#' log-rate ratios and per-person expected surgeries come from a `pop_at_risk` of
#' 1. The offset is passed via the glm argument (not the formula), so prediction
#' via [predict_count_with_uncertainty()] never needs the offset column.
#'
#' @param cells Data frame of cells (e.g. age band x risk stratum) with the count,
#'   the offset column, and predictors.
#' @param formula Model formula for the COUNT (no `offset()` term). Default
#'   `n_slings ~ AgeGrp + Obesity + VaginalParity + Race`.
#' @param offset_col Name of the population-at-risk column (must be > 0).
#' @param weights_col Optional survey/frequency weights column.
#' @param overdispersion If TRUE (default) use quasipoisson; FALSE uses poisson.
#' @return A `glm` object; `attr(., "hdmm_offset_col")` records the offset name.
#' @export
fit_sling_rate_model <- function(cells,
                                 formula = n_slings ~ AgeGrp + Obesity + VaginalParity + Race,
                                 offset_col = "pop_at_risk",
                                 weights_col = NULL,
                                 overdispersion = TRUE) {
  stopifnot(is.data.frame(cells), offset_col %in% names(cells))
  par <- cells[[offset_col]]
  if (any(is.na(par)) || any(par <= 0)) {
    stop("fit_sling_rate_model(): '", offset_col,
         "' (population at risk) must be present and strictly positive.", call. = FALSE)
  }
  fam <- if (overdispersion) stats::quasipoisson(link = "log") else stats::poisson(link = "log")
  d <- cells
  d$.hdmm_off <- log(par)
  fit <- if (!is.null(weights_col)) {
    d$.hdmm_w <- cells[[weights_col]]
    stats::glm(formula, family = fam, data = d, offset = .hdmm_off, weights = .hdmm_w)
  } else {
    stats::glm(formula, family = fam, data = d, offset = .hdmm_off)
  }
  attr(fit, "hdmm_offset_col") <- offset_col
  fit
}

# ---- visits: survey-weighted count model (graceful fallback) ---------------

#' Fit an outpatient visit model, respecting the survey design when possible
#'
#' Preference order: (1) `survey::svyglm` quasipoisson on an svydesign built from
#' the weight/strata/PSU columns (correct design-based SEs), used when the survey
#' package is installed and `weights_col` is supplied; (2) `MASS::glm.nb` when
#' `prefer = "negbin"` and MASS is installed; (3) a weighted `stats::glm`
#' quasipoisson (always available) with a warning that the SEs are not
#' design-based.
#'
#' @param data Person-level MEPS-style data (already filtered to the population).
#' @param formula Visit-count model formula.
#' @param weights_col Person weight column (e.g. `PERWT21F`).
#' @param strata_col,psu_col Survey strata / PSU columns (e.g. `VARSTR`,`VARPSU`).
#' @param prefer Method preference; see Details.
#' @return The fitted model; `attr(., "hdmm_method")` records the path used.
#' @export
fit_visit_model <- function(data,
                            formula = UI_visits ~ Age + I(Age^2) + Obesity +
                              VaginalParity + ChronicCough + HeavyJob + Race + Hysterectomy,
                            weights_col = NULL, strata_col = NULL, psu_col = NULL,
                            prefer = c("survey", "negbin", "quasipoisson")) {
  stopifnot(is.data.frame(data))
  prefer <- match.arg(prefer)
  have_survey <- requireNamespace("survey", quietly = TRUE)
  have_mass   <- requireNamespace("MASS",   quietly = TRUE)

  if (prefer == "survey" && have_survey && !is.null(weights_col)) {
    ids    <- if (!is.null(psu_col))    stats::as.formula(paste0("~", psu_col)) else ~1
    strata <- if (!is.null(strata_col)) stats::as.formula(paste0("~", strata_col)) else NULL
    des <- survey::svydesign(ids = ids, strata = strata,
                             weights = stats::as.formula(paste0("~", weights_col)),
                             data = data, nest = TRUE)
    fit <- survey::svyglm(formula, design = des, family = stats::quasipoisson())
    attr(fit, "hdmm_method") <- "survey::svyglm(quasipoisson)"
    return(fit)
  }

  if (prefer == "negbin" && have_mass) {
    d <- data
    if (!is.null(weights_col)) {
      d$.hdmm_w <- data[[weights_col]]
      fit <- MASS::glm.nb(formula, data = d, weights = .hdmm_w)
      warning("fit_visit_model(): negbin uses weights but not strata/PSU; SEs are not design-based.")
    } else {
      fit <- MASS::glm.nb(formula, data = d)
    }
    attr(fit, "hdmm_method") <- "MASS::glm.nb"
    return(fit)
  }

  d <- data
  fit <- if (!is.null(weights_col)) {
    d$.hdmm_w <- data[[weights_col]]
    stats::glm(formula, family = stats::quasipoisson(link = "log"), data = d, weights = .hdmm_w)
  } else {
    stats::glm(formula, family = stats::quasipoisson(link = "log"), data = d)
  }
  attr(fit, "hdmm_method") <- "stats::glm(quasipoisson)"
  if (!have_survey || is.null(weights_col)) {
    warning("fit_visit_model(): fell back to weighted quasipoisson; standard errors ",
            "do NOT reflect the survey design. Install 'survey' and pass ",
            "weights_col/strata_col/psu_col for design-based inference.")
  }
  fit
}

# ---- parameter-uncertainty propagation ------------------------------------

# Multivariate-normal draws around coef(model) with covariance vcov(model), via a
# base-R Cholesky (no MASS). NA coefficients dropped; non-PD covariance projected
# to the nearest PD matrix. Internal.
.param_draw <- function(model, n = 1L) {
  b <- stats::coef(model); V <- stats::vcov(model)
  keep <- !is.na(b)
  b <- b[keep]; V <- V[keep, keep, drop = FALSE]
  V <- (V + t(V)) / 2
  R <- tryCatch(chol(V), error = function(e) {
    e2 <- eigen(V, symmetric = TRUE)
    vals <- pmax(e2$values, 1e-10)
    chol(e2$vectors %*% diag(vals, length(vals)) %*% t(e2$vectors))
  })
  p <- length(b)
  z <- matrix(stats::rnorm(n * p), nrow = n)
  draws <- matrix(b, nrow = n, ncol = p, byrow = TRUE) + z %*% R
  colnames(draws) <- names(b)
  draws
}

# Offset-safe point prediction for a log-link count model. Builds the linear
# predictor directly, so it never needs the offset column in newdata. Internal.
.expected_count <- function(model, newdata, offset = 0) {
  b <- stats::coef(model); keep <- !is.na(b)
  tt <- stats::delete.response(stats::terms(model))
  mf <- stats::model.frame(tt, data = newdata, xlev = model$xlevels)
  X  <- stats::model.matrix(tt, mf, contrasts.arg = model$contrasts)
  cols <- intersect(names(b)[keep], colnames(X))
  if (!length(cols)) stop(".expected_count(): no overlapping model terms.", call. = FALSE)
  as.numeric(exp(X[, cols, drop = FALSE] %*% b[cols] + offset))
}

#' Predict a log-link count model with parameter uncertainty
#'
#' Returns an `nrow(newdata) x n_draws` matrix of response-scale predictions, one
#' column per multivariate-normal coefficient draw, so callers can take row-wise
#' quantiles for prediction intervals (combine with a `rpois`/`rnbinom` draw per
#' cell to add individual-level uncertainty on top of parameter uncertainty).
#'
#' @param model Fitted log-link model (e.g. from [fit_sling_rate_model()]).
#' @param newdata Data to predict on; factor levels are aligned to the model.
#' @param n_draws Number of parameter draws (columns).
#' @param offset Optional numeric offset on the LINK scale (length 1 or
#'   `nrow(newdata)`); default 0 gives the per-person rate for an offset model.
#' @return Numeric matrix, `nrow(newdata) x n_draws`, on the response scale.
#' @export
predict_count_with_uncertainty <- function(model, newdata, n_draws = 1000L, offset = 0) {
  b_all <- stats::coef(model); keep <- !is.na(b_all)
  tt <- stats::delete.response(stats::terms(model))
  mf <- stats::model.frame(tt, data = newdata, xlev = model$xlevels)
  X  <- stats::model.matrix(tt, mf, contrasts.arg = model$contrasts)
  cols <- intersect(names(b_all)[keep], colnames(X))
  if (!length(cols)) stop("predict_count_with_uncertainty(): no overlapping model terms.", call. = FALSE)
  X <- X[, cols, drop = FALSE]
  draws <- .param_draw(model, n_draws)[, cols, drop = FALSE]
  eta <- X %*% t(draws)
  eta <- sweep(eta, 1, offset, `+`)
  exp(eta)
}
