# Geographic held-out (spatial cross-validation) for external validity ---------
#
# WHY THIS EXISTS
# A green R CMD check validates the SOFTWARE, not the SCIENCE. The headline
# workforce numbers are back-tested temporally, but the entrant-regime model was
# specified after inspecting the 2021-2023 miss, so a temporal back-test against
# those years is contaminated by model selection. Geography is a dimension that
# played NO part in that selection, so predicting held-out geographies' observed
# stock is a genuinely out-of-sample external check.
#
# This is a pure, data-agnostic harness: you supply a per-geography table of an
# observed count/stock plus predictors (e.g. ABOG/NPPES provider counts by state
# with a demand or population predictor), and it refits the model on the TRAINING
# geographies only and scores the held-out ones. Leakage-free by construction --
# a fold's fit never sees the held-out rows, so a geography's own observed value
# cannot inform its prediction. Base R (stats::glm/lm), so it is unit-testable by
# recovering a known geographic relationship out-of-sample.

# Default per-fold model: refit on train, predict test. A Poisson log-link count
# model (optionally rate, via a log-offset) for stock counts; gaussian for a
# continuous target. Override with a custom `fit_predict(train, test)`.
.geo_default_fit_predict <- function(observed, predictors, family, offset_col) {
  function(train, test) {
    rhs <- paste(predictors, collapse = " + ")
    off <- if (!is.null(offset_col)) sprintf(" + offset(log(%s))", offset_col) else ""
    form <- stats::as.formula(sprintf("%s ~ %s%s", observed, rhs, off))
    fit <- if (identical(family, "gaussian")) {
      stats::lm(form, data = train)
    } else {
      stats::glm(form, family = stats::poisson(link = "log"), data = train)
    }
    as.numeric(stats::predict(fit, newdata = test, type = "response"))
  }
}

.geo_folds <- function(data, scheme, k, region, seed) {
  n <- nrow(data)
  if (identical(scheme, "loo")) return(as.list(seq_len(n)))
  if (identical(scheme, "region")) {
    if (is.null(region) || !region %in% names(data))
      stop("geographic_holdout_cv(): scheme = 'region' needs a `region` column.", call. = FALSE)
    grp <- as.character(data[[region]])
    return(unname(lapply(unique(grp), function(g) which(grp == g))))
  }
  # k-fold: partition rows into k contiguous groups of a reproducible shuffle.
  if (!is.null(seed)) set.seed(seed)
  k <- min(k, n)
  ord <- sample.int(n)
  split(ord, cut(seq_len(n), breaks = k, labels = FALSE))
}

#' Geographic held-out (spatial) cross-validation of predicted vs observed stock
#'
#' Refits the model on the training geographies and predicts the held-out ones,
#' so the score is genuinely out-of-sample along a dimension that played no part
#' in model selection. Supports leave-one-geography-out (`"loo"`),
#' leave-one-region-out (`"region"`, the classic geographic hold-out), and random
#' `"kfold"`. Leakage-free by construction: each fold's fit sees only its training
#' rows.
#'
#' @details
#' Holding out random rows overstates accuracy whenever neighbouring units
#' resemble one another, because a held-out tract's neighbours -- carrying
#' nearly the same information -- stay in the training set. The `region` scheme
#' holds out whole regions and is the honest test of transfer to unseen
#' geography; `loo` and `kfold` are retained for comparison, and a large gap
#' between them is itself the finding.
#' @param data Data frame, one row per geography, with the observed column, the
#'   predictor column(s), and optionally a `region` grouping and an offset column.
#' @param observed Name of the observed count/stock column (the validation target).
#' @param predictors Character vector of predictor column names.
#' @param geo Optional id column for labelling rows. Default row index.
#' @param region Optional grouping column for `scheme = "region"`.
#' @param scheme `"loo"` (default), `"region"`, or `"kfold"`.
#' @param k Number of folds for `"kfold"`. Default 10 (capped at `nrow(data)`).
#' @param family `"poisson"` (default; log-link count model) or `"gaussian"`.
#' @param offset_col Optional column for a Poisson log-offset (turns the count
#'   model into a rate model, e.g. providers per capita). Default `NULL`.
#' @param fit_predict Optional `function(train, test)` returning predictions for
#'   `test`, overriding the default model. Must not look at `test`'s observed.
#' @param seed Optional RNG seed for `"kfold"` reproducibility.
#' @return A list: `predictions` (one row per geography: `geo`, `region`, `fold`,
#'   `observed`, `predicted`), `metrics` (`n`, `mape`, `rmse`, `r2_oos`,
#'   `calibration_slope`, `spearman`), and the `scheme`/`family` used.
#' @family geographic holdout
#' @concept validation
#' @export
geographic_holdout_cv <- function(data, observed, predictors, geo = NULL,
                                  region = NULL, scheme = c("loo", "region", "kfold"),
                                  k = 10L, family = c("poisson", "gaussian"),
                                  offset_col = NULL, fit_predict = NULL, seed = NULL) {
  scheme <- match.arg(scheme)
  family <- match.arg(family)
  stopifnot(is.data.frame(data), is.character(observed), length(observed) == 1L,
            is.character(predictors), length(predictors) >= 1L)
  need <- c(observed, predictors, region, offset_col, geo)
  miss <- setdiff(need, names(data))
  if (length(miss))
    stop("geographic_holdout_cv(): missing column(s): ", paste(miss, collapse = ", "),
         call. = FALSE)
  if (nrow(data) < length(predictors) + 2L)
    stop("geographic_holdout_cv(): need at least (n predictors + 2) geographies to ",
         "hold one out and still fit.", call. = FALSE)

  fp <- if (!is.null(fit_predict)) fit_predict else
    .geo_default_fit_predict(observed, predictors, family, offset_col)
  folds <- .geo_folds(data, scheme, k, region, seed)

  geo_id <- if (!is.null(geo)) as.character(data[[geo]]) else as.character(seq_len(nrow(data)))
  reg_id <- if (!is.null(region)) as.character(data[[region]]) else NA_character_
  pred <- rep(NA_real_, nrow(data)); fold_id <- rep(NA_integer_, nrow(data))

  for (f in seq_along(folds)) {
    test_idx <- folds[[f]]
    train <- data[-test_idx, , drop = FALSE]
    test  <- data[test_idx, , drop = FALSE]
    yhat <- tryCatch(fp(train, test),
                     error = function(e) stop("geographic_holdout_cv(): fold ", f,
                       " failed: ", conditionMessage(e), call. = FALSE))
    if (length(yhat) != length(test_idx))
      stop("geographic_holdout_cv(): fit_predict returned ", length(yhat),
           " values for a ", length(test_idx), "-row fold.", call. = FALSE)
    pred[test_idx] <- as.numeric(yhat)
    fold_id[test_idx] <- f
  }

  obs <- as.numeric(data[[observed]])
  predictions <- data.frame(geo = geo_id, region = reg_id, fold = fold_id,
                            observed = obs, predicted = pred, stringsAsFactors = FALSE)

  ok <- is.finite(obs) & is.finite(pred)
  o <- obs[ok]; p <- pred[ok]
  pos <- o != 0
  mape <- if (any(pos)) mean(100 * abs(p[pos] - o[pos]) / o[pos]) else NA_real_
  rmse <- sqrt(mean((p - o)^2))
  ss_res <- sum((o - p)^2); ss_tot <- sum((o - mean(o))^2)
  r2_oos <- if (ss_tot > 0) 1 - ss_res / ss_tot else NA_real_   # out-of-sample R^2 (can be < 0)
  calib <- if (stats::sd(p) > 0) unname(stats::coef(stats::lm(o ~ p))[2]) else NA_real_
  spear <- if (stats::sd(p) > 0 && stats::sd(o) > 0)
    suppressWarnings(stats::cor(o, p, method = "spearman")) else NA_real_

  metrics <- data.frame(n = length(o), mape = mape, rmse = rmse, r2_oos = r2_oos,
                        calibration_slope = calib, spearman = spear,
                        stringsAsFactors = FALSE)
  list(predictions = predictions, metrics = metrics, scheme = scheme, family = family)
}
