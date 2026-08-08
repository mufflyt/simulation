# Forecast-evaluation scorecard for back-test model comparison ----------------
#
# WHY THIS EXISTS
# Coverage alone is a broken success measure: a deliberately wide interval
# "passes" 95% coverage while saying almost nothing, so it can beat a sharp,
# informative model merely because the truth fell somewhere inside a 292-provider
# band. Model comparison needs the full picture -- point accuracy, bias,
# sharpness-penalized interval scores, calibration, and stability -- scored
# against a simple benchmark so "better than green CI" becomes "better than a
# naive forecast".
#
# The interval score (Gneiting & Raftery 2007) and the weighted interval score
# (Bracher et al. 2021) are the antidote: both add the interval WIDTH to the miss
# penalty, so among intervals that cover, the sharper one wins, and a wide
# non-informative interval is charged for its width. Pure base R; unit-testable by
# construction (a sharp covering interval must beat a wide covering one, which
# must beat a sharp miss).

# Interval score for a central (1 - alpha) interval [lower, upper] vs observation
# y. Lower is better. Width is always paid; a miss adds (2/alpha) x the shortfall.
.interval_score <- function(y, lower, upper, alpha) {
  (upper - lower) +
    (2 / alpha) * pmax(lower - y, 0) +
    (2 / alpha) * pmax(y - upper, 0)
}

#' Weighted interval score (WIS) for quantile forecasts
#'
#' The proper multi-level generalization of the interval score (Bracher et al.
#' 2021): a weighted average of the median's absolute error and the interval
#' scores of the central intervals implied by symmetric quantile pairs. Rewards
#' both calibration and sharpness, so a wide, uninformative forecast cannot win.
#'
#' @param y Numeric observation(s), length `n` (recycled if length 1).
#' @param quantiles Numeric matrix `n x m` of quantile forecasts (rows = cases,
#'   columns = `quantile_levels`); a length-`m` vector is treated as one case.
#' @param quantile_levels Numeric length-`m` vector of probability levels in
#'   (0, 1); must include `0.5` and symmetric pairs (`a` and `1 - a`).
#' @return Numeric vector of length `n`: the WIS per case (lower is better).
#' @family forecast scorecard
#' @concept validation
#' @examples
#' # WIS for one forecast: observation 1300 scored against a 50% central interval
#' # (lower is better). quantile_levels must include the median.
#' weighted_interval_score(
#'   y = 1300,
#'   quantiles = c(1200, 1300, 1400),
#'   quantile_levels = c(0.25, 0.5, 0.75)
#' )
#' @export
weighted_interval_score <- function(y, quantiles, quantile_levels) {
  if (is.null(dim(quantiles))) quantiles <- matrix(quantiles, nrow = 1)
  m <- ncol(quantiles)
  stopifnot(length(quantile_levels) == m, all(quantile_levels > 0 & quantile_levels < 1))
  if (!any(abs(quantile_levels - 0.5) < 1e-9))
    stop("weighted_interval_score(): quantile_levels must include the median (0.5).",
         call. = FALSE)
  if (!length(y) %in% c(1L, nrow(quantiles)))
    stop("weighted_interval_score(): length(y) must be 1 or nrow(quantiles) (",
         nrow(quantiles), "); got ", length(y),
         " -- rep_len would silently partial-recycle and score the wrong cases.",
         call. = FALSE)
  y <- rep_len(y, nrow(quantiles))
  med <- quantiles[, which.min(abs(quantile_levels - 0.5)), drop = TRUE]
  lows <- quantile_levels[quantile_levels < 0.5]
  vapply(seq_len(nrow(quantiles)), function(i) {
    acc <- 0.5 * abs(y[i] - med[i]); k <- 0L
    for (a_lo in lows) {
      hi <- 1 - a_lo
      j_lo <- which.min(abs(quantile_levels - a_lo))
      j_hi <- which.min(abs(quantile_levels - hi))
      alpha <- 2 * a_lo                              # central (1 - alpha) interval
      acc <- acc + (alpha / 2) *
        .interval_score(y[i], quantiles[i, j_lo], quantiles[i, j_hi], alpha)
      k <- k + 1L
    }
    acc / (k + 0.5)
  }, numeric(1))
}

#' Forecast-evaluation scorecard for one model
#'
#' Computes the full suite of comparison metrics for a set of forecasts, so no
#' single number (least of all coverage) decides the winner: point error (MAPE,
#' RMSE), signed bias, and -- when interval columns are present -- empirical
#' coverage, mean interval width, the mean interval score (width-penalized), and
#' the calibration slope.
#'
#' @param data Data frame of forecasts, one row per (cutoff x target).
#' @param observed,point Column names for the observed value and point forecast.
#' @param lower,upper Column names for the interval bounds, or `NULL` to skip
#'   interval metrics.
#' @param interval_level Central coverage of `[lower, upper]` (e.g. 0.95, so
#'   `alpha = 0.05`). Used by the interval score.
#' @param label Optional model label carried into the result.
#' @return A one-row data frame: `label`, `n`, `mape`, `rmse`, `signed_bias`,
#'   `signed_pct_bias`, and (when intervals given) `coverage`, `mean_width`,
#'   `mean_interval_score`, `calibration_slope`.
#' @family forecast scorecard
#' @concept validation
#' @export
forecast_scorecard <- function(data, observed = "observed", point = "predicted",
                               lower = "lower", upper = "upper",
                               interval_level = 0.95, label = NA_character_) {
  stopifnot(is.data.frame(data), all(c(observed, point) %in% names(data)))
  y <- as.numeric(data[[observed]]); yhat <- as.numeric(data[[point]])
  ok <- is.finite(y) & is.finite(yhat)   # a single NA row must not null every metric
  pos <- ok & y != 0
  out <- data.frame(
    label = label, n = length(y),
    mape = if (any(pos)) mean(100 * abs(yhat[pos] - y[pos]) / abs(y[pos])) else NA_real_,
    rmse = if (any(ok)) sqrt(mean((yhat[ok] - y[ok])^2)) else NA_real_,
    signed_bias = if (any(ok)) mean(yhat[ok] - y[ok]) else NA_real_,
    signed_pct_bias = if (any(pos)) mean(100 * (yhat[pos] - y[pos]) / y[pos]) else NA_real_,
    stringsAsFactors = FALSE)
  has_int <- !is.null(lower) && !is.null(upper) &&
    all(c(lower, upper) %in% names(data))
  if (has_int) {
    lo <- as.numeric(data[[lower]]); hi <- as.numeric(data[[upper]])
    alpha <- 1 - interval_level
    out$coverage <- mean(y >= lo & y <= hi)
    out$mean_width <- mean(hi - lo)
    out$mean_interval_score <- mean(.interval_score(y, lo, hi, alpha))
    out$calibration_slope <- if (stats::sd(yhat) > 0)
      unname(stats::coef(stats::lm(y ~ yhat))[2]) else NA_real_
  }
  out
}

#' Compare forecast models: scorecard, skill vs a benchmark, rank stability
#'
#' Scores each model with [forecast_scorecard()], expresses accuracy relative to a
#' simple benchmark model (skill = 1 - model / benchmark, positive is better), and
#' -- the check a single aggregate hides -- measures each model's *rank stability*
#' across cutoffs: a model that only wins on average but swings between best and
#' worst across origins is not a reliable forecaster.
#'
#' @param data Long data frame with a model-id column plus the forecast columns.
#' @param model Column naming the model.
#' @param cutoff Column naming the back-test origin/cutoff (required for rank
#'   stability; `NULL` skips it).
#' @param observed,point,lower,upper,interval_level Passed to [forecast_scorecard()].
#' @param benchmark Optional model id to score skill against (e.g. a naive/random-walk
#'   forecast). `NULL` skips skill.
#' @param rank_metric Per-cutoff metric to rank by: `"interval_score"` (default,
#'   sharpness-penalized) or `"abs_pct_error"`. Lower is better.
#' @return A list: `scorecard` (per-model metrics + skill columns), `rank_stability`
#'   (`model`, `mean_rank`, `rank_sd`, `best_fraction`, `n_cutoffs`) or `NULL`, and
#'   `benchmark`.
#' @family forecast scorecard
#' @concept validation
#' @export
compare_forecasts <- function(data, model = "model", cutoff = "cutoff",
                              observed = "observed", point = "predicted",
                              lower = "lower", upper = "upper", interval_level = 0.95,
                              benchmark = NULL, rank_metric = c("interval_score", "abs_pct_error")) {
  rank_metric <- match.arg(rank_metric)
  stopifnot(is.data.frame(data), model %in% names(data))
  models <- unique(as.character(data[[model]]))
  cards <- lapply(models, function(mm) {
    forecast_scorecard(data[as.character(data[[model]]) == mm, , drop = FALSE],
                       observed, point, lower, upper, interval_level, label = mm)
  })
  scorecard <- do.call(rbind, cards)
  names(scorecard)[names(scorecard) == "label"] <- "model"

  if (!is.null(benchmark)) {
    if (!benchmark %in% scorecard$model)
      stop("compare_forecasts(): benchmark '", benchmark, "' is not among the models.",
           call. = FALSE)
    b <- scorecard[scorecard$model == benchmark, , drop = FALSE]
    scorecard$mape_skill <- if (!is.na(b$mape)) 1 - scorecard$mape / b$mape else NA_real_
    if ("mean_interval_score" %in% names(scorecard))
      scorecard$interval_score_skill <- 1 - scorecard$mean_interval_score / b$mean_interval_score
  }

  rank_stability <- NULL
  if (!is.null(cutoff) && cutoff %in% names(data)) {
    have_int <- all(c(lower, upper) %in% names(data))
    if (identical(rank_metric, "interval_score") && !have_int)
      warning("compare_forecasts(): rank_metric = 'interval_score' but no lower/upper ",
              "columns are present; ranking silently falls back to abs_pct_error.",
              call. = FALSE)
    per_metric <- function(df) {
      y <- as.numeric(df[[observed]]); yhat <- as.numeric(df[[point]])
      if (rank_metric == "interval_score" && have_int) {
        mean(.interval_score(y, as.numeric(df[[lower]]), as.numeric(df[[upper]]),
                             1 - interval_level))
      } else {
        pos <- y != 0
        if (any(pos)) mean(100 * abs(yhat[pos] - y[pos]) / abs(y[pos])) else NA_real_
      }
    }
    cuts <- unique(data[[cutoff]])
    # per-cutoff model ranks (1 = best = lowest metric)
    rank_rows <- lapply(cuts, function(cc) {
      sub <- data[data[[cutoff]] == cc, , drop = FALSE]
      mv <- vapply(models, function(mm)
        per_metric(sub[as.character(sub[[model]]) == mm, , drop = FALSE]), numeric(1))
      data.frame(cutoff = cc, model = models, metric = unname(mv),
                 rank = rank(unname(mv), ties.method = "average"), stringsAsFactors = FALSE)
    })
    rr <- do.call(rbind, rank_rows)
    rank_stability <- do.call(rbind, lapply(models, function(mm) {
      rk <- rr$rank[rr$model == mm]
      data.frame(model = mm, mean_rank = mean(rk),
                 rank_sd = stats::sd(rk),
                 best_fraction = mean(rk == 1),
                 n_cutoffs = length(rk), stringsAsFactors = FALSE)
    }))
    rank_stability <- rank_stability[order(rank_stability$mean_rank), , drop = FALSE]
  }

  list(scorecard = scorecard, rank_stability = rank_stability, benchmark = benchmark)
}
