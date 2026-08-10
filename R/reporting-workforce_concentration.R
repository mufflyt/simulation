# Workforce geographic concentration metrics ----
#
# R/geography-provider_geography.R reasons about maldistribution ("today's
# maldistribution is reproduced forever") but does not MEASURE it. These
# functions quantify how unevenly a provider workforce is distributed across
# geographic units -- Gini, Herfindahl-Hirschman Index, Lorenz curve, top-k
# concentration -- so a scenario's distributional consequences can be compared,
# not just its national headcount. Include zero-provider units (pass the full
# unit universe) to measure concentration across the whole geography.
#
# Ported from cliff/R/workforce_concentration_metrics.R (unit-tested there).
# Dependency-light (base R + tibble); no individual roster required.

# A negative provider count is a data error, not a small one. Only
# workforce_gini() refused it; the other three returned numbers outside their
# own documented ranges -- top-k share 1.2, a Lorenz curve running to -1 -- and
# said nothing. The whole family shares one guard now.
#
# DELIBERATE DIVERGENCE FROM CANONICAL. cliff::herfindahl_index() documents
# "zero/negative entries are dropped (they contribute no share)" and this port
# inherited it. Dropping is defensible for HHI alone, but not beside a sibling
# that stops on the same input: a caller who ran both got an error from one and
# a confident number from the other on identical data. Recorded here rather
# than silently reconciled.
.assert_nonneg_counts <- function(x, fn) {
  bad <- is.finite(x) & x < 0
  if (any(bad)) {
    stop(sprintf(paste("%s(): negative counts are not allowed; found %s.",
                       "A negative provider count is a data error, and every",
                       "share it enters is outside [0, 1]."),
                 fn, paste(utils::head(x[bad], 5L), collapse = ", ")), call. = FALSE)
  }
  invisible(x)
}

#' Gini coefficient of a non-negative count/weight vector
#'
#' A vector with all mass in one unit tends to (n-1)/n; an even split gives 0.
#' Include zero-valued units to measure concentration across the full geography.
#' @param x Numeric vector of non-negative counts or weights.
#' @return Gini in [0, 1), or `NA_real_` if the total is 0.
#' @family workforce concentration
#' @concept reporting
#' @export
workforce_gini <- function(x) {
  x <- x[is.finite(x)]
  if (any(x < 0)) stop("workforce_gini(): negative values are not allowed.", call. = FALSE)
  x <- sort(x)
  n <- length(x); total <- sum(x)
  if (n == 0L || total == 0) return(NA_real_)
  (2 * sum(seq_len(n) * x)) / (n * total) - (n + 1) / n
}

#' Herfindahl-Hirschman Index of provider share
#' @param counts Numeric counts per unit; zero entries contribute no share.
#'   Negative counts are refused (see `.assert_nonneg_counts`), which is a
#'   deliberate divergence from `cliff::herfindahl_index()`.
#' @param normalized If TRUE, size-corrected HHI* = (H - 1/n)/(1 - 1/n).
#' @return HHI in `[0, 1]`; `NA_real_` if the total is 0.
#' @family workforce concentration
#' @concept reporting
#' @export
workforce_hhi <- function(counts, normalized = FALSE) {
  .assert_nonneg_counts(counts, "workforce_hhi")
  counts <- counts[is.finite(counts) & counts > 0]
  total <- sum(counts)
  if (total == 0) return(NA_real_)
  h <- sum((counts / total)^2)
  if (!normalized) return(h)
  n <- length(counts)
  if (n <= 1L) return(NA_real_)
  (h - 1 / n) / (1 - 1 / n)
}

#' Lorenz-curve coordinates for a count vector
#' @param x Numeric counts per unit (include zeros for the full geography).
#' @return Tibble `cum_unit_share`, `cum_value_share`, prepended with (0, 0).
#' @examples
#' workforce_lorenz(c(120, 80, 40, 10, 0, 0))
#' @family workforce concentration
#' @concept reporting
#' @export
workforce_lorenz <- function(x) {
  .assert_nonneg_counts(x, "workforce_lorenz")
  x <- sort(x[is.finite(x)])
  n <- length(x); total <- sum(x)
  if (n == 0L || total == 0)
    return(tibble::tibble(cum_unit_share = 0, cum_value_share = 0))
  tibble::tibble(
    cum_unit_share  = c(0, seq_len(n) / n),
    cum_value_share = c(0, cumsum(x) / total)
  )
}

#' Share of the total held by the k largest units
#' @param counts Numeric counts per unit.
#' @param k Number of top units. Default 5.
#' @return Fraction in `[0, 1]`, or `NA_real_` if the total is 0.
#' @family workforce concentration
#' @concept reporting
#' @export
workforce_top_k_share <- function(counts, k = 5L) {
  .assert_nonneg_counts(counts, "workforce_top_k_share")
  if (!is.numeric(k) || length(k) != 1L || is.na(k) || k < 0)
    stop("workforce_top_k_share(): k must be a single non-negative number.", call. = FALSE)
  counts <- counts[is.finite(counts)]
  total <- sum(counts)
  if (total == 0) return(NA_real_)
  sum(sort(counts, decreasing = TRUE)[seq_len(min(k, length(counts)))]) / total
}

#' One-row provider-concentration summary for a geography level
#'
#' @param counts Numeric provider counts for the OCCUPIED units.
#' @param n_units_total Size of the full unit universe (e.g. 51 states, 3143
#'   counties). Zero-provider units are padded in so Gini and the zero-share
#'   reflect the whole geography. Defaults to `length(counts)` (occupied-only).
#' @param label Geography label for the output row.
#' @return A one-row tibble: geography, n_units, n_occupied, pct_units_zero,
#'   gini, hhi, top5_share, top10_share.
#' @examples
#' # Workload is concentrated: two of six providers deliver most of the volume,
#' # and two deliver none at all. Board certification is not the same as
#' # delivering care, which is why this is reported beside any headcount.
#' provider_concentration(c(120, 80, 40, 10, 0, 0))
#' @family workforce concentration
#' @concept reporting
#' @export
provider_concentration <- function(counts, n_units_total = length(counts),
                                   label = NA_character_) {
  counts <- as.numeric(counts)
  # Checked here, not left to workforce_gini() deeper in the tibble() call: the
  # denominator arithmetic below runs first otherwise, and the caller gets a
  # message about Gini for what is really a bad input to this function.
  .assert_nonneg_counts(counts, "provider_concentration")
  n_occupied <- sum(counts > 0, na.rm = TRUE)
  # Restores the guard cliff::concentration_summary() carries and this port
  # dropped. Without it a too-small universe reported pct_units_zero = -100 --
  # a negative share of empty units -- while Gini and HHI were computed over
  # MORE units than n_units claimed. Every number in the row disagreed with its
  # own denominator, in silence.
  if (n_units_total < n_occupied) {
    stop(sprintf(paste("provider_concentration(): n_units_total (%s) is smaller than the",
                       "number of occupied units (%s); the unit universe cannot be smaller",
                       "than the units that already contain providers."),
                 n_units_total, n_occupied), call. = FALSE)
  }
  full <- c(counts, rep(0, max(0L, n_units_total - length(counts))))
  tibble::tibble(
    geography      = label,
    n_units        = n_units_total,
    n_occupied     = n_occupied,
    pct_units_zero = if (n_units_total > 0)
      round(100 * (n_units_total - n_occupied) / n_units_total, 1) else NA_real_,
    gini           = round(workforce_gini(full), 4),
    hhi            = round(workforce_hhi(full), 4),
    top5_share     = round(workforce_top_k_share(full, 5L), 4),
    top10_share    = round(workforce_top_k_share(full, 10L), 4)
  )
}
