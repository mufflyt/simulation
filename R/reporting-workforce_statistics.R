# Workforce Inferential Statistics + Sensitivity Registry ----
#
# Harvested from cliff/R/calculate_retirement_cliff_statistics.R (item 1) and
# cliff/data/*sensitivity*.csv (item 2), sourced from cliff `main`.
#
# Item 1 -- small-sample-safe inferential statistics for workforce comparisons:
#   proportion CIs (Wilson), a two-proportion test with an n>=30 guard, a
#   rural-vs-metro comparison, a subspecialty replacement-gap analysis, and a
#   state-vulnerability ranking. All base-R / dplyr, no new dependencies.
#
# Item 2 -- a canonical registry over 13 vetted sensitivity-analysis datasets
#   (departure-rate, departure-window, age-shift, feminization, mortality, ...),
#   reached through resolve_canonical() + a generic loader.
#
# Logging follows the package convention: chatty [STAT] progress goes through
# .msg_debug() (silent unless MICROSIM_VERBOSE), not base::message().

# ---- Internal safe-arithmetic helpers (cliff safe_divide semantics) --------

.wf_safe_divide <- function(numerator, denominator, default = NA_real_) {
  ifelse(is.na(denominator) | denominator == 0, default, numerator / denominator)
}

.wf_safe_percentage <- function(part, total) {
  100 * .wf_safe_divide(part, total, NA_real_)
}

# ---- Proportion confidence interval (Wilson score) -------------------------

#' Wilson-score confidence interval for a single proportion
#'
#' Port of cliff::calculate_proportion_ci. The Wilson interval is well-behaved
#' for small samples and near 0/1, so it is the right choice for
#' rural/subgroup access-desert or at-risk proportions.
#'
#' @param x Successes (at-risk count).
#' @param n Sample size.
#' @param conf_level Confidence level (default 0.95).
#' @return List: `proportion`, `lower_ci`, `upper_ci` (proportions in `[0,1]`),
#'   `method`, `note`.
#' @family workforce statistics
#' @concept reporting
#' @export
calculate_proportion_ci <- function(x, n, conf_level = 0.95) {
  if (isTRUE(n == 0) || is.na(n)) {
    return(list(proportion = NA_real_, lower_ci = NA_real_, upper_ci = NA_real_,
                method = NA_character_, note = "Zero denominator"))
  }
  prop <- x / n
  z <- stats::qnorm(1 - (1 - conf_level) / 2)
  denom <- 1 + z^2 / n
  center <- (prop + z^2 / (2 * n)) / denom
  margin <- z * sqrt(prop * (1 - prop) / n + z^2 / (4 * n^2)) / denom
  list(
    proportion = prop,
    lower_ci = pmax(0, center - margin),
    upper_ci = pmin(1, center + margin),
    method = "Wilson",
    note = sprintf("%.0f%% confidence interval", conf_level * 100)
  )
}

# ---- Two-proportion test with a small-sample guard -------------------------

#' Two-proportion z-test with an n>=30 guard
#'
#' Port of cliff::calculate_two_prop_test. Refuses (descriptive-only) when either
#' group is below `min_sample_size`, so a workforce comparison never reports a
#' significant p-value off a handful of physicians.
#'
#' @param x1,n1 Successes and total in group 1.
#' @param x2,n2 Successes and total in group 2.
#' @param min_sample_size Minimum per-group n for an inferential test.
#' @return List with `method`, `p_value`, `p_value_formatted`, `significant`, `note`.
#' @family workforce statistics
#' @concept reporting
#' @export
calculate_two_prop_test <- function(x1, n1, x2, n2, min_sample_size = 30) {
  if (n1 < min_sample_size || n2 < min_sample_size) {
    .msg_debug(sprintf("[STAT] small-sample: n1=%d n2=%d (min=%d)", n1, n2, min_sample_size))
    return(list(method = "descriptive_only", p_value = NA_real_,
                p_value_formatted = "n<30", significant = FALSE,
                note = sprintf("Sample sizes too small for a test (n1=%d, n2=%d)", n1, n2)))
  }
  if (n1 == 0 || n2 == 0) {
    return(list(method = "insufficient_data", p_value = NA_real_,
                p_value_formatted = "insufficient data", significant = FALSE,
                note = "One or both groups have zero total"))
  }
  tryCatch({
    tr <- stats::prop.test(c(x1, x2), c(n1, n2))
    p <- tr$p.value
    fmt <- if (p < 0.001) "<0.001" else if (p < 0.01) sprintf("%.3f", p) else sprintf("%.2f", p)
    list(method = "prop.test", p_value = p, p_value_formatted = fmt,
         significant = p < 0.05, test_statistic = unname(tr$statistic),
         note = "Two-proportion z-test")
  }, error = function(e) {
    .msg_debug(sprintf("[STAT] two-proportion test failed: %s", conditionMessage(e)))
    list(method = "test_failed", p_value = NA_real_, p_value_formatted = "test failed",
         significant = FALSE, note = paste("Test error:", conditionMessage(e)))
  })
}

# ---- Rural vs metro comparison ---------------------------------------------

#' Rural-vs-metro at-risk comparison (rates, Wilson CIs, two-proportion test)
#'
#' Port of cliff::calculate_rural_metro_comparison. Bundles the two group rates,
#' their Wilson CIs, and the two-proportion test into one result for reporting a
#' rural access-desert disparity.
#'
#' @param rural_at_risk,rural_total Rural at-risk count and denominator.
#' @param metro_at_risk,metro_total Metro at-risk count and denominator.
#' @return List: `rural`, `metro`, `comparison` (rate difference + test).
#' @family workforce statistics
#' @concept reporting
#' @export
calculate_rural_metro_comparison <- function(rural_at_risk, rural_total,
                                             metro_at_risk, metro_total) {
  rural_rate <- .wf_safe_percentage(rural_at_risk, rural_total)
  metro_rate <- .wf_safe_percentage(metro_at_risk, metro_total)
  rural_ci <- calculate_proportion_ci(rural_at_risk, rural_total)
  metro_ci <- calculate_proportion_ci(metro_at_risk, metro_total)
  test <- calculate_two_prop_test(rural_at_risk, rural_total, metro_at_risk, metro_total)

  rate_diff <- if (!is.na(rural_rate) && !is.na(metro_rate)) rural_rate - metro_rate else NA_real_

  list(
    rural = list(at_risk = rural_at_risk, total = rural_total, rate_pct = rural_rate,
                 ci_lower = rural_ci$lower_ci * 100, ci_upper = rural_ci$upper_ci * 100),
    metro = list(at_risk = metro_at_risk, total = metro_total, rate_pct = metro_rate,
                 ci_lower = metro_ci$lower_ci * 100, ci_upper = metro_ci$upper_ci * 100),
    comparison = list(rate_difference_pct = rate_diff, p_value = test$p_value,
                      p_value_formatted = test$p_value_formatted,
                      significant = test$significant, test_method = test$method,
                      note = test$note)
  )
}

# ---- Replacement-gap analysis ----------------------------------------------

#' Subspecialty replacement-gap analysis (retirees vs fellowship graduates)
#'
#' Port of cliff::calculate_replacement_gap. Projects 5 years of graduates
#' against projected retirements per subspecialty and reports the net gap,
#' replacement ratio, and an adequacy flag.
#'
#' @param retirees_by_subspec Tibble: `subspecialty`, `retiring_count`.
#' @param fellowship_grads Tibble: `subspecialty`, `graduates` (per year rows).
#' @param horizon_years Projection horizon (default 5).
#' @return List: `by_subspecialty` (tibble) and `overall` (summary list).
#' @family workforce statistics
#' @concept reporting
#' @export
calculate_replacement_gap <- function(retirees_by_subspec, fellowship_grads,
                                       horizon_years = 5) {
  assertthat::assert_that(all(c("subspecialty", "retiring_count") %in% names(retirees_by_subspec)))
  assertthat::assert_that(all(c("subspecialty", "graduates") %in% names(fellowship_grads)))

  grad_summary <- fellowship_grads %>%
    dplyr::group_by(.data$subspecialty) %>%
    dplyr::summarise(annual_grads = mean(.data$graduates, na.rm = TRUE),
                     total_grads = sum(.data$graduates, na.rm = TRUE), .groups = "drop")

  by_sub <- retirees_by_subspec %>%
    safe_left_join(grad_summary, by = "subspecialty") %>%
    dplyr::mutate(
      annual_grads = tidyr::replace_na(.data$annual_grads, 0),
      projected_grads = .data$annual_grads * horizon_years,
      replacement_ratio = .wf_safe_divide(.data$projected_grads, .data$retiring_count),
      net_gap = .data$retiring_count - .data$projected_grads,
      gap_percentage = .wf_safe_percentage(.data$net_gap, .data$retiring_count),
      adequate_replacement = .data$replacement_ratio >= 1.0
    ) %>%
    dplyr::arrange(.data$subspecialty)

  total_retiring <- sum(by_sub$retiring_count, na.rm = TRUE)
  total_proj <- sum(by_sub$projected_grads, na.rm = TRUE)
  overall_gap <- total_retiring - total_proj

  list(
    by_subspecialty = by_sub,
    overall = list(
      total_retiring = total_retiring,
      total_graduates_projected = total_proj,
      net_gap = overall_gap,
      gap_percentage = .wf_safe_percentage(overall_gap, total_retiring),
      replacement_ratio = .wf_safe_divide(total_proj, total_retiring)
    )
  )
}

# ---- State vulnerability ranking -------------------------------------------

#' Rank states by workforce vulnerability
#'
#' Port of cliff::calculate_state_vulnerability. Ranks states by the percent of
#' active providers lost if the at-risk cohort retires, weighted by
#' log10(active count) so a large state's loss outweighs a tiny state's.
#'
#' @param state_impacts Tibble with `state`, `count_active`, `count_at_risk`,
#'   `pct_loss_if_retire`, and `zero_coverage_if_retire`.
#' @param top_n Number of top-vulnerable states to return.
#' @return Tibble of the `top_n` most vulnerable states with a
#'   `vulnerability_score`.
#' @family workforce statistics
#' @concept reporting
#' @export
calculate_state_vulnerability <- function(state_impacts, top_n = 10) {
  needed <- c("state", "count_active", "count_at_risk", "pct_loss_if_retire",
              "zero_coverage_if_retire")
  assertthat::assert_that(all(needed %in% names(state_impacts)))

  state_impacts %>%
    dplyr::filter(!is.na(.data$pct_loss_if_retire)) %>%
    dplyr::mutate(vulnerability_score = .data$pct_loss_if_retire *
                    log10(pmax(1, .data$count_active))) %>%
    dplyr::arrange(dplyr::desc(.data$vulnerability_score)) %>%
    dplyr::slice_head(n = top_n) %>%
    dplyr::select(dplyr::all_of(c(needed, "vulnerability_score")))
}

# ---- Sensitivity-analysis registry (item 2) --------------------------------

# Logical name -> canonical-source key. Each key resolves through
# config/canonical_sources.yml to a vetted cliff sensitivity dataset.
SENSITIVITY_TABLES <- c(
  abu_pathway = "sensitivity_abu_pathway",
  age_shift = "sensitivity_age_shift",
  consistent_definition_baseline = "sensitivity_consistent_definition_baseline",
  departure_rate = "sensitivity_departure_rate",
  departure_window = "sensitivity_departure_window",
  feminization = "sensitivity_feminization",
  inactivity_threshold = "sensitivity_inactivity_threshold",
  mortality = "sensitivity_mortality",
  open_payments = "sensitivity_open_payments",
  retirement = "sensitivity_retirement",
  grid = "sensitivity_grid",
  grid_summary = "sensitivity_grid_summary",
  demand_denominators = "sensitivity_demand_denominators"
)

#' List the available vetted sensitivity-analysis datasets
#'
#' @return Character vector of logical sensitivity-table names.
#' @family workforce statistics
#' @concept reporting
#' @export
sensitivity_registry <- function() names(SENSITIVITY_TABLES)

#' Load a vetted sensitivity-analysis dataset by logical name
#'
#' Reads a cliff-derived sensitivity table through the canonical-source resolver
#' (path + SHA-256 verified), so a workforce sensitivity analysis never reads
#' from an ad-hoc path.
#'
#' @param name One of [sensitivity_registry()].
#' @param mode Reproducibility mode.
#' @return Tibble of the sensitivity dataset.
#' @family workforce statistics
#' @concept reporting
#' @export
load_sensitivity_table <- function(name, mode = resolve_reproducibility_mode()) {
  if (!name %in% names(SENSITIVITY_TABLES)) {
    stop(sprintf("Unknown sensitivity table '%s'. Available: %s",
                 name, paste(sensitivity_registry(), collapse = ", ")), call. = FALSE)
  }
  path <- resolve_canonical(SENSITIVITY_TABLES[[name]], mode = mode)
  tibble::as_tibble(utils::read.csv(path, stringsAsFactors = FALSE))
}
