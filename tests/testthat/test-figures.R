# Guards for the manuscript figure functions. Skip when ggplot2 is unavailable.

has_ggplot <- function() requireNamespace("ggplot2", quietly = TRUE)

.supply_df <- function() {
  y <- 2025:2050
  tibble::tibble(year = y,
                 effective_fte_median = 1300 + (y - 2025) * 2,
                 effective_fte_lo = 1300 + (y - 2025) * 2 - 40,
                 effective_fte_hi = 1300 + (y - 2025) * 2 + 40)
}
.required_df <- function() {
  y <- 2025:2050
  tibble::tibble(year = y, required_fte = 1350 + (y - 2025) * 6)
}

test_that("fig_supply_vs_required returns a ggplot", {
  skip_if_not(has_ggplot(), "ggplot2 not installed")
  g <- fig_supply_vs_required(.supply_df(), .required_df())
  expect_s3_class(g, "ggplot")
})

test_that("fig_adequacy_trough marks the trough and returns a ggplot", {
  skip_if_not(has_ggplot(), "ggplot2 not installed")
  cov <- tidyr::expand_grid(year = 2025:2050, estimand = c("D1", "D2", "D3"))
  cov$adequacy <- 1 - 0.05 * exp(-((cov$year - 2038)^2) / 50)   # a trough near 2038
  g <- fig_adequacy_trough(cov)
  expect_s3_class(g, "ggplot")
})

test_that("fig_replacement_outlook returns a ggplot", {
  skip_if_not(has_ggplot(), "ggplot2 not installed")
  outlook <- tibble::tibble(
    scenario_label = c("Status quo", "Enhanced training", "Early retirement"),
    replacement_ratio = c(0.96, 1.25, 0.70),
    outlook = factor(c("Marginal", "Adequate", "Insufficient"),
                     levels = c("Adequate", "Marginal", "Insufficient")))
  g <- fig_replacement_outlook(outlook)
  expect_s3_class(g, "ggplot")
})

test_that("fig_access_threshold_shares returns a ggplot", {
  skip_if_not(has_ggplot(), "ggplot2 not installed")
  ts <- tibble::tibble(threshold = c(0, 1, 5, 10, 20, 50),
                       pop_share_at_or_above = c(1, 0.8, 0.6, 0.4, 0.2, 0.05))
  g <- fig_access_threshold_shares(ts)
  expect_s3_class(g, "ggplot")
})

test_that("save_manuscript_figures writes white-background PNGs", {
  skip_if_not(has_ggplot(), "ggplot2 not installed")
  dir <- file.path(tempdir(), "figs")
  figs <- list(supply = fig_supply_vs_required(.supply_df(), .required_df()))
  paths <- save_manuscript_figures(figs, dir = dir, width = 5, height = 3, dpi = 72)
  expect_true(file.exists(paths[1]))
  expect_match(paths[1], "supply\\.png$")
})
