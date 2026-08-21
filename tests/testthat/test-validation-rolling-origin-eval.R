testthat::test_that("evaluate_rolling_origin_forecasts evaluates multi-horizon rolling origin forecasts", {
  # Synthetic observed panel: 2010-2025
  years <- 2010L:2025L
  observed_panel <- tibble::tibble(
    series_id = "US",
    year = years,
    observed = 1000 + (years - 2010L) * 20 + stats::rnorm(length(years), 0, 5)
  )

  # Dummy forecast function: linear extrapolation with noise
  dummy_forecast_fun <- function(training_panel, forecast_years, id_cols, year_col, value_col, n_draws, seed) {
    withr::with_seed(seed, {
      max_train_year <- max(training_panel[[year_col]])
      last_val <- tail(training_panel[[value_col]], 1)

      grid <- tidyr::crossing(
        year = forecast_years,
        draw = seq_len(n_draws)
      )
      grid$series_id <- "US"
      grid$prediction <- last_val + (grid$year - max_train_year) * 20 + stats::rnorm(nrow(grid), 0, 10)
      grid
    })
  }

  res <- evaluate_rolling_origin_forecasts(
    observed_panel = observed_panel,
    forecast_fun = dummy_forecast_fun,
    id_cols = "series_id",
    year_col = "year",
    value_col = "observed",
    horizons = c(1L, 3L, 5L),
    first_origin = 2015L,
    last_origin = 2020L,
    min_train_years = 5L,
    n_draws = 50L,
    seed = 123L
  )

  testthat::expect_true(is.list(res))
  testthat::expect_true("horizon_summary" %in% names(res))
  testthat::expect_true("summary_sentence" %in% names(res))
  testthat::expect_true(is.character(res$summary_sentence))
  testthat::expect_true(nrow(res$horizon_summary) > 0L)
})
