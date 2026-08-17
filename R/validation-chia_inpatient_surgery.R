# Rolling-Origin Backtest Validation for Inpatient Surgical Demand ----
#
# Calibration tier: validation_backtest (Massachusetts FY2004-FY2018)
#
# Tests whether the simulation's surgical demand model accurately predicts the
# historical age-specific trajectory of serious inpatient pelvic reconstructive
# surgery across held-out rolling-origin evaluation windows.

#' Validate inpatient surgical demand against CHIA rolling-origin series
#'
#' Evaluates out-of-sample prediction accuracy of the inpatient surgical demand model
#' across successive rolling historical cutoff years (2010..2017).
#'
#' @param chia_d6_tbl Table from [build_chia_inpatient_urps_series()].
#' @param start_cutoff First cutoff year for training (default 2010).
#' @param save_dir Directory for timestamped validation artifacts.
#'
#' @return A list containing `backtest_summary`, `arm_metrics`, `residuals`,
#'   and overall validation scores (MAPE, signed bias, RMSE, calibration slope).
#'
#' @family chia inpatient surgery
#' @concept validation
#' @export
validate_chia_inpatient_demand <- function(
    chia_d6_tbl,
    start_cutoff = 2010L,
    save_dir = "artifacts/chia_validation") {

  base::message("validate_chia_inpatient_demand(): starting rolling-origin backtest.")

  timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
  base::dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)

  years <- sort(unique(chia_d6_tbl$year))
  cutoff_years <- years[years >= start_cutoff & years < max(years)]

  results_list <- list()

  for (cutoff in cutoff_years) {
    target_year <- cutoff + 1L

    train_data <- chia_d6_tbl |> dplyr::filter(year <= cutoff)
    test_data  <- chia_d6_tbl |> dplyr::filter(year == target_year)

    if (nrow(train_data) == 0 || nrow(test_data) == 0) next

    # Fit rate model on training period
    fit_res <- fit_inpatient_surgery_rate_model(train_data, family = "quasipoisson", include_interaction = FALSE)

    # Predict target year
    pred_cases <- stats::predict(fit_res$model, newdata = test_data, type = "response")

    arm_eval <- test_data |>
      dplyr::mutate(
        cutoff_year = cutoff,
        target_year = target_year,
        predicted_cases = pred_cases,
        abs_error = abs(inpatient_cases - predicted_cases),
        ape = abs(inpatient_cases - predicted_cases) / pmax(1, inpatient_cases),
        signed_error = predicted_cases - inpatient_cases
      )

    results_list[[as.character(cutoff)]] <- arm_eval
  }

  eval_df <- dplyr::bind_rows(results_list)

  overall_mape <- mean(eval_df$ape, na.rm = TRUE) * 100
  overall_bias <- mean(eval_df$signed_error, na.rm = TRUE)
  overall_rmse <- sqrt(mean((eval_df$signed_error)^2, na.rm = TRUE))

  # Calibration slope fit: observed ~ predicted
  cal_fit <- stats::lm(inpatient_cases ~ predicted_cases, data = eval_df)
  cal_slope <- stats::coef(cal_fit)[["predicted_cases"]]

  summary_tbl <- tibble::tibble(
    metric = c("n_evaluations", "mape_percent", "signed_bias_cases", "rmse_cases", "calibration_slope"),
    value  = c(nrow(eval_df), overall_mape, overall_bias, overall_rmse, cal_slope)
  )

  base::message("Rolling-Origin Backtest Completed:")
  base::message("  Evaluated predictions: ", nrow(eval_df))
  base::message("  MAPE: ", sprintf("%.2f%%", overall_mape))
  base::message("  Signed Bias: ", sprintf("%.2f cases", overall_bias))
  base::message("  RMSE: ", sprintf("%.2f cases", overall_rmse))
  base::message("  Calibration Slope: ", sprintf("%.3f", cal_slope))

  summary_path <- base::file.path(save_dir, paste0("chia_inpatient_backtest_summary_", timestamp, ".csv"))
  eval_path    <- base::file.path(save_dir, paste0("chia_inpatient_backtest_eval_", timestamp, ".csv"))

  readr::write_csv(summary_tbl, summary_path)
  readr::write_csv(eval_df, eval_path)

  list(
    summary = summary_tbl,
    evaluations = eval_df,
    mape = overall_mape,
    bias = overall_bias,
    rmse = overall_rmse,
    calibration_slope = cal_slope,
    paths = list(summary = summary_path, evaluations = eval_path)
  )
}
