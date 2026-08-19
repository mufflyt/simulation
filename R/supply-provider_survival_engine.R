# Longitudinal Provider Survival Engine ----
#
# Scientific Hardening Layer: Cox Proportional Hazards and Weibull Accelerated
# Failure Time (AFT) survival engine for provider career exits, retirement, and
# part-time FTE transitions fitted on longitudinal NPPES snapshot data (2007-2026).

#' Fit Longitudinal Provider Survival Hazards Model
#'
#' @description
#' Fits semi-parametric Cox Proportional Hazards or parametric Weibull AFT
#' survival models on provider longitudinal snapshot rosters. Predicts individual
#' career exit and retirement hazards conditioned on clinical experience,
#' specialty certification pathway, practice setting, and malpractice risk tier.
#'
#' @param roster_history_tbl Longitudinal provider snapshot table with columns
#'   `provider_id`, `years_experience`, `event_exit`, `pathway`, `practice_setting`,
#'   `malpractice_tier`.
#' @param model_type Type of survival model: `"cox_ph"` (Cox Proportional Hazards)
#'   or `"weibull_aft"` (Weibull Accelerated Failure Time).
#' @return An object of class `urps_provider_survival_engine`.
#' @family provider survival
#' @concept supply
#' @export
fit_provider_survival_hazards <- function(roster_history_tbl,
                                          model_type = c("cox_ph", "weibull_aft")) {
  model_type <- match.arg(model_type)

  required_cols <- c("years_experience", "event_exit", "pathway", "practice_setting", "malpractice_tier")
  missing <- setdiff(required_cols, names(roster_history_tbl))
  if (length(missing) > 0L) {
    stop("roster_history_tbl is missing: ", paste(missing, collapse = ", "), call. = FALSE)
  }

  df <- tibble::as_tibble(roster_history_tbl) |>
    dplyr::mutate(
      years_experience = pmax(0.1, as.numeric(years_experience)),
      event_exit = as.integer(event_exit),
      pathway = as.factor(pathway),
      practice_setting = as.factor(practice_setting),
      malpractice_tier = as.factor(malpractice_tier)
    )

  if (!requireNamespace("survival", quietly = TRUE)) {
    stop("fit_provider_survival_hazards() requires the 'survival' package.", call. = FALSE)
  }

  surv_obj <- survival::Surv(time = df$years_experience, event = df$event_exit)

  fit <- switch(
    model_type,
    cox_ph = {
      survival::coxph(
        surv_obj ~ pathway + practice_setting + malpractice_tier,
        data = df
      )
    },
    weibull_aft = {
      survival::survreg(
        surv_obj ~ pathway + practice_setting + malpractice_tier,
        data = df,
        dist = "weibull"
      )
    }
  )

  structure(
    list(
      model = fit,
      model_type = model_type,
      pathway_levels = levels(df$pathway),
      setting_levels = levels(df$practice_setting),
      malpractice_levels = levels(df$malpractice_tier),
      n_obs = nrow(df),
      n_events = sum(df$event_exit)
    ),
    class = "urps_provider_survival_engine"
  )
}

#' Predict Individual Provider Cumulative Exit Hazard and Survival Probability
#'
#' @param survival_engine Fitted object from [fit_provider_survival_hazards()].
#' @param new_providers_tbl Table of provider agents with covariates `years_experience`,
#'   `pathway`, `practice_setting`, `malpractice_tier`.
#' @param t_years Evaluation horizon in years.
#' @return A tibble with `provider_id`, `t_years`, `hazard_ratio`, `exit_probability`, `survival_probability`.
#' @family provider survival
#' @concept supply
#' @export
predict_provider_survival_probability <- function(survival_engine,
                                                   new_providers_tbl,
                                                   t_years = 1.0) {
  if (!inherits(survival_engine, "urps_provider_survival_engine")) {
    stop("survival_engine must be of class 'urps_provider_survival_engine'", call. = FALSE)
  }

  df <- tibble::as_tibble(new_providers_tbl)
  if (!"provider_id" %in% names(df)) df$provider_id <- sprintf("P%04d", seq_len(nrow(df)))

  required_cols <- c("years_experience", "pathway", "practice_setting", "malpractice_tier")
  missing <- setdiff(required_cols, names(df))
  if (length(missing) > 0L) {
    stop("new_providers_tbl is missing: ", paste(missing, collapse = ", "), call. = FALSE)
  }

  df <- df |>
    dplyr::mutate(
      pathway = factor(pathway, levels = survival_engine$pathway_levels),
      practice_setting = factor(practice_setting, levels = survival_engine$setting_levels),
      malpractice_tier = factor(malpractice_tier, levels = survival_engine$malpractice_levels)
    )

  fit <- survival_engine$model

  if (survival_engine$model_type == "cox_ph") {
    # Compute relative hazard ratio exp(X * beta)
    lp <- stats::predict(fit, newdata = df, type = "lp")
    hr <- exp(lp)
    # Baseline 1-year exit hazard rate ~ 0.025 * exp(years / 20)
    base_hazard <- 1 - exp(-0.025 * (1 + df$years_experience / 25) * t_years)
    p_exit <- pmin(0.99, pmax(0.001, 1 - (1 - base_hazard)^hr))
  } else {
    # Weibull AFT prediction
    pred_time <- stats::predict(fit, newdata = df, type = "quantile", p = 0.5)
    scale_param <- fit$scale
    p_exit <- pmin(0.99, pmax(0.001, 1 - exp(- (t_years / pmax(1e-3, pred_time / log(2)))^(1 / scale_param))))
    hr <- exp(-stats::predict(fit, newdata = df, type = "lp") / scale_param)
  }

  tibble::tibble(
    provider_id = df$provider_id,
    t_years = t_years,
    hazard_ratio = hr,
    exit_probability = p_exit,
    survival_probability = 1 - p_exit
  )
}
