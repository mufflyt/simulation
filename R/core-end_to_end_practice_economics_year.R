# Per-year practice-economics diagnostic step for run_end_to_end_simulation --

#' Run the practice-economics diagnostic for one simulation year
#'
#' Extracted from [run_end_to_end_simulation()]'s per-year loop so that
#' function stays under the repository's module code-line ceiling; this is a
#' pure code move (identical logic, now callable on its own) not a behavior
#' change. Builds one practice-year row per active provider, draws practice
#' economics via [simulate_practice_economics()], summarizes the
#' cost/revenue decomposition, compares modeled physician compensation
#' capacity against the MedPAC surgical-specialty benchmark, and fires the
#' fail-loud plausibility alarms (margin, loss probability, revenue-per-wRVU)
#' documented in [simulate_practice_economics()].
#'
#' @param active_providers Active-provider tibble for `simulation_year`.
#' @param simulation_year The calendar year being simulated.
#' @param year_index Position of `simulation_year` within the run's `years`
#'   vector; used only to vary the practice-economics draw seed per year.
#' @param supplied_fte Total supplied clinical FTE for `simulation_year`.
#' @param wrvu_total Total wRVUs delivered in `simulation_year`.
#' @param practice_payer_mix Named list/row with `medicare_share`,
#'   `medicaid_share`, `commercial_share`, `self_pay_share`.
#' @param app_delegation_rate APP-to-physician FTE delegation rate.
#' @param seed Base simulation seed (the practice-economics draw uses
#'   `seed + year_index`).
#' @param practice_economics_draws Monte Carlo draw count for
#'   [simulate_practice_economics()].
#'
#' @return One-row diagnostic tibble for `practice_economics_diagnostics`.
#' @keywords internal
.run_practice_economics_year <- function(active_providers,
                                          simulation_year,
                                          year_index,
                                          supplied_fte,
                                          wrvu_total,
                                          practice_payer_mix,
                                          app_delegation_rate,
                                          seed,
                                          practice_economics_draws) {
  practice_tbl <- active_providers |>
    dplyr::transmute(
      practice_id = .data$provider_id,
      year = simulation_year,
      clinical_fte = .data$fte,
      annual_wrvu = wrvu_total * .data$fte / supplied_fte,
      medicare_share = practice_payer_mix$medicare_share,
      medicaid_share = practice_payer_mix$medicaid_share,
      commercial_share = practice_payer_mix$commercial_share,
      self_pay_share = practice_payer_mix$self_pay_share,
      practice_setting = dplyr::case_when(
        .data$academic_setting ~ "academic",
        .data$hospital_outpatient ~ "hospital_employed",
        TRUE ~ "independent"
      ),
      app_fte = .data$fte * app_delegation_rate
    )
  practice_result <- simulate_practice_economics(
    practice_tbl,
    draws = practice_economics_draws,
    seed = seed + year_index
  )
  practice_draws <- practice_result$draws
  # Exact cost-component decomposition (verified by accounting identity
  # in tests/testthat/test-supply-practice-economics.R): operating_cost
  # == clinical_fte*(overhead_per_fte + malpractice_per_fte) +
  # app_fte*app_compensation. There is NO physician-compensation line
  # item anywhere in this cost model -- mean_operating_income is money
  # available to compensate the physician BEFORE they are paid, not a
  # true bottom-line practice profit. A negative value does not mean
  # cash losses in the ordinary sense; it means the composed overhead +
  # malpractice + APP-labor cost exceeds wRVU-proxy revenue before
  # physician pay is even considered.
  practice_diagnostic_row <- practice_result$summary |>
    dplyr::summarise(
      year = simulation_year,
      n_practices = dplyr::n(),
      mean_wrvu_per_fte = base::sum(practice_tbl$annual_wrvu) /
        base::sum(practice_tbl$clinical_fte),
      mean_revenue_per_fte = base::mean(
        practice_draws$gross_revenue / practice_draws$clinical_fte
      ),
      mean_expense_per_fte = base::mean(
        practice_draws$operating_cost / practice_draws$clinical_fte
      ),
      mean_overhead_per_fte = base::mean(practice_draws$overhead_per_fte),
      mean_malpractice_per_fte = base::mean(
        practice_draws$malpractice_per_fte
      ),
      mean_app_labor_per_fte = base::mean(
        practice_draws$app_fte * practice_draws$app_compensation /
          practice_draws$clinical_fte
      ),
      mean_revenue_per_wrvu = base::sum(practice_draws$gross_revenue) /
        base::sum(practice_draws$annual_wrvu),
      mean_break_even_wrvu_per_fte = base::mean(
        practice_draws$break_even_wrvu_per_fte, na.rm = TRUE
      ),
      mean_required_revenue_per_wrvu = base::mean(
        practice_draws$required_revenue_per_wrvu, na.rm = TRUE
      ),
      mean_operating_income = base::mean(practice_draws$operating_income),
      # Legacy alias, identical arithmetic to mean_operating_income --
      # see the file-level comment above on why this is the primary
      # name going forward: there is no physician-compensation line
      # item anywhere in this cost model, so a negative value means
      # modeled professional revenue does not cover nonphysician
      # practice costs, before the physician is paid at all.
      mean_physician_compensation_capacity = base::mean(
        practice_draws$physician_compensation_capacity
      ),
      # Two distinct estimands, kept separate rather than assumed equal:
      # the sum-based aggregate margin (national revenue-weighted) and
      # the distributional mean-of-per-practice-median margin (equal
      # weight per practice regardless of size).
      aggregate_operating_margin =
        (base::sum(practice_draws$gross_revenue) -
          base::sum(practice_draws$operating_cost)) /
          base::sum(practice_draws$gross_revenue),
      mean_operating_margin = base::mean(
        .data$median_operating_margin, na.rm = TRUE
      ),
      mean_loss_probability = base::mean(
        .data$loss_probability, na.rm = TRUE
      ),
      mean_acquisition_probability = base::mean(
        .data$acquisition_probability, na.rm = TRUE
      ),
      mean_cash_pay_probability = base::mean(
        .data$cash_pay_probability, na.rm = TRUE
      )
    )

  # EXTERNAL PLAUSIBILITY CONTEXT, not an internal-consistency alarm and
  # never a calibration target: compares modeled compensation capacity
  # against MedPAC's real, cited surgical-specialty compensation median
  # (FPMRS/urogyn falls under MedPAC's "surgical" grouping, which
  # explicitly includes OB/GYN and urology). Reported via message(), not
  # warning() -- disagreeing with an external benchmark is a different
  # kind of signal than the internal accounting alarms below.
  surgical_benchmark <- physician_compensation_plausibility(
    practice_draws$physician_compensation_capacity
  ) |>
    dplyr::filter(.data$benchmark_name == "surgical")
  practice_diagnostic_row$pct_of_medpac_surgical_benchmark <-
    surgical_benchmark$pct_of_benchmark
  practice_diagnostic_row$medpac_plausibility_band <-
    surgical_benchmark$plausibility_band
  base::message(
    "practice-economics year ", simulation_year,
    ": modeled physician compensation capacity is ",
    surgical_benchmark$plausibility_band,
    " vs. MedPAC's $496,000 surgical-specialty median (",
    base::sprintf("%.1f%%", surgical_benchmark$pct_of_benchmark),
    " of benchmark)."
  )

  # FAIL-LOUD PLAUSIBILITY ALARMS, not calibration targets: these warn
  # (never silently adjust an input) when the composed cost/revenue
  # model produces an implausible headline result, so a wrong number
  # cannot pass through unnoticed the way -56% margins / 99.98% loss
  # probability did on the first wiring pass. See R/supply-
  # practice_economics.R's overhead assumption -- "User-specified
  # scenario"/"assumption" status, the one cost input with no external
  # citation -- as the most likely thing to revisit if these fire.
  if (base::isTRUE(practice_diagnostic_row$mean_operating_margin < -0.25)) {
    base::warning(
      "run_end_to_end_simulation(): year ", simulation_year,
      " practice-economics mean operating margin is ",
      base::sprintf("%.1f%%", 100 * practice_diagnostic_row$mean_operating_margin),
      " (< -25% plausibility bound). This is a fail-loud alarm about the",
      " composed revenue/cost model, not a claim the simulation is wrong;",
      " see practice_economics_diagnostics for the revenue/expense",
      " decomposition.",
      call. = FALSE
    )
  }
  if (base::isTRUE(practice_diagnostic_row$mean_loss_probability > 0.90)) {
    base::warning(
      "run_end_to_end_simulation(): year ", simulation_year,
      " practice-economics loss probability is ",
      base::sprintf("%.1f%%", 100 * practice_diagnostic_row$mean_loss_probability),
      " (> 90% plausibility bound).",
      call. = FALSE
    )
  }
  if (base::isTRUE(practice_diagnostic_row$mean_revenue_per_wrvu < 15 ||
      practice_diagnostic_row$mean_revenue_per_wrvu > 100)) {
    base::warning(
      "run_end_to_end_simulation(): year ", simulation_year,
      " realized revenue per wRVU is $",
      base::sprintf("%.2f", practice_diagnostic_row$mean_revenue_per_wrvu),
      ", outside the [$15, $100] plausibility bound around real Medicare",
      " conversion factors (~$33-34/RVU).",
      call. = FALSE
    )
  }

  practice_diagnostic_row
}
