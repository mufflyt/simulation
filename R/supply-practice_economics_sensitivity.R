# Practice economics sensitivity, elasticity, and compensation benchmarking -

#' One-at-a-time sensitivity decomposition for practice economics
#'
#' @description
#' Holds every assumption at baseline except one, sets that one family to a
#' single favorable alternative, and reports how much of the baseline
#' shortfall to break-even (`0 - physician_compensation_capacity`) closing
#' just that one lever would explain. Answers "is this a productivity
#' problem, a reimbursement problem, or a cost-model problem?" rather than
#' leaving six confounded assumptions inside one margin number.
#'
#' Each lever's alternative is a single, named, defensible value -- not a
#' search for whatever number makes the practice profitable:
#' \describe{
#'   \item{revenue_realization}{All four payer collection rates set to 1.0
#'     (perfect billing/collections) instead of `inputs$*_collection`.}
#'   \item{wrvu_productivity}{`annual_wrvu` per FTE raised to
#'     `WRVU_PER_FTE_BENCHMARK[["high"]]` instead of the supplied value.}
#'   \item{overhead}{Overhead fixed at `inputs$overhead_lower` instead of
#'     drawn from the triangular distribution.}
#'   \item{malpractice}{Malpractice fixed at the 10th percentile of its
#'     lognormal distribution instead of drawn.}
#'   \item{app_intensity}{`app_fte` set to 0 (no APP cost) instead of the
#'     supplied value.}
#'   \item{payer_mix}{Payer mix set to 100% commercial (this model's
#'     highest-paying payer) instead of the supplied mix.}
#' }
#'
#' @param practice_tbl Baseline practice-tbl, as for
#'   [simulate_practice_economics()].
#' @param inputs Named list from [practice_economics_defaults()].
#' @param draws Monte Carlo draws per scenario.
#' @param seed Reproducible random seed (same seed reused per scenario so
#'   differences reflect the perturbation, not draw noise).
#'
#' @return Tibble: `assumption_family`, `baseline_physician_compensation_capacity`,
#'   `perturbed_physician_compensation_capacity`, `delta`,
#'   `counterfactual_shortfall_closed_pct` (`NA` when the baseline is
#'   already break-even or better). Named deliberately as a COUNTERFACTUAL
#'   percentage -- how much of the shortfall THIS ONE arbitrary favorable
#'   value would close, holding everything else fixed -- not a variance
#'   attribution or a causal decomposition. The six levers' alternatives
#'   differ in magnitude and their effects can overlap, so the values do not
#'   partition 100% of anything; a lever showing 137.8% does not mean it
#'   "explains" 137.8% of the problem, only that its one chosen alternative
#'   value, alone, would over-close the shortfall.
#' @concept economics
#' @export
practice_economics_sensitivity_decomposition <- function(
    practice_tbl,
    inputs = practice_economics_defaults(),
    draws = 1000L,
    seed = 20260821L) {
  capacity_of <- function(tbl, inputs_arg) {
    base::mean(
      simulate_practice_economics(
        tbl, draws = draws, seed = seed, inputs = inputs_arg
      )$draws$physician_compensation_capacity
    )
  }
  baseline_capacity <- capacity_of(practice_tbl, inputs)
  shortfall <- -baseline_capacity

  perfect_collection_inputs <- inputs
  perfect_collection_inputs$medicare_collection <- 1
  perfect_collection_inputs$medicaid_collection <- 1
  perfect_collection_inputs$commercial_collection <- 1
  perfect_collection_inputs$self_pay_collection <- 1

  high_wrvu_tbl <- practice_tbl |>
    dplyr::mutate(
      annual_wrvu = WRVU_PER_FTE_BENCHMARK[["high"]] * .data$clinical_fte
    )

  low_overhead_inputs <- inputs
  low_overhead_inputs$overhead_mode <- inputs$overhead_lower
  low_overhead_inputs$overhead_upper <- inputs$overhead_lower

  malpractice_p10 <- stats::qlnorm(
    0.10, base::log(inputs$malpractice_median),
    base::sqrt(base::log1p(inputs$malpractice_cv^2))
  )
  low_malpractice_inputs <- inputs
  low_malpractice_inputs$malpractice_median <- malpractice_p10
  low_malpractice_inputs$malpractice_cv <- 1e-6

  no_app_tbl <- practice_tbl |> dplyr::mutate(app_fte = 0)

  commercial_only_tbl <- practice_tbl |>
    dplyr::mutate(
      medicare_share = 0, medicaid_share = 0,
      commercial_share = 1, self_pay_share = 0
    )

  scenarios <- base::list(
    revenue_realization = base::list(
      tbl = practice_tbl, inputs = perfect_collection_inputs
    ),
    wrvu_productivity = base::list(tbl = high_wrvu_tbl, inputs = inputs),
    overhead = base::list(tbl = practice_tbl, inputs = low_overhead_inputs),
    malpractice = base::list(
      tbl = practice_tbl, inputs = low_malpractice_inputs
    ),
    app_intensity = base::list(tbl = no_app_tbl, inputs = inputs),
    payer_mix = base::list(tbl = commercial_only_tbl, inputs = inputs)
  )

  results <- purrr::imap_dfr(scenarios, function(scenario, family_name) {
    perturbed_capacity <- capacity_of(scenario$tbl, scenario$inputs)
    tibble::tibble(
      assumption_family = family_name,
      baseline_physician_compensation_capacity = baseline_capacity,
      perturbed_physician_compensation_capacity = perturbed_capacity,
      delta = perturbed_capacity - baseline_capacity,
      counterfactual_shortfall_closed_pct = if (shortfall > 0) {
        100 * (perturbed_capacity - baseline_capacity) / shortfall
      } else {
        NA_real_
      }
    )
  })
  results |> dplyr::arrange(dplyr::desc(.data$delta))
}

#' Standardized elasticities for practice-economics assumptions
#'
#' @description
#' [practice_economics_sensitivity_decomposition()] perturbs each assumption
#' family to a single, differently-sized "favorable" alternative -- useful
#' for ranking levers, not for comparing their sensitivity on a common
#' scale, since a bigger counterfactual shift on one input isn't the same
#' claim as a bigger underlying sensitivity. This function reports the
#' standard elasticity, `(pct change in physician_compensation_capacity) /
#' (pct change in the input)`, for the model's continuous, distributionally-
#' declared inputs only.
#'
#' `payer_mix` is excluded: it is compositional (four shares summing to 1,
#' not a single scalar that moves by "+-10\%"). `annual_wrvu`/`app_fte` are
#' excluded too: they are `practice_tbl` fields with no uncertainty
#' distribution declared for them in this model -- see
#' [practice_economics_sensitivity_decomposition()]'s `wrvu_productivity`/
#' `app_intensity` scenarios for those instead.
#'
#' Two perturbation schemes, reported side by side:
#' \describe{
#'   \item{pct10}{Every continuous input's central value (mode/median/mean)
#'     moved +-`pct_perturbation`, holding its declared spread/shape fixed
#'     -- symmetric and comparable across inputs.}
#'   \item{p25_p75}{Only for `overhead`, `malpractice`, `app_compensation`,
#'     and `commercial_ratio` -- inputs with an uncertainty distribution
#'     ALREADY declared in `inputs`. Uses the real p25/p75 of that exact
#'     declared distribution (collapsed to a point at that value, the same
#'     technique [practice_economics_sensitivity_decomposition()] uses),
#'     not an invented range. `NA` for the four collection rates, which
#'     have no declared distribution at all.}
#' }
#'
#' Elasticity on a near-zero or sign-changing baseline
#' `physician_compensation_capacity` is numerically unstable (a small
#' denominator inflates the ratio, and a perturbation that crosses zero
#' makes "percent change" discontinuous) -- `unstable_baseline` flags this
#' rather than reporting a number that looks precise but isn't.
#'
#' @param practice_tbl Baseline practice-tbl, as for
#'   [simulate_practice_economics()].
#' @param inputs Named list from [practice_economics_defaults()].
#' @param draws Monte Carlo draws per scenario.
#' @param seed Reproducible random seed (same seed reused per scenario so
#'   differences reflect the perturbation, not draw noise).
#' @param pct_perturbation Symmetric perturbation fraction for the `pct10`
#'   scheme (default `0.10`).
#'
#' `elasticity_low`/`elasticity_high` are not forced to be equal, even for
#' an input (like `overhead`) that enters the cost formula linearly: the
#' same `seed` is reused across scenarios so differences reflect the
#' perturbation rather than fresh draw noise, but pinning one input to a
#' fixed value changes how many random draws the OTHER stochastic inputs
#' consume from the same seeded stream, so a few-percent low/high asymmetry
#' on an otherwise-linear input is expected Monte Carlo artifact, not a
#' sign of real nonlinearity -- a large asymmetry (multiples, not a few
#' percent) is the signal worth investigating.
#'
#' @return Tibble: `input_name`, `perturbation_type` (`"pct10"`/`"p25_p75"`),
#'   `baseline_value`, `low_value`, `high_value`, `elasticity_low`,
#'   `elasticity_high`, `unstable_baseline`.
#' @concept economics
#' @export
practice_economics_elasticity <- function(
    practice_tbl,
    inputs = practice_economics_defaults(),
    draws = 1000L,
    seed = 20260821L,
    pct_perturbation = 0.10) {
  capacity_of <- function(inputs_arg) {
    base::mean(
      simulate_practice_economics(
        practice_tbl, draws = draws, seed = seed, inputs = inputs_arg
      )$draws$physician_compensation_capacity
    )
  }
  baseline_capacity <- capacity_of(inputs)
  baseline_revenue <- base::mean(
    simulate_practice_economics(
      practice_tbl, draws = draws, seed = seed, inputs = inputs
    )$draws$gross_revenue
  )
  # A baseline within 1% of national mean gross revenue is "near enough to
  # zero" for a percent-change ratio to blow up; this is a property of the
  # denominator, not of any one input's real effect.
  unstable_baseline <- base::abs(baseline_capacity) < 0.01 * baseline_revenue

  elasticity_row <- function(input_name, baseline_value, low_value,
                              high_value, low_inputs, high_inputs,
                              perturbation_type) {
    low_capacity <- capacity_of(low_inputs)
    high_capacity <- capacity_of(high_inputs)
    pct_change_capacity_low <-
      (low_capacity - baseline_capacity) / base::abs(baseline_capacity)
    pct_change_capacity_high <-
      (high_capacity - baseline_capacity) / base::abs(baseline_capacity)
    pct_change_input_low <-
      (low_value - baseline_value) / base::abs(baseline_value)
    pct_change_input_high <-
      (high_value - baseline_value) / base::abs(baseline_value)
    tibble::tibble(
      input_name = input_name,
      perturbation_type = perturbation_type,
      baseline_value = baseline_value,
      low_value = low_value,
      high_value = high_value,
      elasticity_low = pct_change_capacity_low / pct_change_input_low,
      elasticity_high = pct_change_capacity_high / pct_change_input_high,
      unstable_baseline = unstable_baseline
    )
  }

  scale_central <- function(inputs_arg, field, factor) {
    inputs_arg[[field]] <- inputs_arg[[field]] * factor
    inputs_arg
  }
  collapse_to_point <- function(inputs_arg, mode_field, lower_field,
                                 upper_field, value) {
    inputs_arg[[mode_field]] <- value
    if (!base::is.null(lower_field)) inputs_arg[[lower_field]] <- value
    if (!base::is.null(upper_field)) inputs_arg[[upper_field]] <- value
    inputs_arg
  }

  rows <- base::list()

  # --- overhead: triangular(lower, mode, upper) -----------------------
  rows$overhead_pct10 <- elasticity_row(
    "overhead", inputs$overhead_mode,
    inputs$overhead_mode * (1 - pct_perturbation),
    inputs$overhead_mode * (1 + pct_perturbation),
    low_inputs = base::within(inputs, {
      overhead_lower <- overhead_lower * (1 - pct_perturbation)
      overhead_mode <- overhead_mode * (1 - pct_perturbation)
      overhead_upper <- overhead_upper * (1 - pct_perturbation)
    }),
    high_inputs = base::within(inputs, {
      overhead_lower <- overhead_lower * (1 + pct_perturbation)
      overhead_mode <- overhead_mode * (1 + pct_perturbation)
      overhead_upper <- overhead_upper * (1 + pct_perturbation)
    }),
    perturbation_type = "pct10"
  )
  overhead_p25 <- .practice_triangular_quantile(
    0.25, inputs$overhead_lower, inputs$overhead_mode, inputs$overhead_upper
  )
  overhead_p75 <- .practice_triangular_quantile(
    0.75, inputs$overhead_lower, inputs$overhead_mode, inputs$overhead_upper
  )
  rows$overhead_p25p75 <- elasticity_row(
    "overhead", inputs$overhead_mode, overhead_p25, overhead_p75,
    low_inputs = collapse_to_point(
      inputs, "overhead_mode", "overhead_lower", "overhead_upper",
      overhead_p25
    ),
    high_inputs = collapse_to_point(
      inputs, "overhead_mode", "overhead_lower", "overhead_upper",
      overhead_p75
    ),
    perturbation_type = "p25_p75"
  )

  # --- malpractice: lognormal(median, cv) -----------------------------
  malpractice_sdlog <- base::sqrt(base::log1p(inputs$malpractice_cv^2))
  rows$malpractice_pct10 <- elasticity_row(
    "malpractice", inputs$malpractice_median,
    inputs$malpractice_median * (1 - pct_perturbation),
    inputs$malpractice_median * (1 + pct_perturbation),
    low_inputs = scale_central(
      inputs, "malpractice_median", 1 - pct_perturbation
    ),
    high_inputs = scale_central(
      inputs, "malpractice_median", 1 + pct_perturbation
    ),
    perturbation_type = "pct10"
  )
  malpractice_p25 <- stats::qlnorm(
    0.25, base::log(inputs$malpractice_median), malpractice_sdlog
  )
  malpractice_p75 <- stats::qlnorm(
    0.75, base::log(inputs$malpractice_median), malpractice_sdlog
  )
  rows$malpractice_p25p75 <- elasticity_row(
    "malpractice", inputs$malpractice_median, malpractice_p25,
    malpractice_p75,
    low_inputs = base::within(inputs, {
      malpractice_median <- malpractice_p25
      malpractice_cv <- 1e-6
    }),
    high_inputs = base::within(inputs, {
      malpractice_median <- malpractice_p75
      malpractice_cv <- 1e-6
    }),
    perturbation_type = "p25_p75"
  )

  # --- APP compensation: normal(mean, sd) -----------------------------
  rows$app_compensation_pct10 <- elasticity_row(
    "app_compensation", inputs$app_compensation_mean,
    inputs$app_compensation_mean * (1 - pct_perturbation),
    inputs$app_compensation_mean * (1 + pct_perturbation),
    low_inputs = scale_central(
      inputs, "app_compensation_mean", 1 - pct_perturbation
    ),
    high_inputs = scale_central(
      inputs, "app_compensation_mean", 1 + pct_perturbation
    ),
    perturbation_type = "pct10"
  )
  app_p25 <- stats::qnorm(
    0.25, inputs$app_compensation_mean, inputs$app_compensation_sd
  )
  app_p75 <- stats::qnorm(
    0.75, inputs$app_compensation_mean, inputs$app_compensation_sd
  )
  rows$app_compensation_p25p75 <- elasticity_row(
    "app_compensation", inputs$app_compensation_mean, app_p25, app_p75,
    low_inputs = base::within(inputs, {
      app_compensation_mean <- app_p25
      app_compensation_sd <- 1e-6
    }),
    high_inputs = base::within(inputs, {
      app_compensation_mean <- app_p75
      app_compensation_sd <- 1e-6
    }),
    perturbation_type = "p25_p75"
  )

  # --- commercial ratio: lognormal(median, sd from 95% CI [lower,upper]) --
  commercial_sdlog <- (base::log(inputs$commercial_ratio_upper) -
    base::log(inputs$commercial_ratio_lower)) / (2 * 1.96)
  rows$commercial_ratio_pct10 <- elasticity_row(
    "commercial_ratio", inputs$commercial_ratio_median,
    inputs$commercial_ratio_median * (1 - pct_perturbation),
    inputs$commercial_ratio_median * (1 + pct_perturbation),
    low_inputs = scale_central(
      inputs, "commercial_ratio_median", 1 - pct_perturbation
    ),
    high_inputs = scale_central(
      inputs, "commercial_ratio_median", 1 + pct_perturbation
    ),
    perturbation_type = "pct10"
  )
  commercial_p25 <- stats::qlnorm(
    0.25, base::log(inputs$commercial_ratio_median), commercial_sdlog
  )
  commercial_p75 <- stats::qlnorm(
    0.75, base::log(inputs$commercial_ratio_median), commercial_sdlog
  )
  rows$commercial_ratio_p25p75 <- elasticity_row(
    "commercial_ratio", inputs$commercial_ratio_median, commercial_p25,
    commercial_p75,
    low_inputs = scale_central(
      inputs, "commercial_ratio_median",
      commercial_p25 / inputs$commercial_ratio_median
    ),
    high_inputs = scale_central(
      inputs, "commercial_ratio_median",
      commercial_p75 / inputs$commercial_ratio_median
    ),
    perturbation_type = "p25_p75"
  )

  # --- payer collection rates: point values, no declared distribution --
  # +-10% only, capped at 1.0 (collection cannot exceed 100%); p25/p75 is
  # NA because none is declared for these four inputs.
  for (rate_field in base::c(
    "medicare_collection", "medicaid_collection",
    "commercial_collection", "self_pay_collection"
  )) {
    baseline_rate <- inputs[[rate_field]]
    low_rate <- baseline_rate * (1 - pct_perturbation)
    high_rate <- base::min(1, baseline_rate * (1 + pct_perturbation))
    rows[[base::paste0(rate_field, "_pct10")]] <- elasticity_row(
      rate_field, baseline_rate, low_rate, high_rate,
      low_inputs = scale_central(inputs, rate_field, 1 - pct_perturbation),
      high_inputs = `[[<-`(
        inputs, rate_field, base::min(1, baseline_rate * (1 + pct_perturbation))
      ),
      perturbation_type = "pct10"
    )
  }

  dplyr::bind_rows(rows)
}

#' MedPAC physician/APP compensation benchmarks
#'
#' @description
#' Extracts the three `external_benchmark` rows from
#' [practice_economics_evidence()] (MedPAC March 2025 Report to Congress,
#' Ch. 4, citing SullivanCotter's 2024 survey, 2023 compensation data) as a
#' small lookup tibble, so [physician_compensation_plausibility()] and any
#' other caller read the same cited numbers rather than a second copy.
#'
#' @return Tibble: `benchmark_name` (`"all_specialties"`, `"surgical"`,
#'   `"app"`), `value` (2023 USD).
#' @concept economics
#' @export
physician_compensation_benchmarks <- function() {
  evidence_tbl <- practice_economics_evidence()
  benchmark_rows <- evidence_tbl |>
    dplyr::filter(.data$status == "external_benchmark")
  tibble::tibble(
    benchmark_name = base::c("all_specialties", "surgical", "app"),
    value = base::c(
      benchmark_rows$value[
        base::grepl("all specialties", benchmark_rows$estimand)
      ],
      benchmark_rows$value[
        base::grepl("surgical specialties", benchmark_rows$estimand)
      ],
      benchmark_rows$value[
        base::grepl("advanced practice provider", benchmark_rows$estimand)
      ]
    )
  )
}

#' Physician-compensation plausibility check (benchmark, not calibration target)
#'
#' @description
#' Reports whether modeled `physician_compensation_capacity` overlaps a
#' plausible real-world range implied by [physician_compensation_benchmarks()]
#' -- it does not adjust any input, and nothing in this package feeds this
#' comparison back into `practice_economics_defaults()` or any simulation
#' parameter. The banding (implausibly low / plausible range / implausibly
#' high) is a documented heuristic (50%-150% of the benchmark), not a
#' statistical test -- MedPAC publishes a median, not a distribution this
#' model's compensation-capacity draws could be formally tested against.
#'
#' @param physician_compensation_capacity Numeric vector of
#'   `physician_compensation_capacity` draws (or their mean), as produced by
#'   [simulate_practice_economics()].
#' @param benchmarks Tibble from [physician_compensation_benchmarks()];
#'   recomputed when `NULL`.
#'
#' @return Tibble: `benchmark_name`, `benchmark_value`,
#'   `modeled_compensation_capacity`, `pct_of_benchmark`, `plausibility_band`.
#' @concept economics
#' @export
physician_compensation_plausibility <- function(
    physician_compensation_capacity, benchmarks = NULL) {
  if (base::is.null(benchmarks)) benchmarks <- physician_compensation_benchmarks()
  modeled_capacity <- base::mean(physician_compensation_capacity, na.rm = TRUE)

  benchmarks |>
    dplyr::mutate(
      modeled_compensation_capacity = modeled_capacity,
      pct_of_benchmark = 100 * modeled_capacity / .data$value,
      plausibility_band = dplyr::case_when(
        modeled_capacity <= 0 ~
          "non-positive (model implies unpayable compensation)",
        .data$pct_of_benchmark < 50 ~ "implausibly low (<50% of benchmark)",
        .data$pct_of_benchmark <= 150 ~
          "plausible range (50-150% of benchmark)",
        TRUE ~ "implausibly high (>150% of benchmark)"
      )
    ) |>
    dplyr::rename(benchmark_value = "value")
}

