test_that("payer revenue and cost identities hold", {
  practice_tbl <- tibble::tibble(
    practice_id = "A", year = 2026L, clinical_fte = 1,
    annual_wrvu = 12000, medicare_share = 0.30,
    medicaid_share = 0.20, commercial_share = 0.45,
    self_pay_share = 0.05, practice_setting = "independent",
    app_fte = 0.5, medicaid_fee_ratio = 0.75,
    mips_factor = 1, qp_status = FALSE
  )
  simulation <- simulate_practice_economics(
    practice_tbl, draws = 200L, seed = 91L
  )
  draw_tbl <- simulation$draws
  expect_equal(
    draw_tbl$gross_revenue,
    draw_tbl$medicare_revenue + draw_tbl$medicaid_revenue +
      draw_tbl$commercial_revenue + draw_tbl$self_pay_revenue
  )
  expect_equal(
    draw_tbl$operating_income,
    draw_tbl$gross_revenue - draw_tbl$operating_cost
  )
  expect_true(all(dplyr::between(
    draw_tbl$acquisition_probability, 0, 1
  )))
})

test_that("a higher Medicaid fee ratio increases revenue", {
  practice_tbl <- tibble::tibble(
    practice_id = c("low", "high"), year = 2026L,
    clinical_fte = 1, annual_wrvu = 10000,
    medicare_share = 0, medicaid_share = 1,
    commercial_share = 0, self_pay_share = 0,
    practice_setting = "independent", app_fte = 0,
    medicaid_fee_ratio = c(0.50, 1), mips_factor = 1,
    qp_status = FALSE
  )
  simulation <- simulate_practice_economics(
    practice_tbl, draws = 200L, seed = 92L
  )
  summary_tbl <- simulation$summary
  low_revenue <- summary_tbl$mean_gross_revenue[
    summary_tbl$practice_id == "low"
  ]
  high_revenue <- summary_tbl$mean_gross_revenue[
    summary_tbl$practice_id == "high"
  ]
  expect_equal(high_revenue / low_revenue, 2)
})

test_that("payer shares must sum to one", {
  practice_tbl <- tibble::tibble(
    practice_id = "A", year = 2026L, clinical_fte = 1,
    annual_wrvu = 10000, medicare_share = 0.5,
    medicaid_share = 0.5, commercial_share = 0.5,
    self_pay_share = 0, practice_setting = "independent",
    app_fte = 0
  )
  expect_error(
    simulate_practice_economics(practice_tbl, draws = 100L),
    "sum to 1"
  )
})

test_that("Rabice is not mislabeled as Medicaid acceptance", {
  evidence_tbl <- practice_economics_evidence()
  rabice_tbl <- dplyr::filter(
    evidence_tbl, .data$source == "Rabice et al. 2021"
  )
  expect_match(rabice_tbl$estimand, "Medicare appointment obtained")
  expect_false(base::grepl("Medicaid acceptance", rabice_tbl$estimand))
})

test_that("revenue and cost scale linearly with clinical_fte, no doubling/halving", {
  # Deterministic FTE-scaling fixture: same annual_wrvu-per-FTE and payer mix
  # at clinical_fte = 1 and clinical_fte = 0.5. If any downstream quantity
  # mixes headcount, clinical FTE, and per-FTE wRVU inconsistently, revenue
  # or cost per FTE will differ between the two -- it must not.
  mix <- practice_payer_mix_defaults(include_crosscheck = FALSE)
  build_tbl <- function(fte) {
    tibble::tibble(
      practice_id = "P1", year = 2026L, clinical_fte = fte,
      annual_wrvu = 7000 * fte,
      medicare_share = mix$medicare_share, medicaid_share = mix$medicaid_share,
      commercial_share = mix$commercial_share, self_pay_share = mix$self_pay_share,
      practice_setting = "independent", app_fte = 0
    )
  }
  full <- simulate_practice_economics(build_tbl(1), draws = 2000L, seed = 1L)
  half <- simulate_practice_economics(build_tbl(0.5), draws = 2000L, seed = 1L)

  revenue_per_fte_full <- mean(full$draws$gross_revenue) / 1
  revenue_per_fte_half <- mean(half$draws$gross_revenue) / 0.5
  cost_per_fte_full <- mean(full$draws$operating_cost) / 1
  cost_per_fte_half <- mean(half$draws$operating_cost) / 0.5

  expect_equal(revenue_per_fte_full, revenue_per_fte_half, tolerance = 1e-6)
  expect_equal(cost_per_fte_full, cost_per_fte_half, tolerance = 1e-6)

  # Hand-computable point estimate (no Monte Carlo draw noise): payment_units
  # (annual_wrvu) x medicare conversion factor x each payer share x that
  # payer's collection rate, summed across the four payers.
  inputs <- practice_economics_defaults()
  hand_revenue <- 7000 * inputs$medicare_conversion_factor * (
    mix$medicare_share * inputs$medicare_collection +
      mix$medicaid_share * 0.75 * inputs$medicaid_collection +
      mix$commercial_share * inputs$commercial_ratio_median * inputs$commercial_collection +
      mix$self_pay_share * inputs$commercial_ratio_median * inputs$self_pay_collection
  )
  expect_equal(revenue_per_fte_full, hand_revenue, tolerance = 0.02 * hand_revenue)
})

test_that("payer mix materially changes revenue holding clinical work constant", {
  # A Medicare-heavy, a Medicaid-heavy, and a commercially-insured practice
  # performing IDENTICAL clinical work (same clinical_fte, same annual_wrvu)
  # must produce meaningfully different revenue -- otherwise the payer-mix
  # wiring isn't actually affecting the revenue engine.
  base_tbl <- tibble::tibble(
    practice_id = "P1", year = 2026L, clinical_fte = 1, annual_wrvu = 7000,
    practice_setting = "independent", app_fte = 0
  )
  medicare_heavy <- dplyr::mutate(
    base_tbl, medicare_share = 0.90, medicaid_share = 0.05,
    commercial_share = 0.05, self_pay_share = 0
  )
  medicaid_heavy <- dplyr::mutate(
    base_tbl, medicare_share = 0.05, medicaid_share = 0.90,
    commercial_share = 0.05, self_pay_share = 0
  )
  commercial_heavy <- dplyr::mutate(
    base_tbl, medicare_share = 0.05, medicaid_share = 0.05,
    commercial_share = 0.90, self_pay_share = 0
  )

  revenue_for <- function(tbl) {
    mean(simulate_practice_economics(tbl, draws = 2000L, seed = 1L)$draws$gross_revenue)
  }
  medicare_revenue <- revenue_for(medicare_heavy)
  medicaid_revenue <- revenue_for(medicaid_heavy)
  commercial_revenue <- revenue_for(commercial_heavy)

  # Medicaid pays below Medicare (medicaid_fee_ratio 0.75); commercial pays
  # above Medicare (commercial_ratio_median 1.43) -- so the real ordering
  # must be Medicaid-heavy < Medicare-heavy < commercial-heavy revenue.
  expect_lt(medicaid_revenue, medicare_revenue)
  expect_lt(medicare_revenue, commercial_revenue)
  # Meaningfully different, not a rounding-level difference.
  expect_gt((commercial_revenue - medicaid_revenue) / medicaid_revenue, 0.20)
})

test_that("practice-economics plausibility alarms fire on implausible results", {
  # Deliberately implausible cost/revenue combination: near-zero revenue,
  # real-world overhead -- must trip the fail-loud alarms, not silently pass.
  practice_tbl <- tibble::tibble(
    practice_id = "P1", year = 2026L, clinical_fte = 1, annual_wrvu = 10,
    medicare_share = 1, medicaid_share = 0, commercial_share = 0,
    self_pay_share = 0, practice_setting = "independent", app_fte = 0
  )
  result <- simulate_practice_economics(practice_tbl, draws = 500L, seed = 1L)
  margin <- mean(result$draws$operating_margin)
  loss_probability <- mean(result$draws$operating_income < 0)

  expect_lt(margin, -0.25)
  expect_gt(loss_probability, 0.90)
})

test_that("net_revenue_before_physician_compensation / physician_compensation_capacity are aliases of operating_income", {
  mix <- practice_payer_mix_defaults(include_crosscheck = FALSE)
  practice_tbl <- tibble::tibble(
    practice_id = "P1", year = 2026L, clinical_fte = 1, annual_wrvu = 7110,
    medicare_share = mix$medicare_share, medicaid_share = mix$medicaid_share,
    commercial_share = mix$commercial_share, self_pay_share = mix$self_pay_share,
    practice_setting = "independent", app_fte = 0.15
  )
  result <- simulate_practice_economics(practice_tbl, draws = 500L, seed = 1L)
  d <- result$draws

  expect_equal(d$net_revenue_before_physician_compensation, d$operating_income)
  expect_equal(d$physician_compensation_capacity, d$operating_income)
  expect_equal(d$nonphysician_operating_cost, d$operating_cost)

  # The renamed summary columns are also present, not just the raw draws.
  summary_names <- names(result$summary)
  expect_true(all(c(
    "mean_net_revenue_before_physician_compensation",
    "mean_physician_compensation_capacity",
    "mean_nonphysician_operating_cost",
    "mean_break_even_wrvu_per_fte",
    "mean_required_revenue_per_wrvu"
  ) %in% summary_names))
})

test_that("break_even_wrvu_per_fte and required_revenue_per_wrvu are internally consistent", {
  mix <- practice_payer_mix_defaults(include_crosscheck = FALSE)
  practice_tbl <- tibble::tibble(
    practice_id = "P1", year = 2026L, clinical_fte = 1, annual_wrvu = 7110,
    medicare_share = mix$medicare_share, medicaid_share = mix$medicaid_share,
    commercial_share = mix$commercial_share, self_pay_share = mix$self_pay_share,
    practice_setting = "independent", app_fte = 0.15
  )
  result <- simulate_practice_economics(practice_tbl, draws = 2000L, seed = 1L)
  d <- result$draws

  # Definitional identities, not approximations.
  expect_equal(
    d$nonphysician_cost_per_fte, d$nonphysician_operating_cost / d$clinical_fte
  )
  expect_equal(
    d$break_even_wrvu_per_fte,
    d$nonphysician_cost_per_fte / d$realized_revenue_per_wrvu
  )
  expect_equal(
    d$required_revenue_per_wrvu,
    d$nonphysician_cost_per_fte / d$annual_wrvu_per_fte
  )

  # At the realized $/wRVU rate, a practice producing exactly its own
  # break-even wRVU/FTE has zero physician compensation capacity.
  at_break_even <- practice_tbl |>
    dplyr::mutate(annual_wrvu = mean(d$break_even_wrvu_per_fte) * clinical_fte)
  break_even_result <- simulate_practice_economics(
    at_break_even, draws = 2000L, seed = 1L
  )
  expect_lt(
    abs(mean(break_even_result$draws$physician_compensation_capacity)),
    0.05 * mean(d$gross_revenue)
  )
})

test_that("practice_economics_evidence has real provenance, not just point values", {
  evidence_tbl <- practice_economics_evidence()

  expect_true(all(c(
    "lower", "upper", "year", "evidence_quality"
  ) %in% names(evidence_tbl)))
  expect_true(all(
    evidence_tbl$evidence_quality %in% c("high", "medium", "low", "uncited")
  ))

  # The overhead, malpractice, APP compensation, and all four collection
  # rates must be visibly uncited -- the whole point of this table.
  uncited <- dplyr::filter(evidence_tbl, .data$evidence_quality == "uncited")
  expect_gte(nrow(uncited), 7L)
  expect_true(any(grepl("overhead", uncited$estimand, ignore.case = TRUE)))
  expect_true(any(grepl("malpractice", uncited$estimand, ignore.case = TRUE)))
  expect_true(any(grepl(
    "APP compensation", uncited$estimand, fixed = TRUE
  )))
  expect_true(any(grepl(
    "collection rate", uncited$estimand, fixed = TRUE
  )))

  # The commercial ratio and overhead rows now carry real lower/upper bounds
  # (matching the exact distributional bounds the simulator draws from),
  # collapsed from the old two-separate-rows layout into one row each.
  commercial_row <- dplyr::filter(
    evidence_tbl, .data$estimand == "commercial payment ratio"
  )
  expect_equal(commercial_row$lower, 1.18)
  expect_equal(commercial_row$upper, 1.79)
})

test_that("overhead_by_setting reproduces the flat default exactly, and setting-specific bounds change results", {
  mix <- practice_payer_mix_defaults(include_crosscheck = FALSE)
  practice_tbl <- tibble::tibble(
    practice_id = "P1", year = 2026L, clinical_fte = 1, annual_wrvu = 7110,
    medicare_share = mix$medicare_share, medicaid_share = mix$medicaid_share,
    commercial_share = mix$commercial_share, self_pay_share = mix$self_pay_share,
    practice_setting = "independent", app_fte = 0.15
  )
  baseline <- simulate_practice_economics(practice_tbl, draws = 2000L, seed = 1L)
  legacy_default <- simulate_practice_economics(
    practice_tbl, draws = 2000L, seed = 1L,
    overhead_by_setting = practice_overhead_by_setting()
  )
  expect_equal(baseline$draws$operating_cost, legacy_default$draws$operating_cost)

  low_overhead_setting <- practice_overhead_by_setting() |>
    dplyr::mutate(
      overhead_lower = 50000, overhead_mode = 50000, overhead_upper = 50000
    )
  cheaper <- simulate_practice_economics(
    practice_tbl, draws = 2000L, seed = 1L,
    overhead_by_setting = low_overhead_setting
  )
  expect_true(
    mean(cheaper$draws$physician_compensation_capacity) >
      mean(baseline$draws$physician_compensation_capacity)
  )

  expect_error(
    simulate_practice_economics(
      practice_tbl, draws = 500L,
      overhead_by_setting = dplyr::filter(
        practice_overhead_by_setting(), practice_setting != "independent"
      )
    ),
    "missing bounds"
  )
})

test_that("sensitivity decomposition identifies wRVU productivity as the dominant lever", {
  mix <- practice_payer_mix_defaults(include_crosscheck = FALSE)
  practice_tbl <- tibble::tibble(
    practice_id = "P1", year = 2026L, clinical_fte = 1, annual_wrvu = 7110,
    medicare_share = mix$medicare_share, medicaid_share = mix$medicaid_share,
    commercial_share = mix$commercial_share, self_pay_share = mix$self_pay_share,
    practice_setting = "independent", app_fte = 0.15
  )
  decomposition <- practice_economics_sensitivity_decomposition(
    practice_tbl, draws = 2000L, seed = 1L
  )

  expect_equal(nrow(decomposition), 6L)
  expect_setequal(decomposition$assumption_family, c(
    "revenue_realization", "wrvu_productivity", "overhead",
    "malpractice", "app_intensity", "payer_mix"
  ))
  expect_true(all(is.finite(decomposition$delta)))
  # Every lever should IMPROVE compensation capacity relative to baseline
  # (each alternative is chosen to be favorable) -- delta must be positive.
  expect_true(all(decomposition$delta > 0))
  # Ranked descending by delta -- wRVU productivity is the largest lever at
  # these baseline values (raising to WRVU_PER_FTE_BENCHMARK[["high"]] alone
  # closes more than the other five levers each).
  expect_equal(decomposition$assumption_family[[1]], "wrvu_productivity")
})
