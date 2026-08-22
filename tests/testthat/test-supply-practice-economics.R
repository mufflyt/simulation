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
