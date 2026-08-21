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
