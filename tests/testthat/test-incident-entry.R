# Correctness tests for the incident-entry hazard estimator
# (R/demand-incident_entry.R) and its pre-registered sensitivity-matrix
# runner. See docs/INCIDENT_ENTRY_ESTIMAND.md.
#
# WHY THESE GO BEYOND A SMOKE TEST. A 400-line statistical pipeline that
# "runs without erroring" on synthetic input proves nothing about whether its
# washout logic, age-band boundaries, small-cell suppression, or cross-payer
# rollup are actually CORRECT -- exactly the gap this file exists to close,
# since this estimator has no real data to validate against yet and won't
# until the APCD/all-payer claims request is fulfilled. Every fixture below
# is hand-traceable: the expected numbers are derived from the month
# arithmetic and washout rule directly, not from running the function and
# eyeballing the result.
#
# MONTH-ID ARITHMETIC, so the fixtures below are auditable. The function
# encodes a (year, month) pair as `year * 12L + month`. For a 2023 index
# year: index_start_month = 2023*12+1 = 24277. window_start_month =
# index_start_month - washout_months. window_end_month =
# index_start_month + 11 (index_start_month..window_end_month is the ENTRY
# window: Jan-Dec of the index year). A claim counts as PRIOR CARE (washout
# violation) iff window_start_month <= claim_month_id < index_start_month; it
# counts as an ENTRY iff index_start_month <= claim_month_id <= window_end_month.

.fte_stock_rows <- function(age_band, year, payer_group, prob = 1.0) {
  tidyr::crossing(
    condition = c("ui", "pop", "ai"),
    age_band = age_band, year = year, payer_group = payer_group,
    eligible_stock_probability = prob
  )
}

.fte_enrolled <- function(person_ids, coverage_years) {
  tidyr::crossing(person_id = person_ids, coverage_year = coverage_years, coverage_month = 1:12)
}

test_that("incident_entry_wilson matches the standard Wilson score formula", {
  # Ground truth via base R's stats::prop.test(correct = FALSE), which
  # implements the same interval by a different, independently-written
  # code path -- an actual cross-check, not a restatement of the function
  # under test. successes=5, trials=20: prop.test(5, 20, correct =
  # FALSE)$conf.int == c(0.1118617, 0.4687009).
  res <- incident_entry_wilson(successes = 5, trials = 20, conf_level = 0.95)
  expect_equal(res$q_low, 0.1118617, tolerance = 1e-6)
  expect_equal(res$q_high, 0.4687009, tolerance = 1e-6)
})

test_that("estimate_incident_entry_hazard runs on valid synthetic inputs", {
  member_year_tbl <- tibble::tribble(
    ~person_id, ~year, ~female, ~age, ~payer_group,
    "P001", 2023L, TRUE, 52L, "Commercial",
    "P002", 2023L, TRUE, 68L, "Medicare"
  )
  enrollment_tbl <- .fte_enrolled(c("P001", "P002"), 2021:2023)
  claims_tbl <- tibble::tribble(
    ~person_id, ~service_year, ~service_month, ~rendering_npi, ~condition, ~is_outpatient_evaluation, ~is_qualifying_urps_encounter,
    "P001", 2023L, 4L, "1234567890", "ui", TRUE, TRUE
  )
  roster_tbl <- tibble::tibble(rendering_npi = "1234567890")
  stock_probability_tbl <- tidyr::crossing(
    condition = c("ui", "pop", "ai"),
    age_band = c("18-44", "45-54", "55-64", "65-74", "75+"),
    year = 2023L, payer_group = c("Commercial", "Medicare"),
    eligible_stock_probability = 0.25
  )

  res <- estimate_incident_entry_hazard(
    claims_tbl = claims_tbl, enrollment_tbl = enrollment_tbl,
    member_year_tbl = member_year_tbl, roster_tbl = roster_tbl,
    stock_probability_tbl = stock_probability_tbl,
    analysis_years = 2023L, washout_months = 24L, min_cell_n = 1L
  )

  expect_named(res, c("analytic", "public", "diagnostics"))
  expect_s3_class(res$analytic, "tbl_df")
  expect_s3_class(res$public, "tbl_df")
  expect_s3_class(res$diagnostics, "tbl_df")
})

test_that("washout excludes a member with a qualifying encounter in the lookback window", {
  # P001: no prior care, one qualifying encounter in the 2023 entry window ->
  # entrant. P002: a qualifying encounter in Dec 2022 (month_id 24276, inside
  # the 24-month washout window [24253, 24277) for a 2023 index) -> excluded
  # from the at-risk population entirely, even though P002 ALSO has a 2023
  # encounter that would otherwise count as an entry. If washout exclusion is
  # broken, this cell reports at_risk_member_n = 2 and entry_n = 2, not 1/1.
  member_year_tbl <- tibble::tribble(
    ~person_id, ~year, ~female, ~age, ~payer_group,
    "P001", 2023L, TRUE, 50L, "Commercial",
    "P002", 2023L, TRUE, 50L, "Commercial"
  )
  enrollment_tbl <- .fte_enrolled(c("P001", "P002"), 2021:2023)
  claims_tbl <- tibble::tribble(
    ~person_id, ~service_year, ~service_month, ~rendering_npi, ~condition, ~is_outpatient_evaluation, ~is_qualifying_urps_encounter,
    "P001", 2023L, 4L, "1234567890", "ui", TRUE, TRUE,
    "P002", 2022L, 12L, "1234567890", "ui", FALSE, TRUE,
    "P002", 2023L, 4L, "1234567890", "ui", TRUE, TRUE
  )
  roster_tbl <- tibble::tibble(rendering_npi = "1234567890")
  stock_probability_tbl <- .fte_stock_rows("45-54", 2023L, "Commercial", prob = 1.0)

  res <- estimate_incident_entry_hazard(
    claims_tbl = claims_tbl, enrollment_tbl = enrollment_tbl,
    member_year_tbl = member_year_tbl, roster_tbl = roster_tbl,
    stock_probability_tbl = stock_probability_tbl,
    analysis_years = 2023L, washout_months = 24L, min_cell_n = 1L
  )

  cell <- res$analytic |>
    dplyr::filter(condition == "ui", age_band == "45-54", payer_group == "Commercial", year == 2023L)
  expect_equal(nrow(cell), 1L)
  expect_equal(cell$at_risk_member_n, 1L)
  expect_equal(cell$entry_n, 1L)
  expect_equal(cell$eligible_stock_n, 1)
  expect_equal(cell$q, 1.0)
})

test_that("insufficient continuous enrollment excludes a member from the at-risk population", {
  # P001: full 36-month coverage (Jan2021-Dec2023) for a 24-month washout ->
  # required_months = 36, met exactly. P002: missing Jun-Jul 2022 (34 of 36
  # required months) -> excluded from the at-risk denominator entirely,
  # regardless of any claims.
  member_year_tbl <- tibble::tribble(
    ~person_id, ~year, ~female, ~age, ~payer_group,
    "P001", 2023L, TRUE, 50L, "Commercial",
    "P002", 2023L, TRUE, 50L, "Commercial"
  )
  full_enrollment <- .fte_enrolled("P001", 2021:2023)
  gapped_enrollment <- .fte_enrolled("P002", 2021:2023) |>
    dplyr::filter(!(coverage_year == 2022L & coverage_month %in% c(6L, 7L)))
  enrollment_tbl <- dplyr::bind_rows(full_enrollment, gapped_enrollment)

  claims_tbl <- tibble::tibble(
    person_id = character(), service_year = integer(), service_month = integer(),
    rendering_npi = character(), condition = character(),
    is_outpatient_evaluation = logical(), is_qualifying_urps_encounter = logical()
  )
  roster_tbl <- tibble::tibble(rendering_npi = "1234567890")
  stock_probability_tbl <- .fte_stock_rows("45-54", 2023L, "Commercial", prob = 1.0)

  res <- estimate_incident_entry_hazard(
    claims_tbl = claims_tbl, enrollment_tbl = enrollment_tbl,
    member_year_tbl = member_year_tbl, roster_tbl = roster_tbl,
    stock_probability_tbl = stock_probability_tbl,
    analysis_years = 2023L, washout_months = 24L, allowed_gap_months = 0L,
    min_cell_n = 1L
  )

  cell <- res$analytic |>
    dplyr::filter(condition == "ui", age_band == "45-54", payer_group == "Commercial", year == 2023L)
  expect_equal(nrow(cell), 1L)
  expect_equal(cell$at_risk_member_n, 1L)
  expect_equal(cell$entry_n, 0L)
})

test_that("age bands are assigned at the documented boundaries, not off by one", {
  # 44 and 45 must land in different bands (18-44 vs 45-54), likewise 54/55
  # and 64/65 and 74/75. Placing one member at each boundary age and
  # confirming per-band counts is a direct check against an off-by-one in
  # the case_when() cutoffs -- the classic bug this shape of code invites.
  ages <- c(44L, 45L, 54L, 55L, 64L, 65L, 74L, 75L)
  ids <- paste0("P", seq_along(ages))
  member_year_tbl <- tibble::tibble(
    person_id = ids, year = 2023L, female = TRUE, age = ages, payer_group = "Commercial"
  )
  enrollment_tbl <- .fte_enrolled(ids, 2021:2023)
  claims_tbl <- tibble::tibble(
    person_id = character(), service_year = integer(), service_month = integer(),
    rendering_npi = character(), condition = character(),
    is_outpatient_evaluation = logical(), is_qualifying_urps_encounter = logical()
  )
  roster_tbl <- tibble::tibble(rendering_npi = "1234567890")
  stock_probability_tbl <- .fte_stock_rows(
    c("18-44", "45-54", "55-64", "65-74", "75+"), 2023L, "Commercial", prob = 1.0
  )

  res <- estimate_incident_entry_hazard(
    claims_tbl = claims_tbl, enrollment_tbl = enrollment_tbl,
    member_year_tbl = member_year_tbl, roster_tbl = roster_tbl,
    stock_probability_tbl = stock_probability_tbl,
    analysis_years = 2023L, washout_months = 24L, min_cell_n = 1L
  )

  counts <- res$analytic |>
    dplyr::filter(condition == "ui", payer_group == "Commercial", year == 2023L) |>
    dplyr::select(age_band, at_risk_member_n) |>
    dplyr::arrange(age_band)

  expected <- tibble::tribble(
    ~age_band, ~at_risk_member_n,
    "18-44", 1L,   # age 44
    "45-54", 2L,   # ages 45, 54
    "55-64", 2L,   # ages 55, 64
    "65-74", 2L,   # ages 65, 74
    "75+", 1L      # age 75
  ) |> dplyr::arrange(age_band)

  expect_equal(counts, expected)
})

test_that("public output suppresses small cells; analytic output does not", {
  # Cell has 2 at-risk members, 1 entrant. min_cell_n = 2 means entry_n = 1
  # must be suppressed in `public` (NA) while `analytic` retains the real
  # value -- the two tables exist for exactly this distinction.
  member_year_tbl <- tibble::tribble(
    ~person_id, ~year, ~female, ~age, ~payer_group,
    "P001", 2023L, TRUE, 50L, "Commercial",
    "P002", 2023L, TRUE, 50L, "Commercial"
  )
  enrollment_tbl <- .fte_enrolled(c("P001", "P002"), 2021:2023)
  claims_tbl <- tibble::tribble(
    ~person_id, ~service_year, ~service_month, ~rendering_npi, ~condition, ~is_outpatient_evaluation, ~is_qualifying_urps_encounter,
    "P001", 2023L, 4L, "1234567890", "ui", TRUE, TRUE
  )
  roster_tbl <- tibble::tibble(rendering_npi = "1234567890")
  stock_probability_tbl <- .fte_stock_rows("45-54", 2023L, "Commercial", prob = 1.0)

  res <- estimate_incident_entry_hazard(
    claims_tbl = claims_tbl, enrollment_tbl = enrollment_tbl,
    member_year_tbl = member_year_tbl, roster_tbl = roster_tbl,
    stock_probability_tbl = stock_probability_tbl,
    analysis_years = 2023L, washout_months = 24L, min_cell_n = 2L
  )

  a <- res$analytic |> dplyr::filter(condition == "ui", age_band == "45-54", payer_group == "Commercial")
  p <- res$public |> dplyr::filter(condition == "ui", age_band == "45-54", payer_group == "Commercial")

  expect_equal(a$entry_n, 1L)
  expect_equal(a$at_risk_member_n, 2L)
  expect_true(p$suppressed)
  expect_true(is.na(p$entry_n))
  expect_true(is.na(p$q))
})

test_that("the ALL-payer row is the exact aggregate of payer-specific rows, not a re-derived estimate", {
  # Commercial: 8 at-risk, 1 entrant, stock prob 0.5 -> eligible_stock_n = 4,
  #   q = 1/4 = 0.25.
  # Medicare: 6 at-risk, 3 entrants, stock prob 0.5 -> eligible_stock_n = 3,
  #   q = 3/3 = 1.0 (boundary, not exceeded, so no cap warning).
  # ALL must equal entry_n = 4, eligible_stock_n = 7, q = 4/7 -- NOT the
  # average of 0.25 and 1.0 (0.625), which is what a bug that averaged
  # per-payer q instead of summing counts would produce.
  commercial_ids <- paste0("C", 1:8)
  medicare_ids <- paste0("M", 1:6)
  member_year_tbl <- dplyr::bind_rows(
    tibble::tibble(person_id = commercial_ids, year = 2023L, female = TRUE, age = 50L, payer_group = "Commercial"),
    tibble::tibble(person_id = medicare_ids, year = 2023L, female = TRUE, age = 50L, payer_group = "Medicare")
  )
  enrollment_tbl <- .fte_enrolled(c(commercial_ids, medicare_ids), 2021:2023)
  claims_tbl <- dplyr::bind_rows(
    tibble::tibble(person_id = "C1", service_year = 2023L, service_month = 4L,
                   rendering_npi = "1234567890", condition = "ui",
                   is_outpatient_evaluation = TRUE, is_qualifying_urps_encounter = TRUE),
    tibble::tibble(person_id = c("M1", "M2", "M3"), service_year = 2023L, service_month = 4L,
                   rendering_npi = "1234567890", condition = "ui",
                   is_outpatient_evaluation = TRUE, is_qualifying_urps_encounter = TRUE)
  )
  roster_tbl <- tibble::tibble(rendering_npi = "1234567890")
  stock_probability_tbl <- .fte_stock_rows("45-54", 2023L, c("Commercial", "Medicare"), prob = 0.5)

  res <- estimate_incident_entry_hazard(
    claims_tbl = claims_tbl, enrollment_tbl = enrollment_tbl,
    member_year_tbl = member_year_tbl, roster_tbl = roster_tbl,
    stock_probability_tbl = stock_probability_tbl,
    analysis_years = 2023L, washout_months = 24L, min_cell_n = 1L
  )

  cells <- res$analytic |> dplyr::filter(condition == "ui", age_band == "45-54", year == 2023L)
  commercial <- cells |> dplyr::filter(payer_group == "Commercial")
  medicare <- cells |> dplyr::filter(payer_group == "Medicare")
  all_row <- cells |> dplyr::filter(payer_group == "ALL")

  expect_equal(commercial$q, 0.25)
  expect_equal(medicare$q, 1.0)
  expect_equal(all_row$entry_n, commercial$entry_n + medicare$entry_n)
  expect_equal(all_row$eligible_stock_n, commercial$eligible_stock_n + medicare$eligible_stock_n)
  expect_equal(all_row$q, all_row$entry_n / all_row$eligible_stock_n)
  expect_false(isTRUE(all.equal(all_row$q, mean(c(commercial$q, medicare$q)))))
})

# ---------------------------------------------------------------------------
# run_incident_entry_sensitivity_matrix()
# ---------------------------------------------------------------------------

.rism_base_inputs <- function() {
  # P001 has a qualifying encounter 30 months before the 2023 index start
  # (service_year=2020, service_month=7 -> month_id 24247) and a 2023 entry
  # encounter. Under the primary 24-month washout, window_start_month =
  # 24277-24 = 24253 > 24247, so the 2020 claim is OUTSIDE the washout
  # window: P001 is at risk and counts as an entrant. Under a 36-month
  # washout, window_start_month = 24277-36 = 24241 <= 24247 < 24277, so the
  # SAME claim now falls inside the (longer) washout window: P001 is
  # excluded entirely and the whole cell disappears from the output. This is
  # the sensitivity matrix's whole purpose -- proving washout_months is
  # actually varied, not just passed through unused.
  member_year_tbl <- tibble::tibble(
    person_id = "P001", year = 2023L, female = TRUE, age = 50L, payer_group = "Commercial"
  )
  enrollment_tbl <- .fte_enrolled("P001", 2019:2023)
  claims_tbl <- tibble::tribble(
    ~person_id, ~service_year, ~service_month, ~rendering_npi, ~condition, ~is_outpatient_evaluation, ~is_qualifying_urps_encounter,
    "P001", 2020L, 7L, "1234567890", "ui", FALSE, TRUE,
    "P001", 2023L, 4L, "1234567890", "ui", TRUE, TRUE
  )
  roster_tbl <- tibble::tibble(rendering_npi = "1234567890")
  stock_probability_tbl <- .fte_stock_rows("45-54", 2023L, "Commercial", prob = 1.0)
  list(
    claims_tbl = claims_tbl, enrollment_tbl = enrollment_tbl,
    member_year_tbl = member_year_tbl, roster_tbl = roster_tbl,
    stock_probability_tbl = stock_probability_tbl
  )
}

test_that("the sensitivity matrix varies washout_months and changes the result", {
  ins <- .rism_base_inputs()
  res <- run_incident_entry_sensitivity_matrix(
    claims_variants = list(primary = ins$claims_tbl),
    enrollment_tbl = ins$enrollment_tbl,
    member_year_tbl = ins$member_year_tbl,
    roster_variants = list(primary = ins$roster_tbl),
    stock_probability_tbl = ins$stock_probability_tbl,
    washout_variants = c(24L, 36L),
    gap_variants = 0L,
    year_variants = list(primary = c(2023L)),
    min_cell_n = 1L
  )

  expect_true(all(c("sensitivity_dimension", "sensitivity_value") %in% names(res)))

  primary_cell <- res |>
    dplyr::filter(sensitivity_dimension == "primary", condition == "ui",
                  age_band == "45-54", payer_group == "Commercial")
  washout36_cell <- res |>
    dplyr::filter(sensitivity_dimension == "washout_months", sensitivity_value == "36",
                  condition == "ui", age_band == "45-54", payer_group == "Commercial")

  expect_equal(nrow(primary_cell), 1L)
  expect_equal(primary_cell$entry_n, 1L)
  # The cell vanishes entirely under the 36-month washout: P001 was the only
  # at-risk member and is now excluded by prior care, so no row survives the
  # group-by for this condition/age_band/payer/year.
  expect_equal(nrow(washout36_cell), 0L)
})

test_that("the sensitivity matrix requires a 'primary' element in every variant list", {
  ins <- .rism_base_inputs()
  expect_error(
    run_incident_entry_sensitivity_matrix(
      claims_variants = list(not_primary = ins$claims_tbl),
      enrollment_tbl = ins$enrollment_tbl, member_year_tbl = ins$member_year_tbl,
      roster_variants = list(not_primary = ins$roster_tbl),
      stock_probability_tbl = ins$stock_probability_tbl
    ),
    "primary"
  )
  expect_error(
    run_incident_entry_sensitivity_matrix(
      claims_variants = list(primary = ins$claims_tbl),
      enrollment_tbl = ins$enrollment_tbl, member_year_tbl = ins$member_year_tbl,
      roster_variants = list(primary = ins$roster_tbl),
      stock_probability_tbl = ins$stock_probability_tbl,
      washout_variants = c(12L, 36L)   # missing 24
    ),
    "24"
  )
  expect_error(
    run_incident_entry_sensitivity_matrix(
      claims_variants = list(primary = ins$claims_tbl),
      enrollment_tbl = ins$enrollment_tbl, member_year_tbl = ins$member_year_tbl,
      roster_variants = list(primary = ins$roster_tbl),
      stock_probability_tbl = ins$stock_probability_tbl,
      year_variants = list(not_primary = c(2023L))
    ),
    "primary"
  )
})

test_that("a single roster variant is recycled across every named claims variant", {
  ins <- .rism_base_inputs()
  # Second claims variant: identical claims, different name, to prove the
  # single unnamed roster_variants list gets matched to BOTH names rather
  # than only "primary".
  res <- run_incident_entry_sensitivity_matrix(
    claims_variants = list(primary = ins$claims_tbl, alt_definition = ins$claims_tbl),
    enrollment_tbl = ins$enrollment_tbl,
    member_year_tbl = ins$member_year_tbl,
    roster_variants = list(ins$roster_tbl),
    stock_probability_tbl = ins$stock_probability_tbl,
    washout_variants = 24L,
    gap_variants = 0L,
    year_variants = list(primary = c(2023L)),
    min_cell_n = 1L
  )
  alt_cell <- res |>
    dplyr::filter(sensitivity_dimension == "claims_variant", sensitivity_value == "alt_definition",
                  condition == "ui", age_band == "45-54", payer_group == "Commercial")
  expect_equal(nrow(alt_cell), 1L)
  expect_equal(alt_cell$entry_n, 1L)
})
