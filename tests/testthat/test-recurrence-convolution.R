# Known-truth tests for the recurrence cohort convolution.
#
# The formulation being replaced was recurrences_t = procedures_t x hazard,
# which uses THIS YEAR's operations as the entire risk set. Several properties
# below are impossible under that formulation -- they are the tests that would
# have caught it.

testthat::test_that("conditional hazards convert to first-recurrence mass", {
  hazards <- tibble::tribble(
    ~condition, ~years_since_treatment, ~recurrence_hazard,
    "pop", 1L, 0.10,
    "pop", 2L, 0.20
  )
  kernel <- suppressMessages(build_recurrence_kernel(hazards))
  testthat::expect_equal(kernel$survival_start, base::c(1.00, 0.90))
  testthat::expect_equal(kernel$recurrence_prob, base::c(0.10, 0.18))
  testthat::expect_equal(base::sum(kernel$recurrence_prob), 0.28)
})

testthat::test_that("cumulative incidence differences to first-recurrence mass", {
  # SEPARATE ENTRY POINT from hazards, deliberately: a 5-year cumulative
  # proportion is not an annual hazard, and conflating them produced 0.12.
  testthat::expect_equal(recurrence_mass_from_cumulative(c(0.10, 0.15, 0.17)),
                         c(0.10, 0.05, 0.02))
  # the two readings give DIFFERENT answers on the same numbers
  h <- c(0.10, 0.20)
  g_haz <- suppressMessages(build_recurrence_kernel(tibble::tibble(
    condition = "pop", years_since_treatment = 1:2,
    recurrence_hazard = h)))$recurrence_prob
  testthat::expect_false(isTRUE(all.equal(g_haz, recurrence_mass_from_cumulative(h))))
})

testthat::test_that("a decreasing 'cumulative' curve is refused", {
  testthat::expect_error(recurrence_mass_from_cumulative(c(0.10, 0.05)),
                         "non-decreasing")
})

.kern <- function(cond = "pop") tibble::tribble(
  ~condition, ~years_since_treatment, ~recurrence_prob,
  cond, 1L, 0.10,
  cond, 2L, 0.05,
  cond, 3L, 0.02
)

testthat::test_that("known treatment cohort reproduces hand calculation", {
  cohorts <- tibble::tribble(~condition, ~treatment_year, ~treated_n,
                             "pop", 2019L, 100)
  calc <- suppressMessages(compute_recurrence_convolution(
    cohorts, .kern(), forecast_years = 2020:2022,
    tail_policy = "zero_after_kernel"))
  testthat::expect_equal(calc$annual$recurrence_n, base::c(10, 5, 2))
})

testthat::test_that("ZERO current procedures can coexist with recurrence", {
  # THE KILLER TEST. The old formulation returns 0 here by construction.
  cohorts <- tibble::tribble(~condition, ~treatment_year, ~treated_n,
                             "pop", 2019L, 100,
                             "pop", 2021L, 0)
  calc <- suppressMessages(compute_recurrence_convolution(
    cohorts, .kern(), forecast_years = 2021, tail_policy = "zero_after_kernel"))
  testthat::expect_equal(calc$annual$recurrence_n, 5)
})

testthat::test_that("current-year treatment cannot cause same-year recurrence", {
  cohorts <- tibble::tribble(~condition, ~treatment_year, ~treated_n,
                             "pop", 2019L, 100,
                             "pop", 2021L, 10000)
  calc <- suppressMessages(compute_recurrence_convolution(
    cohorts, .kern(), forecast_years = 2021, tail_policy = "zero_after_kernel"))
  testthat::expect_equal(calc$annual$recurrence_n, 5)
})

testthat::test_that("doubling historical treatment doubles recurrence", {
  a <- tibble::tribble(~condition, ~treatment_year, ~treated_n, "pop", 2019L, 100)
  b <- dplyr::mutate(a, treated_n = treated_n * 2)
  ca <- suppressMessages(compute_recurrence_convolution(
    a, .kern(), 2020:2021, tail_policy = "zero_after_kernel"))
  cb <- suppressMessages(compute_recurrence_convolution(
    b, .kern(), 2020:2021, tail_policy = "zero_after_kernel"))
  testthat::expect_equal(cb$annual$recurrence_n, ca$annual$recurrence_n * 2)
})

testthat::test_that("unknown recurrence tail FAILS CLOSED", {
  # Assuming zero recurrence after the evidence horizon is a claim about the
  # disease, not a default.
  cohorts <- tibble::tribble(~condition, ~treatment_year, ~treated_n,
                             "pop", 2010L, 100)
  testthat::expect_error(
    suppressMessages(compute_recurrence_convolution(cohorts, .kern(), 2020)),
    "extends beyond the recurrence kernel")
})

testthat::test_that("TWO INDEX TREATMENTS in one condition carry DIFFERENT kernels", {
  # SUPeR shows different long-term failure trajectories for two vaginal apical
  # procedures, so one generic POP kernel is not defensible.
  gc <- c("condition", "index_treatment")
  cohorts <- tibble::tribble(
    ~condition, ~index_treatment, ~treatment_year, ~treated_n,
    "pop", "vaginal_native", 2019L, 100,
    "pop", "sacrocolpopexy", 2019L, 100)
  kernel <- tibble::tribble(
    ~condition, ~index_treatment, ~years_since_treatment, ~recurrence_prob,
    "pop", "vaginal_native", 1L, 0.20,
    "pop", "sacrocolpopexy", 1L, 0.05)
  calc <- suppressMessages(compute_recurrence_convolution(
    cohorts, kernel, 2020, group_cols = gc))
  vn <- calc$annual$recurrence_n[calc$annual$index_treatment == "vaginal_native"]
  sc <- calc$annual$recurrence_n[calc$annual$index_treatment == "sacrocolpopexy"]
  testthat::expect_equal(vn, 20)
  testthat::expect_equal(sc, 5)
  testthat::expect_true(vn != sc)
})

testthat::test_that("cohorts not unique by group name the fix", {
  # Two index treatments collapsed into one condition row -- the error is to
  # add the stratifier, not to silently sum.
  cohorts <- tibble::tribble(~condition, ~treatment_year, ~treated_n,
                             "pop", 2019L, 100,
                             "pop", 2019L, 50)
  testthat::expect_error(
    suppressMessages(compute_recurrence_convolution(cohorts, .kern(), 2020)),
    "not unique by group")
})

testthat::test_that("contributions make every predicted count traceable", {
  cohorts <- tibble::tribble(~condition, ~treatment_year, ~treated_n,
                             "pop", 2018L, 200,
                             "pop", 2019L, 100)
  calc <- suppressMessages(compute_recurrence_convolution(
    cohorts, .kern(), 2020, tail_policy = "zero_after_kernel"))
  # 2018 cohort is 2 years out (0.05), 2019 cohort 1 year out (0.10)
  testthat::expect_equal(calc$annual$recurrence_n, 200 * 0.05 + 100 * 0.10)
  testthat::expect_equal(base::nrow(calc$contributions), 2L)
  testthat::expect_setequal(calc$contributions$treatment_year, c(2018L, 2019L))
  testthat::expect_equal(calc$annual$source_cohorts_n, 2L)
})

testthat::test_that("probability mass above 1 is refused", {
  bad <- tibble::tribble(~condition, ~years_since_treatment, ~recurrence_prob,
                         "pop", 1L, 0.6, "pop", 2L, 0.6)
  cohorts <- tibble::tribble(~condition, ~treatment_year, ~treated_n,
                             "pop", 2019L, 100)
  testthat::expect_error(
    suppressMessages(compute_recurrence_convolution(cohorts, bad, 2020,
                                                    tail_policy = "zero_after_kernel")),
    "mass exceeds 1")
})

testthat::test_that("a non-contiguous kernel is refused", {
  h <- tibble::tribble(~condition, ~years_since_treatment, ~recurrence_hazard,
                       "pop", 1L, 0.1, "pop", 3L, 0.1)
  testthat::expect_error(suppressMessages(build_recurrence_kernel(h)),
                         "contiguous years")
})

testthat::test_that("same-year recurrence is not supported", {
  h <- tibble::tribble(~condition, ~years_since_treatment, ~recurrence_hazard,
                       "pop", 0L, 0.1)
  testthat::expect_error(suppressMessages(build_recurrence_kernel(h)),
                         "must start at 1")
})

testthat::test_that("the recurrence parameters remain unresolved", {
  testthat::expect_equal(recurrence_parameter_status(), "unresolved_requires_source")
})

# ---------------------------------------------------------------------------
# The evidence register: compatibility is FALSE BY DEFAULT
# ---------------------------------------------------------------------------

testthat::test_that("every registered parameter uses a permitted measure_type", {
  reg <- recurrence_evidence_register()
  testthat::expect_true(all(reg$measure_type %in% RECURRENCE_MEASURE_TYPES))
})

testthat::test_that("NOTHING is currently kernel-compatible", {
  # If this ever passes with a TRUE row, evidence was attached -- check that it
  # was, rather than assuming the register drifted.
  reg <- recurrence_evidence_register()
  testthat::expect_false(any(as.logical(reg$kernel_compatible)))
})

testthat::test_that("0.12 is refused, and the reason names the horizon mismatch", {
  err <- tryCatch(assert_recurrence_kernel_compatible("pop", "followup_p_advance"),
                  error = conditionMessage)
  testthat::expect_match(err, "NOT kernel-compatible")
  testthat::expect_match(err, "CUMULATIVE")
})

testthat::test_that("a repeat_treatment_rate has NO route into g_k", {
  # 0.40 is a reoperation share. Recurrent prolapse and recurrent SURGERY are
  # different quantities (~20% vs ~10% in one USLS cohort).
  err <- tryCatch(assert_recurrence_kernel_compatible("pop", "recurrence_per_entering"),
                  error = conditionMessage)
  testthat::expect_match(err, "repeat_treatment_rate")
  testthat::expect_match(err, "DOWNSTREAM|downstream")
})

testthat::test_that("the AI block is recorded at the index-treatment level", {
  err <- tryCatch(assert_recurrence_kernel_compatible("ai", "followup_p_advance"),
                  error = conditionMessage)
  testthat::expect_match(err, "ptns|not a definitive procedure")
})

testthat::test_that("an unregistered parameter cannot enter the kernel", {
  testthat::expect_error(
    assert_recurrence_kernel_compatible("pop", "invented_parameter"),
    "No recurrence-evidence row")
})

testthat::test_that("changing a reoperation share cannot change recurrent-care episodes", {
  # THE STRUCTURAL SEPARATION. Reoperation is downstream of the recurrent-care
  # episode; g_k counts episodes. Altering the former must not move the latter.
  cohorts <- tibble::tribble(~condition, ~treatment_year, ~treated_n,
                             "pop", 2019L, 100)
  a <- suppressMessages(compute_recurrence_convolution(
    cohorts, .kern(), 2020, tail_policy = "zero_after_kernel"))
  # a reoperation share lives nowhere in this call -- proving it cannot enter
  testthat::expect_false("reoperation" %in% names(a$annual))
  testthat::expect_false(any(grepl("reoper", names(a$contributions))))
  testthat::expect_equal(a$annual$recurrence_n, 10)
})
