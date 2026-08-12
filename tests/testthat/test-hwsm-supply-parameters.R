# HRSA HWSM supply parameters (Surgery proxy): work-effort FTE and a separate
# retirement hazard. The two mechanisms are kept distinct on purpose -- hours are
# work effort AMONG the active, retirement is a separate stochastic event -- so
# these tests pin that separation as much as the arithmetic.
#
# Expected values were established by running a base-R transcription of the HRSA
# tables and the 1 - S(a+1)/S(a) conversion (package deps unavailable in the
# authoring environment); the function itself runs under CI.

# ---- retirement hazard table ------------------------------------------------

test_that("the HWSM retirement hazard is a valid one-year exit probability grid", {
  hz <- hwsm_retirement_hazard_table()
  expect_true(all(c("age", "sex", "prob_exit", "calibration_tier") %in% names(hz)))
  expect_setequal(unique(hz$sex), c("Female", "Male"))
  expect_true(all(hz$prob_exit >= 0 & hz$prob_exit <= 1))
  # Surgery is a proxy for URPS, so it can never be 'calibrated'.
  expect_equal(unique(hz$calibration_tier), "derived_by_analogy")
  # HRSA models permanent retirement only for 50+: nothing exits younger.
  expect_true(all(hz$prob_exit[hz$age < 50] == 0))
})

test_that("the HWSM hazard reproduces 1 - S(a+1)/S(a) at pinned ages", {
  hz <- hwsm_retirement_hazard_table()
  fz <- function(a) hz$prob_exit[hz$sex == "Female" & hz$age == a]
  mz <- function(a) hz$prob_exit[hz$sex == "Male" & hz$age == a]
  # The female Surgery curve steps hard in the early 60s.
  expect_equal(fz(63), 0.493687, tolerance = 1e-5)
  expect_equal(mz(65), 0.061294, tolerance = 1e-5)
  # At age 50 the survival is flat, so the one-year exit probability is 0.
  expect_equal(fz(50), 0)
})

test_that("the hazard table carries its provenance rather than leaving it beside", {
  hz <- hwsm_retirement_hazard_table()
  prov <- attr(hz, "provenance")
  expect_true(is.list(prov))
  expect_match(prov$fte_definition, "40 professional")
  expect_equal(prov$calibration_tier, "derived_by_analogy")
})

# ---- work-effort FTE + the double-count separation --------------------------

test_that("hwsm_fte is professional hours / 40 and may exceed 1.0", {
  roster <- data.frame(
    age = c(40L, 71L),
    sex = c("Male", "Female"),
    stringsAsFactors = FALSE
  )
  out <- add_hwsm_supply_parameters(roster, verbose = FALSE)
  # Male 35-44 band: 50.5 professional hours -> 1.2625 FTE (exceeds 1.0).
  expect_equal(out$hwsm_fte[out$sex == "Male"], 50.5 / 40)
  # Female 70-74 band: 34.3 -> 0.8575.
  expect_equal(out$hwsm_fte[out$sex == "Female"], 34.3 / 40)
})

test_that("retirement is separate from hours: p_retire is 0 below 50 and hours are not haircut by p_active", {
  out <- add_hwsm_supply_parameters(
    data.frame(age = 40L, sex = "Female"), verbose = FALSE
  )
  # Under 50: fully active, no retirement, hours untouched.
  expect_equal(out$p_active, 1)
  expect_equal(out$p_retire_next_year, 0)
  expect_equal(out$hwsm_fte, 48.2 / 40)   # 35-44 band, NOT multiplied by p_active
})

test_that("unrecognized sex yields NA HWSM columns rather than a wrong join", {
  out <- add_hwsm_supply_parameters(
    data.frame(age = 55L, sex = "X"), verbose = FALSE
  )
  expect_true(is.na(out$hwsm_fte))
  expect_true(is.na(out$p_retire_next_year))
})

test_that("a non-data-frame input and a missing column are refused", {
  expect_error(add_hwsm_supply_parameters(1:10), "data frame")
  expect_error(
    add_hwsm_supply_parameters(data.frame(years = 40L), age_col = "age"),
    "Missing required columns"
  )
})
