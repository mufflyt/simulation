# Tests for R/supply-urps_flows.R — Labor force participation model
#
# Guards:
#  1. Calibration anchors: P(active) at key ages matches HWSM Exhibit 17 within tolerance.
#  2. Female odds ratio: ≈ 0.77 at the reference age (HWSM Exhibit 16 analog).
#  3. Scenario shifts: retirement later → higher P(active); earlier → lower.
#  4. Input validation: bad sex, negative years, age-boundary behaviour.
#  5. Vectorisation: scalar and vector inputs produce consistent output.
#  6. p_active_by_age: returns correctly shaped tibble.

# ---- 1. Calibration anchors --------------------------------------------------

test_that("supply_p_active reproduces HWSM Exhibit 17 survival anchors for male", {
  # Reference entry age 33 → years_certified = age - 33 at each point.
  # Tolerance ±0.05 (logistic calibration, not exact survival arithmetic).
  expect_equal(supply_p_active(35, "male",  2), 0.985, tolerance = 0.015)
  expect_equal(supply_p_active(65, "male", 32), 0.550, tolerance = 0.050)
  expect_equal(supply_p_active(75, "male", 42), 0.120, tolerance = 0.030)
})

test_that("P(active) declines monotonically with age holding sex and entry fixed", {
  ages  <- seq(35, 80, by = 5)
  yrs   <- pmax(ages - 33, 0)
  probs <- supply_p_active(ages, "male", yrs)
  expect_true(all(diff(probs) < 0),
              info = paste("Non-monotone at ages:", ages[which(diff(probs) >= 0)]))
})

test_that("P(active) is in [0, 1] for a wide grid of inputs", {
  ages <- rep(seq(20, 85, by = 5), each = 2)
  sex  <- rep(c("male", "female"), length.out = length(ages))
  yrs  <- pmax(ages - 33, 0)
  p    <- supply_p_active(ages, sex, yrs)
  expect_true(all(p >= 0 & p <= 1))
})

# ---- 2. Female sex effect ---------------------------------------------------

test_that("female OR at reference age is approximately 0.77 (HWSM Exhibit 16)", {
  pm <- supply_p_active(65, "male",   32)
  pf <- supply_p_active(65, "female", 32)
  or_female <- (pf / (1 - pf)) / (pm / (1 - pm))
  # HWSM Exhibit 16 RN odds ratio for female: ≈ 0.77; allow ±0.10.
  expect_equal(or_female, 0.77, tolerance = 0.10)
})

test_that("female P(active) is lower than male at all retirement-zone ages", {
  ages <- c(55, 60, 65, 70, 75)
  yrs  <- ages - 33
  expect_true(all(supply_p_active(ages, "female", yrs) <
                  supply_p_active(ages, "male",   yrs)))
})

# ---- 3. Scenario shifts ------------------------------------------------------

test_that("retiring 2 years later raises P(active) at age 65", {
  p_base   <- supply_p_active(65, "male", 32, scenario_id = "baseline")
  p_later  <- supply_p_active(65, "male", 32, scenario_id = "retire_2yr_later")
  expect_gt(p_later, p_base)
})

test_that("retiring 2 years earlier lowers P(active) at age 65", {
  p_base    <- supply_p_active(65, "male", 32, scenario_id = "baseline")
  p_earlier <- supply_p_active(65, "male", 32, scenario_id = "retire_2yr_earlier")
  expect_lt(p_earlier, p_base)
})

test_that("scenario shift is symmetric: earlier + later bracket baseline", {
  p_base    <- supply_p_active(65, "male", 32, scenario_id = "baseline")
  p_later   <- supply_p_active(65, "male", 32, scenario_id = "retire_2yr_later")
  p_earlier <- supply_p_active(65, "male", 32, scenario_id = "retire_2yr_earlier")
  expect_gt(p_later,   p_base)
  expect_lt(p_earlier, p_base)
})

test_that("NULL scenario_id produces the same result as baseline", {
  expect_equal(
    supply_p_active(65, "male", 32, scenario_id = NULL),
    supply_p_active(65, "male", 32, scenario_id = "baseline")
  )
})

test_that("unknown scenario_id warns and falls back to shift = 0", {
  expect_warning(
    p_unknown <- supply_p_active(65, "male", 32, scenario_id = "nonexistent_scenario"),
    regexp = "unknown scenario_id"
  )
  p_null <- supply_p_active(65, "male", 32, scenario_id = NULL)
  expect_equal(p_unknown, p_null)
})

# ---- 4. Input validation ----------------------------------------------------

test_that("invalid sex throws an error", {
  expect_error(
    supply_p_active(45, "other", 10),
    regexp = "sex must be"
  )
})

test_that("negative years_certified throws an error", {
  expect_error(
    supply_p_active(45, "female", -1),
    regexp = "non-negative"
  )
})

test_that("age below 18 returns 0 with a warning", {
  expect_warning(
    p <- supply_p_active(15, "female", 0),
    regexp = "outside \\[18, 100\\]"
  )
  expect_equal(p, 0)
})

test_that("age at or above terminal age (90) returns 0", {
  expect_equal(supply_p_active(90, "male",  57), 0)
  expect_equal(supply_p_active(95, "female", 62), 0)
})

test_that("age just below terminal age is non-zero", {
  expect_gt(supply_p_active(89, "male", 56), 0)
})

# ---- 5. Vectorisation -------------------------------------------------------

test_that("scalar and length-1 vector inputs produce identical output", {
  expect_equal(
    supply_p_active(65, "female", 32),
    supply_p_active(c(65), c("female"), c(32))
  )
})

test_that("vector recycling works for sex when age is a vector", {
  ages  <- c(40, 50, 60, 70)
  yrs   <- ages - 33
  # single sex recycled to length 4
  p_vec <- supply_p_active(ages, "female", yrs)
  p_ind <- vapply(seq_along(ages),
                  function(i) supply_p_active(ages[i], "female", yrs[i]),
                  numeric(1))
  expect_equal(p_vec, p_ind, tolerance = 1e-12)
})

test_that("output length matches the longest input", {
  expect_length(supply_p_active(c(40, 50, 60), "male", 10),        3)
  expect_length(supply_p_active(50, c("male", "female"), c(10, 15)), 2)
})

# ---- 6. p_active_by_age table -----------------------------------------------

test_that("p_active_by_age returns a tibble with the expected columns", {
  tbl <- p_active_by_age(ages = 35:75)
  expect_s3_class(tbl, "tbl_df")
  expect_named(tbl, c("age", "sex", "years_certified", "p_active"),
               ignore.order = FALSE)
})

test_that("p_active_by_age covers both sexes and all requested ages", {
  tbl <- p_active_by_age(ages = c(40, 60, 80))
  expect_equal(nrow(tbl), 6L)   # 3 ages × 2 sexes
  expect_setequal(unique(tbl$sex), c("male", "female"))
  expect_setequal(tbl$age, c(40, 60, 80))
})

test_that("p_active_by_age respects the scenario_id argument", {
  base  <- p_active_by_age(ages = 65, scenario_id = "baseline")
  later <- p_active_by_age(ages = 65, scenario_id = "retire_2yr_later")
  expect_true(all(later$p_active >= base$p_active))
})

# ---- 7. Custom coefficient pass-through -------------------------------------

test_that("custom coef list changes the output predictably", {
  # Flat-zero model: intercept drives everything, all slope coefficients zero.
  flat_coef <- list(intercept = 0, age = 0, age_sq = 0,
                    female = 0, years_cert = 0)
  p <- supply_p_active(c(40, 65, 75), "male", c(7, 32, 42), coef = flat_coef)
  # logistic(0) = 0.5 for all non-boundary ages
  expect_equal(p[1:2], c(0.5, 0.5))
  # age 75 is below terminal age 90, so also 0.5
  expect_equal(p[3], 0.5)
})
