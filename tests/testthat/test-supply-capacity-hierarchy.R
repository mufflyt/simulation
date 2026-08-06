# Guards for R/60-supply_capacity_hierarchy.R -- the tiered supply-capacity
# reporting layer (headcount -> clinical FTE -> wRVU capacity -> accessible).

# ---- Structure --------------------------------------------------------------

test_that("returns the four tiers in order with the documented columns", {
  h <- supply_capacity_hierarchy(1000, 780, accessible_fraction = 0.8, group = "national")
  expect_equal(nrow(h), 4L)
  expect_equal(h$tier, 1:4)
  expect_equal(h$label, c("active_headcount", "clinical_fte",
                          "effective_wrvu_capacity", "accessible_capacity"))
  expect_true(all(c("group", "value", "unit", "provider_equivalent",
                    "retained_vs_headcount") %in% names(h)))
  expect_true(all(h$group == "national"))
})

# ---- Semantic: the descent is real ------------------------------------------

test_that("provider-equivalent capacity is monotonically non-increasing down the tiers", {
  h <- supply_capacity_hierarchy(1000, 780, accessible_fraction = 0.8, insurance_fraction = 0.9)
  expect_true(all(diff(h$provider_equivalent) <= 1e-9))     # count >= FTE >= accessible
  expect_equal(h$provider_equivalent[1], 1000)              # tier 1 = headcount
  expect_equal(h$retained_vs_headcount[1], 1)
})

test_that("tier 3 is a unit change (not attrition) and tier 4 applies reach x insurance", {
  h <- supply_capacity_hierarchy(1000, 780, wrvu_per_fte = 7500,
                                 accessible_fraction = 0.8, insurance_fraction = 0.9)
  expect_equal(h$value[3], 780 * 7500)                      # wRVU capacity
  expect_equal(h$provider_equivalent[3], h$provider_equivalent[2])  # 2->3 no PE loss
  expect_equal(h$value[4], 780 * 7500 * 0.8 * 0.9)          # accessible wRVU
  expect_equal(h$provider_equivalent[4], 780 * 0.8 * 0.9)   # accessible provider-equiv
})

test_that("with full reach and no insurance restriction, accessible equals clinical FTE", {
  h <- supply_capacity_hierarchy(500, 400, accessible_fraction = 1, insurance_fraction = 1)
  expect_equal(h$provider_equivalent[4], h$provider_equivalent[2])
})

test_that("a tighter geographic reach strictly lowers accessible capacity", {
  wide   <- supply_capacity_hierarchy(1000, 780, accessible_fraction = 0.9)
  narrow <- supply_capacity_hierarchy(1000, 780, accessible_fraction = 0.4)
  expect_lt(narrow$value[4], wide$value[4])
  expect_lt(narrow$provider_equivalent[4], wide$provider_equivalent[4])
})

test_that("reporting ABU and ABOG separately does not pool them", {
  abu  <- supply_capacity_hierarchy(308, 240, group = "ABU")
  abog <- supply_capacity_hierarchy(1031, 800, group = "ABOG")
  expect_equal(abu$value[1], 308)
  expect_equal(abog$value[1], 1031)
  expect_false(identical(abu$provider_equivalent, abog$provider_equivalent))
})

# ---- Adversarial: hostile inputs fail loudly or degrade gracefully ----------

test_that("negative or non-numeric inputs are rejected", {
  expect_error(supply_capacity_hierarchy(-1, 1))
  expect_error(supply_capacity_hierarchy(10, -5))
  expect_error(supply_capacity_hierarchy("10", 8))
})

test_that("out-of-range fractions and non-positive productivity are rejected", {
  expect_error(supply_capacity_hierarchy(10, 8, accessible_fraction = 1.2))
  expect_error(supply_capacity_hierarchy(10, 8, accessible_fraction = -0.1))
  expect_error(supply_capacity_hierarchy(10, 8, insurance_fraction = 2))
  expect_error(supply_capacity_hierarchy(10, 8, wrvu_per_fte = 0))
  expect_error(supply_capacity_hierarchy(10, 8, wrvu_per_fte = -100))
})

test_that("non-scalar inputs and non-finite values are rejected", {
  expect_error(supply_capacity_hierarchy(c(10, 20), 8))
  expect_error(supply_capacity_hierarchy(10, Inf))
  expect_error(supply_capacity_hierarchy(NA_real_, 8))
})

test_that("an empty roster yields NA retention rather than Inf/NaN", {
  h <- supply_capacity_hierarchy(0, 0)
  expect_true(all(is.na(h$retained_vs_headcount)))
  expect_equal(h$value[1], 0)
})
