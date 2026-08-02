# Guards for the cliff partial-pooling age-band hazard model.
# The real GO/URPS/MIGS person-year/event counts (cliff
# hazard_by_band_pooled_vs_unpooled.csv) are inlined so the core fit is tested
# without depending on the calibration data source.

.cliff_hazard_wide <- function() {
  tibble::tribble(
    ~band,   ~go_events, ~go_py, ~urps_events, ~urps_py, ~migs_events, ~migs_py,
    "<45",            9,   3781,           15,     3854,            9,     2594,
    "45-49",          2,    857,            3,      973,            0,      369,
    "50-54",          3,    674,            3,      811,            0,      211,
    "55-59",          5,    571,            3,      488,            1,      159,
    "60-64",          7,    389,            6,      221,            0,       27,
    "65-69",         10,    149,            5,       53,            0,        5,
    "70+",            0,     13,            0,        3,            0,        0
  )
}
BAND_LEVELS <- c("<45", "45-49", "50-54", "55-59", "60-64", "65-69", "70+")

test_that("hazard_pooled_long reshapes the wide file and drops empty cells", {
  long <- hazard_pooled_long(.cliff_hazard_wide())
  expect_setequal(unique(long$subspecialty), c("GO", "URPS", "MIGS"))
  expect_true(all(long$py > 0))
  # MIGS 70+ has py = 0 and must be dropped.
  expect_equal(nrow(long[long$subspecialty == "MIGS" & long$band == "70+", ]), 0L)
  # Unpooled hazard sanity: URPS 65-69 = 5/53.
  urps_6569 <- long[long$subspecialty == "URPS" & long$band == "65-69", ]
  expect_equal(urps_6569$ev / urps_6569$py, 5 / 53, tolerance = 1e-9)
})

test_that("partial pooling fits and shrinks between-subspecialty spread", {
  skip_if_not(requireNamespace("blme", quietly = TRUE) &&
                requireNamespace("lme4", quietly = TRUE), "blme/lme4 not installed")
  agg <- hazard_pooled_long(.cliff_hazard_wide())
  fit <- fit_partial_pooled_hazards(agg, band_levels = BAND_LEVELS)

  expect_equal(fit$status, "fitted")
  h <- fit$hazards
  expect_true(all(h$partial > 0 & h$partial < 1))          # valid probabilities
  expect_true(all(c("unpooled", "pooled", "partial") %in% names(h)))

  # Shrinkage: partial-pooled hazards vary LESS across subspecialties than
  # unpooled, within bands observed for >1 subspecialty.
  shr <- hazard_shrinkage_summary(h)
  shr <- shr[is.finite(shr$sd_unpooled) & shr$sd_unpooled > 0, ]
  expect_true(all(shr$sd_partial <= shr$sd_unpooled + 1e-9))

  # The shared age-band shape rises with age: pooled hazard at 65-69 > at <45.
  p <- unique(h[, c("band", "pooled")])
  expect_gt(p$pooled[p$band == "65-69"], p$pooled[p$band == "<45"])
})

test_that("fit errors cleanly if blme is unavailable (documented Suggests)", {
  # Only meaningful when blme is NOT installed; otherwise skip.
  skip_if(requireNamespace("blme", quietly = TRUE), "blme is installed")
  agg <- hazard_pooled_long(.cliff_hazard_wide())
  expect_error(fit_partial_pooled_hazards(agg), "needs the 'blme'")
})
