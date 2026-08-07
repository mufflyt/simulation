# Scientific benchmark regression suite (#8: version-controlled benchmarks).
#
# These lock the model's SCIENTIFIC correctness, not just its software behaviour:
# each assertion pins a value the package must reproduce against a NAMED external
# reference (a published survival curve, a published survey result, a validated
# workforce count). If a commit moves any of them, this fails loudly and the diff
# says which scientific quantity drifted and away from what source. Every value
# here comes from bundled data or a pure function -- no external file, no
# mufflyaccess -- so it runs in CI as a hard gate (no skips). See docs/BENCHMARKS.md.
#
# Absolute-difference checks are used for the "read off a published chart"
# anchors, because testthat's `tolerance` is RELATIVE and too strict for a value
# like 0.117 vs a charted 0.12.
.near <- function(actual, target, tol) {
  testthat::expect_lt(abs(actual - target), tol)
}

# ---- Benchmark 1: physician retirement survival curve ---------------------
# Source: HWSM v5.19.20 Exhibit 17 + Fraher & Knapton FutureDocs. Of 100
# physicians active at 50, the published survival profile is ~80 at 60, ~55 at
# 65, ~30 at 70, ~12 at 75, ~3 at 80 (drawn against the male reference group).
test_that("BENCHMARK retirement survival reproduces the published anchors", {
  s <- retirement_survival(50, c(60, 65, 70, 75, 80), sex = "male")
  .near(unname(s[["60"]]), 0.80, 0.02)   # published anchors (charted, loose)
  .near(unname(s[["65"]]), 0.55, 0.02)
  .near(unname(s[["70"]]), 0.30, 0.02)
  .near(unname(s[["75"]]), 0.12, 0.02)
  .near(unname(s[["80"]]), 0.03, 0.01)
  # Regression lock: the exact curve the current hazard schedule yields.
  expect_equal(unname(round(s, 3)), c(0.796, 0.545, 0.296, 0.117, 0.030))
  expect_true(all(diff(s) < 0))          # monotone non-increasing
})

# ---- Benchmark 2: provider capacity-survey adequacy -----------------------
# Source: Zarek 2025 PTJ. The published four-category example weights to a mean
# adequacy of 94.8% -- a 5.2% base-year gap. capacity_survey_adequacy() must
# reproduce that arithmetic exactly from the shipped example distribution.
test_that("BENCHMARK capacity-survey adequacy reproduces Zarek 2025 (94.8%)", {
  a <- capacity_survey_adequacy(example_capacity_survey())
  .near(a$adequacy, 0.948, 5e-4)
  .near(a$gap_fraction, 0.052, 5e-4)
})

# ---- Benchmark 3: published Dall-family base-year shortfalls --------------
# Source: the three studies the model's methodology is built on. These are
# fixed published facts; the table must not drift.
test_that("BENCHMARK published_baseline_gaps() matches the source studies", {
  g <- published_baseline_gaps()
  row <- function(study) g[grepl(study, g$study), ]
  phys <- row("Dall 2021")
  expect_equal(phys$shortfall_fte, 940)
  .near(phys$shortfall_pct, 10.6, 1e-6)
  pt <- row("Zarek 2025")
  expect_equal(pt$shortfall_fte, 12070)
  .near(pt$shortfall_pct, 5.2, 1e-6)
  neuro <- row("Dall 2013")
  expect_equal(neuro$shortfall_fte, 1814)
  .near(neuro$shortfall_pct, 11.0, 1e-6)
})

# ---- Benchmark 4: validated base-year workforce total ---------------------
# Source: the mufflyaccess URPS contract (v3.0.0). The 2023 national ABOG+ABU
# board-certified-active count is 1,306; the ABOG-only pathway is 1,027. The
# candidate table is bundled, so this pins the count without the contract.
test_that("BENCHMARK the 2023 URPS workforce total is 1,306 (national ABOG+ABU)", {
  cand <- backtest_target_candidates()
  current_natl <- cand[cand$geography == "national" & cand$pathway == "ABOG_PLUS_ABU" &
                         cand$measure == "board_certified_active" & cand$status == "current", ]
  expect_equal(current_natl$value, 1306L)
  # The ABOG-only pathway is a different, valid count -- guard against confusing them.
  abog_only <- cand[cand$pathway == "ABOG" & cand$measure == "board_certified_active", ]
  expect_equal(abog_only$value, 1027L)
  # CONUS is slightly below national (AK/HI/territories excluded).
  conus <- cand[cand$geography == "conus" & cand$pathway == "ABOG_PLUS_ABU" &
                 cand$status == "current", ]
  expect_equal(conus$value, 1303L)
})

# ---- Benchmark 5: FutureDocs categorical participation --------------------
# Source: Fraher & Knapton FutureDocs Fig 9 (digitised). Female expected FTE
# PEAKS at ~50 (0.675) then collapses; male expected FTE is nearly flat. This is
# the sex divergence that matters as the workforce feminises.
test_that("BENCHMARK FutureDocs participation probabilities are well-formed", {
  validate_participation_table()                      # every row sums to 1
  .near(participation_fte(50, "female"), 0.675, 1e-3)
  # Female FTE peaks in mid-career and collapses by 80.
  expect_gt(participation_fte(50, "female"), participation_fte(80, "female"))
  # Male FTE is comparatively flat across the career (50 vs 65 within ~0.1).
  expect_lt(abs(participation_fte(50, "male") - participation_fte(65, "male")), 0.1)
})
