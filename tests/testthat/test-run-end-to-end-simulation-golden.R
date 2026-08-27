# Golden-output regression guard for run_end_to_end_simulation().
#
# WHAT THIS CATCHES THAT test-run-end-to-end-simulation.R DOES NOT. That file
# only checks STRUCTURE -- the right names exist, the right classes come back.
# A code change that silently shifts a coefficient, reorders an operation that
# should be order-independent, or changes which draw a fixed seed produces
# passes every structural check while quietly changing every number the
# package reports. This test pins the NUMBERS from one deterministic run and
# fails the instant they move, whether or not the structure around them still
# looks fine.
#
# WHY THESE PARAMETERS. `run_end_to_end_simulation()` is fully synthetic and
# seed-gated by default (`seed = 20260821L`, `stats::runif`/`rbinom` build the
# provider cohort internally) -- no network, no DuckDB, no external file
# unless `evidence_db`/`policy_evidence_db` are explicitly supplied, which
# they are not here. `n_agents = 50L`, `initial_provider_count = 50L`, and a
# single simulated year (2025-2026) keep the run to seconds while still
# exercising the full eight-step pipeline; a longer horizon or larger cohort
# would not make the check more sensitive, only slower. Verified by hand:
# two consecutive runs with these exact arguments produce bit-identical
# audit_ledger_tbl, final row count, summed FTE, and active-provider count.
#
# HOW TO UPDATE THESE VALUES. A change to this test's expected numbers must
# be an INTENTIONAL model update, never a side effect of unrelated work. If a
# code change is supposed to change the simulation's output (a new
# calibration input, a corrected formula, a deliberately different default),
# regenerate every constant below from a fresh run and update them together,
# in the same commit as the change that caused the shift -- with the commit
# message explaining what changed and why. If a change trips this test and
# you did NOT intend to change simulation output, that is exactly the
# regression this test exists to catch; do not update the constants to make
# it pass.
GOLDEN_SEED_ARGS <- list(
  start_year = 2025L,
  end_year = 2026L,
  n_agents = 50L,
  initial_provider_count = 50L,
  save_outputs = FALSE
)

# Per-year values from audit_ledger_tbl, transcribed from a verified run
# (full double precision via dput(), not rounded by hand -- a rounded
# transcription can disagree with the real value by more than the tolerance
# below and fail for a reason that has nothing to do with a regression).
GOLDEN_SERVED_PATIENTS_N <- c(72319.8171738833, 157171.515296319)
GOLDEN_UNSERVED_DELAYED_N <- c(3932197.05400291, 3931655.70922816)
GOLDEN_DELIVERED_SERVICES_N <- c(34762.1812025712, 75548.0988216101)
GOLDEN_REQUIRED_FTE <- c(213.539113101509, 464.081178475605)
GOLDEN_SUPPLIED_FTE <- c(42.0464053336531, 91.3787879629759)
GOLDEN_FTE_GAP <- c(-171.492707767856, -372.702390512629)
GOLDEN_ADEQUACY_RATIO <- c(0.196902594203741, 0.196902594203741)

# Final provider-cohort summary, transcribed from the same run.
GOLDEN_N_FINAL_ROWS <- 160L
GOLDEN_SUM_FTE_FINAL <- 152.046405333653
GOLDEN_N_ACTIVE_FINAL <- 139L

# SHA-256 of the rounded (4 dp) year/served/unserved/delivered/fte/gap/ratio
# columns of audit_ledger_tbl. A belt-and-suspenders check: the value-by-value
# comparisons above give an actionable failure message (which number moved,
# by how much), and this checksum catches drift in a column none of them
# named individually. See BACKTEST_RECORD_SHA256 (R/validation-backtest_status.R)
# for the same doctrine applied to a real scored artifact.
GOLDEN_AUDIT_LEDGER_SHA256 <- "a4ee0b2ca374830f9498d239aed8971059fa83dc5778c0ddefc43b4beace5916"

GOLDEN_TOLERANCE <- 1e-6

test_that("run_end_to_end_simulation reproduces its golden output exactly", {
  res <- suppressMessages(do.call(run_end_to_end_simulation, GOLDEN_SEED_ARGS))
  alt <- res$audit_ledger_tbl

  expect_equal(alt$served_patients_n, GOLDEN_SERVED_PATIENTS_N,
               tolerance = GOLDEN_TOLERANCE)
  expect_equal(alt$unserved_delayed_n, GOLDEN_UNSERVED_DELAYED_N,
               tolerance = GOLDEN_TOLERANCE)
  expect_equal(alt$delivered_services_n, GOLDEN_DELIVERED_SERVICES_N,
               tolerance = GOLDEN_TOLERANCE)
  expect_equal(alt$required_fte, GOLDEN_REQUIRED_FTE,
               tolerance = GOLDEN_TOLERANCE)
  expect_equal(alt$supplied_fte, GOLDEN_SUPPLIED_FTE,
               tolerance = GOLDEN_TOLERANCE)
  expect_equal(alt$fte_gap, GOLDEN_FTE_GAP, tolerance = GOLDEN_TOLERANCE)
  expect_equal(alt$adequacy_ratio, GOLDEN_ADEQUACY_RATIO,
               tolerance = GOLDEN_TOLERANCE)

  expect_equal(nrow(res$final_provider_cohort), GOLDEN_N_FINAL_ROWS)
  expect_equal(sum(res$final_provider_cohort$fte), GOLDEN_SUM_FTE_FINAL,
               tolerance = GOLDEN_TOLERANCE)
  expect_equal(sum(res$final_provider_cohort$active), GOLDEN_N_ACTIVE_FINAL)

  key_cols <- alt[, c(
    "year", "served_patients_n", "unserved_delayed_n",
    "delivered_services_n", "required_fte", "supplied_fte",
    "fte_gap", "adequacy_ratio"
  )]
  key_cols[-1] <- lapply(key_cols[-1], function(x) {
    if (is.numeric(x)) round(x, 4) else x
  })
  expect_equal(
    digest::digest(key_cols, algo = "sha256"),
    GOLDEN_AUDIT_LEDGER_SHA256,
    info = paste(
      "audit_ledger_tbl's checksum moved even though the tests above did not",
      "catch it -- inspect which column drifted before updating this hash."
    )
  )
})
