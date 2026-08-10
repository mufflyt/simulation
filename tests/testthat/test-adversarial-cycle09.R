# Adversarial cycle 09 -- the projection horizon, and silent coercion at a
# trust boundary.
#
# Cycle 08 carried forward: as.integer()/as.numeric() applied to anything
# arriving from an environment variable, a file, or a user column, where a
# truncation or an NA changes a value rather than rejecting it. The sweep found
# the sharpest instance in the engine's own front door -- `years`.
#
# Every step of the microsimulation advances age by exactly one year, so `years`
# is not a set of labels to report against: it is the number of one-year steps.
# `sort(unique(as.integer(years)))` accepted anything and the panel labelled the
# result whatever the caller asked for.
#
# Mix: 3 boundary-value, 3 semantic/contract, 4 adversarial.

cyc09_agents <- function(n = 30, seed = 901) {
  set.seed(seed)
  data.frame(
    provider_id = sprintf("P%03d", seq_len(n)), subspecialty = "FPMRS",
    sex = rep(c("female", "male"), length.out = n),
    age = seq(40, 60, length.out = n),
    entry_year = 2015L, retirement_year = NA_real_,
    origin_cohort = "baseline", stringsAsFactors = FALSE
  )
}
cyc09_run <- function(years, entrants = 5, ...) {
  ag <- cyc09_agents()
  simulate_provider_career_once(ag, years, entrants,
                                hours_intercept = calibrate_hours_intercept(ag$age, ag$sex),
                                ...)
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: a single-year horizon is legal and a two-year gap is not", {
  # One year is the degenerate horizon: no transitions, so no contiguity to
  # check. Two consecutive years is the smallest horizon that takes a step.
  expect_silent(invisible(cyc09_run(2025L)))
  expect_silent(invisible(cyc09_run(2025:2026)))
  expect_error(cyc09_run(c(2025L, 2027L)), "CONSECUTIVE")
  # The message must name the gap and the horizon the caller probably meant,
  # because the fix is nearly always "you wrote c(a, b) for a:b".
  expect_error(cyc09_run(c(2025L, 2030L)), "2025")
  expect_error(cyc09_run(c(2025L, 2030L)), "2025:2030")
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: years must be whole, and empty or unparseable horizons stop", {
  # as.integer(2025.7) is 2025 -- a silent truncation of the caller's horizon.
  expect_error(cyc09_run(c(2025.7, 2026.7, 2027.7)), "whole years")
  expect_silent(invisible(cyc09_run(c(2025, 2026, 2027))))     # whole doubles are fine

  # A non-numeric year coerced to NA, min() then returned Inf with a base R
  # warning, and the run produced an EMPTY panel rather than failing.
  expect_error(cyc09_run(c("a", "b")), "non-empty vector of whole years")
  expect_error(cyc09_run(integer(0)), "non-empty vector of whole years")
  expect_error(cyc09_run(c(2025L, NA_integer_)), "non-empty vector of whole years")
  # Character years that DO parse are still accepted -- this is a coercion
  # guard, not a type guard.
  expect_silent(invisible(cyc09_run(c("2025", "2026"))))
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: the same guard is on both engine entry points", {
  # project_supply_deterministic() is the expected-value twin of the stochastic
  # engine and had the identical line. A guard on one of two doors is not a guard.
  ag <- cyc09_agents()
  expect_error(project_supply_deterministic(ag, c(2025L, 2030L), 5), "CONSECUTIVE")
  expect_error(project_supply_deterministic(ag, c(2025.5, 2026.5), 5), "whole years")
  expect_error(project_supply_deterministic(ag, integer(0), 5), "non-empty")
  expect_silent(invisible(project_supply_deterministic(ag, 2025:2028, 5)))
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: the panel year advances in lockstep with cohort aging", {
  # THE PROPERTY THE GAP VIOLATED. With no entry and no exit, the mean age must
  # rise by exactly the same number of years as the panel label does. If the two
  # can drift apart, a panel row's year is decoration.
  p <- cyc09_run(2025:2035, entrants = 0,
                 retirement_schedule = setNames(rep(0, 100), 1:100),
                 career_change_hazard = 0)$panel
  expect_equal(diff(p$year), rep(1L, 10))
  expect_equal(diff(p$mean_age), rep(1, 10), tolerance = 1e-9)
  # Elapsed label time equals elapsed cohort time over the whole horizon.
  expect_equal(max(p$year) - min(p$year), max(p$mean_age) - min(p$mean_age),
               tolerance = 1e-9)
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: a longer horizon extends the panel rather than rescaling it", {
  # The first N rows of a long run must equal a short run of length N under the
  # same seed. If they differ, the horizon is being used as a parameter of the
  # dynamics rather than as their length.
  set.seed(31); short <- cyc09_run(2025:2030)$panel
  set.seed(31); long <- cyc09_run(2025:2040)$panel
  expect_equal(nrow(short), 6L)
  expect_equal(nrow(long), 16L)
  expect_equal(long$headcount[1:6], short$headcount)
  expect_equal(long$mean_age[1:6], short$mean_age, tolerance = 1e-9)
  expect_equal(long$year[1:6], short$year)
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: duplicated and unsorted horizons normalise to the same run", {
  # These two ARE safe to normalise -- unlike a gap, they do not change how many
  # steps the engine takes. Pinned so the new guard does not over-reach and
  # start refusing input it should accept.
  set.seed(17); a <- cyc09_run(2025:2028)$panel
  set.seed(17); b <- cyc09_run(c(2028L, 2027L, 2026L, 2025L))$panel
  set.seed(17); c3 <- cyc09_run(c(2025L, 2025L, 2026L, 2027L, 2028L))$panel
  expect_equal(b$year, a$year)
  expect_equal(b$headcount, a$headcount)
  expect_equal(c3$year, a$year)
  expect_equal(c3$headcount, a$headcount)
  expect_equal(nrow(c3), 4L)
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: a gapped horizon can no longer report five years of dynamics as one step", {
  # THE DEFECT, measured. simulate_provider_career_once(agents, c(2025, 2030))
  # returned a two-row panel: row 2 labelled 2030, with mean age 48.6 against
  # the base year's 50.0 and headcount 30 -> 35. That is ONE year of aging and
  # ONE year of entrants (5), wearing a 2030 label. Four years of aging,
  # retirement and entry silently did not happen.
  expect_error(cyc09_run(c(2025L, 2030L)), "silently skip")

  # The run the caller meant produces genuinely different numbers, which is the
  # measure of how much the gap was hiding.
  set.seed(5); real <- cyc09_run(2025:2030)$panel
  expect_equal(nrow(real), 6L)
  expect_gt(real$headcount[6], real$headcount[1])
  # Five steps of entrants at 5/yr, less departures -- never one step's worth.
  expect_gt(real$headcount[6] - real$headcount[1], 5L)
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: no horizon shape produces a panel whose rows outnumber its steps", {
  # Whatever normalisation happens, the panel must have exactly one row per
  # distinct year, and those years must be the ones asked for.
  for (y in list(2025L, 2025:2026, 2025:2035, c(2030L, 2029L, 2028L))) {
    p <- cyc09_run(y)$panel
    expect_equal(nrow(p), length(unique(as.integer(y))))
    expect_equal(p$year, sort(unique(as.integer(y))))
    expect_false(anyDuplicated(p$year) > 0L)
  }
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: the documented permissive fallback for the mode is exactly that, and says so", {
  # resolve_reproducibility_mode() documents that an unrecognised value "warns
  # and falls back rather than failing, so a typo degrades to the permissive
  # mode". That is a deliberate decision, so this pins it rather than changing
  # it -- but a publication run typo'd to 'strct' does silently become RELAXED,
  # and the only signal is a .msg_warn() MESSAGE, which never reaches warnings()
  # and cannot be promoted with options(warn = 2). Recorded here so the exposure
  # is visible rather than implicit.
  old <- Sys.getenv("REPRODUCIBILITY_MODE", unset = NA_character_)
  on.exit(if (is.na(old)) Sys.unsetenv("REPRODUCIBILITY_MODE") else
            Sys.setenv(REPRODUCIBILITY_MODE = old), add = TRUE)

  Sys.setenv(REPRODUCIBILITY_MODE = "strct")
  expect_message(resolve_reproducibility_mode(), "Unknown REPRODUCIBILITY_MODE")
  expect_equal(suppressMessages(resolve_reproducibility_mode()), "relaxed")
  # An explicit default is honoured, so a caller who wants fail-closed can get it.
  expect_equal(suppressMessages(resolve_reproducibility_mode(default = "strict")), "strict")

  # Case and whitespace are normalised, so those are not typos.
  Sys.setenv(REPRODUCIBILITY_MODE = "  STRICT ")
  expect_equal(resolve_reproducibility_mode(), "strict")
})

# ---- ADVERSARIAL 4 ----------------------------------------------------------

test_that("ADVERSARIAL: the guard is not satisfiable by a horizon that merely looks contiguous", {
  # Ways a caller can produce a technically-sorted-and-unique vector that is
  # still not a run of consecutive years.
  expect_error(cyc09_run(c(2025L, 2026L, 2028L, 2029L)), "CONSECUTIVE")   # interior gap
  expect_error(cyc09_run(c(2025L, 2026L, 2027L, 2040L)), "CONSECUTIVE")   # trailing jump
  expect_error(cyc09_run(seq(2025L, 2035L, by = 2L)), "CONSECUTIVE")      # every other year
  # A descending seq is normalised, not rejected -- it IS consecutive.
  expect_silent(invisible(cyc09_run(seq(2030L, 2025L, by = -1L))))

  # And a horizon that is consecutive after de-duplication passes, so the
  # ordering of the checks (dedupe, then contiguity) is itself pinned.
  expect_silent(invisible(cyc09_run(c(2025L, 2026L, 2026L, 2027L))))
})
