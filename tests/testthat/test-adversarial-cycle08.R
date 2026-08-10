# Adversarial cycle 08 -- RNG state and preregistration integrity.
#
# These were the two thinnest priorities in the ledger: RNG state had one test
# (cycle 01) and validation leakage two (cycle 02). Both are places where a run
# can be WRONG while every number in it looks ordinary -- an irreproducible run
# labelled reproducible, or a spec that passes a preregistration gate it does
# not actually match.
#
# Cycle 07's carried-forward sweep (instrumenting all()/any() across every test
# file) found exactly two vacuous assertions; both are repaired in their own
# files rather than here, in test-numeric-guards.R and test-demand-dynamic-open.R.
#
# Mix: 3 boundary-value, 4 semantic/contract, 3 adversarial.

cyc08_with_env <- function(value, code) {
  old <- Sys.getenv("MICROSIM_SEED", unset = NA_character_)
  if (is.na(value)) Sys.unsetenv("MICROSIM_SEED") else Sys.setenv(MICROSIM_SEED = value)
  on.exit({
    if (is.na(old)) Sys.unsetenv("MICROSIM_SEED") else Sys.setenv(MICROSIM_SEED = old)
  }, add = TRUE)
  force(code)
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: an explicit seed always wins over the environment", {
  # Precedence matters because a stale MICROSIM_SEED in a shell is invisible in
  # the call. An explicit argument must never be overridden by it.
  cyc08_with_env("777", {
    expect_equal(suppressMessages(seed_microsimulation(123L, mode = "strict")), 123L)
    expect_equal(suppressMessages(seed_microsimulation(mode = "strict")), 777L)
  })
  # Boundary seeds are legal integers and must be accepted as given.
  for (s in c(0L, 1L, -1L, .Machine$integer.max)) {
    expect_equal(suppressMessages(seed_microsimulation(s, mode = "strict")), s)
  }
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: an unset environment seed falls back to the documented default", {
  cyc08_with_env(NA, {
    expect_equal(suppressMessages(seed_microsimulation(mode = "strict")), 20260801L)
  })
  # An empty string is "unset", not "malformed": nzchar("") is FALSE.
  cyc08_with_env("", {
    expect_equal(suppressMessages(seed_microsimulation(mode = "strict")), 20260801L)
  })
  # Whitespace around an integer still parses.
  cyc08_with_env(" 42 ", {
    expect_equal(suppressMessages(seed_microsimulation(mode = "strict")), 42L)
  })
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: type-tagged canonicalisation separates a value from its own rendering", {
  h2 <- function(x) urpssim:::.prereg_spec_hash(x, version = "2")
  # The four v1 collisions, each now distinct.
  expect_false(identical(h2(list(a = list(b = 1))), h2(list(a = "{b=1}"))))
  expect_false(identical(h2(list(a = c(1, 2))), h2(list(a = "1,2"))))
  expect_false(identical(h2(list(a = TRUE)), h2(list(a = "TRUE"))))
  expect_false(identical(h2(list(a = 1 / 3)), h2(list(a = 0.333333333333333))))
  # Integer and double are different declarations of the same number.
  expect_false(identical(h2(list(a = 1L)), h2(list(a = 1.0))))

  # And the properties that must survive: identical specs agree, and key order
  # is irrelevant (a spec is a set of named choices, not a sequence).
  expect_identical(h2(list(a = 1, b = "x")), h2(list(b = "x", a = 1)))
  expect_identical(h2(list(a = 1)), h2(list(a = 1)))
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: seeding fixes the stream, and the RNG kind is pinned with it", {
  # A seed alone is not reproducibility: the same seed under a different RNGkind
  # gives a different stream. seed_microsimulation() pins both, which is what
  # makes a run comparable across R versions >= 3.6.
  suppressWarnings(RNGkind("L'Ecuyer-CMRG"))
  suppressMessages(seed_microsimulation(4242L, mode = "strict"))
  expect_equal(RNGkind()[1], "Mersenne-Twister")
  a <- stats::runif(5)

  suppressWarnings(RNGkind("L'Ecuyer-CMRG"))     # perturb it again
  suppressMessages(seed_microsimulation(4242L, mode = "strict"))
  expect_equal(stats::runif(5), a)

  # A different seed must give a different stream, or the seed is inert.
  suppressMessages(seed_microsimulation(4243L, mode = "strict"))
  expect_false(isTRUE(all.equal(stats::runif(5), a)))
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: relaxed mode leaves the RNG alone only when NO seed is resolvable", {
  # The one legitimate unseeded path. It must require an explicit NA, not arise
  # from a typo, and it must announce itself.
  expect_true(is.na(suppressMessages(seed_microsimulation(NA_integer_, mode = "relaxed"))))
  expect_message(seed_microsimulation(NA_integer_, mode = "relaxed"), "left unseeded")

  # Strict mode has no unseeded path at all: an NA falls back to the default
  # rather than producing an irreproducible strict run.
  expect_equal(suppressMessages(seed_microsimulation(NA_integer_, mode = "strict")), 20260801L)

  # And an unseeded relaxed call really does leave the stream alone.
  set.seed(999); expected <- stats::runif(3)
  set.seed(999); suppressMessages(seed_microsimulation(NA_integer_, mode = "relaxed"))
  expect_equal(stats::runif(3), expected)
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: the run id is deterministic in strict mode and time-stamped in relaxed", {
  # A run id that changes between identical strict runs makes provenance
  # unmatchable; one that does NOT change between different seeds makes it
  # misleading.
  a <- make_run_id("tag", 1L, mode = "strict")
  expect_identical(a, make_run_id("tag", 1L, mode = "strict"))
  expect_false(identical(a, make_run_id("tag", 2L, mode = "strict")))
  expect_false(identical(a, make_run_id("other", 1L, mode = "strict")))
  expect_match(a, "^tag_strict_[0-9a-f]{12}$")

  # Relaxed carries a timestamp instead, and must not claim to be strict.
  r <- make_run_id("tag", 1L, mode = "relaxed")
  expect_false(grepl("strict", r, fixed = TRUE))
  expect_match(r, "^tag_[0-9]{8}_[0-9]{6}$")
})

# ---- SEMANTIC 4 -------------------------------------------------------------

test_that("SEMANTIC: the frozen v1 preregistration still verifies under its own version", {
  # THE CONSTRAINT ON THE FIX. inst/extdata carries a record frozen 2026-08-07,
  # while board_certified_active still ended at 2023. Changing the hash function
  # under it would be the same offence the module exists to prevent, committed
  # by the guard itself -- so the record declares its canonicalisation version
  # and is verified with it.
  path <- system.file("extdata", "preregistration",
                      "urps_pipeline_forecast_2024_2026.txt", package = "urpssim")
  skip_if(!nzchar(path) || !file.exists(path), "frozen preregistration not installed")
  rec <- urpssim:::.read_preregistration(path)
  expect_identical(rec$prereg_version, "1")
  expect_equal(nchar(rec$spec_hash), 64L)
  # Its recorded canonical_spec must still hash to its recorded spec_hash under
  # v1 -- that is what "immutable once data are observed" means operationally.
  expect_identical(digest::digest(rec$canonical_spec, algo = "sha256"), rec$spec_hash)
  # New records carry v2.
  expect_identical(urpssim:::PREREG_CURRENT_VERSION, "2")
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: a malformed environment seed is refused, not silently replaced", {
  # THE DEFECT. as.integer("twenty") is NA with the warning suppressed, and the
  # two modes then failed in opposite unhelpful directions: strict silently used
  # the DEFAULT seed (reproducible, but not the run anyone asked for) and
  # relaxed left the RNG entirely UNSEEDED (not reproducible at all). Measured
  # before the fix: strict returned 20260801, relaxed returned NA.
  # "3.7" is the sharpest of these: as.integer("3.7") is 3, so the old code did
  # not even produce NA -- it silently truncated to a DIFFERENT valid seed.
  for (bad in c("twenty", "3.7", "3.7.1", "1e999x", "seed=5", "", " ")) {
    if (!nzchar(trimws(bad))) next          # empty is "unset", covered in BVA 2
    cyc08_with_env(bad, {
      expect_error(seed_microsimulation(mode = "strict"), "not a whole number",
                   info = paste("accepted MICROSIM_SEED =", bad))
      expect_error(seed_microsimulation(mode = "relaxed"), "not a whole number",
                   info = paste("accepted MICROSIM_SEED =", bad))
    })
  }
  # Unambiguous integer spellings R accepts are NOT malformed and must work.
  cyc08_with_env("0x1F", {
    expect_equal(suppressMessages(seed_microsimulation(mode = "strict")), 31L)
  })
  cyc08_with_env("1e3", {
    expect_equal(suppressMessages(seed_microsimulation(mode = "strict")), 1000L)
  })
  # An explicit seed still bypasses the environment entirely, so a stale bad
  # variable cannot break a caller who was explicit.
  cyc08_with_env("twenty", {
    expect_equal(suppressMessages(seed_microsimulation(5L, mode = "strict")), 5L)
  })
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: a spec cannot pass the gate by impersonating the registered one", {
  # The module's claim is that assert_spec_matches_prereg() cannot be satisfied
  # by anything other than the frozen spec, because "changing the specification
  # after preregistration is model selection on the held-out data". A hash
  # collision is exactly a way to satisfy it with a different spec, and v1 had
  # four of them.
  d <- withr::local_tempdir()
  path <- file.path(d, "prereg.txt")
  spec <- list(model = "pipeline", knots = c(60, 70), refit = TRUE)
  rec <- preregister_spec(spec, path, frozen_at = "2026-01-01")
  expect_identical(rec$prereg_version, "2")

  expect_true(assert_spec_matches_prereg(spec, path))
  # The rendering of each field must NOT pass as the field.
  expect_error(assert_spec_matches_prereg(
    list(model = "pipeline", knots = "60,70", refit = TRUE), path), "does not match")
  expect_error(assert_spec_matches_prereg(
    list(model = "pipeline", knots = c(60, 70), refit = "TRUE"), path), "does not match")
  # A genuinely different spec is still refused, and the message says why.
  expect_error(assert_spec_matches_prereg(
    list(model = "pipeline", knots = c(60, 75), refit = TRUE), path),
    "model selection on the held-out data")
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: re-registering an unchanged spec is not mistaken for a spec change", {
  # The version dispatch has a failure mode of its own: if a v1 record were
  # re-verified with the v2 hash, an UNCHANGED spec would look like a changed
  # one and the guard would fire on a run that did nothing wrong. A guard that
  # cries wolf gets disabled, so this matters as much as the collision.
  d <- withr::local_tempdir()
  path <- file.path(d, "v1.txt")
  spec <- list(model = "pipeline", horizon = 3L)
  writeLines(c("# frozen under v1",
               "prereg_version: 1",
               paste0("spec_hash: ", urpssim:::.prereg_spec_hash(spec, version = "1")),
               "frozen_at: 2026-01-01",
               "notes: legacy record",
               paste0("canonical_spec: ", urpssim:::.canonicalize_spec(spec))), path)

  expect_true(suppressMessages(assert_spec_matches_prereg(spec, path)))
  # It must SAY it is verifying under the weaker canonicalisation rather than
  # quietly doing so.
  expect_message(assert_spec_matches_prereg(spec, path), "v1")
  # Re-registering the same spec at the v1 path is a no-op, not a conflict.
  expect_silent(preregister_spec(spec, path, frozen_at = "2026-01-01"))
  # A different spec at that path is still refused.
  expect_error(preregister_spec(list(model = "pipeline", horizon = 4L), path,
                                frozen_at = "2026-01-01"),
               "DIFFERENT spec is already registered")
})
