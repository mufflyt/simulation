# Adversarial cycle 11 -- functions that reseed the session as a side effect.
#
# Cycle 10 carried forward unseeded stochastic tests, after one such test spent
# an unknown number of cycles neither reliably passing nor reliably failing.
# Parsing every test file found 79 test_that blocks that call a stochastic
# function with no set.seed(). Seeding all 79 blindly would be busywork, so the
# question asked instead was: which verdicts actually depend on the stream?
#
# Answering that pointed at the other end of the problem. Eight functions in R/
# call set.seed() internally, and set.seed() mutates GLOBAL state -- so a
# function that seeds for its own reproducibility silently reseeds the caller's
# session. The run stays deterministic; it just stops being deterministic from
# the seed anybody chose.
#
# calibration-psa.R already saved and restored .Random.seed around its own
# seeding, so the idiom was established in-repo and six siblings had not adopted
# it.
#
# Mix: 3 boundary-value, 4 semantic/contract, 3 adversarial.

cyc11_geo <- function(n = 20) {
  data.frame(geo = sprintf("g%02d", seq_len(n)),
             region = rep(c("A", "B", "C", "D"), length.out = n),
             y = as.integer(seq(20, 40, length.out = n)),
             x = seq(1, 5, length.out = n),
             pop = 1e5, stringsAsFactors = FALSE)
}

# Draw three numbers from the caller's stream after running `code`, and compare
# with the same three drawn without it. Equality means the stream survived.
cyc11_stream_survives <- function(code) {
  set.seed(4242); invisible(stats::runif(1)); a <- stats::runif(3)
  set.seed(4242); invisible(stats::runif(1)); force(code); b <- stats::runif(3)
  isTRUE(all.equal(a, b))
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: the preserved scope restores an absent stream as absent", {
  # The boundary nobody thinks about: a fresh session has NO .Random.seed at
  # all. Restoring a saved-but-null state by assigning NULL would error, and
  # leaving the seeded state behind would silently make the session seeded.
  if (exists(".Random.seed", envir = globalenv(), inherits = FALSE)) {
    rm(".Random.seed", envir = globalenv())
  }
  expect_false(exists(".Random.seed", envir = globalenv(), inherits = FALSE))
  out <- with_preserved_rng(7L, stats::runif(2))
  expect_length(out, 2L)
  expect_false(exists(".Random.seed", envir = globalenv(), inherits = FALSE))

  # And with a stream present, it comes back byte-identical.
  set.seed(11)
  before <- get(".Random.seed", envir = globalenv())
  invisible(with_preserved_rng(99L, stats::runif(5)))
  expect_identical(get(".Random.seed", envir = globalenv()), before)
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: a NULL seed means leave the stream alone, not seed with nothing", {
  # NULL is the documented "no seed" value on most of these functions, and it
  # must be a no-op rather than a reseed to some default.
  set.seed(21)
  expected <- stats::runif(3)
  set.seed(21)
  got <- with_preserved_rng(NULL, stats::runif(3))
  expect_equal(got, expected)

  # With NULL the scope still restores, so the caller's position is unchanged
  # even though the draws consumed from it were real.
  set.seed(21)
  before <- get(".Random.seed", envir = globalenv())
  invisible(with_preserved_rng(NULL, stats::runif(3)))
  expect_identical(get(".Random.seed", envir = globalenv()), before)
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: the scope returns the expression's value and honours the seed", {
  # It is a value-returning wrapper, not a side-effecting one, and the seed has
  # to actually take effect inside or the wrapper is decoration.
  a <- with_preserved_rng(123L, stats::runif(4))
  b <- with_preserved_rng(123L, stats::runif(4))
  expect_equal(a, b)
  c3 <- with_preserved_rng(124L, stats::runif(4))
  expect_false(isTRUE(all.equal(a, c3)))
  expect_equal(with_preserved_rng(1L, 42), 42)          # non-random values pass through
  expect_null(with_preserved_rng(1L, NULL))
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: geographic_holdout_cv is reproducible without reseeding its caller", {
  # THE DEFECT, measured. .geo_folds() called set.seed(seed) directly, so
  # geographic_holdout_cv(seed = 42) changed the next three runif() draws in the
  # calling scope. `seed` exists so the FOLDS are reproducible, not so a
  # validation call can reach out and reseed the session around it.
  d <- cyc11_geo()
  expect_true(cyc11_stream_survives(
    geographic_holdout_cv(d, "y", "x", geo = "geo", scheme = "kfold", k = 4, seed = 42)))

  # And the reproducibility the seed exists for still holds.
  f1 <- geographic_holdout_cv(d, "y", "x", geo = "geo", scheme = "kfold", k = 4, seed = 42)
  f2 <- geographic_holdout_cv(d, "y", "x", geo = "geo", scheme = "kfold", k = 4, seed = 42)
  expect_equal(f1$predictions$fold, f2$predictions$fold)
  f3 <- geographic_holdout_cv(d, "y", "x", geo = "geo", scheme = "kfold", k = 4, seed = 43)
  expect_false(identical(f1$predictions$fold, f3$predictions$fold))
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: the deterministic schemes consume no randomness at all", {
  # loo and region are deterministic partitions. If they touch the RNG, a
  # "leave-one-region-out" score would move between runs for no modelled reason.
  d <- cyc11_geo()
  expect_true(cyc11_stream_survives(
    geographic_holdout_cv(d, "y", "x", geo = "geo", scheme = "loo")))
  expect_true(cyc11_stream_survives(
    geographic_holdout_cv(d, "y", "x", geo = "geo", region = "region", scheme = "region")))

  # Deterministic means identical across calls with no seed supplied.
  a <- geographic_holdout_cv(d, "y", "x", geo = "geo", region = "region", scheme = "region")
  b <- geographic_holdout_cv(d, "y", "x", geo = "geo", region = "region", scheme = "region")
  expect_equal(a$predictions$predicted, b$predictions$predicted)
  expect_equal(sort(unique(a$predictions$fold)), sort(unique(b$predictions$fold)))
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: seed_microsimulation is the one function allowed to reseed the session", {
  # The exception has to be explicit, or "no function reseeds the session" is a
  # rule with a silent hole in it. Seeding the session IS this function's purpose.
  expect_false(cyc11_stream_survives(suppressMessages(seed_microsimulation(5L, mode = "strict"))))
  # It is also the only one whose effect a caller can observe and record.
  expect_equal(suppressMessages(seed_microsimulation(5L, mode = "strict")), 5L)
})

# ---- SEMANTIC 4 -------------------------------------------------------------

test_that("SEMANTIC: a run's declared seed survives the functions called inside it", {
  # The consequence that matters. seed_microsimulation() sets the run's seed;
  # if anything called afterwards reseeds the session, every later draw is a
  # function of THAT seed instead. Two runs declaring the same seed must agree
  # whatever they call in between.
  d <- cyc11_geo()
  draw_after <- function() {
    suppressMessages(seed_microsimulation(20260810L, mode = "strict"))
    invisible(geographic_holdout_cv(d, "y", "x", geo = "geo", scheme = "kfold",
                                    k = 4, seed = 42))
    stats::runif(5)
  }
  expect_equal(draw_after(), draw_after())

  # And the run's seed, not the inner one, is what determines those draws.
  baseline <- function() {
    suppressMessages(seed_microsimulation(20260810L, mode = "strict"))
    stats::runif(5)
  }
  expect_equal(draw_after(), baseline())
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: no seeded helper in the package leaks its stream", {
  # The sweep, as one assertion. Six functions called set.seed() without
  # restoring; calibration-psa.R already did restore, which is what established
  # the idiom in-repo.
  d <- cyc11_geo()
  expect_true(cyc11_stream_survives(
    geographic_holdout_cv(d, "y", "x", geo = "geo", scheme = "kfold", k = 4, seed = 9)))
  expect_true(cyc11_stream_survives(
    suppressMessages(run_psa(list(psa_uniform("a", 0, 1)), function(p) p$a,
                             n = 8, seed = 3, verbose = FALSE))))

  ages <- 40:80
  inc <- rep(0.02, length(ages))
  expect_true(cyc11_stream_survives(
    prevalence_from_incidence(inc, remission = 0.05)))
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: the scope survives an error inside it", {
  # on.exit-based restoration is the whole mechanism. If it only ran on the
  # success path, any guard that stops mid-function would leave the session
  # reseeded -- and error paths are exactly where nobody looks.
  set.seed(77)
  before <- get(".Random.seed", envir = globalenv())
  expect_error(with_preserved_rng(1L, stop("boom")), "boom")
  expect_identical(get(".Random.seed", envir = globalenv()), before)

  # Same through a real function that validates after seeding.
  set.seed(78)
  before2 <- get(".Random.seed", envir = globalenv())
  expect_error(geographic_holdout_cv(cyc11_geo(), "y", "nope", geo = "geo",
                                     scheme = "kfold", seed = 5), "missing column")
  expect_identical(get(".Random.seed", envir = globalenv()), before2)
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: nesting scopes restores the outer stream, not the inner one", {
  # A validation function calling another seeded helper is the realistic case.
  # If restoration were a single global slot rather than a stack, the outer
  # scope would restore whatever the inner one saved.
  set.seed(31)
  before <- get(".Random.seed", envir = globalenv())
  out <- with_preserved_rng(100L, {
    inner <- with_preserved_rng(200L, stats::runif(3))
    c(inner, stats::runif(2))
  })
  expect_length(out, 5L)
  expect_identical(get(".Random.seed", envir = globalenv()), before)

  # The inner scope must not have disturbed the outer scope's own stream: the
  # outer draws must match a run where the inner call is absent.
  ref <- with_preserved_rng(100L, stats::runif(2))
  expect_equal(out[4:5], ref)
})
