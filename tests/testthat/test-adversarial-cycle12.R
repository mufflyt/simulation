# Adversarial cycle 12 -- session-global state, and the hole in the idiom that
# was supposed to protect it.
#
# Cycle 11 carried forward: other global state mutated as a side effect.
# Swept R/ for options(), Sys.setenv(), Sys.unsetenv(), setwd(), par(),
# Sys.setlocale() and assignment into globalenv(). Result: the ONLY session-global
# state this package writes is .Random.seed. That is a real finding and the
# tests below pin it, because "we do not touch global options" is only worth
# anything if something checks.
#
# The sweep did turn up one thing. TWO functions already carried a hand-rolled
# save/restore of .Random.seed -- and cycle 11's claim that they had not was
# wrong. The idiom they used has a hole:
#
#     old <- if (exists(".Random.seed", ...)) get(...) else NULL
#     on.exit(if (!is.null(old)) assign(".Random.seed", old, ...))
#
# In a FRESH session there is nothing to restore, so `if (!is.null(old))` does
# nothing and the seeded state is simply left behind. Measured: the old form
# leaves .Random.seed present; the helper removes it.
#
# Mix: 3 boundary-value, 3 semantic/contract, 4 adversarial.

cyc12_fresh <- function() {
  if (exists(".Random.seed", envir = globalenv(), inherits = FALSE)) {
    rm(".Random.seed", envir = globalenv())
  }
  invisible(NULL)
}

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: every seeded entry point leaves a fresh session unseeded", {
  # The boundary the old idiom could not express. Each of these seeds
  # internally; in a session that has drawn nothing yet, none may leave a
  # .Random.seed behind, because that silently converts "unseeded" into
  # "seeded from whatever this function chose".
  cyc12_fresh()
  invisible(psa_sample(list(psa_uniform("a", 0, 1)), n = 8, seed = 5L))
  expect_false(exists(".Random.seed", envir = globalenv(), inherits = FALSE))

  cyc12_fresh()
  invisible(access_moe_ci(access = c(1, 2, 3), est = c(10, 20, 30),
                          se = c(1, 2, 3), B = 25L, seed = 5L))
  expect_false(exists(".Random.seed", envir = globalenv(), inherits = FALSE))

  cyc12_fresh()
  d <- data.frame(geo = sprintf("g%02d", 1:12), y = as.integer(seq(20, 31)),
                  x = seq(1, 4, length.out = 12))
  invisible(geographic_holdout_cv(d, "y", "x", geo = "geo", scheme = "kfold",
                                  k = 3, seed = 5L))
  expect_false(exists(".Random.seed", envir = globalenv(), inherits = FALSE))
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: the restore is byte-exact, not merely 'a stream of the right kind'", {
  # .Random.seed is a long integer vector carrying the generator's full position.
  # Restoring a stream that merely produces plausible numbers is not restoring
  # it: the next draw must be the exact one the caller would have got.
  set.seed(1234)
  invisible(stats::runif(17))                    # land mid-stream, not on a boundary
  before <- get(".Random.seed", envir = globalenv())
  expected <- with_preserved_rng(NULL, {         # peek without disturbing
    NULL
  })
  invisible(psa_sample(list(psa_normal("z", 0, 1)), n = 12, seed = 99L))
  expect_identical(get(".Random.seed", envir = globalenv()), before)
  expect_null(expected)

  # And the value the caller then draws matches the no-call counterfactual.
  set.seed(1234); invisible(stats::runif(17)); a <- stats::runif(3)
  set.seed(1234); invisible(stats::runif(17))
  invisible(psa_sample(list(psa_normal("z", 0, 1)), n = 12, seed = 99L))
  expect_equal(stats::runif(3), a)
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: RNGkind is part of the state and is not silently changed", {
  # A restored seed under a different generator is a different stream. Only
  # seed_microsimulation() may pin the kind, and it says so; nothing else may
  # move it as a side effect.
  suppressWarnings(RNGkind("L'Ecuyer-CMRG"))
  on.exit(suppressWarnings(RNGkind("Mersenne-Twister", "Inversion", "Rejection")), add = TRUE)
  before <- RNGkind()
  invisible(psa_sample(list(psa_uniform("a", 0, 1)), n = 8, seed = 5L))
  expect_identical(RNGkind(), before)

  d <- data.frame(geo = sprintf("g%02d", 1:12), y = as.integer(seq(20, 31)),
                  x = seq(1, 4, length.out = 12))
  invisible(geographic_holdout_cv(d, "y", "x", geo = "geo", scheme = "kfold",
                                  k = 3, seed = 5L))
  expect_identical(RNGkind(), before)

  # seed_microsimulation() is the exception, and pins it deliberately.
  suppressMessages(seed_microsimulation(3L, mode = "strict"))
  expect_equal(RNGkind()[1], "Mersenne-Twister")
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: the package writes no session-global state except the RNG", {
  # The sweep's positive result, asserted rather than asserted-about. If a
  # future change starts setting an option or an environment variable to carry
  # state between calls, this is where it shows up.
  opts_before <- options()
  env_before <- Sys.getenv()
  wd_before <- getwd()

  d <- data.frame(geo = sprintf("g%02d", 1:12), region = rep(c("A", "B"), 6),
                  y = as.integer(seq(20, 31)), x = seq(1, 4, length.out = 12))
  invisible(geographic_holdout_cv(d, "y", "x", geo = "geo", region = "region",
                                  scheme = "region"))
  invisible(suppressMessages(compute_demand_denominators(
    data.frame(year = rep(2025L, length(DEMAND_AGE_BANDS)),
               age_band = DEMAND_AGE_BANDS,
               female_pop = rep(1e6, length(DEMAND_AGE_BANDS))))))
  invisible(psa_sample(list(psa_uniform("a", 0, 1)), n = 8, seed = 5L))

  expect_identical(getwd(), wd_before)
  expect_identical(setdiff(names(Sys.getenv()), names(env_before)), character(0))
  # Compare only the options the package could plausibly touch; graphics devices
  # and testthat itself move others.
  watch <- c("stringsAsFactors", "digits", "scipen", "warn", "OutDec",
             "na.action", "contrasts", "dplyr.summarise.inform")
  for (o in watch) {
    expect_identical(options()[[o]], opts_before[[o]], info = paste("option", o))
  }
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: reproducibility mode is read, never written", {
  # resolve_reproducibility_mode() reads an environment variable. A function
  # that also SET it would make the mode sticky across calls, so a single
  # strict-mode run would silently put the rest of the session in strict mode.
  old <- Sys.getenv("REPRODUCIBILITY_MODE", unset = NA_character_)
  on.exit(if (is.na(old)) Sys.unsetenv("REPRODUCIBILITY_MODE") else
            Sys.setenv(REPRODUCIBILITY_MODE = old), add = TRUE)

  Sys.unsetenv("REPRODUCIBILITY_MODE")
  expect_equal(resolve_reproducibility_mode(), "relaxed")
  expect_false(nzchar(Sys.getenv("REPRODUCIBILITY_MODE")))

  expect_equal(resolve_reproducibility_mode(default = "strict"), "strict")
  # Asking for strict via the argument must not persist it into the environment.
  expect_false(nzchar(Sys.getenv("REPRODUCIBILITY_MODE")))
  expect_equal(resolve_reproducibility_mode(), "relaxed")
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: a seeded helper is a pure function of its arguments", {
  # If the result depends on ambient state as well as on (args, seed), then two
  # identical calls in different sessions disagree -- which is what the seed
  # argument exists to rule out.
  args <- list(list(psa_uniform("a", 0, 1), psa_normal("b", 5, 2)), 16L, 7L)
  set.seed(1); r1 <- do.call(psa_sample, args)
  set.seed(9999); invisible(stats::runif(500)); r2 <- do.call(psa_sample, args)
  cyc12_fresh(); r3 <- do.call(psa_sample, args)
  expect_equal(r1, r2)
  expect_equal(r1, r3)

  # And the seed argument is load-bearing: a different one gives a different sample.
  expect_false(isTRUE(all.equal(r1, psa_sample(args[[1]], 16L, 8L))))
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: the fresh-session hole is closed in both places that had it", {
  # THE DEFECT, and cycle 11 mis-reported it. access_moe_ci() and psa_sample()
  # ALREADY carried a save/restore -- cycle 11 said they had not. The form they
  # carried could not restore the absent case, so in a fresh session both left
  # .Random.seed behind and every subsequent draw became a function of their
  # internal seed rather than of the caller's.
  for (call in list(
    function() psa_sample(list(psa_uniform("a", 0, 1)), n = 8, seed = 11L),
    function() access_moe_ci(c(1, 2), c(10, 20), c(1, 2), B = 20L, seed = 11L))) {
    cyc12_fresh()
    invisible(call())
    expect_false(exists(".Random.seed", envir = globalenv(), inherits = FALSE))
  }

  # And with a stream present they are still exactly neutral.
  set.seed(808); before <- get(".Random.seed", envir = globalenv())
  invisible(psa_sample(list(psa_uniform("a", 0, 1)), n = 8, seed = 11L))
  invisible(access_moe_ci(c(1, 2), c(10, 20), c(1, 2), B = 20L, seed = 11L))
  expect_identical(get(".Random.seed", envir = globalenv()), before)
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: repeated calls do not accumulate state", {
  # A leak that restores ALMOST correctly shows up as drift: each call moves the
  # stream a little, so the tenth call sits somewhere the first did not. One
  # call being neutral is weaker evidence than ten being neutral.
  set.seed(555)
  before <- get(".Random.seed", envir = globalenv())
  for (i in 1:10) {
    invisible(psa_sample(list(psa_uniform("a", 0, 1)), n = 6, seed = i))
    invisible(access_moe_ci(c(1, 2), c(10, 20), c(1, 2), B = 15L, seed = i))
  }
  expect_identical(get(".Random.seed", envir = globalenv()), before)
  expect_equal(length(get(".Random.seed", envir = globalenv())), length(before))
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: an interrupted seeded call still restores", {
  # on.exit is the mechanism, so the error path is the one that matters --
  # and validation guards mean the error path is reached often.
  set.seed(606)
  before <- get(".Random.seed", envir = globalenv())
  expect_error(psa_sample(list(psa_discrete("d", c("x", "y"), probs = c(1.5, -0.5))),
                          n = 8, seed = 1L))
  expect_identical(get(".Random.seed", envir = globalenv()), before)

  expect_error(access_moe_ci(c(1, 2), c(10, 20, 30), c(1, 2), B = 10L, seed = 1L))
  expect_identical(get(".Random.seed", envir = globalenv()), before)

  # Fresh session, error path: still nothing left behind.
  cyc12_fresh()
  expect_error(access_moe_ci(c(1, 2), c(10, 20, 30), c(1, 2), B = 10L, seed = 1L))
  expect_false(exists(".Random.seed", envir = globalenv(), inherits = FALSE))
})

# ---- ADVERSARIAL 4 ----------------------------------------------------------

test_that("ADVERSARIAL: two RNG-neutral calls compose without either seeing the other", {
  # The realistic pipeline shape: a PSA inside a validation run inside a seeded
  # session. Each layer seeds for its own reproducibility; none may reach the
  # others. If the restores nested wrongly, the outer layer would resume from
  # the inner layer's position.
  suppressMessages(seed_microsimulation(20260812L, mode = "strict"))
  ref <- stats::runif(4)

  suppressMessages(seed_microsimulation(20260812L, mode = "strict"))
  invisible(psa_sample(list(psa_uniform("a", 0, 1)), n = 10, seed = 1L))
  invisible(access_moe_ci(c(1, 2, 3), c(10, 20, 30), c(1, 1, 1), B = 20L, seed = 2L))
  d <- data.frame(geo = sprintf("g%02d", 1:12), y = as.integer(seq(20, 31)),
                  x = seq(1, 4, length.out = 12))
  invisible(geographic_holdout_cv(d, "y", "x", geo = "geo", scheme = "kfold",
                                  k = 3, seed = 3L))
  expect_equal(stats::runif(4), ref)
})
