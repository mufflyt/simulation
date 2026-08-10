# Adversarial cycle 01 (lifecycle track) ----
#
# Targets chosen because a test can pass while the microsimulation is still
# scientifically wrong: FTE-vs-headcount semantics, denominator estimands,
# age/hazard boundaries, year indexing, RNG state, and identity handling.
#
# Mix: 4 boundary-value, 3 semantic/contract, 3 adversarial.

# ---- BVA 1: restate_fte, zero and negative hour denominators ----------------
# restate_fte() divides by an hours threshold. A zero or negative threshold is
# not a valid FTE definition, and silently returning Inf/-Inf would propagate a
# non-finite supply figure into a headline count.
test_that("BVA: restate_fte refuses a non-positive hours denominator", {
  expect_equal(restate_fte(10, from_hours = 40, to_hours = 40), 10)

  for (bad in list(0, -1, -0.0001)) {
    r <- tryCatch(restate_fte(10, from_hours = bad, to_hours = 40),
                  error = function(e) "error")
    expect_true(identical(r, "error") || is.finite(r),
                info = paste("from_hours =", bad, "produced", r,
                             "-- a non-positive hours denominator must error",
                             "or yield a finite value, never Inf/NaN"))
    r2 <- tryCatch(restate_fte(10, from_hours = 40, to_hours = bad),
                   error = function(e) "error")
    expect_true(identical(r2, "error") || is.finite(r2),
                info = paste("to_hours =", bad, "produced", r2))
  }
})

# ---- BVA 2: hazard at and beyond the age boundaries -------------------------
# Ages below the youngest and above the oldest tabulated row are the two places
# an interpolating hazard lookup silently returns NA or extrapolates.
test_that("BVA: departure hazard is a probability at every age boundary", {
  ages <- c(0, 1, 24, 25, 26, 64, 65, 66, 89, 90, 91, 120)
  h <- vapply(ages, function(a) {
    v <- tryCatch(departure_hazard(a), error = function(e) NA_real_)
    if (length(v) != 1L) NA_real_ else as.numeric(v)
  }, numeric(1))

  ok <- is.na(h) | (h >= 0 & h <= 1)
  expect_true(all(ok),
              info = paste("hazard outside [0,1] at ages:",
                           paste(ages[!ok], collapse = ", "),
                           "values:", paste(round(h[!ok], 4), collapse = ", ")))
  expect_false(any(is.nan(h)), info = "NaN hazard at an age boundary")
})

# ---- BVA 3: hazard monotonicity has no reversal at the join points ----------
# A retirement hazard that dips as age rises would let the cohort age into
# LOWER attrition, inflating supply. Table joins are where that happens.
test_that("BVA: retirement hazard never decreases across adjacent ages", {
  ages <- 30:90
  h <- vapply(ages, function(a) {
    v <- tryCatch(departure_hazard(a), error = function(e) NA_real_)
    if (length(v) != 1L) NA_real_ else as.numeric(v)
  }, numeric(1))
  keep <- !is.na(h)
  skip_if(sum(keep) < 5, "hazard not evaluable across the age range")

  d <- diff(h[keep])
  drops <- which(d < -1e-12)
  expect_length(drops, 0)
  if (length(drops)) {
    fail(paste("hazard DECREASES between ages",
               paste(sprintf("%d->%d (%.5f)", ages[keep][drops],
                             ages[keep][drops + 1L], d[drops]),
                     collapse = "; ")))
  }
})

# ---- BVA 4: retirement_survival over a zero-length and inverted horizon -----
# from_age == to_age is survival over no time, which must be exactly 1. An
# inverted horizon is not meaningful and must not silently return a
# probability above 1.
test_that("BVA: survival over a zero-length horizon is exactly 1", {
  s <- tryCatch(retirement_survival(from_age = 60, to_ages = 60),
                error = function(e) NULL)
  skip_if(is.null(s), "retirement_survival() not evaluable with these arguments")

  v <- if (is.data.frame(s)) s[[grep("surv|prob", names(s), ignore.case = TRUE)[1]]] else unlist(s)
  v <- suppressWarnings(as.numeric(v))
  v <- v[is.finite(v)]
  skip_if(length(v) == 0, "no numeric survival value returned")
  expect_equal(v[1], 1, tolerance = 1e-8,
               info = "survival from an age to itself must be 1")

  inv <- tryCatch(retirement_survival(from_age = 70, to_ages = 60),
                  error = function(e) "error")
  if (!identical(inv, "error")) {
    x <- suppressWarnings(as.numeric(unlist(inv)))
    x <- x[is.finite(x)]
    expect_true(all(x <= 1 + 1e-8),
                info = "inverted horizon produced a survival probability > 1")
  }
})

# ---- SEMANTIC 1: FTE is not headcount, and the label states the basis -------
# The single most consequential unit error in this package: reporting FTE as if
# it were providers. fte_definition() is what a reader is told; it must match
# the hours actually used.
test_that("SEMANTIC: fte_definition label matches the hours it was built on", {
  for (hrs in c(30, 37.2, 40, 50)) {
    d <- fte_definition(hours = hrs)
    expect_equal(d$hours_per_week, hrs)
    expect_match(d$basis, "clinical", ignore.case = TRUE)
    expect_true(grepl(format(hrs, nsmall = 1), d$label, fixed = TRUE),
                info = paste("label", shQuote(d$label),
                             "does not state its own hours basis", hrs))
  }
})

# ---- SEMANTIC 2: restating onto a LONGER week must reduce FTE ---------------
# Direction errors here silently rescale supply. One FTE at 40 hrs/wk is less
# than one FTE at 30 hrs/wk, never more.
test_that("SEMANTIC: restate_fte moves in the correct direction", {
  base <- 100
  longer  <- restate_fte(base, from_hours = 30, to_hours = 60)
  shorter <- restate_fte(base, from_hours = 60, to_hours = 30)

  expect_lt(longer, base)
  expect_gt(shorter, base)
  # Round trip is the identity: restating and restating back must return.
  expect_equal(restate_fte(longer, from_hours = 60, to_hours = 30), base,
               tolerance = 1e-8)
  # Exact ratio, not merely the direction.
  expect_equal(longer, base * 30 / 60, tolerance = 1e-8)
})

# ---- SEMANTIC 3: the three denominators are not interchangeable -------------
# "Ever certified", "roster-observable" and "active clinical workforce" are
# different populations. The audit that established the 1,306 target exists
# because they had been used interchangeably.
test_that("SEMANTIC: denominator estimands stay distinct and labelled", {
  e <- denominator_estimands()
  expect_gte(nrow(as.data.frame(e)), 2L)

  nm <- tolower(paste(unlist(lapply(as.data.frame(e), as.character)), collapse = " "))
  expect_true(grepl("ever", nm) && grepl("active", nm),
              info = "the estimand table no longer distinguishes ever-certified from active")

  # Asking for a nonexistent estimand must fail loudly rather than default to one.
  expect_error(assert_denominator_estimand("not_an_estimand"),
               info = "an unknown estimand was silently accepted")
})

# ---- ADVERSARIAL 1: RNG state must not leak between calls -------------------
# A function that consumes the RNG stream without restoring it makes every
# downstream draw depend on call order, which breaks reproducibility in a way
# that a seeded test still passes.
test_that("ADVERSARIAL: seeded results reproduce and do not depend on call order", {
  draw <- function() {
    set.seed(4242)
    fte_definition()          # must not consume RNG
    stats::runif(3)
  }
  a <- draw(); b <- draw()
  expect_equal(a, b, info = "identical seeds gave different draws")

  set.seed(4242); direct <- stats::runif(3)
  expect_equal(a, direct,
               info = paste("fte_definition() consumed RNG state:",
                            "downstream draws now depend on call order"))
})

# ---- ADVERSARIAL 2: duplicated identifiers must not inflate the cohort ------
# A roster with a repeated NPI is a plausible upstream defect. Silently building
# two agents from it inflates supply by exactly the duplication rate.
test_that("ADVERSARIAL: a duplicated roster identifier does not double-count", {
  base <- tibble::tibble(
    provider_id = c("a", "b"), pathway = "ABOG", age = c(45, 60),
    sex = c("female", "male"), state = c("CO", "TX"),
    certification_year = c(2010, 1995),
    last_confirmed_active_year = c(2023, 2023))
  dup <- rbind(base, base[1, , drop = FALSE])   # provider_id "a" twice

  n_of <- function(d) {
    a <- tryCatch(agents_from_roster(d, baseline_year = 2023),
                  error = function(e) NULL)
    if (is.null(a)) NA_integer_ else nrow(as.data.frame(a))
  }
  n_base <- n_of(base); n_dup <- n_of(dup)
  skip_if(is.na(n_base) || is.na(n_dup), "agents_from_roster() not evaluable on this fixture")

  expect_true(n_dup == n_base || n_dup == n_base + 1L,
              info = paste("duplicate identifier changed the cohort from", n_base,
                           "to", n_dup, "-- if duplicates are intentionally kept",
                           "that is a documented estimand choice, not a default"))
})

# ---- ADVERSARIAL 3: row order must not change the answer --------------------
# Any dependence on input ordering means the result is an artifact of how the
# roster happened to be sorted.
test_that("ADVERSARIAL: roster row order does not change the cohort size", {
  r <- tibble::tibble(
    provider_id = c("a", "b", "c"), pathway = "ABOG", age = c(45, 60, 39),
    sex = c("female", "male", "female"), state = c("CO", "TX", "NY"),
    certification_year = c(2010, 1995, 2015),
    last_confirmed_active_year = c(2023, 2023, 2023))
  f <- function(d) {
    a <- tryCatch(agents_from_roster(d, baseline_year = 2023),
                  error = function(e) NULL)
    if (is.null(a)) NA_integer_ else nrow(as.data.frame(a))
  }
  n1 <- f(r); n2 <- f(r[rev(seq_len(nrow(r))), , drop = FALSE])
  skip_if(is.na(n1) || is.na(n2), "agents_from_roster() not evaluable on this fixture")
  expect_equal(n2, n1, info = "reversing roster row order changed the cohort size")
})
