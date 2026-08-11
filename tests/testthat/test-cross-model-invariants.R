# CROSS-MODEL INVARIANT AUDIT
#
# The 24 adversarial cycles worked module by module. This asks whether the whole
# model hangs together ACROSS module boundaries -- the properties that no single
# module owns and that therefore no single cycle tested.
#
# Ten invariants, each with at least one fixture that would fail if the
# invariant were broken. Where a property is already pinned elsewhere, the test
# here is the CROSS-MODULE form of it: not "the engine conserves headcount" but
# "headcount conserves through the engine, the summary, and the gap".
#
# Constants these rest on, read from the package rather than assumed:
#   MICROSIM_ENTRY_AGE 34   entry is POST-certification
#   CAREER_PIPELINE_STATES  resident, fellow -- never in the active set
#   CAREER_ACTIVE_STATES    early_career, mid_career, late_career
#   RETIREMENT_MIN_AGE 50   MICROSIM_TERMINAL_AGE 90
#
# This file does NOT validate the science. It checks mechanical coherence.
# Nothing here says the productivity denominator, the case mix or the
# delegation shares are right; those remain derived_by_analogy.

cmi_agents <- function(n = 60, lo = 36, hi = 68, female_share = 0.5, ids = TRUE) {
  a <- data.frame(
    provider_id = if (ids) sprintf("P%03d", seq_len(n)) else NA_character_,
    subspecialty = "FPMRS",
    sex = rep(c("female", "male"),
              times = c(round(n * female_share), n - round(n * female_share)))[seq_len(n)],
    age = seq(lo, hi, length.out = n),
    entry_year = 2015L, retirement_year = NA_real_,
    origin_cohort = "baseline", stringsAsFactors = FALSE)
  a
}
cmi_run <- function(a, years = 2025:2035, entrants = 6, ...) {
  simulate_provider_career_once(
    a, years, entrants,
    hours_intercept = calibrate_hours_intercept(a$age, a$sex),
    track_career_states = TRUE, ...)
}
cmi_frozen <- setNames(rep(0, 120), 1:120)     # nobody retires

# ---- 1. POPULATION ACCOUNTING -----------------------------------------------

test_that("INV-1: nobody is duplicated or silently dropped across the horizon", {
  set.seed(1)
  sim <- cmi_run(cmi_agents(60), 2025:2040, entrants = 8)

  # Every provider is one row, once. Cycle 01 established that duplicate ids are
  # treated as distinct clinicians, so a repeated id IS a duplicated person.
  expect_equal(anyDuplicated(sim$agents$provider_id), 0L)
  expect_false(any(is.na(sim$agents$provider_id)))

  # The returned cohort accounts for everyone the panel ever counted: base
  # cohort plus every entrant, none lost, none invented.
  n_entrants <- sum(sim$agents$origin_cohort == "entrant")
  expect_equal(nrow(sim$agents), 60L + n_entrants)
  expect_equal(n_entrants, 8L * (length(2025:2040) - 1L))

  # Nobody is active before entering or after retiring, in any year.
  for (y in c(2025L, 2032L, 2040L)) {
    act <- provider_active_in_year(sim$agents, y)
    expect_true(all(sim$agents$entry_year[act] <= y))
    expect_true(all(is.na(sim$agents$retirement_year[act]) |
                      sim$agents$retirement_year[act] > y))
  }
})

# ---- 2. YEAR-BY-YEAR TIMING -------------------------------------------------

test_that("INV-2: every transition is an ANNUAL step, with no monthly assumption", {
  # A closed cohort ages exactly one year per panel row. If any module worked in
  # months, this drifts by 12x and nothing else would notice.
  set.seed(2)
  p <- cmi_run(cmi_agents(40), 2025:2035, entrants = 0,
               retirement_schedule = cmi_frozen, career_change_hazard = 0)$panel
  expect_equal(diff(p$year), rep(1L, 10))
  expect_equal(diff(p$mean_age), rep(1, 10), tolerance = 1e-9)
  # Elapsed label time equals elapsed cohort time over the whole horizon.
  expect_equal(max(p$year) - min(p$year), max(p$mean_age) - min(p$mean_age),
               tolerance = 1e-9)

  # Hazards are ANNUAL probabilities: in [0, 1] across the whole age range. A
  # monthly rate would sit ~12x low and a monthly-to-annual slip ~12x high.
  h <- departure_hazard(30:89, sex = "female")
  expect_true(all(h >= 0 & h <= 1))
  expect_true(max(h) > 0.01)          # not a monthly rate masquerading as annual
})

# ---- 3. WORKFORCE ENTRY: BOARD-CERTIFIED ATTENDINGS ONLY --------------------

test_that("INV-3: only board-certified attendings are in the practising supply", {
  # The modelled independent workforce begins at certification. Residents and
  # fellows are PIPELINE states -- they may inform the entrant rate, and they
  # must never appear in practising supply.
  expect_setequal(CAREER_PIPELINE_STATES, c("resident", "fellow"))
  expect_equal(length(intersect(CAREER_PIPELINE_STATES, CAREER_ACTIVE_STATES)), 0L)

  set.seed(3)
  sim <- cmi_run(cmi_agents(50), 2025:2035, entrants = 10)
  p <- sim$panel

  # The state-stratified counts are exactly the active states, and they sum to
  # headcount -- so no pipeline state can be inside it.
  expect_equal(p$n_early_career + p$n_mid_career + p$n_late_career, p$headcount)

  # No entrant enters below the certification entry age.
  #
  # CAUGHT BY MUTATION. This originally checked career_state_of() on the active
  # set and passed with entrants injected at age 28 -- because career_state_of()
  # maps AGE BANDS and returns "early_career" for 28; the pipeline states are
  # reachable only via entered = FALSE. A fellow-aged entrant therefore looked
  # like a young attending and the test claiming to enforce
  # board-certified-only enforced nothing.
  #
  # The invariant has to be asserted on the AGE at entry, which is the thing
  # certification fixes.
  ent <- sim$agents[sim$agents$origin_cohort == "entrant", ]
  expect_true(all(ent$entry_year >= 2026L))
  age_at_entry <- ent$age - (max(sim$panel$year) + 1L - ent$entry_year)
  expect_true(all(age_at_entry >= MICROSIM_ENTRY_AGE - 1e-9),
              info = sprintf("youngest entrant entered at age %.2f, below the certification age %d",
                             min(age_at_entry), MICROSIM_ENTRY_AGE))
  expect_equal(unique(round(age_at_entry, 6)), as.numeric(MICROSIM_ENTRY_AGE))

  # And nobody active in any year is younger than the certification age.
  for (y in c(2025L, 2030L, 2035L)) {
    act <- sim$agents[provider_active_in_year(sim$agents, y), ]
    st <- career_state_of(act$age, entered = TRUE, retired = FALSE)
    expect_equal(length(intersect(as.character(st), CAREER_PIPELINE_STATES)), 0L,
                 info = paste("year", y))
    entrants_now <- act[act$origin_cohort == "entrant", ]
    if (nrow(entrants_now)) {
      age_then <- entrants_now$age - (max(sim$panel$year) + 1L - entrants_now$entry_year)
      expect_true(all(age_then >= MICROSIM_ENTRY_AGE - 1e-9), info = paste("year", y))
    }
  }
})

test_that("INV-3b: an under-age or unentered provider contributes no supply", {
  # The adversarial form: put a fellow-aged, not-yet-entered clinician in the
  # roster and confirm they contribute nothing until their entry year.
  a <- cmi_agents(20)
  a$age[1] <- 31                       # pre-certification age
  a$entry_year[1] <- 2030L             # certifies later
  set.seed(31)
  p <- cmi_run(a, 2025:2032, entrants = 0,
               retirement_schedule = cmi_frozen, career_change_hazard = 0)$panel
  expect_equal(p$headcount[1], 19L)                       # 2025: not yet in
  expect_equal(p$headcount[p$year == 2030L], 20L)         # 2030: in
  expect_true(all(diff(p$headcount) >= 0))                # no exits configured
})

# ---- 4. SEX / GENDER SUPPORT ------------------------------------------------

test_that("INV-4: both sexes are modelled and no path assumes all providers are women", {
  # 82% of entrants are female by default, which is not 100%. A module that
  # assumed all-female would produce identical output for an all-male cohort.
  set.seed(4)
  fem <- cmi_run(cmi_agents(40, female_share = 1), 2025:2032, entrants = 0,
                 retirement_schedule = cmi_frozen, career_change_hazard = 0)$panel
  set.seed(4)
  mal <- cmi_run(cmi_agents(40, female_share = 0), 2025:2032, entrants = 0,
                 retirement_schedule = cmi_frozen, career_change_hazard = 0)$panel
  expect_equal(fem$headcount, mal$headcount)              # headcount is sex-blind
  expect_false(isTRUE(all.equal(fem$effective_fte, mal$effective_fte)))

  # The hours schedule and the retirement multiplier both distinguish sex.
  expect_false(isTRUE(all.equal(
    unname(hwsm_reference_hours(50, "female", intercept = 40)),
    unname(hwsm_reference_hours(50, "male", intercept = 40)))))
  expect_false(isTRUE(all.equal(
    unname(departure_hazard(65, "female")), unname(departure_hazard(65, "male")))))

  # Entrants are a MIX, not a single sex.
  set.seed(44)
  sim <- cmi_run(cmi_agents(30), 2025:2040, entrants = 12)
  ent <- sim$agents[sim$agents$origin_cohort == "entrant", ]
  expect_true(all(c("female", "male") %in% ent$sex))
  expect_gt(mean(ent$sex == "female"), 0.6)
  expect_lt(mean(ent$sex == "female"), 0.95)
})

# ---- 5. STOCK-FLOW IDENTITY -------------------------------------------------

test_that("INV-5: stock[t] = stock[t-1] + entrants - exits, under several configurations", {
  # Cycle 04 pinned this for one configuration. Across configurations is the
  # cross-module form: entrants, hazards and the panel must agree whatever the
  # levers are.
  for (cfg in list(list(e = 0, h = 0), list(e = 6, h = 0),
                   list(e = 6, h = 0.05), list(e = 20, h = 0.15))) {
    set.seed(5)
    p <- cmi_run(cmi_agents(60), 2025:2033, entrants = cfg$e,
                 retirement_schedule = setNames(rep(cfg$h, 120), 1:120),
                 career_change_hazard = cfg$h)$panel
    exits <- diff(p$n_retired)
    expect_equal(diff(p$headcount), cfg$e - exits,
                 info = sprintf("entrants=%g hazard=%g", cfg$e, cfg$h))
    expect_true(all(exits >= 0),
                info = "retirement is absorbing; the retired count fell")
  }
})

# ---- 6. HEADCOUNT / FTE SEPARATION ------------------------------------------

test_that("INV-6: headcount and FTE are never substituted for one another", {
  set.seed(6)
  p <- cmi_run(cmi_agents(50), 2025:2035, entrants = 5)$panel
  expect_true(all(p$headcount == trunc(p$headcount)))      # a count
  expect_false(all(p$effective_fte == trunc(p$effective_fte)))  # not a count

  # An hours lever moves FTE and leaves headcount exactly alone.
  set.seed(6)
  q <- cmi_run(cmi_agents(50), 2025:2035, entrants = 5, hours_multiplier = 0.7)$panel
  expect_equal(q$headcount, p$headcount)
  expect_true(all(q$effective_fte < p$effective_fte))

  # And the base year ties FTE to headcount by construction, so the two are
  # comparable rather than interchangeable.
  expect_equal(p$effective_fte[1] / p$headcount[1], 1, tolerance = 1e-6)
})

# ---- 7. DEMAND / SUPPLY IDENTITY --------------------------------------------

test_that("INV-7: gap_fte is exactly supply minus demand, and scenarios cannot mix", {
  supply <- tibble::tibble(year = 2025:2030,
                           effective_fte_median = seq(900, 945, by = 9))
  required <- tibble::tibble(year = 2025:2030, required_fte = seq(1200, 1250, by = 10))
  g <- compute_fte_gap(supply, required)
  expect_equal(g$gap_fte, supply$effective_fte_median - required$required_fte)
  expect_equal(sign(g$gap_pct), sign(g$gap_fte))

  # A frame assembled from two different scenarios breaks the identity and is
  # refused -- the guard reports WHERE, per the cycle 22-23 rule.
  p <- data.frame(year = 2025:2027, scenario_id = "baseline", specialty = "FPMRS",
                  geography_type = "national", geography_id = "US",
                  supply_headcount = c(1000, 1010, 1020),
                  supply_clinical_fte = c(900, 909, 918),
                  supply_cohort_basis = "certification_cohorts",
                  demand_headcount = c(1300, 1310, 1320),
                  demand_clinical_fte = c(1200, 1210, 1220),
                  gap_fte = c(-300, -301, -302),
                  gap_headcount = c(-300, -300, -300), stringsAsFactors = FALSE)
  contaminated <- p
  contaminated$demand_clinical_fte[2] <- 1500        # from another scenario
  err <- tryCatch(suppressMessages(validate_urps_gap_projection(contaminated, mode = "strict")),
                  error = function(e) conditionMessage(e))
  expect_match(err, "does not equal")
  expect_match(err, "2026")                          # locates the row
  expect_false(grepl("because|probably|usual cause", err))   # states what, not why
})

# ---- 8. SCENARIO ISOLATION --------------------------------------------------

test_that("INV-8: one scenario cannot contaminate another", {
  # Mutable state, RNG reuse, a join or a cached object could all leak. The test
  # is order-independence: running a scenario grid in either order gives the
  # same answers, and running one alone matches running it in company.
  runs <- function(order) {
    out <- list()
    for (nm in order) {
      lever <- switch(nm, base = list(), more = list(entrants_per_year = 12),
                      hours = list(hours_multiplier = 0.8))
      a <- cmi_agents(40)
      args <- c(list(a, 2025:2032, 6,
                     hours_intercept = calibrate_hours_intercept(a$age, a$sex)), lever)
      set.seed(8)
      out[[nm]] <- do.call(simulate_provider_career_once, args)$panel$effective_fte
    }
    out
  }
  fwd <- runs(c("base", "more", "hours"))
  rev <- runs(c("hours", "more", "base"))
  for (nm in names(fwd)) expect_equal(fwd[[nm]], rev[[nm]], info = nm)

  # Alone matches in-company: no scenario's result depends on its neighbours.
  solo <- runs("more")
  expect_equal(solo$more, fwd$more)

  # And the levers genuinely differ, so the equality above is not trivial.
  expect_false(isTRUE(all.equal(fwd$base, fwd$more)))
  expect_false(isTRUE(all.equal(fwd$base, fwd$hours)))
})

# ---- 9. REPRODUCIBILITY -----------------------------------------------------

test_that("INV-9: identical seed and inputs give identical outputs; different seeds differ", {
  a <- cmi_agents(50)
  one <- function(s) { set.seed(s); cmi_run(a, 2025:2035, entrants = 7)$panel }
  expect_equal(one(99), one(99))
  expect_false(isTRUE(all.equal(one(99)$headcount, one(100)$headcount)))

  # Reproducibility survives an intervening seeded helper: nothing may reseed
  # the session out from under a run.
  set.seed(99); ref <- cmi_run(a, 2025:2035, entrants = 7)$panel$headcount
  set.seed(99)
  invisible(psa_sample(list(psa_uniform("x", 0, 1)), n = 8, seed = 5L))
  expect_equal(cmi_run(a, 2025:2035, entrants = 7)$panel$headcount, ref)
})

# ---- 10. BOUNDARY YEARS -----------------------------------------------------

test_that("INV-10: first year, last year, one-year horizons and gaps cannot go wrong", {
  a <- cmi_agents(30)
  # A one-year horizon: no transitions, so no entrants and no aging.
  one <- cmi_run(a, 2025L, entrants = 50)$panel
  expect_equal(nrow(one), 1L)
  expect_equal(one$headcount, 30L)

  # The first row is the base year, untouched by any transition.
  set.seed(10)
  p <- cmi_run(a, 2025:2030, entrants = 5)$panel
  expect_equal(p$year[1], 2025L)
  expect_equal(p$headcount[1], 30L)
  expect_equal(p$year[nrow(p)], 2030L)

  # A gapped horizon is refused rather than reported as though the missing years
  # had happened -- the descending-range and off-by-one family.
  expect_error(cmi_run(a, c(2025L, 2030L), entrants = 5), "CONSECUTIVE")
  expect_error(cmi_run(a, integer(0), entrants = 5), "non-empty")

  # Zero-length windows elsewhere in the model do not reverse.
  expect_equal(unname(retirement_survival(50, 50)), 1)
  expect_length(prevalence_from_incidence(0.02, remission = 0.01), 1L)
  expect_equal(unname(e2sfca_incremental_weights(c("30" = 1))), 1)
})
