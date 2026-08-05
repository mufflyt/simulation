# Semantic and adversarial tests for the demand stack.
#
# SEMANTIC: the results must be epidemiologically/arithmetically sensible, not
#   just structurally well-formed -- monotonicity, orderings, and the direction
#   of effects the model exists to represent.
# ADVERSARIAL: degenerate or hostile inputs (empty, all-NA, wrong type, extreme,
#   non-finite parameters) must error cleanly or degrade sanely, never silently
#   return a wrong number.
#
# Covers R/32 (geographic + tract-need bridge), R/58 (literature POP), R/31 (fit
# cores), R/29 (engine), and the DMDM contract exporter. Internal helpers are
# reached with urpssim:::.

PREV <- c("20-39" = .05, "40-59" = .20, "60-64" = .35, "65-79" = .45, "80+" = .50)
mk_tract <- function(p20, p40, p60, p65, p80, tmin = 60, acc = 1, cap = NA_real_)
  data.frame(GEOID = "x", female_20_39 = p20, female_40_59 = p40, female_60_64 = p60,
             female_65_79 = p65, female_80plus = p80,
             nearest_provider_min = tmin, access_ratio = acc, capacity = cap)

# ============================================================================
# SEMANTIC
# ============================================================================

test_that("tract need scales linearly with population and rises with an older age mix", {
  base <- tract_need_from_population(mk_tract(100, 100, 100, 100, 100), PREV)$need
  dbl  <- tract_need_from_population(mk_tract(200, 200, 200, 200, 200), PREV)$need
  expect_equal(dbl, 2 * base)                                   # linear in population
  # Same total population (1000), shifted old vs young -> more need when older,
  # because PFD prevalence rises with age.
  young <- tract_need_from_population(mk_tract(1000, 0, 0, 0, 0), PREV)$need
  old   <- tract_need_from_population(mk_tract(0, 0, 0, 0, 1000), PREV)$need
  expect_gt(old, young)
  expect_equal(old, 1000 * PREV[["80+"]])
})

test_that("travel-band need is cumulative and conserves total need", {
  geo <- data.frame(need = c(100, 200, 300, 400),
                    nearest_provider_min = c(15, 45, 90, 240))
  bb <- demand_by_travel_band(geo)
  expect_true(all(diff(bb$need_within) >= 0))                   # cumulative, monotone
  within_max <- bb$need_within[bb$threshold_min == max(bb$threshold_min)]
  beyond <- unname(attr(bb, "beyond")["need"])
  expect_equal(within_max + beyond, sum(geo$need))             # nothing lost
})

test_that("need-weighted access moves toward where the need actually is", {
  # Need concentrated in the low-access geography pulls the weighted mean below
  # the unweighted mean; concentrated in the high-access one pulls it above.
  low  <- data.frame(need = c(10, 1000), access_ratio = c(5, 0.2))
  high <- data.frame(need = c(1000, 10), access_ratio = c(5, 0.2))
  expect_lt(need_weighted_access(low),  mean(low$access_ratio))
  expect_gt(need_weighted_access(high), mean(high$access_ratio))
})

test_that("national adequacy is capacity/need and underserved share is a proportion", {
  g <- data.frame(need = c(100, 100), capacity = c(150, 50))
  anc <- accessible_need_vs_capacity(g)
  expect_equal(anc$national_adequacy, sum(g$capacity) / sum(g$need))
  expect_gte(anc$underserved_need_share, 0)
  expect_lte(anc$underserved_need_share, 1)
  expect_equal(anc$underserved_need_share, 0.5)                # only the cap<need tract
})

test_that("literature POP transitions carry the right epidemiological orderings", {
  tr <- dmdm_transitions_with_pop_literature()
  expect_gt(tr$onset$pop[["avag"]], 0)                         # vaginal delivery raises onset
  expect_gt(tr$onset$pop[["aage"]], 0)                         # age raises onset
  # progression and regression both slow at higher stages
  expect_true(tr$pop_progression[["1"]] > tr$pop_progression[["2"]] &&
              tr$pop_progression[["2"]] > tr$pop_progression[["3"]])
  expect_true(tr$pop_regression[["1"]] > tr$pop_regression[["2"]] &&
              tr$pop_regression[["2"]] > tr$pop_regression[["3"]])
  # mild POP regresses faster than it progresses -- the feature UI lacks
  expect_gt(tr$pop_regression[["1"]], tr$pop_progression[["1"]])
})

test_that("the any-PFD tier is a union: never below any single condition", {
  tj <- data.frame(year = 2025:2026, population = 45e6,
                   prev_ui = c(.2, .3), prev_pop = c(.1, .15), prev_ai = c(.05, .07))
  ex <- export_dmdm_demand_contract(tj, output_directory = tempfile("sem_"),
                                    verbose = FALSE, allow_uncalibrated = TRUE)$data
  t3 <- ex$prevalence[ex$denominator_tier == "tier3_prevalent_pfd"]
  for (cc in c("dmdm_ui", "dmdm_pop", "dmdm_ai"))
    expect_true(all(t3 >= ex$prevalence[ex$denominator_tier == cc]))
})

test_that("onset and stage fitters recover the direction and magnitude of a known effect", {
  set.seed(1); N <- 12000; vag <- rpois(N, 2); age <- sample(40:80, N, TRUE)
  cov <- function(ev) data.frame(from = 0L, event = ev, age = age,
    cumulative_vaginal_deliveries = vag, years_since_last_vaginal_birth = 0,
    bmi = 28, hysterectomy = 0, menopause_status = 0, comorbidity = 0)
  pos  <- urpssim:::.fit_onset_coefs(cov(rbinom(N, 1, plogis(-3 + 0.40 * vag))))
  null <- urpssim:::.fit_onset_coefs(cov(rbinom(N, 1, 0.10)))
  expect_gt(pos[["avag"]], 0.2)                                # real effect recovered
  expect_lt(abs(null[["avag"]]), 0.1)                         # no effect -> ~0
  # per-stage progression fit is monotone in the true rate
  fit_rate <- function(p) { set.seed(2); n <- 20000
    to <- ifelse(stats::runif(n) < p, 2L, 1L)
    urpssim:::.fit_stage_transitions(
      data.frame(from_stage = rep(1L, n), to_stage = to))$progression[["1"]] }
  expect_lt(fit_rate(0.05), fit_rate(0.20))
})

# ============================================================================
# ADVERSARIAL
# ============================================================================

test_that("tract_need_from_population handles degenerate inputs", {
  # zero rows -> zero-row result, still carrying the need column
  z <- tract_need_from_population(mk_tract(1, 1, 1, 1, 1)[0, ], PREV)
  expect_equal(nrow(z), 0L)
  expect_true("need" %in% names(z))
  # NA population is treated as zero, not propagated
  expect_equal(tract_need_from_population(mk_tract(NA, NA, NA, NA, NA), PREV)$need, 0)
  # prevalence bands beyond the mapped set are ignored
  expect_equal(
    tract_need_from_population(mk_tract(100, 100, 100, 100, 100), c(PREV, "999" = 0.9))$need,
    tract_need_from_population(mk_tract(100, 100, 100, 100, 100), PREV)$need)
})

test_that("tract_need_from_population refuses malformed columns and non-finite prevalence", {
  ch <- mk_tract(1, 1, 1, 1, 1); ch$female_20_39 <- "x"      # character population
  expect_error(tract_need_from_population(ch, PREV))
  expect_error(tract_need_from_population(mk_tract(1, 1, 1, 1, 1), PREV[1:3]),
               "no value for band")                          # missing band
  bad <- PREV; bad[["40-59"]] <- NA_real_                     # NA would poison every tract
  expect_error(tract_need_from_population(mk_tract(10, 10, 10, 10, 10), bad),
               "non-finite prevalence")
})

test_that("travel-band and access measures survive NA and all-zero inputs", {
  # NA travel time is excluded from every band rather than counted or erroring
  geo <- data.frame(need = c(100, 200), nearest_provider_min = c(15, NA))
  bb <- demand_by_travel_band(geo, bands = c(120, 30, 60))     # unsorted on purpose
  expect_equal(bb$threshold_min, c(30, 60, 120))              # sorted
  expect_equal(bb$need_within[bb$threshold_min == 30], 100)   # NA-travel tract excluded
  # all-zero need -> NA weighted access and NA adequacy, never NaN/Inf
  expect_true(is.na(need_weighted_access(data.frame(need = c(0, 0), access_ratio = c(1, 2)))))
  a <- accessible_need_vs_capacity(data.frame(need = c(0, 0), capacity = c(1, 2)))
  expect_true(is.na(a$national_adequacy))
})

test_that("POP onset compiler rejects an out-of-range baseline probability", {
  bad <- pop_transition_parameters()
  bad$value[bad$transition == "onset" & bad$term == "baseline_annual"] <- 0
  expect_error(urpssim:::.pop_onset_coefs(bad), "probability in \\(0, 1\\)")
})

test_that("fit cores fall back cleanly on empty / no-variation data", {
  empty <- data.frame(from = integer(0), event = integer(0), age = numeric(0),
    cumulative_vaginal_deliveries = numeric(0), years_since_last_vaginal_birth = numeric(0),
    bmi = numeric(0), hysterectomy = numeric(0), menopause_status = numeric(0),
    comorbidity = numeric(0))
  e <- suppressWarnings(urpssim:::.fit_onset_coefs(empty))
  expect_equal(unname(e[["a0"]]), -6)                         # intercept-only fallback
  expect_true(all(e[setdiff(names(e), "a0")] == 0))           # every slope 0
  st <- urpssim:::.fit_stage_transitions(data.frame(from_stage = integer(0), to_stage = integer(0)))
  expect_length(st$progression, 0)
  expect_true(is.na(st$max_stage))
})

test_that("the engine keeps prevalence in [0,1] under an extreme onset rate", {
  tr <- dmdm_default_transitions(); tr$onset$ui["a0"] <- 20    # onset probability ~1
  co <- data.frame(age = 50:60, cumulative_vaginal_deliveries = 2L,
                   years_since_last_vaginal_birth = 0, bmi = 28, hysterectomy = 0,
                   menopause_status = 1L, comorbidity = 0)
  out <- suppressMessages(simulate_dmdm(co, 2025, 2030, transitions = tr, seed = 1,
                                        allow_uncalibrated = TRUE))
  expect_true(all(out$prev_ui >= 0 & out$prev_ui <= 1))
})
