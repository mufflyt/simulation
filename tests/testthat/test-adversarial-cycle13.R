# Adversarial cycle 13 -- logic written twice.
#
# Cycle 12 carried forward: guards written twice. `.Random.seed` restoration
# existed as two hand-rolled copies plus a shared helper, and the copies were
# the ones with the hole. That is a shape, not an incident, so this cycle went
# looking for the rest of it.
#
# Swept R/ for character vectors written verbatim in more than one file. Nine
# came back. Most are harmless coincidences of vocabulary ("ui","pop","ai"), but
# three were the same list or the same rule maintained in two places:
#
#   DEMAND_AGE_BANDS          package constant, plus a LOCAL SHADOW of the same
#                             five labels inside brfss_pfd_prevalence_for_demand_bands()
#   MICROSIM_AGE_BAND_LABELS  package constant, plus seven hardcoded labels in
#                             urps_partial_pooled_hazards()
#   gap identity tolerance    0.01 written into validate_urps_gap_projection()
#                             AND into validation_report()
#
# None had diverged yet. That is exactly when to fix them: a test that pins two
# copies as equal is weaker than one copy.
#
# Mix: 4 boundary-value, 3 semantic/contract, 3 adversarial.

# ---- BVA 1 ------------------------------------------------------------------

test_that("BVA: the gap identity tolerance is one constant, and is closed at it", {
  # A residual exactly AT the tolerance is acceptable; anything above is not.
  # Both copies of this check used a literal 0.01, so "the boundary" was two
  # boundaries that happened to coincide.
  expect_equal(GAP_IDENTITY_TOLERANCE_FTE, 0.01)

  mk <- function(residual) {
    data.frame(year = 2025L, scenario_id = "baseline", specialty = "FPMRS",
               geography_type = "national", geography_id = "US",
               supply_headcount = 1000, supply_clinical_fte = 900,
               supply_cohort_basis = "certification_cohorts",
               demand_headcount = 1300, demand_clinical_fte = 1200,
               gap_fte = -300 + residual, gap_headcount = -300,
               stringsAsFactors = FALSE)
  }
  expect_silent(suppressMessages(
    validate_urps_gap_projection(mk(GAP_IDENTITY_TOLERANCE_FTE), mode = "strict")))
  expect_error(suppressMessages(
    validate_urps_gap_projection(mk(GAP_IDENTITY_TOLERANCE_FTE * 1.001), mode = "strict")),
    "does not equal")
  # Symmetric: the residual is an absolute value, so the sign cannot smuggle one
  # side past the guard.
  expect_error(suppressMessages(
    validate_urps_gap_projection(mk(-GAP_IDENTITY_TOLERANCE_FTE * 1.001), mode = "strict")),
    "does not equal")
})

# ---- BVA 2 ------------------------------------------------------------------

test_that("BVA: the demand age bands are one vocabulary, exactly five labels wide", {
  expect_length(DEMAND_AGE_BANDS, 5L)
  expect_equal(DEMAND_AGE_BANDS, c("20-39", "40-59", "60-64", "65-79", "80+"))
  expect_false(anyDuplicated(DEMAND_AGE_BANDS) > 0L)

  # The bands must tile the adult lifespan without a gap: each label's lower
  # bound is the previous label's upper bound plus one.
  lo <- as.integer(sub("([0-9]+).*", "\\1", DEMAND_AGE_BANDS))
  hi <- as.integer(ifelse(grepl("\\+$", DEMAND_AGE_BANDS), NA,
                          sub(".*-([0-9]+)$", "\\1", DEMAND_AGE_BANDS)))
  expect_false(is.unsorted(lo))
  expect_equal(lo[-1], utils::head(hi, -1) + 1L)
  expect_true(is.na(utils::tail(hi, 1)))          # the last band is open-ended
})

# ---- BVA 3 ------------------------------------------------------------------

test_that("BVA: the microsim age bands and their labels stay one label short of the breaks", {
  # cut() with n breaks yields n-1 intervals. An off-by-one here silently shifts
  # every provider one band, which changes the retirement hazard they draw.
  expect_equal(length(MICROSIM_AGE_BANDS), length(MICROSIM_AGE_BAND_LABELS) + 1L)
  expect_false(is.unsorted(MICROSIM_AGE_BANDS))
  expect_equal(MICROSIM_AGE_BANDS[1], 0)
  expect_true(is.infinite(utils::tail(MICROSIM_AGE_BANDS, 1)))

  # Boundary ages land in the band the labels claim: breaks are right-open.
  expect_equal(as.character(microsim_age_band_of(44.999)), "<45")
  expect_equal(as.character(microsim_age_band_of(45)), "45-49")
  expect_equal(as.character(microsim_age_band_of(49.999)), "45-49")
  expect_equal(as.character(microsim_age_band_of(70)), "70+")
  expect_equal(as.character(microsim_age_band_of(120)), "70+")
})

# ---- BVA 4 ------------------------------------------------------------------

test_that("BVA: the calibration rank vocabulary is closed and ordered", {
  # The rank decides what is reportable. Two copies of it exist by design (a
  # local status_rank in the export contract), and a test already greps both;
  # this pins the properties the ranking itself must have.
  r <- CALIBRATION_STATUS_RANK
  expect_true(all(r >= 1L))
  expect_false(is.unsorted(sort(r)))
  expect_true(REPORTABLE_MIN_CALIBRATION %in% names(r))
  # Every tier below the reportable minimum must rank strictly below it.
  cutoff <- r[[REPORTABLE_MIN_CALIBRATION]]
  expect_true(any(r < cutoff))
  expect_true(any(r >= cutoff))
  # An unrecognised status must not silently rank as anything. Single-bracket:
  # `[[` on a missing name is an error, which is a different (also acceptable)
  # failure mode and not the one being asserted here.
  expect_true(is.na(r["not_a_tier"]))
  expect_true(is.na(unname(r["not_a_tier"])))
})

# ---- SEMANTIC 1 -------------------------------------------------------------

test_that("SEMANTIC: the BRFSS band crosswalk uses the package bands, not a private copy", {
  # THE DEFECT. brfss_pfd_prevalence_for_demand_bands() defined its own
  # DEMAND_AGE_BANDS, shadowing the package constant. They agreed, so nothing
  # was wrong yet -- and the copy would have gone stale silently the moment the
  # real one changed, while compute_brfss_demand_estimand()'s "unknown age band"
  # guard went on comparing against bands nobody else used.
  cells <- data.frame(
    age_group = rep(URPS_POP_AGE_BANDS, each = 2),
    pop_weight = rep(c(6e6, 2e6), times = length(URPS_POP_AGE_BANDS)),
    ui_prevalence = rep(c(0.12, 0.24, 0.35, 0.44, 0.51), each = 2),
    pop_prevalence = 0.1, fi_prevalence = 0.05,
    stringsAsFactors = FALSE)
  out <- brfss_pfd_prevalence_for_demand_bands(cells, condition = "ui")
  expect_named(out, DEMAND_AGE_BANDS)
  expect_length(out, length(DEMAND_AGE_BANDS))
  expect_true(all(out >= 0 & out <= 1))
})

# ---- SEMANTIC 2 -------------------------------------------------------------

test_that("SEMANTIC: the pooled hazard keys to the same bands the microsimulation ages through", {
  # urps_partial_pooled_hazards() hardcoded the seven labels. The pooled hazard
  # is looked up by the band a provider is IN, so the two vocabularies have to
  # be the same object, not two lists that currently match.
  expect_equal(MICROSIM_AGE_BAND_LABELS,
               c("<45", "45-49", "50-54", "55-59", "60-64", "65-69", "70+"))
  # Every age the microsimulation can produce maps into that set.
  ages <- c(25, 34, 44, 45, 55, 64, 65, 69, 70, 85, 100)
  expect_true(all(as.character(microsim_age_band_of(ages)) %in% MICROSIM_AGE_BAND_LABELS))
  # And the mapping is total: no age falls outside a band.
  expect_false(any(is.na(microsim_age_band_of(ages))))
})

# ---- SEMANTIC 3 -------------------------------------------------------------

test_that("SEMANTIC: both copies of the gap check agree on the same frame", {
  # The real property: two implementations of one rule must return the same
  # verdict. validate_urps_gap_projection() and validation_report() each check
  # gap = supply - demand, and they now read one tolerance.
  ok <- data.frame(
    year = 2025:2027, scenario_id = "baseline", specialty = "FPMRS",
    geography_type = "national", geography_id = "US",
    supply_headcount = c(1000, 1010, 1020), supply_clinical_fte = c(900, 909, 918),
    supply_cohort_basis = "certification_cohorts",
    demand_headcount = c(1300, 1310, 1320), demand_clinical_fte = c(1200, 1210, 1220),
    gap_fte = c(-300, -301, -302), gap_headcount = c(-300, -300, -300),
    stringsAsFactors = FALSE)
  broken <- ok
  broken$gap_fte[2] <- broken$gap_fte[2] + 5

  expect_silent(suppressMessages(validate_urps_gap_projection(ok, mode = "strict")))
  expect_error(suppressMessages(validate_urps_gap_projection(broken, mode = "strict")),
               "does not equal")

  supply <- tibble::tibble(year = 2025:2027, effective_fte_median = c(900, 909, 918))
  rep_ok <- suppressMessages(validation_report(supply, gap_projection = ok))
  rep_bad <- suppressMessages(validation_report(supply, gap_projection = broken))
  arith <- function(r) r$passed[r$check == "gap_projection_arithmetic"]
  expect_true(arith(rep_ok))
  expect_false(arith(rep_bad))
})

# ---- ADVERSARIAL 1 ----------------------------------------------------------

test_that("ADVERSARIAL: no duplicated definition of a shared vocabulary survives in R/", {
  # The sweep, as a standing gate. Each of these labels sets is now defined
  # exactly once; a future copy-paste re-introduces the drift this cycle removed.
  root <- Filter(function(p) file.exists(file.path(p, "DESCRIPTION")),
                 c(".", "..", file.path("..", "..")))
  skip_if(length(root) == 0L, "package root not reachable")
  src <- unlist(lapply(list.files(file.path(root[1], "R"), "[.]R$", full.names = TRUE),
                       function(f) sub("#.*$", "", readLines(f, warn = FALSE))))
  count_literal <- function(lit) sum(grepl(lit, src, fixed = TRUE))

  # The demand bands appear as a literal only where the constant is DEFINED
  # (plus its use as a default in one table), never as a second assignment.
  expect_equal(sum(grepl("^\\s*DEMAND_AGE_BANDS\\s*<-", src)), 1L)
  expect_equal(sum(grepl("^\\s*MICROSIM_AGE_BAND_LABELS\\s*<-", src)), 1L)
  expect_equal(sum(grepl("^\\s*GAP_IDENTITY_TOLERANCE_FTE\\s*<-", src)), 1L)
  # And the seven microsim labels are not written out anywhere else.
  expect_equal(count_literal('c("<45", "45-49", "50-54", "55-59", "60-64", "65-69", "70+")'), 1L)
})

# ---- ADVERSARIAL 2 ----------------------------------------------------------

test_that("ADVERSARIAL: every consumer reads the shared constant rather than a private copy", {
  # The point of one definition, tested by identity rather than by rebinding:
  # package constants are LOCKED bindings, so a rebinding test cannot run and a
  # skip would prove nothing. Instead assert that what each consumer actually
  # returns is keyed to the shared object.
  cells <- data.frame(
    age_group = rep(URPS_POP_AGE_BANDS, each = 2),
    pop_weight = rep(c(6e6, 2e6), times = length(URPS_POP_AGE_BANDS)),
    ui_prevalence = rep(c(0.12, 0.24, 0.35, 0.44, 0.51), each = 2),
    pop_prevalence = 0.1, fi_prevalence = 0.05, stringsAsFactors = FALSE)

  # The BRFSS crosswalk's output names ARE the shared bands, in the shared order.
  expect_identical(names(brfss_pfd_prevalence_for_demand_bands(cells, condition = "ui")),
                   DEMAND_AGE_BANDS)
  # The demand-denominator rate tables are keyed to them too, so a band added to
  # the constant without a rate would be caught rather than contribute zero.
  expect_identical(names(CONSULT_RATE_BY_AGE), DEMAND_AGE_BANDS)
  expect_identical(names(WU2011_SURGERY_RATE_PER_1000), DEMAND_AGE_BANDS)
  expect_setequal(names(pfd_prevalence_by_band()), DEMAND_AGE_BANDS)

  # And the microsim labels are what the band mapper actually emits, over the
  # whole age range the engine can produce.
  expect_setequal(unique(as.character(microsim_age_band_of(seq(18, 100, by = 1)))),
                  MICROSIM_AGE_BAND_LABELS)
})

# ---- ADVERSARIAL 3 ----------------------------------------------------------

test_that("ADVERSARIAL: the two age-band vocabularies are deliberately different and cannot be swapped", {
  # DEMAND_AGE_BANDS (5 labels, demand side) and URPS_POP_AGE_BANDS (5 labels,
  # population side) are BOTH five long and neither is the other. Deduplicating
  # by counting labels would have merged them. The crosswalk between them exists
  # for exactly this reason.
  expect_length(DEMAND_AGE_BANDS, 5L)
  expect_length(URPS_POP_AGE_BANDS, 5L)
  expect_false(identical(DEMAND_AGE_BANDS, URPS_POP_AGE_BANDS))
  expect_equal(length(intersect(DEMAND_AGE_BANDS, URPS_POP_AGE_BANDS)), 0L)

  # A population band passed where a demand band is expected must be refused,
  # not silently contribute zero demand.
  pop <- data.frame(year = rep(2025L, length(URPS_POP_AGE_BANDS)),
                    age_band = URPS_POP_AGE_BANDS,
                    female_pop = rep(1e6, length(URPS_POP_AGE_BANDS)),
                    stringsAsFactors = FALSE)
  expect_error(compute_demand_denominators(pop), "unknown age band")
})
