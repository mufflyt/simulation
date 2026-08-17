# Anchoring the life-course disease model to published symptomatic prevalence.
#
# The model produced POP prevalence 5.6x above published symptomatic values
# (population-weighted 24.3% vs 4.4%), and 59.7% among women 75+ -- closer to
# exam-detected POP-Q stage >=2, which is largely asymptomatic, than to a bulge
# a woman reports. Because treated = prevalence x cascade, that error passed
# straight into procedure volume.

.cal_cohort <- function(n = 20000L) {
  set.seed(20260817)
  .lifecourse_population(data.frame(age = 18:90, population = 1e5),
                         year = 2025L, n = n)
}

test_that("targets are SYMPTOMATIC prevalence, and POP is the small one", {
  # The discriminating fact: symptomatic POP is a few percent. If these ever
  # rise toward 40% someone has substituted exam-detected POP-Q.
  pop <- lifecourse_prevalence_targets("pop")
  expect_true(all(pop <= 0.08))
  expect_lt(pop[["18-34"]], pop[["75+"]])
  ui <- lifecourse_prevalence_targets("ui")
  expect_true(all(ui > lifecourse_prevalence_targets("pop")))
})

test_that("calibration adjusts ONLY the placeholder coefficients", {
  # bvag and bbmi carry citations (Hendrix WHI / Mant Oxford-FPA; Giri 2017)
  # and the scenario levers act through them. Replacing the risk model with an
  # age-band lookup would match prevalence and destroy every scenario.
  before <- lifecourse_risk_params()
  after <- calibrate_lifecourse_prevalence(.cal_cohort())$risk_params
  for (cond in c("ui", "pop", "ai")) {
    for (keep in c("bvag", "bysl", "bbmi", "bhyst", "bmeno", "bcomorb")) {
      expect_equal(after[[cond]][[keep]], before[[cond]][[keep]],
                   info = paste(cond, keep, "must not move"))
    }
    expect_false(isTRUE(all.equal(after[[cond]]$b0, before[[cond]]$b0)))
  }
})

test_that("POP prevalence comes down by roughly the observed factor", {
  # `lifecourse_risk_params()` is NOW the anchored set, so the before/after
  # comparison must start from the SUPERSEDED placeholders, which is exactly
  # why they were kept reachable rather than deleted.
  ch <- .cal_cohort()
  before <- lifecourse_risk_params_placeholder()
  after <- calibrate_lifecourse_prevalence(ch, risk_params = before)$risk_params
  lp <- function(p) stats::plogis(
    p$b0 + p$bvag * ch$cumulative_vaginal_deliveries +
      p$bage * ((ch$age - 50) / 10) +
      p$bysl * (ch$years_since_last_vaginal_birth / 10) +
      p$bbmi * ((ch$bmi - 27) / 5) + p$bhyst * ch$hysterectomy +
      p$bmeno * ch$menopause_status + p$bcomorb * ch$comorbidity)
  expect_gt(mean(lp(before$pop)) / mean(lp(after$pop)), 3)
})

test_that("the fit REPORTS its residuals rather than hiding them", {
  # Two free parameters against five targets cannot absorb an arbitrary shape.
  # A poor match means the covariate structure is wrong, and that must stay
  # visible -- the current fit runs roughly 0.5x-1.3x by band.
  f <- calibrate_lifecourse_prevalence(.cal_cohort())$fit
  expect_true(all(c("target", "achieved", "ratio") %in% names(f)))
  expect_equal(nrow(f), 15L)
  expect_true(all(f$ratio > 0.3 & f$ratio < 3),
              info = "a ratio outside 0.3-3 means the fit failed, not merely fitted imperfectly")
})

test_that("the anchored parameters are deterministic", {
  a <- lifecourse_risk_params_prevalence_anchored(n = 8000L)
  b <- lifecourse_risk_params_prevalence_anchored(n = 8000L)
  expect_equal(a$pop$b0, b$pop$b0)
  expect_equal(a$pop$bage, b$pop$bage)
})

test_that("HOLDOUT: the procedure anchor was never used in the calibration", {
  # THE VALIDATION DESIGN. Prevalence is fitted to Nygaard/Whitehead ONLY.
  # The prolapse_procedure anchor is not read, so agreement with it afterwards
  # is evidence rather than circularity. Pinned as source, because the failure
  # would be someone adding the anchor as a target.
  root <- .source_tree_root()
  skip_if(length(root) == 0, "repository sources not present")
  src <- readLines(file.path(root[1], "R", "demand-prevalence_calibration.R"),
                   warn = FALSE)
  # CODE lines only. The header comment discusses the anchor deliberately --
  # explaining why it is excluded is the opposite of using it -- so a check
  # that scanned prose would fire on its own documentation.
  code <- src[!grepl("^\\s*#", src)]
  expect_false(any(grepl("prolapse_procedure_volume|data/anchors|read\\.csv", code)),
               info = "the calibration must not READ any utilization anchor")
})

test_that("scenario levers still work after anchoring", {
  # The reason cited coefficients were preserved. If the delivery-mode lever
  # stops moving demand, the anchoring destroyed the model's purpose.
  pa <- data.frame(age = 18:90, population = 1e5)
  rp <- lifecourse_risk_params_prevalence_anchored(n = 8000L)
  pw <- condition_service_pathway()
  pw$per_entering[pw$service == "new_consultation"] <- 0.25
  base <- simulate_lifecourse_demand(pa, 2025L, n = 6000, seed = 1,
                                     risk_params = rp, pathway = pw)
  csec <- simulate_lifecourse_demand(pa, 2025L, n = 6000, seed = 1,
                                     risk_params = rp, pathway = pw,
                                     cesarean_rate = 0.90)
  bv <- mean(base$person_years$vaginal_births)
  cv <- mean(csec$person_years$vaginal_births)
  expect_lt(cv, bv)
})
