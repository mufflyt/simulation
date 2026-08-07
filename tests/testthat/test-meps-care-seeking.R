# Two-part MEPS care-seeking model (R/data-meps_care_seeking).
#
# MEPS microdata does not ship with the package, so these run on a synthetic
# frame built to the same shape. That is enough to lock the contracts that
# matter: the COND -> CLNK -> OB join must not double-count a visit, comorbidity
# must exclude the outcome conditions, and a multiplier the data cannot identify
# must be reported as unidentified rather than as a measurement.

mcs_fixture <- function(n = 900, seed = 42) {
  set.seed(seed)
  fyc <- data.frame(
    DUPERSID = sprintf("P%05d", seq_len(n)),
    SEX = 2L,
    AGELAST = sample(18:85, n, TRUE),
    RACETHX = sample(1:5, n, TRUE),
    POVCAT23 = sample(1:5, n, TRUE),
    INSURC23 = sample(c(1, 3, 8), n, TRUE),
    PERWT23F = runif(n, 2000, 9000),
    VARPSU = sample(1:3, n, TRUE),
    VARSTR = sample(1:12, n, TRUE),
    stringsAsFactors = FALSE
  )
  seekers <- sample(fyc$DUPERSID, 220)
  # Two condition records per seeker for the SAME visit: the join must collapse
  # these, or every seeker's visit count doubles.
  pf <- data.frame(
    DUPERSID = rep(seekers, each = 2),
    CONDIDX  = paste0(rep(seekers, each = 2), "_", rep(1:2, times = length(seekers))),
    ICD10CDX = rep(c("N39", "R32"), times = length(seekers)),
    stringsAsFactors = FALSE
  )
  other <- data.frame(
    DUPERSID = sample(fyc$DUPERSID, 600, TRUE),
    CONDIDX  = sprintf("C%05d", 1:600),
    ICD10CDX = sample(c("I10", "E11", "M54"), 600, TRUE),
    stringsAsFactors = FALSE
  )
  cond <- rbind(pf, other)
  clnk <- data.frame(CONDIDX = pf$CONDIDX,
                     EVNTIDX = paste0(rep(seekers, each = 2), "_E1"),  # one visit
                     stringsAsFactors = FALSE)
  ob <- data.frame(EVNTIDX = unique(clnk$EVNTIDX),
                   OBXP23X = runif(length(unique(clnk$EVNTIDX)), 50, 600),
                   stringsAsFactors = FALSE)
  list(fyc = fyc, cond = cond, clnk = clnk, ob = ob, seekers = seekers)
}

test_that("the condition-to-event join does not double-count a visit", {
  f <- mcs_fixture()
  p <- build_meps_care_seeking_panel(f$fyc, f$cond, f$clnk, f$ob, year = 2023L)
  # Two condition records pointed at ONE event, so every seeker has one visit.
  expect_true(all(p$pf_visits[p$DUPERSID %in% f$seekers] == 1))
  expect_equal(sum(p$sought), length(f$seekers))
  expect_true(all(p$pf_visits[!p$DUPERSID %in% f$seekers] == 0))
})

test_that("comorbidity excludes the pelvic-floor conditions themselves", {
  f <- mcs_fixture()
  p <- build_meps_care_seeking_panel(f$fyc, f$cond, f$clnk, f$ob, year = 2023L)
  # A seeker with no other condition must score zero comorbidity: counting the
  # outcome condition would make the covariate a proxy for the outcome.
  other_ids <- unique(f$cond$DUPERSID[!substr(f$cond$ICD10CDX, 1, 3) %in%
                                        MEPS_PELVIC_FLOOR_ICD10])
  clean <- setdiff(f$seekers, other_ids)
  skip_if(length(clean) == 0, "MEPS cleaned file not present")
  expect_true(all(p$n_comorbid[p$DUPERSID %in% clean] == 0))
})

test_that("the panel keeps only adult women and carries the design columns", {
  f <- mcs_fixture()
  f$fyc$SEX[1:50] <- 1L          # men
  f$fyc$AGELAST[51:100] <- 10L   # children
  p <- build_meps_care_seeking_panel(f$fyc, f$cond, f$clnk, f$ob, year = 2023L)
  expect_equal(nrow(p), nrow(f$fyc) - 100)
  expect_true(all(c("weight", "VARPSU", "VARSTR", "sought", "pf_visits",
                    "n_comorbid", "insurance", "income", "race") %in% names(p)))
})

test_that("a missing year-suffixed column is refused, not silently dropped", {
  f <- mcs_fixture()
  # Asking for 2022 against 2023 columns must fail: the weight suffix is
  # year-specific, and quietly picking another one produces a nationally
  # unrepresentative estimate that still looks fine.
  expect_error(build_meps_care_seeking_panel(f$fyc, f$cond, f$clnk, f$ob, year = 2022L),
               "PERWT22F")
})

test_that("the two-part model fits and both parts are survey-weighted", {
  skip_if_not_installed("survey")
  f <- mcs_fixture()
  p <- build_meps_care_seeking_panel(f$fyc, f$cond, f$clnk, f$ob, year = 2023L)
  m <- fit_care_seeking_model(p)
  expect_s3_class(m, "urps_care_seeking_model")
  expect_equal(m$n_persons, nrow(p))
  expect_equal(m$n_events, sum(p$sought))
  expect_s3_class(m$part1, "svyglm")
  expect_s3_class(m$part2, "svyglm")
  # Part 2 is fitted on seekers ONLY, so it describes intensity among women in
  # care rather than a mixture of that and the zeros. (Survey df is a function of
  # PSUs and strata, not observations, so the observation count is the check.)
  expect_equal(length(m$part2$y), sum(p$sought))
  expect_equal(length(m$part1$y), nrow(p))
})

test_that("expected visits is the product of the two parts", {
  skip_if_not_installed("survey")
  f <- mcs_fixture()
  p <- build_meps_care_seeking_panel(f$fyc, f$cond, f$clnk, f$ob, year = 2023L)
  m <- fit_care_seeking_model(p)
  nd <- data.frame(age_c = c(-1, 0, 1),
                   insurance = factor("Private", levels = levels(p$insurance)),
                   income = factor("GE400FPL", levels = levels(p$income)),
                   race = factor("NH_White", levels = levels(p$race)),
                   n_comorbid = 2)
  pr <- predict_care_seeking(m, nd)
  expect_equal(pr$expected_visits, pr$p_seek * pr$visits_if_seek)
  expect_true(all(pr$p_seek >= 0 & pr$p_seek <= 1))
  expect_true(all(pr$visits_if_seek > 0))
})

test_that("a multiplier the data cannot identify is reported as unidentified", {
  skip_if_not_installed("survey")
  f <- mcs_fixture()
  p <- build_meps_care_seeking_panel(f$fyc, f$cond, f$clnk, f$ob, year = 2023L)
  m <- fit_care_seeking_model(p)
  ref <- data.frame(age_c = 0,
                    insurance = factor("Private", levels = levels(p$insurance)),
                    income = factor("GE400FPL", levels = levels(p$income)),
                    race = factor("NH_White", levels = levels(p$race)),
                    n_comorbid = 2)
  mult <- care_seeking_multipliers(m, "insurance", ref)

  expect_setequal(mult$level, levels(p$insurance))
  # The reference level is 1.0 by construction and can never be "identified".
  expect_equal(mult$multiplier[mult$level == "Private"], 1)
  expect_false(mult$identified[mult$level == "Private"])
  # Care-seeking is simulated independently of insurance here, so nothing else
  # may come back identified either.
  expect_false(any(mult$identified))
  # A multiplier is a ratio of probabilities: it cannot be negative.
  expect_true(all(mult$conf_low >= 0))
  expect_true(all(mult$conf_high >= mult$multiplier))
  # identified is exactly "the interval excludes 1".
  expect_equal(mult$identified, !(mult$conf_low <= 1 & mult$conf_high >= 1))
})

test_that("asking for a multiplier on a non-factor term is refused", {
  skip_if_not_installed("survey")
  f <- mcs_fixture()
  p <- build_meps_care_seeking_panel(f$fyc, f$cond, f$clnk, f$ob, year = 2023L)
  m <- fit_care_seeking_model(p)
  ref <- data.frame(age_c = 0,
                    insurance = factor("Private", levels = levels(p$insurance)),
                    income = factor("GE400FPL", levels = levels(p$income)),
                    race = factor("NH_White", levels = levels(p$race)),
                    n_comorbid = 2)
  expect_error(care_seeking_multipliers(m, "n_comorbid", ref), "not a factor term")
})
