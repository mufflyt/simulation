# The SWAN -> DMDM panel bridge (R/data-swan_dmdm_panel.R).
#
# Every fixture is built in memory, so this file needs no SIMULATION_DATA_ROOT
# and no 2.6 GB of SWAN on disk. The traps encoded here are the ones the real
# data actually contains: the UI gate renames at visits 7-9, hysterectomy is
# unsuffixed at visit 0, menopause "unknown" codes must not read as pre-
# menopausal, and the primary exposure is a proxy that has to stay declared.

mk_swan_wide <- function(n = 6) {
  ids <- seq_len(n)
  yesno <- function(x) ifelse(x == 1L, "(2) Yes", "(1) No")
  d <- data.frame(SWANID = ids, stringsAsFactors = FALSE)
  d$NUMCHILD <- c("(0) No children", "(1) 1 child", "(2) 2 children",
                  "(3) 3 children", "(4) 4 children", "(5) 5 or more children")[seq_len(n)]
  for (item in c("HIGH_BP", "DIABETE", "HEART", "ARTHRIT", "OSTEOPR", "CANCER")) {
    d[[item]] <- yesno(rep(0L, n))
  }
  d$HIGH_BP <- yesno(c(1L, rep(0L, n - 1L)))
  d$HYSTERE <- yesno(rep(0L, n))                     # visit 0: unsuffixed
  for (v in 0:10) {
    d[[paste0("AGE", v)]]    <- 45 + v
    d[[paste0("BMI", v)]]    <- 27 + (v / 10)
    d[[paste0("STATUS", v)]] <- rep("(5) Pre-menopausal", n)
    if (v > 0) d[[paste0("HYSTERE", v)]] <- yesno(rep(0L, n))
    gate <- if (v %in% 7:9) paste0("LEKINVO", v) else paste0("INVOLEA", v)
    d[[gate]] <- yesno(rep(c(0L, 1L), length.out = n))
    if (v >= 2) d[[paste0("PROLAPS", v)]] <- yesno(rep(0L, n))
  }
  d
}

test_that("the panel carries exactly the columns dmdm_transition_data requires", {
  p <- build_swan_dmdm_panel(mk_swan_wide(), verbose = FALSE)
  need <- c("person_id", "year", "age", "cumulative_vaginal_deliveries",
            "years_since_last_vaginal_birth", "bmi", "hysterectomy",
            "menopause_status", "comorbidity", "has_ui")
  expect_true(all(need %in% names(p)))
  # The contract is satisfied end to end, not just column-wise.
  expect_s3_class(dmdm_transition_data(p, conditions = "ui"), "data.frame")
})

test_that("visits 7-9 are READABLE through LEKINVO, though quarantined by default", {
  # swan_ui_modeling.csv loses these three visits by looking only for INVOLEA,
  # and reading the items directly still recovers them -- the crosswalk is
  # correct about which column holds the answer.
  #
  # This test used to assert they were INCLUDED, which was the wrong lesson from
  # a right observation. Being able to read a column is not evidence it measures
  # the same thing as the column it replaces; here it does not, so the default
  # excludes them and this asserts only that the rename is still resolved.
  p <- build_swan_dmdm_panel(mk_swan_wide(), visits = 0:10, verbose = FALSE)
  expect_true(all(7:9 %in% unique(p$year)))
  expect_equal(sort(unique(p$year)), 0:10)
  expect_false(all(is.na(p$has_ui[p$year %in% 7:9])))
})

test_that("visit 0 hysterectomy comes from the unsuffixed column", {
  # HYSTERE0 does not exist; every other visit suffixes the item. Pasting the
  # suffix would yield an all-NA hysterectomy term at baseline.
  w <- mk_swan_wide()
  expect_false("HYSTERE0" %in% names(w))
  p <- build_swan_dmdm_panel(w, verbose = FALSE)
  expect_false(any(is.na(p$hysterectomy[p$year == 0])))
})

test_that("menopause unknown codes become NA rather than 'not post'", {
  w <- mk_swan_wide()
  w$STATUS2 <- c("(7) Unknown due to HT use", "(8) Unknown due to hysterectomy",
                 "(2) Natural post", "(5) Pre-menopausal", "(4) Early peri", "(1) Post by BSO")
  p <- build_swan_dmdm_panel(w, verbose = FALSE)
  v2 <- p[p$year == 2, ]
  v2 <- v2[order(v2$person_id), ]
  expect_true(all(is.na(v2$menopause_status[1:2])))   # 7, 8 -> NA
  expect_equal(v2$menopause_status[3], 1L)            # natural post
  expect_equal(v2$menopause_status[4], 0L)            # pre
  expect_equal(v2$menopause_status[5], 0L)            # early peri
  expect_equal(v2$menopause_status[6], 1L)            # post by BSO
})

test_that("the parity proxy and the unmeasured term are declared, not hidden", {
  p <- build_swan_dmdm_panel(mk_swan_wide(), verbose = FALSE)
  prov <- attr(p, "swan_dmdm_provenance")
  expect_true("cumulative_vaginal_deliveries" %in% names(prov$proxied))
  expect_true("years_since_last_vaginal_birth" %in% names(prov$unmeasured))
  # years_since_last_vaginal_birth is constant by construction, so a fitted
  # slope for it is structurally zero and must not be read as "no effect".
  expect_equal(length(unique(p$years_since_last_vaginal_birth)), 1L)

  caveats <- swan_panel_fit_caveats(p)
  expect_true(any(grepl("TOTAL parity", caveats)))
  expect_true(any(grepl("structurally 0", caveats)))
})

test_that("caveats survive losing the attribute rather than silently vanishing", {
  expect_match(swan_panel_fit_caveats(data.frame(a = 1)),
               "did not come from build_swan_dmdm_panel", all = FALSE)
})

test_that("anal incontinence is refused outright", {
  expect_error(build_swan_dmdm_panel(mk_swan_wide(), conditions = "ai", verbose = FALSE),
               "does not follow anal incontinence")
})

test_that("requesting POP warns that the fit is degenerate", {
  expect_message(build_swan_dmdm_panel(mk_swan_wide(), conditions = c("ui", "pop"),
                                       verbose = FALSE),
                 "degenerate")
})

test_that("UI is the default condition, so POP is not fitted by accident", {
  p <- build_swan_dmdm_panel(mk_swan_wide(), verbose = FALSE)
  expect_true("has_ui" %in% names(p))
  expect_false("has_pop" %in% names(p))
})

test_that("participant-visits with no observed state are dropped", {
  w <- mk_swan_wide()
  w$INVOLEA3 <- NA_character_
  p <- build_swan_dmdm_panel(w, verbose = FALSE)
  expect_equal(sum(p$year == 3), 0L)
})

# ---- LEKINVO quarantine (visits 7-9) ---------------------------------------
#
# Visits 7-9 gate UI on LEKINVO instead of INVOLEA. Recovering them looked like
# a win -- three extra visits the pre-built CSV drops -- and it was wrong: the
# two items disagree by ~25 points of prevalence in adjacent visits, and
# including them flips the fitted age coefficient from -0.05 to +0.43, charging
# a questionnaire change to age. These lock the exclusion in.

test_that("the default visit set excludes the LEKINVO visits", {
  expect_equal(SWAN_UI_QUARANTINED_VISITS, 7:9)
  expect_equal(SWAN_UI_DEFAULT_VISITS, c(0:6, 10))
  p <- build_swan_dmdm_panel(mk_swan_wide(), verbose = FALSE)
  expect_false(any(p$year %in% 7:9))
  expect_setequal(unique(p$year), c(0:6, 10))
})

test_that("asking for a LEKINVO visit is possible but never silent", {
  # Quarantine, not prohibition: someone inspecting the discontinuity needs
  # these rows. They just must not arrive by accident.
  expect_message(
    p <- build_swan_dmdm_panel(mk_swan_wide(), visits = 0:10, verbose = FALSE),
    "LEKINVO")
  expect_true(any(p$year %in% 7:9))

  # A visit set that touches the boundary from either side stays quiet.
  expect_no_message(build_swan_dmdm_panel(mk_swan_wide(), visits = c(0:6, 10),
                                          verbose = FALSE))
})

test_that("the 6->10 gap costs no transitions, so the exclusion is free", {
  # dmdm_transition_data() keeps only pairs one visit apart, so the gap left by
  # the quarantine drops itself. If that ever changed, the exclusion would start
  # silently fabricating a four-year interval as a one-year transition.
  p <- build_swan_dmdm_panel(mk_swan_wide(), verbose = FALSE)
  d <- dmdm_transition_data(p, conditions = "ui")
  expect_false(any(d$year == 6L))
  expect_true(all(d$year %in% 0:5))
})

test_that("UI prevalence is continuous across the visit-7 boundary in real SWAN", {
  # THE CHECK THAT WOULD HAVE CAUGHT THIS. On the real archive, prevalence at
  # visits 6, 7 and 10 was 0.650 / 0.906 / 0.673 -- a 25-point jump on a 1.1-year
  # age gap, reversing at visit 10 while the cohort got older. Skipped wherever
  # the archive is absent; it is a data-integrity assertion, not a unit test.
  archive <- swan_path("swan_all_visits.rds")
  skip_if_not(file.exists(archive), "SWAN archive not present")

  wide <- load_swan_archive(path = archive, verbose = FALSE)
  p <- build_swan_dmdm_panel(wide, visits = 0:10, verbose = FALSE)
  prev <- vapply(split(p$has_ui, p$year), mean, numeric(1))

  # Within INVOLEA, four years of ageing moved prevalence ~2 points. Any gate
  # swapped in for a quarantined visit must sit inside that envelope, not 25
  # points above it.
  involea <- prev[as.character(c(0:6, 10))]
  expect_lt(max(diff(involea)), 0.10)

  # And the quarantined visits must still look different -- if they ever stop
  # looking different, the quarantine can be revisited on evidence.
  expect_gt(mean(prev[as.character(7:9)]) - mean(involea), 0.15)
})
