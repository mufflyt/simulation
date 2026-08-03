library(testthat)
library(urpssim)

# Minimal synthetic BRFSS-shaped data ----------------------------------------

.mini_brfss <- function(n = 200L) {
  set.seed(1L)
  data.frame(
    SEQNO      = seq_len(n),
    X_STATE    = sample(c(6L, 48L, 17L), n, replace = TRUE),
    X_LLCPWT   = runif(n, 50, 2000),
    X_AGEG5YR  = sample(1:14, n, replace = TRUE),
    X_IMPRACE  = sample(1:6,  n, replace = TRUE),
    X_HLTHPL1  = sample(c(1L, 2L, 9L), n, replace = TRUE,
                        prob = c(0.85, 0.10, 0.05)),
    INCOME3    = sample(c(1:11, 77L, 99L), n, replace = TRUE,
                        prob = c(rep(0.08, 11), 0.04, 0.04)),
    X_METSTAT  = sample(c(1L, 2L), n, replace = TRUE),
    X_BMI5CAT  = sample(c(1L:4L, NA), n, replace = TRUE,
                        prob = c(0.03, 0.30, 0.28, 0.29, 0.10)),
    X_SMOKER3  = sample(c(1L:4L, 9L), n, replace = TRUE,
                        prob = c(0.07, 0.03, 0.20, 0.60, 0.10)),
    CHILDREN   = sample(c(0L:5L, 88L, 99L), n, replace = TRUE,
                        prob = c(0.15, 0.20, 0.25, 0.20, 0.10, 0.05, 0.03, 0.02)),
    stringsAsFactors = FALSE
  )
}

# Replicate the harmonisation logic locally so tests don't depend on file I/O.
.harmonise_mini <- function(df) {
  age_grp  <- urpssim:::.AGEG5YR_TO_URPS_BAND[as.character(df$X_AGEG5YR)]
  race_eth <- urpssim:::.IMPRACE_LABELS[as.character(df$X_IMPRACE)]
  insurance <- urpssim:::.HLTHPL_LABELS[as.character(df$X_HLTHPL1)]
  income_tier <- urpssim:::.income3_to_tier(df$INCOME3)
  metro <- ifelse(df$X_METSTAT == 1L, "Metro", "NonMetro")
  bmi_class <- urpssim:::.BMI5CAT_LABELS[as.character(df$X_BMI5CAT)]
  smoker    <- urpssim:::.SMOKER3_LABELS[as.character(df$X_SMOKER3)]
  n_ch <- df$CHILDREN
  n_ch[n_ch %in% c(88L, 99L)] <- NA_integer_

  tibble::tibble(
    seqno       = df$SEQNO,
    state_fips  = df$X_STATE,
    survey_wt   = df$X_LLCPWT,
    age_group   = factor(age_grp, levels = URPS_POP_AGE_BANDS),
    race_eth    = race_eth,
    insurance   = insurance,
    income_tier = income_tier,
    metro       = metro,
    bmi_class   = bmi_class,
    smoker      = smoker,
    n_children  = n_ch,
    ui_flag     = NA_integer_,
    pop_flag    = NA_integer_,
    fi_flag     = NA_integer_
  )
}

# ---- Unit tests -------------------------------------------------------------

test_that("URPS_POP_AGE_BANDS has five levels", {
  expect_equal(length(URPS_POP_AGE_BANDS), 5L)
})

test_that("age band mapping covers all 14 BRFSS codes", {
  expect_equal(length(urpssim:::.AGEG5YR_TO_URPS_BAND), 14L)
  expect_true(all(urpssim:::.AGEG5YR_TO_URPS_BAND %in% URPS_POP_AGE_BANDS))
})

test_that("income tier mapping returns NA for refused/dk", {
  tiers <- urpssim:::.income3_to_tier(c(1L, 5L, 10L, 77L, 99L, NA))
  expect_equal(tiers[4:6], rep(NA_character_, 3))
  expect_equal(tiers[1], "LT25k")
  expect_equal(tiers[3], "GT100k")
})

test_that("build_urps_population_cells returns non-empty tibble", {
  mini <- .harmonise_mini(.mini_brfss())
  cells <- build_urps_population_cells(mini, verbose = FALSE)
  expect_gt(nrow(cells), 0L)
  expect_true(inherits(cells, "tbl_df"))
})

test_that("cells have required columns", {
  mini <- .harmonise_mini(.mini_brfss())
  cells <- build_urps_population_cells(mini, verbose = FALSE)
  required <- c("age_group", "race_eth", "insurance", "income_tier", "metro",
                "bmi_class", "n_respondents", "pop_weight",
                "pct_smoker", "mean_children",
                "ui_prevalence", "pop_prevalence", "fi_prevalence", "pfd_source")
  expect_true(all(required %in% names(cells)))
})

test_that("prevalence values are in [0, 1]", {
  mini <- .harmonise_mini(.mini_brfss())
  cells <- build_urps_population_cells(mini, verbose = FALSE)
  expect_true(all(cells$ui_prevalence  >= 0 & cells$ui_prevalence  <= 1))
  expect_true(all(cells$pop_prevalence >= 0 & cells$pop_prevalence <= 1))
  expect_true(all(cells$fi_prevalence  >= 0 & cells$fi_prevalence  <= 1))
})

test_that("pfd_source is 'imputed_nygaard_wu' when no PFD module", {
  mini <- .harmonise_mini(.mini_brfss())
  cells <- build_urps_population_cells(mini, verbose = FALSE)
  expect_true(all(cells$pfd_source == "imputed_nygaard_wu"))
})

test_that("pop_weight sums are positive and finite", {
  mini <- .harmonise_mini(.mini_brfss())
  cells <- build_urps_population_cells(mini, verbose = FALSE)
  expect_true(all(is.finite(cells$pop_weight)))
  expect_true(sum(cells$pop_weight) > 0)
})

test_that("project_urps_demand returns demand_fte > 0 in each age band", {
  mini <- .harmonise_mini(.mini_brfss())
  cells <- build_urps_population_cells(mini, verbose = FALSE)
  demand <- project_urps_demand(cells, us_female_pop = 138e6,
                                care_seeking_rate = 0.25,
                                referral_rate = 0.50,
                                verbose = FALSE)
  expect_true(nrow(demand) >= 1L)
  expect_true(all(demand$demand_fte > 0))
})

test_that("higher care-seeking rate increases total demand FTE", {
  mini <- .harmonise_mini(.mini_brfss())
  cells <- build_urps_population_cells(mini, verbose = FALSE)
  d_low  <- project_urps_demand(cells, care_seeking_rate = 0.10,
                                referral_rate = 0.50, verbose = FALSE)
  d_high <- project_urps_demand(cells, care_seeking_rate = 0.40,
                                referral_rate = 0.50, verbose = FALSE)
  expect_gt(sum(d_high$demand_fte), sum(d_low$demand_fte))
})

test_that("summarise_stratum_coverage returns share in (0, 1]", {
  mini <- .harmonise_mini(.mini_brfss())
  cells <- build_urps_population_cells(mini, verbose = FALSE)
  cov <- summarise_stratum_coverage(cells)
  expect_true(cov["complete_cell_share"] > 0)
  expect_true(cov["complete_cell_share"] <= 1)
})

test_that("brfss_pfd_prevalence_for_demand_bands returns named vector over DEMAND_AGE_BANDS", {
  mini <- .harmonise_mini(.mini_brfss())
  cells <- build_urps_population_cells(mini, verbose = FALSE)
  prev <- brfss_pfd_prevalence_for_demand_bands(cells, "ui")
  expected_bands <- c("20-39", "40-59", "60-64", "65-79", "80+")
  expect_equal(sort(names(prev)), sort(expected_bands))
  expect_true(all(prev >= 0 & prev <= 1, na.rm = TRUE))
})

test_that("brfss_pfd_prevalence_for_demand_bands: any_pfd capped at 1", {
  mini <- .harmonise_mini(.mini_brfss())
  cells <- build_urps_population_cells(mini, verbose = FALSE)
  prev <- brfss_pfd_prevalence_for_demand_bands(cells, "any_pfd")
  expect_true(all(prev <= 1.0 + .Machine$double.eps))
})

test_that("compute_brfss_demand_estimand returns D4 in correct format", {
  mini <- .harmonise_mini(.mini_brfss())
  cells <- build_urps_population_cells(mini, verbose = FALSE)
  pop <- example_female_population_by_band(2025:2030)
  d4 <- compute_brfss_demand_estimand(pop, cells)
  expect_true(all(d4$estimand == "D4"))
  expect_equal(nrow(d4), 6L)
  expect_true(all(d4$demand_cases > 0))
})

test_that("D4 grows monotonically with population (no referral change)", {
  mini <- .harmonise_mini(.mini_brfss())
  cells <- build_urps_population_cells(mini, verbose = FALSE)
  pop <- example_female_population_by_band(2025:2040)
  d4 <- compute_brfss_demand_estimand(pop, cells)
  expect_false(is.unsorted(d4$demand_cases))
})

test_that("HALL_OF_SHAME: zero-weight row does not inflate FTE", {
  mini <- .harmonise_mini(.mini_brfss(50L))
  mini$survey_wt[1:5] <- 0
  cells <- build_urps_population_cells(mini, verbose = FALSE)
  demand_all <- project_urps_demand(cells, verbose = FALSE)
  mini2 <- mini[6:nrow(mini), ]
  cells2 <- build_urps_population_cells(mini2, verbose = FALSE)
  demand_nozero <- project_urps_demand(cells2, verbose = FALSE)
  expect_equal(sum(demand_all$demand_fte), sum(demand_nozero$demand_fte),
               tolerance = 1e-6)
})
