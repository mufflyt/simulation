# Back-Test Runner and Scoring ----
#
# Four prespecified arms, fixed before any 2023 error was examined:
#
#   1  derived   entrants = 55   (the model's shipped assumption)
#   2  derived   entrants estimated from pre-cutoff data only
#   3  synthetic entrants = 55   (the previous rnorm(52, 9) cohort)
#   4  synthetic entrants estimated from pre-cutoff data only
#
# Arms 1 and 3 isolate the effect of the cohort construction; arms 2 and 4
# isolate the effect of the entrant estimate. No parameter is re-tuned after the
# result is seen.

BACKTEST_ARMS <- tibble::tribble(
  ~arm, ~cohort,      ~entrants,   ~label,
  1L,   "derived",    "assumed",   "Derived cohort, entrants = 55 (shipped assumption)",
  2L,   "derived",    "estimated", "Derived cohort, entrants from pre-2021 data",
  3L,   "synthetic",  "assumed",   "Synthetic rnorm(52, 9), entrants = 55",
  4L,   "synthetic",  "estimated", "Synthetic rnorm(52, 9), entrants from pre-2021 data"
)

#' Run one back-test arm
#'
#' @param cohort "derived" or "synthetic".
#' @param entrants_per_year Gross annual entrants.
#' @param cutoff_year Last year the model may see.
#' @param target_year Final projection year.
#' @param n_iterations Monte Carlo replicates.
#' @param apply_attrition Apply retirement hazards. FALSE gives the
#'   definition-matched comparison against a series that applies none.
#' @param seed RNG seed.
#' @return List with per-iteration trajectories and the arm's settings.
#' @export
run_backtest_arm <- function(cohort = c("derived", "synthetic"),
                             entrants_per_year,
                             cutoff_year = BACKTEST_CUTOFF_YEAR,
                             target_year = BACKTEST_TARGET_YEAR,
                             n_iterations = 1000L,
                             apply_attrition = TRUE,
                             seed = 20260802L) {
  cohort <- match.arg(cohort)
  seed_microsimulation(seed)

  n0 <- sum(backtest_cohorts_through(cutoff_year)$n_certified)
  years <- cutoff_year:target_year

  # No attrition is represented by a zero hazard schedule, so the SAME engine
  # runs both comparisons -- the requirement is that the Monte Carlo machinery is
  # identical to the main projections.
  sched <- if (isTRUE(apply_attrition)) {
    RETIREMENT_HAZARD_BY_AGE
  } else {
    RETIREMENT_HAZARD_BY_AGE * 0
  }
  career_change <- if (isTRUE(apply_attrition)) CAREER_CHANGE_HAZARD_UNDER_50 else 0

  iters <- vector("list", n_iterations)
  for (it in seq_len(n_iterations)) {
    agents <- if (cohort == "derived") {
      backtest_cohort_at(cutoff_year)
    } else {
      a <- initialize_provider_agents(n0, "URPS", cutoff_year)
      a$sex <- ifelse(stats::runif(nrow(a)) < 0.55, "female", "male")
      a
    }
    sim <- simulate_provider_career_once(
      agents, years, entrants_per_year,
      retirement_schedule = sched,
      career_change_hazard = career_change,
      fte_method = "hours"
    )
    iters[[it]] <- tibble::tibble(iteration = it, year = sim$panel$year,
                                  headcount = sim$panel$headcount)
  }

  list(
    iterations = dplyr::bind_rows(iters),
    settings = list(cohort = cohort, entrants_per_year = entrants_per_year,
                    cutoff_year = cutoff_year, target_year = target_year,
                    n_iterations = n_iterations, apply_attrition = apply_attrition,
                    n0 = n0, seed = seed)
  )
}

#' Score one arm against the observed series
#'
#' @param arm Result of [run_backtest_arm()].
#' @param observed Named numeric vector of observed counts keyed by year.
#' @param label Arm label.
#' @return One-row tibble of metrics.
#' @export
score_backtest_arm <- function(arm, observed, label = "") {
  # Settings are pulled out first: inside tibble(), `arm = label` would rebind
  # `arm` in the data mask for every later expression.
  st <- arm$settings
  ty <- st$target_year
  cy <- st$cutoff_year
  pred <- arm$iterations$headcount[arm$iterations$year == ty]
  obs <- unname(observed[as.character(ty)])
  obs0 <- unname(observed[as.character(cy)])

  q <- stats::quantile(pred, c(0.025, 0.10, 0.90, 0.975), names = FALSE)
  med <- stats::median(pred); mn <- mean(pred)

  # Calibration slope: observed regressed on the per-year predicted median over
  # the projected years. 1.0 means the trajectory shape is right.
  yrs <- setdiff(sort(unique(arm$iterations$year)), cy)
  pm <- vapply(yrs, function(y) stats::median(arm$iterations$headcount[arm$iterations$year == y]),
               numeric(1))
  ov <- unname(observed[as.character(yrs)])
  ok <- is.finite(pm) & is.finite(ov)
  slope <- if (sum(ok) >= 2) unname(stats::coef(stats::lm(ov[ok] ~ pm[ok]))[2]) else NA_real_

  tibble::tibble(
    arm = label,
    cohort = st$cohort,
    entrants_per_year = st$entrants_per_year,
    apply_attrition = st$apply_attrition,
    n_iterations = st$n_iterations,
    baseline_year = cy,
    baseline_count = obs0,
    target_year = ty,
    observed = obs,
    predicted_median = med,
    predicted_mean = mn,
    pi80_lower = q[2], pi80_upper = q[3],
    pi95_lower = q[1], pi95_upper = q[4],
    absolute_error = med - obs,
    percent_error = 100 * (med - obs) / obs,
    within_80 = obs >= q[2] && obs <= q[3],
    within_95 = obs >= q[1] && obs <= q[4],
    observed_annual_change = (obs - obs0) / (ty - cy),
    predicted_annual_change = (med - obs0) / (ty - cy),
    calibration_slope = slope,
    mc_standard_error = stats::sd(pred) / sqrt(length(pred))
  )
}

#' Run the full prespecified back-test
#'
#' @param cutoff_year Last year any model parameter may use.
#' @param target_year Final projection year.
#' @param n_iterations Monte Carlo replicates per arm.
#' @param assumed_entrants The model's shipped entrant assumption.
#' @param seed RNG seed.
#' @param acknowledge_no_attrition Proceed despite the observed series applying
#'   no attrition (recorded in every output).
#' @param expected_target The 2023 count the run is scored against, stated
#'   explicitly so a different-but-valid count cannot be substituted silently.
#' @return List with `summary`, `iterations`, `trajectory`, `target`, `entrants`.
#' @export
run_backtest <- function(cutoff_year = BACKTEST_CUTOFF_YEAR,
                         target_year = BACKTEST_TARGET_YEAR,
                         n_iterations = 1000L,
                         assumed_entrants = 55,
                         seed = 20260802L,
                         acknowledge_no_attrition = TRUE,
                         expected_target = 1306L) {
  reset_leakage_audit()

  # 1. Validate the target BEFORE anything is fitted.
  target <- validate_backtest_target(
    target_year = target_year,
    acknowledge_no_attrition = acknowledge_no_attrition,
    expected_value = expected_target
  )
  .msg_info("Back-test target: ", target$rationale)

  # 2. Estimate parameters using pre-cutoff information ONLY.
  seed_microsimulation(seed)
  cohort0 <- backtest_cohort_at(cutoff_year)
  est <- backtest_entrant_estimate(cutoff_year, agents = cohort0)
  .msg_info(sprintf(
    "Pre-cutoff entrant estimate: net %.1f + departures %.1f = %.1f/yr (window %d-%d).",
    est$net_growth, est$departures, est$gross_entrants, est$window[1], est$window[2]))

  # 3. Assert no read touched the validation window.
  assert_no_leakage(cutoff_year)

  # 4. Observed series for scoring -- read AFTER the leakage assertion, and
  #    never fed back into any parameter.
  obs_years <- cutoff_year:target_year
  observed <- stats::setNames(
    vapply(obs_years, function(y) {
      mufflyaccess::urps_count(y, geography = target$geography,
                               include_urology = TRUE)
    }, numeric(1)),
    as.character(obs_years)
  )

  entrant_of <- function(kind) if (kind == "assumed") assumed_entrants else est$gross_entrants

  rows <- list(); iter_rows <- list(); traj_rows <- list()
  for (i in seq_len(nrow(BACKTEST_ARMS))) {
    a <- BACKTEST_ARMS[i, ]
    for (att in c(TRUE, FALSE)) {
      res <- run_backtest_arm(
        cohort = a$cohort, entrants_per_year = entrant_of(a$entrants),
        cutoff_year = cutoff_year, target_year = target_year,
        n_iterations = n_iterations, apply_attrition = att,
        seed = seed + i
      )
      lab <- sprintf("%d. %s%s", a$arm, a$label,
                     if (att) "" else " [no-attrition, definition-matched]")
      rows[[length(rows) + 1L]] <- score_backtest_arm(res, observed, lab)
      iter_rows[[length(iter_rows) + 1L]] <-
        dplyr::mutate(res$iterations, arm = lab, apply_attrition = att)
      traj_rows[[length(traj_rows) + 1L]] <- res$iterations %>%
        dplyr::group_by(.data$year) %>%
        dplyr::summarise(
          predicted_median = stats::median(.data$headcount),
          pi80_lower = stats::quantile(.data$headcount, 0.10, names = FALSE),
          pi80_upper = stats::quantile(.data$headcount, 0.90, names = FALSE),
          pi95_lower = stats::quantile(.data$headcount, 0.025, names = FALSE),
          pi95_upper = stats::quantile(.data$headcount, 0.975, names = FALSE),
          .groups = "drop") %>%
        dplyr::mutate(arm = lab, apply_attrition = att,
                      observed = unname(observed[as.character(.data$year)]))
    }
  }

  list(
    summary = dplyr::bind_rows(rows),
    iterations = dplyr::bind_rows(iter_rows),
    trajectory = dplyr::bind_rows(traj_rows),
    target = target,
    observed = observed,
    entrants = list(assumed = assumed_entrants, estimated = est),
    leakage_audit = .backtest_audit$reads,
    settings = list(cutoff_year = cutoff_year, target_year = target_year,
                    n_iterations = n_iterations, seed = seed)
  )
}
