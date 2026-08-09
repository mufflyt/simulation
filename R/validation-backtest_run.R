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

#   5  derived   entrants from the NRMP fellowship match, pre-cutoff reports
#
# ARM 5 IS AN ADDITION, NOT A REPLACEMENT, and it is scored alongside the
# original four rather than instead of them. Its justification is prospective:
# the certification flow is a LAGGING measure of entry that was corrupted in
# exactly the estimation window (the COVID-disrupted 2020 examination produced a
# cohort of 10 whose backlog cleared in 2021), whereas NRMP counts fellows at
# APPOINTMENT and publishes each report in its own appointment year. Fellowship
# is three years, so the 2017-2020 appointment cohorts are precisely the people
# who certify across the validation window -- and every one of those reports was
# in print before the cutoff. No parameter is re-tuned after seeing 2023.

# TEMPORAL PROVENANCE OF THE TWO EXIT PARAMETERS.
#
# "Forecast from a 2020 origin" binds PARAMETERS, not just data. Both exit
# processes were audited against that claim and they do not have the same
# standing:
#
#   * RETIREMENT_HAZARD_BY_AGE is anchored to HWSM Exhibit 17 (Florida physician
#     survey 2012-2013, documentation v5.19.20, MAY 2020) and the FutureDocs
#     curve (2017). Both precede the cutoff, so retirement runs UNCHANGED.
#   * CAREER_CHANGE_HAZARD_UNDER_50 (1.42%/yr) is a CPS ASEC occupational-
#     separation estimate published in Zarek et al., Phys Ther 2025;105:pzaf014.
#     It did not exist in 2020. A 2020 analyst could not have used it, so it
#     cannot appear in a forecast that claims that origin.
#
# ZERO HERE IS AN OMISSION, NOT AN ESTIMATE. It does not assert that no
# urogynecologist under 50 left the specialty between 2021 and 2023. It records
# that no contemporaneous estimate of a PERMANENT under-50 separation process
# was available to parameterise one. HWSM represented under-50 exit as temporary
# labour-force participation WITH re-entry -- a different process this model
# does not implement -- so there was no 2020-vintage value to substitute. If a
# pre-2020 CPS ASEC re-estimation is ever done, it belongs here.
#
# A sensitivity analysis is not a cure for this. Leakage is a property of the
# primary analysis, so the primary analysis omits the parameter and the 2025
# value is scored separately:
#
#   run_backtest(career_change_hazard = CAREER_CHANGE_HAZARD_UNDER_50)
#
# The PRODUCTION model is untouched: `run_supply_microsimulation()` still
# applies CAREER_CHANGE_HAZARD_UNDER_50, because a forward projection made today
# may legitimately use evidence published in 2025.
BACKTEST_CAREER_CHANGE_HAZARD <- 0

BACKTEST_ARMS <- tibble::tribble(
  ~arm, ~cohort,      ~entrants,   ~label,
  1L,   "derived",    "assumed",   "Derived cohort, entrants = 55 (shipped assumption)",
  2L,   "derived",    "estimated", "Derived cohort, entrants from pre-2021 data",
  3L,   "synthetic",  "assumed",   "Synthetic rnorm(52, 9), entrants = 55",
  4L,   "synthetic",  "estimated", "Synthetic rnorm(52, 9), entrants from pre-2021 data",
  5L,   "derived",    "nrmp",      "Derived cohort, entrants from pre-cutoff NRMP match"
)

#' Run one back-test arm
#'
#' @param cohort "derived" or "synthetic".
#' @param entrants_per_year Gross annual entrants.
#' @param cutoff_year Last year the model may see.
#' @param target_year Final projection year.
#' @param n_iterations Monte Carlo replicates.
#' @param apply_attrition Apply the exit processes. FALSE gives the
#'   definition-matched comparison against a series that applies none.
#' @param career_change_hazard Annual permanent separation hazard under age 50.
#'   Defaults to [BACKTEST_CAREER_CHANGE_HAZARD], which is 0: the 1.42% estimate
#'   postdates the 2020 origin, so the historical forecast OMITS the process
#'   rather than asserting the hazard is zero. Pass
#'   `CAREER_CHANGE_HAZARD_UNDER_50` for the sensitivity analysis.
#' @param param_spec Optional [supply_parameter_spec()] built from PRE-CUTOFF
#'   data only. Supplying it redraws the entrant rate each iteration so the
#'   intervals carry forecast uncertainty. It does NOT move the point estimate.
#' @param seed RNG seed.
#' @return List with per-iteration trajectories and the arm's settings.
#' @family backtest run
#' @concept validation
#' @export
run_backtest_arm <- function(cohort = c("derived", "synthetic"),
                             entrants_per_year,
                             cutoff_year = BACKTEST_CUTOFF_YEAR,
                             target_year = BACKTEST_TARGET_YEAR,
                             n_iterations = 1000L,
                             apply_attrition = TRUE,
                             career_change_hazard = BACKTEST_CAREER_CHANGE_HAZARD,
                             param_spec = NULL,
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
  career_change <- if (isTRUE(apply_attrition)) career_change_hazard else 0

  iters <- vector("list", n_iterations)
  for (it in seq_len(n_iterations)) {
    agents <- if (cohort == "derived") {
      backtest_cohort_at(cutoff_year)
    } else {
      a <- initialize_provider_agents(n0, "URPS", cutoff_year)
      a$sex <- ifelse(stats::runif(nrow(a)) < 0.55, "female", "male")
      a
    }
    it_entrants <- entrants_per_year
    it_sched <- sched
    if (!is.null(param_spec)) {
      d <- draw_supply_parameters(param_spec, sched, years = years)
      # Take the draw ONLY when it is a usable number, matching
      # run_supply_microsimulation(). `d$entrants` is `spec$entrant_mean`
      # verbatim when the entrant rate is not quantified, so a spec that
      # quantifies only the hazard carries NULL here; assigning it produced a
      # zero-length capacity and an unreadable `rep()` error deep in the engine.
      #
      # A spec carrying an entrant regime model returns a PATH -- one value per
      # transition -- rather than a scalar, and the engine accepts either. The
      # test is therefore "non-empty and finite throughout", not "length one";
      # requiring a scalar here would silently discard the regime structure.
      if (is.numeric(d$entrants) && length(d$entrants) >= 1L &&
          all(is.finite(d$entrants))) {
        it_entrants <- d$entrants
      }
      it_sched <- d$retirement_schedule
    }
    sim <- simulate_provider_career_once(
      agents, years, it_entrants,
      retirement_schedule = it_sched,
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
                    career_change_hazard = career_change,
                    n0 = n0, seed = seed,
                    parameter_uncertainty = !is.null(param_spec))
  )
}

#' Score one arm against the observed series
#'
#' @param arm Result of [run_backtest_arm()].
#' @param observed Named numeric vector of observed counts keyed by year.
#' @param label Arm label.
#' @return One-row tibble of metrics.
#' @family backtest run
#' @concept validation
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
    parameter_uncertainty = isTRUE(st$parameter_uncertainty),
    apply_attrition = st$apply_attrition,
    career_change_hazard = st$career_change_hazard %||% NA_real_,
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
#' @param career_change_hazard Passed to [run_backtest_arm()]. Defaults to
#'   [BACKTEST_CAREER_CHANGE_HAZARD] (0) so the primary historical back-test
#'   uses no post-cutoff parameter.
#' @param seed RNG seed.
#' @param acknowledge_no_attrition Proceed despite the observed series applying
#'   no attrition (recorded in every output).
#' @param expected_target The 2023 count the run is scored against, stated
#'   explicitly so a different-but-valid count cannot be substituted silently.
#' @return List with `summary`, `iterations`, `trajectory`, `target`, `entrants`.
#' @family backtest run
#' @concept validation
#' @export
run_backtest <- function(cutoff_year = BACKTEST_CUTOFF_YEAR,
                         target_year = BACKTEST_TARGET_YEAR,
                         n_iterations = 1000L,
                         assumed_entrants = 55,
                         career_change_hazard = BACKTEST_CAREER_CHANGE_HAZARD,
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
    paste("Pre-cutoff entrant estimate: %.1f/yr, the observed certification flow",
          "over %d-%d (modelled departures %.1f/yr are NOT added -- the series is",
          "already gross)."),
    est$gross_entrants, est$window[1], est$window[2], est$departures))

  # 3. Assert no read touched the validation window, and no PARAMETER postdates
  #    it either. The second check exists because the first one passed for
  #    months while a 2025 career-change estimate sat in the 2020 forecast.
  assert_no_leakage(cutoff_year)
  assert_backtest_parameters_precede_cutoff(cutoff_year)

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

  # NRMP reports published by the cutoff ONLY. `available_by` is the leakage
  # guard: it filters on publication year, not appointment year, so a report
  # that appeared after the cutoff cannot enter however tempting its value.
  nrmp <- tryCatch(nrmp_entrant_series(available_by = cutoff_year),
                   error = function(e) NULL)
  if (!is.null(nrmp)) {
    .msg_info(sprintf(
      "Pre-cutoff NRMP series (appointment years %s): %s filled, mean %.1f/yr.",
      paste(range(nrmp$appointment_year), collapse = "-"),
      paste(nrmp$positions_filled, collapse = ", "),
      mean(nrmp$positions_filled)))
  }

  entrant_of <- function(kind) {
    switch(kind,
      assumed = assumed_entrants,
      estimated = est$gross_entrants,
      nrmp = if (is.null(nrmp)) NA_real_ else mean(nrmp$positions_filled),
      stop("unknown entrant kind: ", kind, call. = FALSE))
  }
  series_of <- function(kind) {
    if (identical(kind, "nrmp") && !is.null(nrmp)) nrmp$positions_filled
    else unname(est$yearly)
  }

  # PARAMETER UNCERTAINTY IN THE INTERVALS.
  #
  # `run_backtest_arm()` has always accepted a `param_spec`, and nothing ever
  # passed one. Every arm therefore reported intervals built from individual
  # stochasticity alone -- Bernoulli retirement, the fractional entrant draw --
  # with the entrant rate pinned at a single value across all 1,000 replicates.
  # The result was PI95 widths of 0-40 providers on a count near 1,300, two arms
  # with LITERALLY ZERO width (no attrition, integral entrant rate: nothing left
  # to vary), and 0/8 coverage. Those were never forecast intervals, so failing
  # coverage told us nothing about the forecast.
  #
  # The spec is built from PRE-CUTOFF certifications only, so it adds no
  # information about the validation window, and it does NOT move the point
  # estimate -- the draw is centred on the same mean the fixed run uses. Coverage
  # is therefore re-scored fairly rather than tuned into passing.
  #
  # EACH ARM KEEPS ITS OWN CENTRE. The spec is built per arm so the draw is
  # centred on that arm's entrant value and takes only the SPREAD from the
  # observed series. Passing one shared spec would overwrite `entrants_per_year`
  # with the estimated rate on every iteration, silently collapsing arms 1 and 3
  # (the shipped assumption) into arms 2 and 4 and destroying the prespecified
  # contrast the design exists to measure.
  arm_spec <- function(entrants, series) {
    supply_parameter_spec(
      entrant_series = series,
      entrant_mean = entrants,
      departures = est$departures
    )
  }

  rows <- list(); iter_rows <- list(); traj_rows <- list()
  for (i in seq_len(nrow(BACKTEST_ARMS))) {
    a <- BACKTEST_ARMS[i, ]
    for (att in c(TRUE, FALSE)) {
      res <- run_backtest_arm(
        cohort = a$cohort, entrants_per_year = entrant_of(a$entrants),
        cutoff_year = cutoff_year, target_year = target_year,
        n_iterations = n_iterations, apply_attrition = att,
        career_change_hazard = career_change_hazard,
        param_spec = arm_spec(entrant_of(a$entrants), series_of(a$entrants)),
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

  # PROVENANCE. A frozen back-test artifact that does not record which contract
  # snapshot it was scored against is untraceable: if mufflyaccess ships a new
  # artifact where 2023 reads 1,310, this CSV becomes silently stale and nothing
  # in it says so. Every row carries the artifact identity.
  prov <- ssot_provenance()
  summary_tbl <- dplyr::bind_rows(rows)
  summary_tbl$contract_version <- target$contract_version
  summary_tbl$artifact_version <- prov$artifact_version %||% NA_character_
  summary_tbl$artifact_source <- prov$artifact_source %||% NA_character_
  summary_tbl$snapshot_date <- as.character(prov$snapshot_date %||% NA)
  summary_tbl$source_sha256 <- prov$source_sha256 %||% NA_character_
  summary_tbl$canonical_release <- prov$canonical_release %||% NA
  summary_tbl$target_basis <- target$basis
  summary_tbl$observed_applies_attrition <- target$observed_series_applies_attrition

  list(
    summary = summary_tbl,
    provenance = prov,
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
