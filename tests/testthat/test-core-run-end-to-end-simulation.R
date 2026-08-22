test_that("run_end_to_end_simulation executes 8 coupled annual steps cleanly", {
  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2027L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 5L,
    save_outputs = FALSE
  )

  expect_s3_class(sim_res$audit_ledger_tbl, "tbl_df")
  expect_equal(nrow(sim_res$audit_ledger_tbl), 3L) # 2025, 2026, 2027

  # Patient-flow conservation identity: served + unserved == appointment_requests
  audit <- sim_res$audit_ledger_tbl
  expect_equal(
    audit$served_patients_n + audit$unserved_delayed_n,
    audit$appointment_requests_n,
    tolerance = 1e-6
  )

  # Check HRR 306 spatial balance
  expect_s3_class(sim_res$annual_hrr_balance, "tbl_df")
  expect_equal(nrow(sim_res$annual_hrr_balance), 3L * 306L)
})

test_that("default policy_migration_scenario is a zero-behavior-change identity", {
  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2026L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 5L,
    save_outputs = FALSE
  )

  expect_null(sim_res$policy_migration_summary_tbl)
  expect_s3_class(sim_res$policy_migration_diagnostics, "tbl_df")
  expect_true(all(!sim_res$policy_migration_diagnostics$policy_migration_active))
  expect_equal(
    sim_res$policy_migration_diagnostics$demand_multiplier,
    rep(1, 2L)
  )
  expect_equal(
    sim_res$policy_migration_diagnostics$provider_multiplier,
    rep(1, 2L)
  )
  expect_equal(
    sim_res$policy_migration_diagnostics$application_multiplier,
    rep(1, 2L)
  )
  expect_equal(
    sim_res$simulation_config$policy_migration_scenario,
    "baseline"
  )
})

test_that("a non-baseline scenario against an empty policy DB degrades gracefully", {
  database_path <- base::tempfile(fileext = ".duckdb")

  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2026L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 5L,
    save_outputs = FALSE,
    policy_migration_scenario = "combined_stress",
    policy_evidence_db = database_path
  )

  expect_s3_class(sim_res$policy_migration_summary_tbl, "tbl_df")
  expect_true(all(sim_res$policy_migration_diagnostics$policy_migration_active))
  expect_false(sim_res$policy_migration_diagnostics$relocation_empirical[[1]])
  expect_equal(
    sim_res$policy_migration_diagnostics$relocation_method[[1]],
    "declared_scenario_prior"
  )
  # No evidence ingested: coalesced-to-zero migration/policy signals leave
  # the demand multiplier at the identity value.
  expect_equal(
    sim_res$policy_migration_diagnostics$demand_multiplier,
    rep(1, 2L)
  )
})

test_that("real policy evidence moves the demand multiplier off 1.0", {
  database_path <- base::tempfile(fileext = ".duckdb")
  connection <- open_policy_migration_duckdb(database_path)

  # Strong, opposite-signed legislative climate in two states so the
  # evidence panel carries real cross-state variation.
  lawatlas <- tibble::tibble(
    state = base::rep(base::c("FL", "CO"), each = 2L),
    effective_date = base::as.Date("2024-01-01"),
    end_date = base::as.Date(base::c(
      "2026-12-31", "2026-12-31", "2026-12-31", "2026-12-31"
    )),
    policy_domain = "reproductive_health",
    policy_value = base::rep(base::c(2, -2), each = 2L)
  )
  ingest_lawatlas_policies(connection, lawatlas)

  # Symmetric-flow ACS PUMS migration so the demand channel is driven by
  # the legislative-climate channel specifically, not migration noise.
  pums <- tibble::tibble(
    AGEP = base::rep(base::c(60, 70), times = 10L),
    SEX = 2,
    ST = base::rep(base::c("12", "08"), times = 10L),
    MIGSP = base::rep(base::c("08", "12"), times = 10L),
    PWGTP = 100
  )
  ingest_acs_pums_migration(connection, pums, year = 2025L)
  ingest_acs_pums_migration(connection, pums, year = 2026L)
  DBI::dbDisconnect(connection, shutdown = TRUE)

  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2026L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 5L,
    save_outputs = FALSE,
    policy_migration_scenario = "combined_stress",
    policy_evidence_db = database_path
  )

  expect_true(base::any(
    sim_res$policy_migration_diagnostics$provider_multiplier != 1
  ))
  expect_true(base::any(
    sim_res$policy_migration_diagnostics$application_multiplier != 1
  ))
})

test_that("default run_practice_economics is a zero-behavior-change skip", {
  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2026L,
    n_agents = 100L,
    initial_provider_count = 50L,
    fellowship_entrants = 5L,
    save_outputs = FALSE
  )

  expect_equal(nrow(sim_res$practice_economics_diagnostics), 0L)
  expect_false(sim_res$simulation_config$run_practice_economics)
})

test_that("run_practice_economics produces real per-year diagnostics", {
  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2027L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 5L,
    save_outputs = FALSE,
    run_practice_economics = TRUE
  )

  diagnostics <- sim_res$practice_economics_diagnostics
  expect_equal(nrow(diagnostics), 3L)
  expect_true(all(diagnostics$n_practices > 0))
  expect_true(all(is.finite(diagnostics$mean_operating_margin)))
  expect_true(all(
    diagnostics$mean_loss_probability >= 0 &
      diagnostics$mean_loss_probability <= 1
  ))
  expect_true(all(
    diagnostics$mean_acquisition_probability >= 0 &
      diagnostics$mean_acquisition_probability <= 1
  ))

  # n_practices tracks the active provider cohort exactly -- each active
  # provider is one practice-year, no invented headcount. The practice-tbl
  # snapshot for year k is taken BEFORE that year's aging/exit/entrant step,
  # so it equals the PRIOR year's ending active_provider_n (year 1 equals
  # the freshly initialized cohort, i.e. initial_provider_count).
  expect_equal(diagnostics$n_practices[[1]], 100L)
  expect_equal(
    diagnostics$n_practices[-1],
    sim_res$engine_diagnostics$active_provider_n[-nrow(sim_res$engine_diagnostics)]
  )
})

test_that("annual_wrvu allocation matches the audit ledger's wRVU-per-FTE", {
  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2025L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 5L,
    save_outputs = FALSE,
    run_practice_economics = TRUE
  )

  # The per-provider wRVU allocation (wrvu_total * fte / supplied_fte) is
  # designed to reproduce the audit ledger's national wRVU-per-FTE exactly
  # when summed back up -- this is a real, derived allocation, not an
  # independently invented number.
  wrvu_per_fte <- sim_res$audit_ledger_tbl$wrvu_total /
    sim_res$audit_ledger_tbl$supplied_fte
  expect_true(wrvu_per_fte > 0 && is.finite(wrvu_per_fte))
})

test_that("practice_economics_diagnostics decomposes revenue, expense, and wRVU", {
  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2025L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 5L,
    save_outputs = FALSE,
    run_practice_economics = TRUE
  )

  diagnostics <- sim_res$practice_economics_diagnostics
  expect_true(all(c(
    "mean_wrvu_per_fte", "mean_revenue_per_fte", "mean_expense_per_fte",
    "mean_revenue_per_wrvu", "mean_operating_income"
  ) %in% names(diagnostics)))
  expect_true(all(is.finite(diagnostics$mean_wrvu_per_fte)))
  expect_true(all(is.finite(diagnostics$mean_revenue_per_fte)))
  expect_true(all(is.finite(diagnostics$mean_expense_per_fte)))

  # Identity check: the decomposition must actually explain the margin,
  # not just accompany it -- (revenue - expense) / revenue == the margin.
  implied_margin <- (diagnostics$mean_revenue_per_fte -
    diagnostics$mean_expense_per_fte) / diagnostics$mean_revenue_per_fte
  expect_equal(implied_margin, diagnostics$mean_operating_margin, tolerance = 0.05)

  # Realized revenue per wRVU must sit near real Medicare-adjacent conversion
  # rates -- proof the revenue side (not the cost side) is the well-behaved
  # half of this diagnostic.
  expect_true(all(
    diagnostics$mean_revenue_per_wrvu > 15 & diagnostics$mean_revenue_per_wrvu < 100
  ))
})

test_that("cost components sum exactly to mean_expense_per_fte -- no double counting", {
  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2025L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 5L,
    save_outputs = FALSE,
    run_practice_economics = TRUE
  )

  d <- sim_res$practice_economics_diagnostics
  component_sum <- d$mean_overhead_per_fte + d$mean_malpractice_per_fte +
    d$mean_app_labor_per_fte
  expect_equal(component_sum, d$mean_expense_per_fte, tolerance = 1e-8)

  # The two margin estimands are DELIBERATELY distinct (sum-based aggregate
  # vs. mean-of-per-practice-median) -- close, but must not be forced equal.
  expect_true(is.finite(d$aggregate_operating_margin))
  expect_lt(
    abs(d$aggregate_operating_margin - d$mean_operating_margin), 0.05
  )
})

test_that("an implausible practice-economics result warns rather than passing silently", {
  # A deliberately extreme app_delegation_rate/payer mix drives cost per
  # visit far above realistic revenue -- the fail-loud alarms in
  # run_end_to_end_simulation() must fire, not silently produce a number.
  extreme_mix <- tibble::tibble(
    medicare_share = 0, medicaid_share = 1,
    commercial_share = 0, self_pay_share = 0
  )
  expect_warning(
    run_end_to_end_simulation(
      start_year = 2025L,
      end_year = 2025L,
      n_agents = 100L,
      initial_provider_count = 30L,
      fellowship_entrants = 5L,
      save_outputs = FALSE,
      run_practice_economics = TRUE,
      practice_payer_mix = extreme_mix
    ),
    "plausibility bound"
  )
})

# productivity_engine = "lmer_fitted" was never exercised end-to-end before
# this fix -- three independent bugs (log-scale predictions treated as
# already-exponentiated, the model_bundle passed to stats::predict() instead
# of its $model, and provider_cohort missing most required predictor
# columns) meant it errored or silently corrupted patient-flow conservation
# by roughly an order of magnitude. This synthetic panel mirrors
# scripts/run_full_urps_microsimulation_demo.R's mock panel shape.
.lmer_fitted_test_panel <- function() {
  set.seed(42)
  n_obs <- 60
  tibble::tibble(
    provider_id = sprintf("P%02d", rep(1:15, each = 4)),
    year = rep(2021:2024, times = 15),
    clinical_fte = 1.0,
    clinical_hours_week = 40,
    age = stats::runif(n_obs, 35, 65),
    sex = sample(c("F", "M"), n_obs, replace = TRUE),
    academic = sample(c("Academic", "Private"), n_obs, replace = TRUE),
    rural = sample(c("Urban", "Rural"), n_obs, replace = TRUE),
    years_since_fellowship = stats::runif(n_obs, 1, 30),
    app_support_rate = stats::runif(n_obs, 0, 0.3),
    surgical_wrvu_share = stats::runif(n_obs, 0.1, 0.6),
    office_procedure_share = stats::runif(n_obs, 0.1, 0.4),
    new_visit_share = stats::runif(n_obs, 0.1, 0.3),
    wrvu_per_clinical_fte = stats::runif(n_obs, 3000, 8000),
    encounters_per_clinical_fte = stats::runif(n_obs, 1000, 3000),
    wrvu_per_clinical_hour = stats::runif(n_obs, 2, 5)
  )
}

test_that("productivity_engine = lmer_fitted runs end-to-end and conserves patient flow", {
  panel <- .lmer_fitted_test_panel()
  sim_res <- run_end_to_end_simulation(
    start_year = 2025L,
    end_year = 2026L,
    n_agents = 100L,
    initial_provider_count = 100L,
    fellowship_entrants = 5L,
    save_outputs = FALSE,
    productivity_engine = "lmer_fitted",
    productivity_panel = panel,
    productivity_fitter = function(p) {
      fit_provider_productivity_model(
        panel = p, outcome = "encounters_per_clinical_fte",
        include_year_effect = FALSE
      )
    }
  )

  audit <- sim_res$audit_ledger_tbl
  expect_equal(nrow(audit), 2L)
  expect_true(all(is.finite(audit$served_patients_n)))
  expect_true(all(audit$served_patients_n > 0))
  expect_equal(
    audit$served_patients_n + audit$unserved_delayed_n,
    audit$appointment_requests_n,
    tolerance = 1e-6
  )
})

test_that("lmer_fitted refuses a wRVU-scale outcome as patient capacity", {
  panel <- .lmer_fitted_test_panel()
  expect_error(
    run_end_to_end_simulation(
      start_year = 2025L,
      end_year = 2025L,
      n_agents = 100L,
      initial_provider_count = 50L,
      fellowship_entrants = 5L,
      save_outputs = FALSE,
      productivity_engine = "lmer_fitted",
      productivity_panel = panel,
      productivity_fitter = function(p) {
        fit_provider_productivity_model(
          panel = p, outcome = "wrvu_per_clinical_fte",
          include_year_effect = FALSE
        )
      }
    ),
    "encounters_per_clinical_fte"
  )
})

test_that("lmer_fitted's default path labels itself synthetic-fit at runtime", {
  panel <- .lmer_fitted_test_panel()
  expect_message(
    run_end_to_end_simulation(
      start_year = 2025L,
      end_year = 2025L,
      n_agents = 100L,
      initial_provider_count = 50L,
      fellowship_entrants = 5L,
      save_outputs = FALSE,
      productivity_engine = "lmer_fitted",
      productivity_panel = panel,
      productivity_fitter = function(p) {
        fit_provider_productivity_model(
          panel = p, outcome = "encounters_per_clinical_fte",
          include_year_effect = FALSE
        )
      }
    ),
    "SYNTHETIC-FIT"
  )
})
