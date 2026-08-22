# URPS Workforce Microsimulation End-to-End Master Demonstration ------------
#
# Runs the national accounting demonstration for 2025-2035, fits a synthetic
# productivity smoke model, calibrates synthetic adequacy recovery, and prints
# the audit ledger. The readiness table identifies missing empirical sources.

if (base::requireNamespace("devtools", quietly = TRUE)) {
  devtools::load_all(".")
} else {
  base::stop("Run from the repository with `devtools` installed.")
}

base::message("=============================================================")
base::message(" URPS MICROSIMULATION: EMPIRICAL-HYBRID DEMONSTRATION")
base::message("=============================================================")

# Step 0: Build the empirical evidence lake
base::message("\n--- Building National Empirical Evidence DuckDB ---")
timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
evidence_dir <- base::file.path("artifacts", "evidence")
if (!base::dir.exists(evidence_dir)) {
  base::dir.create(evidence_dir, recursive = TRUE)
}
evidence_db <- base::file.path(
  evidence_dir,
  base::paste0("urps_demo_evidence_", timestamp, ".duckdb")
)
evidence_bundle <- build_urps_national_evidence_lake(
  duckdb_path = evidence_db,
  project_root = ".",
  overwrite = TRUE
)
base::message("Empirical parameter provenance:")
base::print(evidence_bundle$parameter_estimates)
base::message("Evidence-source readiness:")
base::print(evidence_bundle$source_readiness)

# Step 1: Run 8-step longitudinal simulation
base::message(
  "\n--- Executing 10-Year Longitudinal Simulation (2025-2035) ---"
)
sim_res <- run_end_to_end_simulation(
  start_year = 2025L,
  end_year = 2035L,
  n_agents = 1000L,
  initial_provider_count = 1200L,
  fellowship_entrants = 55L,
  app_delegation_rate = 0.15,
  medicaid_fee_ratio = 0.75,
  evidence_db = evidence_db
)

base::message("Patient-Flow Conservation Audit Ledger:")
base::print(sim_res$audit_ledger_tbl)

# Step 2: Fit Provider Productivity Model
base::message("\n--- Fitting Synthetic Productivity Smoke Model (lme4) ---")
base::set.seed(42)
n_obs <- 60
prov_ids <- base::rep(base::sprintf("P%02d", 1:15), each = 4)
years <- base::rep(2021:2024, times = 15)

panel_mock <- tibble::tibble(
  provider_id = prov_ids,
  year = years,
  clinical_fte = 1.0,
  clinical_hours_week = 40,
  age = stats::runif(n_obs, 35, 65),
  sex = base::sample(base::c("F", "M"), n_obs, replace = TRUE),
  academic = base::sample(
    base::c("Academic", "Private"), n_obs, replace = TRUE
  ),
  rural = base::sample(
    base::c("Urban", "Rural"), n_obs, replace = TRUE
  ),
  years_since_fellowship = stats::runif(n_obs, 1, 30),
  app_support_rate = stats::runif(n_obs, 0, 0.3),
  surgical_wrvu_share = stats::runif(n_obs, 0.1, 0.6),
  office_procedure_share = stats::runif(n_obs, 0.1, 0.4),
  new_visit_share = stats::runif(n_obs, 0.1, 0.3),
  wrvu_per_clinical_fte = stats::runif(n_obs, 3000, 8000),
  encounters_per_clinical_fte = stats::runif(n_obs, 1000, 3000),
  wrvu_per_clinical_hour = stats::runif(n_obs, 2, 5)
)

prod_model <- fit_provider_productivity_model(
  panel = panel_mock,
  outcome = "wrvu_per_clinical_fte",
  include_year_effect = FALSE
)
base::message("Productivity smoke-model diagnostics:")
base::print(prod_model$diagnostics)

# Step 3: Calibrate Latent Adequacy & Dual Denominator Sensitivity
base::message("\n--- Evaluating Synthetic Latent Adequacy Recovery ---")
synth_data <- generate_synthetic_adequacy_data(
  n_counties = 50L,
  seed = 20260821L
)
adequacy_fit <- calibrate_latent_adequacy(synth_data$county_data)
adequacy_eval <- evaluate_adequacy_synthetic_recovery(
  adequacy_fit,
  synth_data$true_parameters
)

base::message(base::sprintf(
  "Synthetic Recovery Pass Status: %s",
  base::ifelse(adequacy_eval$pass_status, "PASS", "FAIL")
))
base::message(base::sprintf(
  "Geographic Correlation: %.3f",
  adequacy_eval$geographic_correlation
))
base::message(base::sprintf(
  "National Adequacy Estimate: %.1f%%",
  100 * adequacy_fit$national_adequacy
))

# Step 4 (opt-in, not run by default): Policy migration scenario ------------
#
# run_end_to_end_simulation()'s default `policy_migration_scenario =
# "baseline"` above is an identity transform, so Steps 1-3 see no behavior
# change from the policy migration module. To see it drive real national
# multipliers, open a policy evidence DuckDB, ingest whatever public
# ACS/IRS/NPPES/LawAtlas evidence is available, and pass a non-baseline
# scenario. Left as a manual, commented block because it (a) opens a
# separate DuckDB round-trip on every demo run and (b) with no evidence
# ingested, the module still runs but degrades to its declared prior --
# nothing to demonstrate without at least one ingest call first.
#
# policy_evidence_db <- file.path(evidence_dir, "urps_policy_migration_demo.duckdb")
# policy_con <- open_policy_migration_duckdb(policy_evidence_db, overwrite = TRUE)
# # ingest_lawatlas_policies(policy_con, my_lawatlas_extract)
# # ingest_acs_pums_migration(policy_con, my_acs_pums_extract, year = 2024L)
# DBI::dbDisconnect(policy_con, shutdown = TRUE)
#
# sim_res_policy <- run_end_to_end_simulation(
#   start_year = 2025L,
#   end_year = 2035L,
#   n_agents = 1000L,
#   initial_provider_count = 1200L,
#   fellowship_entrants = 55L,
#   app_delegation_rate = 0.15,
#   medicaid_fee_ratio = 0.75,
#   evidence_db = evidence_db,
#   policy_migration_scenario = "combined_stress",
#   policy_evidence_db = policy_evidence_db
# )
# base::message("Policy migration diagnostics:")
# base::print(sim_res_policy$policy_migration_diagnostics)

base::message("\n=============================================================")
base::message(" DEMONSTRATION EXECUTED SUCCESSFULLY")
base::message("=============================================================")
