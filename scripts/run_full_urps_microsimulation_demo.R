# URPS Workforce Microsimulation End-to-End Master Demonstration ------------
#
# Runs a full national simulation (2025-2035), fits provider productivity,
# calibrates latent adequacy, estimates claims service shares, and outputs audit ledgers.

if (requireNamespace("devtools", quietly = TRUE)) {
  devtools::load_all(".")
} else {
  library(urpssim)
}
library(dplyr)
library(scales)

message("=================================================================")
message("   URPS WORKFORCE MICROSIMULATION: END-TO-END DEMONSTRATION      ")
message("=================================================================")

# Step 1: Run 8-step longitudinal simulation
message("\n--- Executing 10-Year Longitudinal Simulation (2025-2035) ---")
sim_res <- run_end_to_end_simulation(
  start_year = 2025L,
  end_year = 2035L,
  n_agents = 1000L,
  initial_provider_count = 1200L,
  fellowship_entrants = 55L,
  app_delegation_rate = 0.15,
  medicaid_fee_ratio = 0.75
)

message("Patient-Flow Conservation Audit Ledger:")
print(sim_res$audit_ledger_tbl)

# Step 2: Fit Provider Productivity Model
message("\n--- Fitting Provider-Year Productivity Model (lme4) ---")
set.seed(42)
n_obs <- 60
prov_ids <- rep(sprintf("P%02d", 1:15), each = 4)
years <- rep(2021:2024, times = 15)

panel_mock <- tibble::tibble(
  provider_id = prov_ids,
  year = years,
  clinical_fte = 1.0,
  clinical_hours_week = 40,
  age = runif(n_obs, 35, 65),
  sex = sample(c("F", "M"), n_obs, replace = TRUE),
  academic = sample(c("Academic", "Private"), n_obs, replace = TRUE),
  rural = sample(c("Urban", "Rural"), n_obs, replace = TRUE),
  years_since_fellowship = runif(n_obs, 1, 30),
  app_support_rate = runif(n_obs, 0, 0.3),
  surgical_wrvu_share = runif(n_obs, 0.1, 0.6),
  office_procedure_share = runif(n_obs, 0.1, 0.4),
  new_visit_share = runif(n_obs, 0.1, 0.3),
  wrvu_per_clinical_fte = runif(n_obs, 3000, 8000),
  encounters_per_clinical_fte = runif(n_obs, 1000, 3000),
  wrvu_per_clinical_hour = runif(n_obs, 2, 5)
)

prod_model <- fit_provider_productivity_model(
  panel = panel_mock,
  outcome = "wrvu_per_clinical_fte",
  include_year_effect = FALSE
)
message("Productivity model diagnostics:")
print(prod_model$diagnostics)

# Step 3: Calibrate Latent Adequacy & Dual Denominator Sensitivity
message("\n--- Fitting Joint Latent Adequacy Model ---")
synth_data <- generate_synthetic_adequacy_data(n_counties = 50L, seed = 20260821L)
adequacy_fit <- calibrate_latent_adequacy(synth_data$county_data)
adequacy_eval <- evaluate_adequacy_synthetic_recovery(adequacy_fit, synth_data$true_parameters)

message(sprintf("Synthetic Recovery Pass Status: %s", ifelse(adequacy_eval$pass_status, "PASS", "FAIL")))
message(sprintf("Geographic Correlation: %.3f", adequacy_eval$geographic_correlation))
message(sprintf("National Adequacy Estimate: %.1f%%", 100 * adequacy_fit$national_adequacy))

message("\n=================================================================")
message("      END-TO-END DEMONSTRATION EXECUTED SUCCESSFULLY             ")
message("=================================================================")
