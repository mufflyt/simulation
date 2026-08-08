#!/usr/bin/env Rscript
#
# URPS model scenario & diagnostics scorecard
# ------------------------------------------------------------------------------
# A single runnable diagnostic that exercises the small "summariser" capabilities
# of the URPS workforce model -- each one characterizes ONE lever or distribution
# of the model and returns a compact table or vector:
#
#   demand levers    setting_scenario_summary()      care-setting shift (telehealth)
#                    prevention_volume_summary()     conservative-management shift
#   supply levers    delegation_capacity_sensitivity()  NPP capacity-factor sweep
#                    entrant_trajectory_scenarios()  NRMP-grounded entry arms
#   geography        agent_urbanicity_summary()      metro/nonmetro population split
#                    migration_drift_scenario()      rural<->urban drift lever
#   calibration      urps_hazard_exposure()          empirical retirement exposure
#
# These are exported, tested capabilities that no pipeline called -- the
# "dormant" surface tracked in tests/export-registry.csv. This script is their
# pipeline: a reproducible scorecard a user can run to see, side by side, how the
# model's demand, supply, geography, and calibration levers move. Every input is
# built from the package's own example helpers or frozen reference tables, so the
# script runs without any external data download.
#
# Run:  Rscript scripts/diagnostics/urps_model_diagnostics_scorecard.R

suppressWarnings(suppressMessages(pkgload::load_all(quiet = TRUE)))

## --- shared, dependency-free inputs -------------------------------------------
# The demand denominators and example service volumes are the spine shared by the
# three demand/supply summarisers; they come straight from the package examples.
demand <- suppressMessages(
  compute_demand_denominators(example_female_population_by_band()))
volumes <- suppressMessages(example_service_volumes(demand))

# A synthetic agent roster (state column only) to characterize the urbanicity mix.
states <- names(CONUS_STATE_URBANICITY)
set.seed(1)
agents <- data.frame(state = sample(states, 500L, replace = TRUE),
                     stringsAsFactors = FALSE)

## --- 1. demand-side scenario levers -------------------------------------------
setting <- suppressMessages(
  setting_scenario_summary(volumes, scenario_id = "telehealth_10pct",
                           year_filter = 2025L))
prevention <- suppressMessages(
  prevention_volume_summary(demand, scenario_id = "conservative_25pct",
                            year_filter = 2025L))

## --- 2. supply-side levers ----------------------------------------------------
delegation <- suppressMessages(delegation_capacity_sensitivity(volumes))
entrants   <- entrant_trajectory_scenarios(years = 2025:2050)

## --- 3. geography: distribution + drift lever ---------------------------------
urban_mix <- agent_urbanicity_summary(agents)
drift     <- migration_drift_scenario(rural_to_urban = 0.50)
drift_mat <- urps_migration_matrix(c("NY", "CA", "WV", "MT", "TX"),
                                   rural_to_urban = drift$rural_to_urban,
                                   urban_to_rural = drift$urban_to_rural)

## --- 4. calibration exposure underpinning retirement --------------------------
hazard_exposure <- urps_hazard_exposure()

## --- assemble & print ---------------------------------------------------------
banner <- function(x) cat("\n== ", x, " ", strrep("=", max(0, 60 - nchar(x))), "\n", sep = "")

banner("demand: care-setting shift (telehealth_10pct, 2025)")
print(setting)
banner("demand: conservative management (conservative_25pct, 2025)")
print(prevention)
banner("supply: delegation capacity-factor sweep")
print(delegation)
banner("supply: entrant trajectory arms (2025-2050)")
print(vapply(entrants, function(v) c(first = v[1], last = v[length(v)]), numeric(2)))
banner("geography: urbanicity population split")
print(urban_mix)
banner("geography: rural<->urban drift migration matrix")
print(drift_mat)
banner("calibration: empirical retirement hazard exposure")
print(hazard_exposure)

invisible(list(setting = setting, prevention = prevention, delegation = delegation,
               entrants = entrants, urban_mix = urban_mix, drift_matrix = drift_mat,
               hazard_exposure = hazard_exposure))
