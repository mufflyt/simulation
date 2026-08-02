# Integrated Workforce Microsimulation Orchestrator ----
#
# Ties the ported modules together into ONE reproducible run:
#   supply (12) x demand (13) x access (14), under reproducibility + provenance (10, 11).
#
# This is the entry point that replaces the ad-hoc top-level script in
# R/workforce.R with a seeded, scenario-driven, provenance-tagged pipeline that
# produces DISTRIBUTIONAL (not single-line) projections.
#
# Load order (all modules are plain sourced files):
#   source("R/10-repro_provenance.R"); source("R/11-canonical_and_joins.R")
#   source("R/12-provider_microsimulation.R"); source("R/13-demand_urps.R")
#   source("R/14-spatial_access_e2sfca.R"); source("R/15-run_workforce_microsimulation.R")

#' Load every microsimulation module in dependency order
#' @param r_dir Directory holding the numbered R modules.
#' @return (Invisibly) the sourced file paths.
#' @export
load_workforce_microsimulation <- function(r_dir = "R") {
  files <- c(
    "10-repro_provenance.R", "11-canonical_and_joins.R",
    "12-provider_microsimulation.R", "13-demand_urps.R",
    "14-spatial_access_e2sfca.R"
  )
  paths <- file.path(r_dir, files)
  for (p in paths) {
    if (!file.exists(p)) stop(sprintf("Module not found: %s", p), call. = FALSE)
    source(p)
  }
  invisible(paths)
}

# ---- Scenario definitions (cliff-style knobs) -----------------------------

#' Default supply scenarios (entrant / retirement / conversion knobs)
#'
#' Mirrors the scenario menu in R/supply_of_the_urogyn_workforce.R and cliff's
#' graduate/retirement sensitivity, but expressed as multiplicative knobs on the
#' microsimulation's stochastic engine.
#'
#' @param baseline_entrants Baseline annual entrants.
#' @return Named list of scenario parameter lists.
#' @export
default_supply_scenarios <- function(baseline_entrants = 55) {
  list(
    "Status Quo"            = list(entrants = baseline_entrants,            conversion = 1.00),
    "Enhanced Training"     = list(entrants = round(baseline_entrants * 1.10), conversion = 1.00),
    "70% Conversion"        = list(entrants = baseline_entrants,            conversion = 0.70),
    "Delayed Retirement"    = list(entrants = baseline_entrants,            conversion = 1.00, hazard_mult = 0.73),
    "Early Retirement"      = list(entrants = baseline_entrants,            conversion = 1.00, hazard_mult = 1.27)
  )
}

# ---- Example demand input (runs without external data) --------------------

#' Example women-65+ population series (illustrative, CONUS)
#'
#' Lets the pipeline run end-to-end without the Census NPP Excel file. Replace
#' with a canonical source via [resolve_canonical()] for production runs.
#'
#' @param years Integer years.
#' @param base_pop Women 65+ in the first year.
#' @param annual_growth Constant annual growth rate.
#' @return Tibble `year`, `population_65_plus`.
#' @export
example_population_65plus <- function(years = 2025:2050,
                                      base_pop = 30e6,
                                      annual_growth = 0.018) {
  tibble::tibble(
    year = years,
    population_65_plus = base_pop * (1 + annual_growth)^(years - min(years))
  )
}

# ---- Main orchestrator ----------------------------------------------------

#' Run the integrated workforce microsimulation
#'
#' @param initial_workforce Base-year FPMRS workforce size (cliff: ~1,295-1,339;
#'   the legacy model used 1,169).
#' @param years Integer projection years.
#' @param subspecialty Subspecialty label (selects the baseline hazard).
#' @param population_65plus Demand-driver tibble (`year`, `population_65_plus`).
#'   Defaults to [example_population_65plus()].
#' @param scenarios Named scenario list ([default_supply_scenarios()]).
#' @param n_iterations Monte-Carlo replicates per scenario.
#' @param baseline_entrants Baseline annual entrants (for replacement ratio).
#' @param output_dir If non-NULL, write provenance-tagged artifacts here.
#' @param seed RNG seed.
#' @param verbose Logical.
#' @return List with `supply`, `demand`, `coverage`, `concordance`, `outlook`,
#'   `run_id`, and `scenario_meta`.
#' @export
run_workforce_microsimulation <- function(initial_workforce = 1295,
                                          years = 2025:2050,
                                          subspecialty = "FPMRS",
                                          population_65plus = NULL,
                                          scenarios = NULL,
                                          n_iterations = 500,
                                          baseline_entrants = 55,
                                          output_dir = NULL,
                                          seed = 20260801L,
                                          verbose = TRUE) {
  mode <- resolve_reproducibility_mode()
  seed_microsimulation(seed, mode)
  run_id <- make_run_id(paste0("workforce_", tolower(subspecialty)), seed, mode)

  if (is.null(population_65plus)) population_65plus <- example_population_65plus(years)
  if (is.null(scenarios)) scenarios <- default_supply_scenarios(baseline_entrants)

  if (verbose) {
    logger::log_info("=== Workforce microsimulation run {run_id} (mode = {mode}) ===")
    logger::log_info("Subspecialty {subspecialty}; {length(scenarios)} scenarios; {n_iterations} iterations each")
  }

  baseline_rate <- microsim_baseline_rate(subspecialty)

  # --- Supply: one Monte-Carlo microsimulation per scenario ---
  supply_by_scenario <- purrr::imap_dfr(scenarios, function(params, scenario_name) {
    if (verbose) logger::log_info("Scenario: {scenario_name}")
    hazard_mult <- params$hazard_mult %||% 1.0

    # Scenario-specific hazard table (retirement-timing knob).
    hz_table <- build_hazard_table(baseline_rate * hazard_mult)

    # Re-run the microsim inline so we can apply the scenario hazard table.
    seed_microsimulation(seed, mode)  # same seed per scenario -> shared entrant/age draws
    iters <- vector("list", n_iterations)
    for (it in seq_len(n_iterations)) {
      agents <- initialize_provider_agents(initial_workforce, subspecialty, min(years))
      sim <- simulate_provider_career_once(
        agents, years, params$entrants, hz_table, params$conversion, subspecialty
      )
      iters[[it]] <- dplyr::mutate(sim$panel, iteration = it)
    }
    all_it <- dplyr::bind_rows(iters)

    all_it %>%
      dplyr::group_by(.data$year) %>%
      dplyr::summarise(
        headcount_median = stats::median(.data$headcount),
        headcount_lo = stats::quantile(.data$headcount, 0.025),
        headcount_hi = stats::quantile(.data$headcount, 0.975),
        effective_fte_median = stats::median(.data$effective_fte),
        effective_fte_lo = stats::quantile(.data$effective_fte, 0.025),
        effective_fte_hi = stats::quantile(.data$effective_fte, 0.975),
        mean_age_median = stats::median(.data$mean_age, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::mutate(scenario = scenario_name, subspecialty = subspecialty)
  })

  # --- Demand: three estimands (D1/D2/D3) ---
  demand_long <- compute_demand_denominators(population_65plus)

  # --- Coverage + concordance for the Status Quo scenario ---
  status_quo <- dplyr::filter(supply_by_scenario, .data$scenario == "Status Quo")
  base_year <- min(years)
  coverage <- compute_demand_coverage(
    status_quo, demand_long, supply_col = "effective_fte_median", base_year = base_year
  )
  concordance <- assess_demand_concordance(coverage)

  # --- Replacement-ratio outlook per scenario ---
  outlook <- purrr::imap_dfr(scenarios, function(params, scenario_name) {
    expected_departures <- initial_workforce * baseline_rate * (params$hazard_mult %||% 1.0)
    rr <- (params$entrants * params$conversion) / expected_departures
    tibble::tibble(
      scenario = scenario_name,
      annual_entrants_effective = params$entrants * params$conversion,
      expected_annual_departures = expected_departures,
      replacement_ratio = rr,
      outlook = classify_workforce_outlook(rr)
    )
  })

  result <- list(
    supply = supply_by_scenario,
    demand = demand_long,
    coverage = coverage,
    concordance = concordance,
    outlook = outlook,
    run_id = run_id,
    scenario_meta = list(
      subspecialty = subspecialty,
      initial_workforce = initial_workforce,
      years = range(years),
      baseline_rate = baseline_rate,
      n_iterations = n_iterations,
      mode = mode,
      seed = seed
    )
  )

  # --- Provenance-tagged persistence ---
  if (!is.null(output_dir)) {
    artifact_path <- file.path(output_dir, sprintf("workforce_microsim_%s.rds", run_id))
    write_artifact_with_provenance(
      result,
      artifact_path,
      inputs = list(initial_workforce, years, scenarios, population_65plus),
      code_paths = file.path("R", c(
        "12-provider_microsimulation.R", "13-demand_urps.R",
        "14-spatial_access_e2sfca.R", "15-run_workforce_microsimulation.R"
      )),
      run_id = run_id,
      mode = mode,
      source = sprintf("workforce microsimulation (%s)", subspecialty)
    )
  }

  if (verbose) {
    final_year <- max(years)
    sq_final <- dplyr::filter(status_quo, .data$year == final_year)
    logger::log_info("Status Quo {final_year}: effective FTE {round(sq_final$effective_fte_median)} ({round(sq_final$effective_fte_lo)}-{round(sq_final$effective_fte_hi)})")
    logger::log_info("Demand concordance robust = {concordance$robust}; adequacy trough year = {concordance$trough_year}")
  }

  result
}
