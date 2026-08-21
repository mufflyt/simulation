#' Build a model-ready state-year policy and migration panel
#'
#' Combines six independent evidence arms staged by the policy DuckDB loader.
#' Missing arms remain missing and are reported; they are never silently
#' replaced with zero. NRMP data enter only when explicitly authorized for
#' model use.
#'
#' @param connection Open policy evidence DuckDB connection.
#' @param state_crosswalk Table with `state` and `state_fips`.
#' @param minimum_nppes_snapshots Minimum snapshots required for relocation.
#'
#' @return State-year evidence tibble.
#' @export
build_policy_migration_evidence <- function(
    connection,
    state_crosswalk,
    minimum_nppes_snapshots = 3L) {
  .policy_require_columns(
    state_crosswalk,
    c("state", "state_fips"),
    "State crosswalk"
  )
  crosswalk <- state_crosswalk |>
    dplyr::transmute(
      state = stringr::str_to_upper(base::as.character(.data$state)),
      state_fips = stringr::str_pad(
        base::as.character(.data$state_fips), 2L, pad = "0"
      )
    ) |>
    dplyr::distinct()
  available <- DBI::dbListTables(connection)
  base::message(
    "Policy evidence DuckDB tables: ",
    base::paste(available, collapse = ", ")
  )
  pieces <- list()
  if ("acs_pums_migration" %in% available) {
    pums <- dplyr::tbl(connection, "acs_pums_migration") |>
      dplyr::filter(
        .data$sex == "female",
        .data$age_band %in% c("55-64", "65-74", "75+")
      ) |>
      dplyr::collect()
    destination <- pums |>
      dplyr::filter(.data$moved_interstate) |>
      dplyr::group_by(.data$year, .data$destination_state_fips) |>
      dplyr::summarise(
        older_female_inflow = base::sum(.data$weighted_people),
        .groups = "drop"
      ) |>
      dplyr::rename(state_fips = .data$destination_state_fips)
    origin <- pums |>
      dplyr::filter(.data$moved_interstate) |>
      dplyr::group_by(.data$year, .data$origin_state_fips) |>
      dplyr::summarise(
        older_female_outflow = base::sum(.data$weighted_people),
        .groups = "drop"
      ) |>
      dplyr::rename(state_fips = .data$origin_state_fips)
    pieces$pums <- dplyr::full_join(
      destination,
      origin,
      by = c("year", "state_fips")
    ) |>
      dplyr::mutate(
        older_female_net_migration = dplyr::coalesce(
          .data$older_female_inflow, 0
        ) - dplyr::coalesce(.data$older_female_outflow, 0)
      )
    base::message("Built ACS PUMS older-female migration evidence.")
  }
  if ("acs_state_migration_flows" %in% available) {
    flows <- dplyr::tbl(connection, "acs_state_migration_flows") |>
      dplyr::collect()
    flow_in <- flows |>
      dplyr::group_by(.data$year, .data$destination_state_fips) |>
      dplyr::summarise(
        acs_total_inflow = base::sum(.data$flow),
        acs_flow_variance = base::sum(
          (.data$margin_of_error / 1.645)^2,
          na.rm = TRUE
        ),
        .groups = "drop"
      ) |>
      dplyr::rename(state_fips = .data$destination_state_fips)
    flow_out <- flows |>
      dplyr::group_by(.data$year, .data$origin_state_fips) |>
      dplyr::summarise(
        acs_total_outflow = base::sum(.data$flow),
        .groups = "drop"
      ) |>
      dplyr::rename(state_fips = .data$origin_state_fips)
    pieces$acs_flows <- dplyr::full_join(
      flow_in,
      flow_out,
      by = c("year", "state_fips")
    ) |>
      dplyr::mutate(
        acs_net_migration = dplyr::coalesce(.data$acs_total_inflow, 0) -
          dplyr::coalesce(.data$acs_total_outflow, 0),
        acs_net_migration_se = base::sqrt(.data$acs_flow_variance)
      )
    base::message("Built ACS aggregate migration evidence.")
  }
  if ("irs_migration_flows" %in% available) {
    irs <- dplyr::tbl(connection, "irs_migration_flows") |>
      dplyr::collect()
    irs_in <- irs |>
      dplyr::group_by(.data$year, .data$destination_state_fips) |>
      dplyr::summarise(
        irs_in_exemptions = base::sum(.data$exemptions),
        irs_in_agi = base::sum(.data$adjusted_gross_income),
        .groups = "drop"
      ) |>
      dplyr::rename(state_fips = .data$destination_state_fips)
    irs_out <- irs |>
      dplyr::group_by(.data$year, .data$origin_state_fips) |>
      dplyr::summarise(
        irs_out_exemptions = base::sum(.data$exemptions),
        .groups = "drop"
      ) |>
      dplyr::rename(state_fips = .data$origin_state_fips)
    pieces$irs <- dplyr::full_join(
      irs_in,
      irs_out,
      by = c("year", "state_fips")
    ) |>
      dplyr::mutate(
        irs_net_exemptions = dplyr::coalesce(
          .data$irs_in_exemptions, 0
        ) - dplyr::coalesce(.data$irs_out_exemptions, 0),
        irs_in_agi_per_exemption = dplyr::if_else(
          .data$irs_in_exemptions > 0,
          .data$irs_in_agi / .data$irs_in_exemptions,
          NA_real_
        )
      )
    base::message("Built IRS migration validation evidence.")
  }
  if ("nppes_snapshots" %in% available) {
    snapshots <- dplyr::tbl(connection, "nppes_snapshots") |>
      dplyr::collect() |>
      dplyr::group_by(.data$npi) |>
      dplyr::arrange(.data$snapshot_date, .by_group = TRUE) |>
      dplyr::mutate(
        previous_state = dplyr::lag(.data$practice_state),
        next_state = dplyr::lead(.data$practice_state),
        snapshot_count = dplyr::n(),
        move_candidate = !base::is.na(.data$previous_state) &
          .data$practice_state != .data$previous_state,
        move_confirmed = .data$move_candidate &
          .data$snapshot_count >= minimum_nppes_snapshots &
          !base::is.na(.data$next_state) &
          .data$next_state == .data$practice_state
      ) |>
      dplyr::ungroup() |>
      dplyr::filter(.data$move_candidate) |>
      dplyr::mutate(year = base::as.integer(base::format(
        .data$snapshot_date, "%Y"
      )))
    move_in <- snapshots |>
      dplyr::group_by(.data$year, state = .data$practice_state) |>
      dplyr::summarise(
        provider_moves_in = base::sum(.data$move_confirmed),
        provider_move_candidates_in = dplyr::n(),
        .groups = "drop"
      )
    move_out <- snapshots |>
      dplyr::group_by(.data$year, state = .data$previous_state) |>
      dplyr::summarise(
        provider_moves_out = base::sum(.data$move_confirmed),
        provider_move_candidates_out = dplyr::n(),
        .groups = "drop"
      )
    pieces$nppes <- dplyr::full_join(
      move_in,
      move_out,
      by = c("year", "state")
    ) |>
      dplyr::mutate(
        provider_net_moves = dplyr::coalesce(
          .data$provider_moves_in, 0L
        ) - dplyr::coalesce(.data$provider_moves_out, 0L)
      )
    base::message("Built confirmed NPPES relocation evidence.")
  }
  if ("lawatlas_state_policies" %in% available) {
    policies <- dplyr::tbl(connection, "lawatlas_state_policies") |>
      dplyr::collect() |>
      dplyr::filter(.data$enforceable) |>
      dplyr::mutate(
        start_year = base::as.integer(base::format(
          .data$effective_date, "%Y"
        )),
        end_year = dplyr::if_else(
          base::is.na(.data$end_date),
          base::as.integer(base::format(base::Sys.Date(), "%Y")),
          base::as.integer(base::format(.data$end_date, "%Y"))
        ),
        year = purrr::map2(.data$start_year, .data$end_year, base::seq.int)
      ) |>
      tidyr::unnest(.data$year)
    pieces$policy <- policies |>
      dplyr::group_by(.data$state, .data$year) |>
      dplyr::summarise(
        legislative_climate = base::sum(.data$policy_value),
        active_policy_domains = dplyr::n_distinct(.data$policy_domain),
        .groups = "drop"
      )
    base::message("Built longitudinal legislative-climate evidence.")
  }
  fips_pieces <- pieces[base::names(pieces) %in% c(
    "pums", "acs_flows", "irs"
  )]
  state_pieces <- pieces[base::names(pieces) %in% c("nppes", "policy")]
  fips_panel <- purrr::reduce(
    fips_pieces,
    dplyr::full_join,
    by = c("year", "state_fips"),
    .init = tibble::tibble(year = integer(), state_fips = character())
  ) |>
    dplyr::left_join(crosswalk, by = "state_fips")
  state_panel <- purrr::reduce(
    state_pieces,
    dplyr::full_join,
    by = c("year", "state"),
    .init = tibble::tibble(year = integer(), state = character())
  )
  evidence <- dplyr::full_join(
    fips_panel,
    state_panel,
    by = c("year", "state")
  )
  evidence <- .policy_add_columns(
    evidence,
    list(
      older_female_net_migration = NA_real_,
      acs_net_migration = NA_real_,
      irs_net_exemptions = NA_real_,
      provider_net_moves = NA_real_,
      legislative_climate = NA_real_
    )
  ) |>
    dplyr::arrange(.data$state, .data$year) |>
    dplyr::mutate(
      migration_direction_agrees = base::sign(
        .data$older_female_net_migration
      ) == base::sign(.data$irs_net_exemptions),
      evidence_arm_count = base::rowSums(!base::is.na(dplyr::pick(
        dplyr::any_of(c(
          "older_female_net_migration", "acs_net_migration",
          "irs_net_exemptions", "provider_net_moves",
          "legislative_climate"
        ))
      )))
    )
  .policy_replace_table(
    connection,
    "policy_migration_state_year",
    evidence
  )
  base::message(
    "Built model-ready evidence panel with ",
    scales::comma(base::nrow(evidence)), " state-year rows."
  )
  evidence
}

#' Calibrate relocation response to legislative policy
#'
#' Estimates a two-way fixed-effect association between the legislative index
#' and confirmed NPPES net moves per provider FTE. If there is insufficient
#' identifying variation, the function returns the declared scenario prior and
#' records that no empirical estimate was used.
#'
#' @param state_year_history Historical state-year provider FTE.
#' @param evidence Policy migration evidence panel.
#' @param prior_mean Prior log response per policy-index unit.
#' @param prior_sd Prior standard deviation.
#' @param minimum_rows Minimum complete state-years.
#'
#' @return A named calibration list.
#' @export
calibrate_legislative_relocation <- function(
    state_year_history,
    evidence,
    prior_mean = -0.06,
    prior_sd = 0.025,
    minimum_rows = 20L) {
  .policy_require_columns(
    state_year_history,
    c("state", "year", "provider_fte"),
    "State-year provider history"
  )
  calibration_panel <- state_year_history |>
    dplyr::select(.data$state, .data$year, .data$provider_fte) |>
    dplyr::left_join(
      evidence |>
        dplyr::select(
          .data$state,
          .data$year,
          .data$provider_net_moves,
          .data$legislative_climate
        ),
      by = c("state", "year")
    ) |>
    dplyr::filter(
      !base::is.na(.data$provider_net_moves),
      !base::is.na(.data$legislative_climate),
      !base::is.na(.data$provider_fte),
      .data$provider_fte > 0
    ) |>
    dplyr::mutate(
      provider_net_move_rate = .data$provider_net_moves /
        .data$provider_fte,
      state = base::factor(.data$state),
      year = base::factor(.data$year)
    )
  sufficient <- base::nrow(calibration_panel) >= minimum_rows &&
    dplyr::n_distinct(calibration_panel$legislative_climate) >= 2L &&
    dplyr::n_distinct(calibration_panel$state) >= 2L &&
    dplyr::n_distinct(calibration_panel$year) >= 2L
  if (!sufficient) {
    base::message(
      "Insufficient NPPES-policy variation; retaining relocation prior."
    )
    return(list(
      provider_policy_mean = prior_mean,
      provider_policy_sd = prior_sd,
      empirical = FALSE,
      complete_rows = base::nrow(calibration_panel),
      method = "declared_scenario_prior"
    ))
  }
  fitted <- stats::lm(
    provider_net_move_rate ~ legislative_climate + state + year,
    data = calibration_panel,
    weights = provider_fte
  )
  coefficient <- stats::coef(fitted)[["legislative_climate"]]
  standard_error <- base::sqrt(stats::vcov(fitted)[
    "legislative_climate", "legislative_climate"
  ])
  if (!base::is.finite(coefficient) || !base::is.finite(standard_error)) {
    base::message("Relocation fit was singular; retaining prior.")
    return(list(
      provider_policy_mean = prior_mean,
      provider_policy_sd = prior_sd,
      empirical = FALSE,
      complete_rows = base::nrow(calibration_panel),
      method = "declared_scenario_prior"
    ))
  }
  base::message(
    "Estimated NPPES relocation response from ",
    scales::comma(base::nrow(calibration_panel)), " state-years."
  )
  list(
    provider_policy_mean = coefficient,
    provider_policy_sd = base::max(standard_error, 0.001),
    empirical = TRUE,
    complete_rows = base::nrow(calibration_panel),
    method = "nppes_two_way_fixed_effects"
  )
}

#' Apply empirical state policy and migration scenarios
#'
#' @param state_year Baseline state-year simulation inputs.
#' @param evidence State-year evidence from
#'   `build_policy_migration_evidence()`.
#' @param scenario Scenario names.
#' @param draws Uncertainty draws.
#' @param seed Reproducible seed.
#' @param calibration Optional output from
#'   `calibrate_legislative_relocation()`.
#' @param save_path Optional directory for timestamped output.
#'
#' @return State-year scenario draws.
#' @export
simulate_policy_migration_scenarios <- function(
    state_year,
    evidence,
    scenario = c(
      "baseline", "observed_migration", "migration_stress",
      "legislative_climate", "combined_stress"
    ),
    draws = 500L,
    seed = 20260821L,
    calibration = NULL,
    save_path = NULL) {
  required <- c(
    "state", "year", "female_older_population", "pfd_demand",
    "provider_fte", "fellowship_applications"
  )
  .policy_require_columns(state_year, required, "State-year baseline")
  allowed <- c(
    "baseline", "observed_migration", "migration_stress",
    "legislative_climate", "combined_stress"
  )
  if (base::any(!scenario %in% allowed)) {
    base::stop("Unknown policy migration scenario.", call. = FALSE)
  }
  if (draws < 1L || draws != base::as.integer(draws)) {
    base::stop("draws must be a positive integer.", call. = FALSE)
  }
  joined <- state_year |>
    dplyr::left_join(evidence, by = c("state", "year")) |>
    dplyr::mutate(
      observed_migration_rate = dplyr::if_else(
        .data$female_older_population > 0,
        .data$older_female_net_migration /
          .data$female_older_population,
        NA_real_
      )
    )
  base::set.seed(seed)
  provider_policy_mean <- if (base::is.null(calibration)) {
    -0.06
  } else {
    calibration$provider_policy_mean
  }
  provider_policy_sd <- if (base::is.null(calibration)) {
    0.025
  } else {
    calibration$provider_policy_sd
  }
  draw_table <- tibble::tibble(draw = base::seq_len(draws)) |>
    dplyr::mutate(
      stress_increment = stats::runif(draws, 0.15, 0.25),
      provider_policy_beta = stats::rnorm(
        draws,
        provider_policy_mean,
        provider_policy_sd
      ),
      applicant_policy_beta = stats::rnorm(draws, -0.08, 0.035)
    )
  simulated <- tidyr::crossing(
    joined,
    scenario = scenario,
    draw_table
  ) |>
    dplyr::mutate(
      migration_active = .data$scenario %in% c(
        "observed_migration", "migration_stress", "combined_stress"
      ),
      stress_active = .data$scenario %in% c(
        "migration_stress", "combined_stress"
      ),
      policy_active = .data$scenario %in% c(
        "legislative_climate", "combined_stress"
      ),
      empirical_migration_rate = dplyr::if_else(
        .data$migration_active,
        dplyr::coalesce(.data$observed_migration_rate, 0),
        0
      ),
      stress_rate = dplyr::if_else(
        .data$stress_active & .data$empirical_migration_rate > 0,
        .data$stress_increment,
        0
      ),
      migration_multiplier = base::pmax(
        0,
        1 + .data$empirical_migration_rate + .data$stress_rate
      ),
      policy_score = dplyr::if_else(
        .data$policy_active,
        dplyr::coalesce(.data$legislative_climate, 0),
        0
      ),
      provider_multiplier = base::exp(
        .data$provider_policy_beta * .data$policy_score
      ),
      applicant_multiplier = base::exp(
        .data$applicant_policy_beta * .data$policy_score
      ),
      pfd_demand_scenario = .data$pfd_demand *
        .data$migration_multiplier,
      provider_fte_scenario = .data$provider_fte *
        .data$provider_multiplier,
      fellowship_applications_scenario =
        .data$fellowship_applications *
        .data$applicant_multiplier,
      demand_change = .data$pfd_demand_scenario - .data$pfd_demand,
      provider_fte_change = .data$provider_fte_scenario -
        .data$provider_fte,
      application_change = .data$fellowship_applications_scenario -
        .data$fellowship_applications
    )
  base::message(
    "Generated ", scales::comma(base::nrow(simulated)),
    " policy scenario rows."
  )
  if (!base::is.null(save_path)) {
    timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
    saved_file <- base::file.path(
      save_path,
      base::paste0("policy_migration_scenarios_", timestamp, ".csv")
    )
    readr::write_csv(simulated, saved_file)
    base::message("Saved exact file path: ", saved_file)
  }
  simulated
}

#' Summarise state policy and migration scenario draws
#'
#' @param simulated Tibble produced by [simulate_policy_migration_scenarios()].
#'
#' @return Summary statistics (mean, SD, median, p25, p75) per state, year, and scenario.
#' @export
summarize_policy_migration_scenarios <- function(simulated) {
  simulated |>
    dplyr::group_by(.data$state, .data$year, .data$scenario) |>
    dplyr::summarise(
      mean_demand_change = base::mean(.data$demand_change, na.rm = TRUE),
      sd_demand_change = stats::sd(.data$demand_change, na.rm = TRUE),
      median_demand_change = stats::median(.data$demand_change, na.rm = TRUE),
      p25_demand_change = stats::quantile(.data$demand_change, 0.25, na.rm = TRUE),
      p75_demand_change = stats::quantile(.data$demand_change, 0.75, na.rm = TRUE),
      mean_provider_change = base::mean(.data$provider_fte_change, na.rm = TRUE),
      sd_provider_change = stats::sd(.data$provider_fte_change, na.rm = TRUE),
      median_provider_change = stats::median(.data$provider_fte_change, na.rm = TRUE),
      p25_provider_change = stats::quantile(.data$provider_fte_change, 0.25, na.rm = TRUE),
      p75_provider_change = stats::quantile(.data$provider_fte_change, 0.75, na.rm = TRUE),
      .groups = "drop"
    )
}
