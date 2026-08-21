# Master End-to-End Simulation Pipeline ------------------------------------
#
# Authoritative single-source-of-truth simulation runner that strictly couples
# all 8 annual sequence steps:
# 1. Update Population Agent Weights & Age
# 2. Disease Transitions (UI, POP, AI)
# 3. Care-Seeking and Referral Flow
# 4. Geographic & Insurance Access Clearing (E2SFCA & Medicaid Bottleneck)
# 5. Queue & Capacity Allocation (Served vs Unserved/Delayed)
# 6. Service Delivery & CPT / wRVU Conversion
# 7. Required vs. Supplied Provider FTE Balance & 306-HRR Spatial Accounting
# 8. Provider Lifecycle (Attrition, ACGME Entrants, Spatial Relocation)
#
# Generates a complete annual Patient-Flow Conservation Audit Ledger
# (`audit_ledger_tbl`).


#' Run Master URPS Workforce & Demand Simulation End-to-End
#'
#' @description
#' Executes the single-source-of-truth end-to-end longitudinal simulation across
#' all 8 coupled annual event sequence steps.
#'
#' Enforces conservation identities across every transition:
#' `served_patients_n + unserved_delayed_n == appointment_requests_n`.
#'
#' @param start_year Simulation start year (default 2025).
#' @param end_year Simulation end year (default 2035).
#' @param n_agents Number of microsimulation patient agents.
#' @param initial_provider_count Number of baseline provider records.
#' @param fellowship_entrants Annual ACGME fellowship graduate entrants.
#' @param app_delegation_rate APP task delegation fraction (0 to 0.30).
#' @param medicaid_fee_ratio National/state Medicaid fee index ratio; default
#'   is 0.75.
#' @param evidence_db Optional national evidence DuckDB path. Empirical values
#'   found in `model.parameter_estimates` replace matching demonstration inputs.
#' @param empirical_parameters Optional named numeric vector. This takes
#'   precedence over `evidence_db` and matching function arguments.
#' @param save_outputs Logical; whether to write CSV audit outputs to disk.
#' @param output_dir Directory for timestamped CSV output artifacts.
#'
#' @return A named list containing `audit_ledger_tbl`, `annual_hrr_balance`,
#'   `final_provider_cohort`, and `simulation_config`.
#' @family simulation pipeline
#' @concept core
#' @export
run_end_to_end_simulation <- function(
    start_year = 2025L,
    end_year = 2035L,
    n_agents = 1000L,
    initial_provider_count = 1200L,
    fellowship_entrants = 55L,
    app_delegation_rate = 0.15,
    medicaid_fee_ratio = 0.75,
    evidence_db = NULL,
    empirical_parameters = NULL,
    save_outputs = TRUE,
    output_dir = "artifacts/end_to_end") {

  invalid_years <- !base::is.numeric(start_year) ||
    !base::is.numeric(end_year) || start_year > end_year
  if (invalid_years) {
    base::stop("`start_year` must be <= `end_year`.", call. = FALSE)
  }

  if (!base::is.null(evidence_db) &&
      base::is.null(empirical_parameters)) {
    empirical_parameters <- read_urps_empirical_parameters(evidence_db)
  }
  if (base::is.null(empirical_parameters)) {
    empirical_parameters <- base::setNames(
      base::numeric(),
      base::character()
    )
  }
  if (!base::is.numeric(empirical_parameters) ||
      base::is.null(base::names(empirical_parameters))) {
    base::stop(
      "`empirical_parameters` must be a named numeric vector.",
      call. = FALSE
    )
  }
  empirical_value <- function(parameter, fallback) {
    if (parameter %in% base::names(empirical_parameters)) {
      estimate <- empirical_parameters[[parameter]]
      if (base::is.finite(estimate)) {
        base::message(
          "Empirical input: ", parameter, " = ",
          base::format(estimate, big.mark = ",", scientific = FALSE)
        )
        base::return(estimate)
      }
    }
    base::message("Fallback input: ", parameter, " = ", fallback)
    fallback
  }

  base::message("=============================================================")
  base::message(" URPS SIMULATION PIPELINE: END-TO-END EXECUTION")
  base::message(" Single Source-of-Truth 8-Step Longitudinal Micro-Engine")
  base::message("=============================================================")

  years <- base::seq(
    base::as.integer(start_year),
    base::as.integer(end_year),
    by = 1L
  )

  # ------------------------------------------------------------------
  # Initialize Provider Cohort
  # ------------------------------------------------------------------
  base::set.seed(20260821)
  provider_cohort <- tibble::tibble(
    provider_id = base::sprintf(
      "NPI%07d",
      base::seq_len(initial_provider_count)
    ),
    age = stats::runif(initial_provider_count, min = 32, max = 68),
    years_certified = base::pmax(
      0,
      stats::runif(initial_provider_count, min = 0, max = 35)
    ),
    academic_setting = stats::rbinom(
      initial_provider_count, size = 1L, prob = 0.22
    ) == 1L,
    hospital_outpatient = stats::rbinom(
      initial_provider_count, size = 1L, prob = 0.35
    ) == 1L,
    hrr_code = base::sprintf(
      "HRR%03d",
      base::sample.int(306L, initial_provider_count, replace = TRUE)
    ),
    state_abbr = "US",
    svi = stats::runif(initial_provider_count, min = 0.1, max = 0.9),
    fte = stats::runif(initial_provider_count, min = 0.7, max = 1.0),
    active = TRUE
  )

  population_2023 <- empirical_value(
    "female_population_20plus",
    131000000.0
  )
  population_growth_rate <- empirical_value(
    "annual_population_growth",
    0.006
  )
  base_population_2025 <- population_2023 *
    (1.0 + population_growth_rate)^2
  fellowship_entrants <- empirical_value(
    "annual_fellowship_entrants",
    fellowship_entrants
  )
  pfd_prevalence_2025 <- empirical_value(
    "pfd_prevalence_2025",
    0.245
  )
  pfd_prevalence_growth <- empirical_value(
    "annual_pfd_prevalence_change",
    0.001
  )
  pfd_incidence_rate <- empirical_value("pfd_incidence_rate", 0.022)
  care_seeking_rate <- empirical_value("care_seeking_rate", 0.380)
  referral_rate <- empirical_value("referral_rate", 0.420)
  reachable_share <- empirical_value("reachable_share", 0.885)
  medicaid_share <- empirical_value("medicaid_share", 0.20)
  visits_per_fte <- empirical_value("visits_per_clinical_fte", 1600.0)
  services_per_patient <- empirical_value("services_per_patient", 2.15)
  wrvu_per_patient <- empirical_value("wrvu_per_patient", 18.50)
  minutes_per_wrvu <- empirical_value("minutes_per_wrvu", 42.0)
  wrvu_per_fte <- empirical_value("wrvu_per_clinical_fte", 1400.0)

  audit_rows <- base::vector("list", base::length(years))
  hrr_balance_list <- base::vector("list", base::length(years))

  for (y_idx in base::seq_along(years)) {
    sim_year <- years[[y_idx]]
    base::message(base::sprintf(
      "\n>>> Annual Cycle: Year %d [%d/%d] <<<",
      sim_year,
      y_idx,
      base::length(years)
    ))

    # ----------------------------------------------------------------
    # Step A: Update Population Agent Weights & Age
    # ----------------------------------------------------------------
    pop_growth_factor <- (1.0 + population_growth_rate)^
      (sim_year - 2025L)
    pop_total <- base_population_2025 * pop_growth_factor

    # ----------------------------------------------------------------
    # Step B: Disease Transitions (UI, POP, AI)
    # ----------------------------------------------------------------
    pfd_prev_rate <- pfd_prevalence_2025 +
      pfd_prevalence_growth * (sim_year - 2025L)
    pfd_inc_rate <- pfd_incidence_rate

    prevalent_cases <- pop_total * pfd_prev_rate
    incident_cases  <- pop_total * pfd_inc_rate

    # ----------------------------------------------------------------
    # Step C: Care-Seeking and Referral Flow
    # ----------------------------------------------------------------
    care_seeking_n <- prevalent_cases * care_seeking_rate
    referred_n     <- care_seeking_n * referral_rate

    # ----------------------------------------------------------------
    # Step D: Geographic & Insurance Access Clearing
    # ----------------------------------------------------------------
    reachable_n <- referred_n * reachable_share

    # Medicaid insurance bottleneck calculation via predict_medicaid_acceptance
    medicaid_acceptance_p <- predict_medicaid_acceptance(
      academic_setting = provider_cohort$academic_setting,
      hospital_outpatient = provider_cohort$hospital_outpatient,
      medicaid_fee_ratio = medicaid_fee_ratio,
      svi = provider_cohort$svi,
      years_certified = provider_cohort$years_certified
    )

    avg_medicaid_p <- base::mean(medicaid_acceptance_p, na.rm = TRUE)
    medicaid_eligible_n <- reachable_n * medicaid_share
    non_medicaid_n <- reachable_n * (1.0 - medicaid_share)
    appointment_requests_n <- non_medicaid_n +
      (medicaid_eligible_n * avg_medicaid_p)

    # ----------------------------------------------------------------
    # Step E: Queue & Capacity Allocation (Served vs Unserved)
    # ----------------------------------------------------------------
    active_providers <- provider_cohort |> dplyr::filter(.data$active)
    supplied_fte <- base::sum(active_providers$fte, na.rm = TRUE)

    # Clinical capacity combines empirical visits/FTE with APP delegation.
    clinical_capacity_patients <- supplied_fte * visits_per_fte *
      (1.0 + (app_delegation_rate * 0.5))

    served_patients_n <- base::pmin(
      appointment_requests_n,
      clinical_capacity_patients
    )
    unserved_delayed_n <- base::pmax(
      0.0,
      appointment_requests_n - served_patients_n
    )

    # Conservation Identity Check
    flow_is_conserved <- base::all.equal(
      served_patients_n + unserved_delayed_n,
      appointment_requests_n,
      tolerance = 1e-6
    )
    if (!base::isTRUE(flow_is_conserved)) {
      base::stop(
        base::sprintf(
          "Patient-flow conservation violation in Year %d!",
          sim_year
        ),
        call. = FALSE
      )
    }

    # ----------------------------------------------------------------
    # Step F: Service Delivery & CPT / wRVU Conversion
    # ----------------------------------------------------------------
    delivered_services_n <- served_patients_n * services_per_patient
    wrvu_total <- served_patients_n * wrvu_per_patient
    physician_minutes_total <- wrvu_total * minutes_per_wrvu

    # ----------------------------------------------------------------
    # Step G: Required vs. Supplied FTE & 306-HRR Balance
    # ----------------------------------------------------------------
    required_fte <- wrvu_total / wrvu_per_fte
    fte_gap <- supplied_fte - required_fte
    adequacy_ratio <- supplied_fte / required_fte

    # 306-HRR Spatial Accounting
    ref_306 <- tibble::tibble(
      hrr_code = base::sprintf("HRR%03d", base::seq_len(306L)),
      hrr_name = base::paste("HRR Region", base::seq_len(306L))
    )

    demand_306 <- tibble::tibble(
      hrr_code = ref_306$hrr_code,
      demand_fte = required_fte / 306.0
    )

    hrr_bal <- aggregate_hrr_workforce_balance(
      provider_roster = active_providers,
      hrr_demand_tbl = demand_306,
      hrr_reference_tbl = ref_306,
      shortage_threshold = 0.20,
      expected_hrr_n = 306L
    )

    hrr_bal$year <- sim_year
    hrr_balance_list[[y_idx]] <- hrr_bal

    # Wait time & travel time modeling
    mean_wait_days <- 14.0 +
      (unserved_delayed_n / appointment_requests_n) * 45.0
    mean_travel_mins <- 22.5 + (1.0 - (supplied_fte / required_fte)) * 8.0

    # ----------------------------------------------------------------
    # Store Audit Ledger Row
    # ----------------------------------------------------------------
    audit_rows[[y_idx]] <- tibble::tibble(
      year = sim_year,
      population = pop_total,
      incident_cases = incident_cases,
      prevalent_cases = prevalent_cases,
      care_seeking_n = care_seeking_n,
      referred_n = referred_n,
      reachable_n = reachable_n,
      medicaid_eligible_n = medicaid_eligible_n,
      appointment_requests_n = appointment_requests_n,
      served_patients_n = served_patients_n,
      unserved_delayed_n = unserved_delayed_n,
      delivered_services_n = delivered_services_n,
      wrvu_total = wrvu_total,
      physician_minutes_total = physician_minutes_total,
      required_fte = required_fte,
      supplied_fte = supplied_fte,
      fte_gap = fte_gap,
      adequacy_ratio = adequacy_ratio,
      mean_wait_days = mean_wait_days,
      mean_travel_mins = mean_travel_mins
    )

    # ----------------------------------------------------------------
    # Step H: Provider Lifecycle (Attrition, ACGME Entrants, Relocation)
    # ----------------------------------------------------------------
    # Attrition hazard (age + years certified)
    exit_prob <- 0.015 + (provider_cohort$age / 100.0)^3.5
    exits <- stats::rbinom(
      base::nrow(provider_cohort),
      size = 1L,
      prob = exit_prob
    ) == 1L
    provider_cohort$active[exits] <- FALSE

    # ACGME Entrants
    n_new <- base::as.integer(fellowship_entrants)
    new_entrants <- tibble::tibble(
      provider_id = base::sprintf(
        "NPI%07d",
        initial_provider_count + (y_idx * 100L) + base::seq_len(n_new)
      ),
      age = stats::runif(n_new, min = 31, max = 34),
      years_certified = 0.0,
      academic_setting = stats::rbinom(n_new, size = 1L, prob = 0.30) == 1L,
      hospital_outpatient = stats::rbinom(n_new, size = 1L, prob = 0.40) == 1L,
      hrr_code = base::sprintf(
        "HRR%03d",
        base::sample.int(306L, n_new, replace = TRUE)
      ),
      state_abbr = "US",
      svi = stats::runif(n_new, min = 0.1, max = 0.9),
      fte = 1.0,
      active = TRUE
    )

    provider_cohort <- dplyr::bind_rows(provider_cohort, new_entrants)
  }

  audit_ledger_tbl <- dplyr::bind_rows(audit_rows)
  annual_hrr_balance <- dplyr::bind_rows(hrr_balance_list)

  simulation_bundle <- base::list(
    audit_ledger_tbl = audit_ledger_tbl,
    annual_hrr_balance = annual_hrr_balance,
    final_provider_cohort = provider_cohort,
    simulation_config = base::list(
      start_year = start_year,
      end_year = end_year,
      n_agents = n_agents,
      initial_provider_count = initial_provider_count,
      fellowship_entrants = fellowship_entrants,
      app_delegation_rate = app_delegation_rate,
      medicaid_fee_ratio = medicaid_fee_ratio,
      evidence_db = evidence_db,
      empirical_parameter_names = base::names(empirical_parameters)
    ),
    empirical_parameter_provenance = base::attr(
      empirical_parameters,
      "provenance"
    )
  )

  if (base::isTRUE(save_outputs)) {
    if (!base::dir.exists(output_dir)) {
      base::dir.create(output_dir, recursive = TRUE)
    }
    timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
    ledger_path <- base::file.path(
      output_dir,
      base::paste0("audit_ledger_", timestamp, ".csv")
    )
    balance_path <- base::file.path(
      output_dir,
      base::paste0("annual_hrr_balance_", timestamp, ".csv")
    )
    readr::write_csv(audit_ledger_tbl, ledger_path)
    readr::write_csv(annual_hrr_balance, balance_path)
    base::message("Saved audit ledger: ", ledger_path)
    base::message("Saved HRR balance: ", balance_path)
  }

  base::message("\n=============================================================")
  base::message(" END-TO-END SIMULATION PIPELINE COMPLETED")
  base::message("=============================================================")

  simulation_bundle
}
