# Incident-Entry Hazard Estimator q(c,a,t) ----
#
# Source-agnostic longitudinal claims estimator for q(c,a,t): unique women newly
# entering condition-specific FPMRS care divided by the prevalent eligible
# disease stock. Claims define entry; external prevalence science supplies the
# disease-stock probability. See docs/INCIDENT_ENTRY_ESTIMAND.md.

#' Compute Wilson Score Confidence Interval for Incident Entry Rate
#'
#' @param successes Number of observed incident entrants.
#' @param trials Estimated eligible disease stock population size.
#' @param conf_level Confidence level (default 0.95).
#' @return A tibble with `q_low` and `q_high`.
#' @family incident entry
#' @concept demand
#' @export
incident_entry_wilson <- function(successes, trials, conf_level = 0.95) {
  z_value <- stats::qnorm(1 - (1 - conf_level) / 2)
  trials_safe <- pmax(trials, 1e-6)
  estimate <- successes / trials_safe
  denom <- 1 + z_value^2 / trials_safe
  center <- (estimate + z_value^2 / (2 * trials_safe)) / denom
  half <- z_value * sqrt(
    pmax(0, estimate * (1 - estimate) / trials_safe) +
      z_value^2 / (4 * trials_safe^2)
  ) / denom

  tibble::tibble(
    q_low = pmax(0, center - half),
    q_high = pmin(1, center + half)
  )
}

#' Internal Check for Required Table Columns
#' @noRd
incident_entry_check <- function(tbl, required, label) {
  missing <- setdiff(required, names(tbl))
  if (length(missing) > 0L) {
    stop(label, " is missing: ", paste(missing, collapse = ", "), call. = FALSE)
  }
  invisible(TRUE)
}

#' Convert Table to DuckDB Tibble if Available
#' @noRd
incident_entry_as_duck <- function(tbl) {
  if (requireNamespace("duckplyr", quietly = TRUE)) {
    if (duckplyr::is_duckdb_tibble(tbl)) return(tbl)
    return(duckplyr::duckdb_tibble(tibble::as_tibble(tbl)))
  }
  tibble::as_tibble(tbl)
}

#' Estimate Annual Incident Specialist-Care Entry Hazard q(c,a,t)
#'
#' @description
#' Estimates q(c,a,t): unique women newly entering condition-specific FPMRS
#' care divided by the prevalent eligible disease stock. Claims define entry;
#' external prevalence science supplies the disease-stock probability.
#'
#' @param claims_tbl Canonical longitudinal claims table.
#' @param enrollment_tbl Canonical monthly enrollment table.
#' @param member_year_tbl Canonical member-year demographics table.
#' @param roster_tbl FPMRS roster with rendering_npi.
#' @param stock_probability_tbl External eligible-disease probabilities.
#' @param analysis_years Incidence years. Primary is 2023-2024.
#' @param washout_months Event-free lookback. Primary is 24 months.
#' @param allowed_gap_months Enrollment gaps. Primary is 0.
#' @param min_cell_n Small-cell suppression threshold (CMS DUA default 11).
#' @param conf_level Confidence level for conditional Wilson intervals.
#'
#' @return List with `analytic`, `public`, and `diagnostics` tibbles.
#' @family incident entry
#' @concept demand
#' @export
estimate_incident_entry_hazard <- function(
    claims_tbl,
    enrollment_tbl,
    member_year_tbl,
    roster_tbl,
    stock_probability_tbl,
    analysis_years = c(2023L, 2024L),
    washout_months = 24L,
    allowed_gap_months = 0L,
    min_cell_n = 11L,
    conf_level = 0.95) {

  message("Starting incident-entry hazard estimation.")
  message("Years: ", paste(analysis_years, collapse = ", "))
  message("Washout months: ", washout_months)
  message("Allowed enrollment gaps: ", allowed_gap_months)

  incident_entry_check(
    claims_tbl,
    c(
      "person_id", "service_year", "service_month", "rendering_npi",
      "condition", "is_outpatient_evaluation", "is_qualifying_urps_encounter"
    ),
    "claims_tbl"
  )

  incident_entry_check(
    enrollment_tbl,
    c("person_id", "coverage_year", "coverage_month"),
    "enrollment_tbl"
  )

  incident_entry_check(
    member_year_tbl,
    c("person_id", "year", "female", "age", "payer_group"),
    "member_year_tbl"
  )

  incident_entry_check(roster_tbl, "rendering_npi", "roster_tbl")

  incident_entry_check(
    stock_probability_tbl,
    c("condition", "age_band", "year", "payer_group", "eligible_stock_probability"),
    "stock_probability_tbl"
  )

  if (washout_months < 1L) {
    stop("washout_months must be >= 1.", call. = FALSE)
  }

  risk_window_months <- washout_months + 12L

  if (allowed_gap_months < 0L || allowed_gap_months >= risk_window_months) {
    stop("allowed_gap_months is invalid.", call. = FALSE)
  }

  claims_tbl <- incident_entry_as_duck(claims_tbl)
  enrollment_tbl <- incident_entry_as_duck(enrollment_tbl)
  member_year_tbl <- incident_entry_as_duck(member_year_tbl)
  roster_tbl <- incident_entry_as_duck(roster_tbl)
  stock_probability_tbl <- incident_entry_as_duck(stock_probability_tbl)

  condition_levels <- c("ui", "pop", "ai")

  message("Validating external eligible disease stock.")

  stock_check_tbl <- stock_probability_tbl |>
    dplyr::collect() |>
    dplyr::mutate(
      condition = tolower(condition),
      age_band = as.character(age_band),
      payer_group = as.character(payer_group)
    )

  bad_stock_tbl <- stock_check_tbl |>
    dplyr::filter(
      is.na(eligible_stock_probability) |
        eligible_stock_probability <= 0 |
        eligible_stock_probability > 1
    )

  if (nrow(bad_stock_tbl) > 0L) {
    stop("eligible_stock_probability must be in (0, 1].", call. = FALSE)
  }

  duplicate_stock_tbl <- stock_check_tbl |>
    dplyr::count(condition, age_band, year, payer_group, name = "cell_n") |>
    dplyr::filter(cell_n > 1L)

  if (nrow(duplicate_stock_tbl) > 0L) {
    stop("Duplicate stock cells found.", call. = FALSE)
  }

  unknown_conditions <- setdiff(unique(stock_check_tbl$condition), condition_levels)

  if (length(unknown_conditions) > 0L) {
    stop("Unknown condition(s): ", paste(unknown_conditions, collapse = ", "), call. = FALSE)
  }

  stock_probability_tbl <- incident_entry_as_duck(stock_check_tbl)

  message("Building continuous-enrollment windows.")

  enrollment_month_tbl <- enrollment_tbl |>
    dplyr::filter(coverage_month >= 1L, coverage_month <= 12L) |>
    dplyr::distinct(person_id, coverage_year, coverage_month) |>
    dplyr::mutate(coverage_month_id = coverage_year * 12L + coverage_month) |>
    dplyr::compute()

  duplicate_member_year_n <- member_year_tbl |>
    dplyr::filter(year %in% analysis_years) |>
    dplyr::count(person_id, year, name = "row_n") |>
    dplyr::filter(row_n > 1L) |>
    dplyr::summarise(duplicate_key_n = dplyr::n()) |>
    dplyr::collect() |>
    dplyr::pull(duplicate_key_n)

  if (duplicate_member_year_n > 0L) {
    stop("member_year_tbl must have one row per person-year.", call. = FALSE)
  }

  message("Building adult female member-year risk windows.")

  candidate_tbl <- member_year_tbl |>
    dplyr::filter(year %in% analysis_years, female, age >= 18) |>
    dplyr::mutate(
      age_band = dplyr::case_when(
        age <= 44L ~ "18-44",
        age <= 54L ~ "45-54",
        age <= 64L ~ "55-64",
        age <= 74L ~ "65-74",
        TRUE ~ "75+"
      ),
      index_start_month = year * 12L + 1L,
      window_start_month = index_start_month - washout_months,
      window_end_month = index_start_month + 11L
    ) |>
    dplyr::compute()

  coverage_count_tbl <- candidate_tbl |>
    dplyr::select(person_id, year, window_start_month, window_end_month) |>
    dplyr::inner_join(enrollment_month_tbl, by = "person_id") |>
    dplyr::filter(
      coverage_month_id >= window_start_month,
      coverage_month_id <= window_end_month
    ) |>
    dplyr::group_by(person_id, year) |>
    dplyr::summarise(
      observed_months = dplyr::n_distinct(coverage_month_id),
      .groups = "drop"
    ) |>
    dplyr::compute()

  required_months <- risk_window_months - allowed_gap_months

  covered_member_year_tbl <- candidate_tbl |>
    dplyr::inner_join(coverage_count_tbl, by = c("person_id", "year")) |>
    dplyr::filter(observed_months >= required_months) |>
    dplyr::compute()

  message("Required observed months: ", required_months)

  message("Checking disease-stock coverage by analytic cell.")

  observed_cell_tbl <- covered_member_year_tbl |>
    dplyr::distinct(age_band, year, payer_group) |>
    dplyr::collect()

  needed_stock_tbl <- tidyr::crossing(observed_cell_tbl, condition = condition_levels)

  missing_stock_tbl <- needed_stock_tbl |>
    dplyr::anti_join(stock_check_tbl, by = c("condition", "age_band", "year", "payer_group"))

  if (nrow(missing_stock_tbl) > 0L) {
    stop("Missing stock probabilities for observed analytic cells.", call. = FALSE)
  }

  covered_condition_tbl <- covered_member_year_tbl |>
    dplyr::inner_join(stock_probability_tbl, by = c("age_band", "year", "payer_group")) |>
    dplyr::compute()

  message("Roster-linking FPMRS claims.")

  roster_npi_tbl <- roster_tbl |>
    dplyr::select(rendering_npi) |>
    dplyr::distinct()

  roster_claim_tbl <- claims_tbl |>
    dplyr::filter(condition %in% condition_levels, service_month >= 1L, service_month <= 12L) |>
    dplyr::inner_join(roster_npi_tbl, by = "rendering_npi") |>
    dplyr::mutate(claim_month_id = service_year * 12L + service_month) |>
    dplyr::select(
      person_id, condition, claim_month_id,
      is_outpatient_evaluation, is_qualifying_urps_encounter
    ) |>
    dplyr::compute()

  message("Applying condition-specific washout.")

  prior_care_tbl <- covered_condition_tbl |>
    dplyr::select(person_id, year, condition, window_start_month, index_start_month) |>
    dplyr::inner_join(
      roster_claim_tbl |>
        dplyr::filter(is_qualifying_urps_encounter) |>
        dplyr::select(person_id, condition, claim_month_id),
      by = c("person_id", "condition")
    ) |>
    dplyr::filter(
      claim_month_id >= window_start_month,
      claim_month_id < index_start_month
    ) |>
    dplyr::distinct(person_id, year, condition) |>
    dplyr::compute()

  at_risk_tbl <- covered_condition_tbl |>
    dplyr::anti_join(prior_care_tbl, by = c("person_id", "year", "condition")) |>
    dplyr::compute()

  message("Identifying first observed annual entry.")

  entry_person_tbl <- at_risk_tbl |>
    dplyr::select(person_id, year, condition, age_band, payer_group, index_start_month) |>
    dplyr::inner_join(
      roster_claim_tbl |>
        dplyr::filter(is_outpatient_evaluation) |>
        dplyr::select(person_id, condition, claim_month_id),
      by = c("person_id", "condition")
    ) |>
    dplyr::filter(
      claim_month_id >= index_start_month,
      claim_month_id <= index_start_month + 11L
    ) |>
    dplyr::distinct(person_id, year, condition, age_band, payer_group) |>
    dplyr::compute()

  message("Aggregating numerator and disease stock.")

  denominator_tbl <- at_risk_tbl |>
    dplyr::group_by(condition, age_band, year, payer_group, eligible_stock_probability) |>
    dplyr::summarise(
      at_risk_member_n = dplyr::n_distinct(person_id),
      .groups = "drop"
    ) |>
    dplyr::mutate(eligible_stock_n = at_risk_member_n * eligible_stock_probability) |>
    dplyr::collect()

  numerator_tbl <- entry_person_tbl |>
    dplyr::group_by(condition, age_band, year, payer_group) |>
    dplyr::summarise(entry_n = dplyr::n_distinct(person_id), .groups = "drop") |>
    dplyr::collect()

  payer_cell_tbl <- denominator_tbl |>
    dplyr::left_join(numerator_tbl, by = c("condition", "age_band", "year", "payer_group")) |>
    dplyr::mutate(
      entry_n = dplyr::coalesce(entry_n, 0L),
      q = pmin(1.0, entry_n / pmax(eligible_stock_n, 1e-6))
    )

  if (any(payer_cell_tbl$entry_n > payer_cell_tbl$eligible_stock_n, na.rm = TRUE)) {
    warning("Observed entry_n exceeds estimated eligible_stock_n in small cells; q capped at 1.0.", call. = FALSE)
  }

  all_payer_tbl <- payer_cell_tbl |>
    dplyr::group_by(condition, age_band, year) |>
    dplyr::summarise(
      entry_n = sum(entry_n),
      at_risk_member_n = sum(at_risk_member_n),
      eligible_stock_n = sum(eligible_stock_n),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      payer_group = "ALL",
      eligible_stock_probability = eligible_stock_n / at_risk_member_n,
      q = pmin(1.0, entry_n / pmax(eligible_stock_n, 1e-6))
    )

  analytic_tbl <- dplyr::bind_rows(payer_cell_tbl, all_payer_tbl) |>
    dplyr::arrange(condition, age_band, year, payer_group)

  interval_tbl <- incident_entry_wilson(
    successes = analytic_tbl$entry_n,
    trials = analytic_tbl$eligible_stock_n,
    conf_level = conf_level
  )

  analytic_tbl <- dplyr::bind_cols(analytic_tbl, interval_tbl) |>
    dplyr::mutate(ci_type = "Wilson conditional on eligible disease stock")

  public_tbl <- analytic_tbl |>
    dplyr::mutate(
      suppressed = entry_n < min_cell_n,
      entry_n = dplyr::if_else(suppressed, NA_real_, as.numeric(entry_n)),
      at_risk_member_n = dplyr::if_else(suppressed, NA_real_, as.numeric(at_risk_member_n)),
      eligible_stock_n = dplyr::if_else(suppressed, NA_real_, eligible_stock_n),
      q = dplyr::if_else(suppressed, NA_real_, q),
      q_low = dplyr::if_else(suppressed, NA_real_, q_low),
      q_high = dplyr::if_else(suppressed, NA_real_, q_high)
    )

  payer_only_tbl <- analytic_tbl |> dplyr::filter(payer_group != "ALL")

  message("Incident entrants: ", format(sum(payer_only_tbl$entry_n), big.mark = ",", scientific = FALSE))
  message("Eligible disease stock: ", format(round(sum(payer_only_tbl$eligible_stock_n)), big.mark = ",", scientific = FALSE))
  message("Suppressed public cells: ", format(sum(public_tbl$suppressed), big.mark = ","))

  diagnostics_tbl <- tibble::tibble(
    years = paste(analysis_years, collapse = ","),
    washout_months = washout_months,
    allowed_gap_months = allowed_gap_months,
    required_observed_months = required_months,
    min_cell_n = min_cell_n,
    conf_level = conf_level,
    calibration_used = FALSE
  )

  message("Incident-entry hazard estimation complete.")

  list(
    analytic = analytic_tbl,
    public = public_tbl,
    diagnostics = diagnostics_tbl
  )
}

#' Save Disclosure-Safe Incident-Entry Estimates
#'
#' @param estimate_bundle Return value from estimate_incident_entry_hazard().
#' @param save_dir Destination directory.
#' @param prefix File-name prefix.
#' @return Saved path, invisibly.
#' @family incident entry
#' @concept demand
#' @export
save_incident_entry_hazard <- function(
    estimate_bundle,
    save_dir = "artifacts/incident_entry",
    prefix = "incident_entry_hazard") {

  dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  save_path <- file.path(save_dir, paste0(prefix, "_", timestamp, ".csv"))

  message("Saving disclosure-safe incident-entry estimates.")
  readr::write_csv(estimate_bundle$public, save_path, na = "")
  exact_path <- normalizePath(save_path, winslash = "/", mustWork = TRUE)
  message("Saved file: ", exact_path)

  invisible(exact_path)
}

# Pre-Registered Sensitivity Matrix ------------------------------------------
#
# docs/INCIDENT_ENTRY_ESTIMAND.md #8 fixes seven dimensions BEFORE the answer
# is seen: washout, index-event definition, diagnosis window, enrollment gap,
# provider definition, incidence years, and case rule. Three of those --
# washout_months, allowed_gap_months, analysis_years -- are parameters
# estimate_incident_entry_hazard() already exposes natively, so this function
# sweeps them directly. The other four -- index-event definition, diagnosis
# window, provider definition, case rule -- are properties of HOW claims_tbl
# and roster_tbl were classified upstream (which encounters count as
# "qualifying", which NPIs count as the roster), not properties this function
# can vary on its own: the classification happens before the claims ever
# reach this contract. Those four are swept by supplying multiple NAMED
# variants of claims_tbl/roster_tbl, one per upstream classification choice.
#
# ONE-AT-A-TIME, NOT FULL FACTORIAL. Each row in the pre-registered table
# holds every dimension at its primary value except the one being tested --
# this mirrors that directly, rather than the combinatorial explosion a full
# factorial sweep would produce, so it stays a small, auditable set of runs
# that maps one-to-one onto the pre-registered table.

#' Run the Pre-Registered Incident-Entry Sensitivity Matrix
#'
#' @description
#' Runs [estimate_incident_entry_hazard()] once per dimension of the
#' pre-registered sensitivity matrix (docs/INCIDENT_ENTRY_ESTIMAND.md #8),
#' holding every other dimension at its primary value. Existence of a swing
#' across reasonable definitions is itself a finding -- see the estimand's own
#' framing: "If `q` swings from 0.08 to 0.40 across reasonable case
#' definitions, that uncertainty belongs in the simulation." This function
#' produces the comparison table that swing is read from; it does not
#' interpret it.
#'
#' @param claims_variants Named list of `claims_tbl` variants, one per
#'   index-event / diagnosis-window / provider-definition / case-rule
#'   combination to test. Must include an element named `"primary"`.
#' @param roster_variants Named list of `roster_tbl` variants, parallel to
#'   `claims_variants` by name. A single unnamed/length-1 list is recycled
#'   across every claims variant (the common case: only the claims
#'   classification changes, the roster does not).
#' @param enrollment_tbl,member_year_tbl,stock_probability_tbl Passed through
#'   unchanged to every run -- these are not part of the pre-registered
#'   sensitivity matrix.
#' @param washout_variants Washout months to sweep one at a time against the
#'   primary spec. Must include 24 (the primary value); default matches the
#'   pre-registered `c(12, 24, 36)`.
#' @param gap_variants Allowed enrollment-gap months to sweep one at a time.
#'   Must include 0 (the primary value); default matches the pre-registered
#'   `c(0, 1)`.
#' @param year_variants Named list of `analysis_years` vectors to sweep one at
#'   a time. Must include an element named `"primary"`; default matches the
#'   pre-registered primary (2023-2024) against the 2022-24 sensitivity.
#' @param ... Passed to every call of [estimate_incident_entry_hazard()]
#'   (e.g. `min_cell_n`, `conf_level`).
#' @return A tibble: `estimate_incident_entry_hazard()$public` row-bound
#'   across every run, with two added columns identifying which single axis
#'   moved from the primary spec for that row -- `sensitivity_dimension`
#'   (`"primary"`, `"washout_months"`, `"allowed_gap_months"`,
#'   `"analysis_years"`, or `"claims_variant"`) and `sensitivity_value` (the
#'   value or variant name that produced it).
#' @family incident entry
#' @concept demand
#' @export
run_incident_entry_sensitivity_matrix <- function(
    claims_variants,
    enrollment_tbl,
    member_year_tbl,
    roster_variants,
    stock_probability_tbl,
    washout_variants = c(12L, 24L, 36L),
    gap_variants = c(0L, 1L),
    year_variants = list(
      primary = c(2023L, 2024L),
      sensitivity_2022_24 = c(2022L, 2023L, 2024L)
    ),
    ...) {

  if (!"primary" %in% names(claims_variants)) {
    stop("claims_variants must include an element named 'primary'.", call. = FALSE)
  }
  if (!24L %in% washout_variants) {
    stop("washout_variants must include the primary value, 24.", call. = FALSE)
  }
  if (!0L %in% gap_variants) {
    stop("gap_variants must include the primary value, 0.", call. = FALSE)
  }
  if (!"primary" %in% names(year_variants)) {
    stop("year_variants must include an element named 'primary'.", call. = FALSE)
  }

  if (length(roster_variants) == 1L) {
    roster_variants <- stats::setNames(
      rep(roster_variants, length(claims_variants)),
      names(claims_variants)
    )
  }
  missing_roster <- setdiff(names(claims_variants), names(roster_variants))
  if (length(missing_roster) > 0L) {
    stop("roster_variants is missing a variant matching claims_variants: ",
         paste(missing_roster, collapse = ", "), call. = FALSE)
  }

  run_one <- function(claims_name, washout_months, allowed_gap_months,
                      year_label, analysis_years, dimension, value) {
    message("Sensitivity run: ", dimension, " = ", value)
    res <- estimate_incident_entry_hazard(
      claims_tbl = claims_variants[[claims_name]],
      enrollment_tbl = enrollment_tbl,
      member_year_tbl = member_year_tbl,
      roster_tbl = roster_variants[[claims_name]],
      stock_probability_tbl = stock_probability_tbl,
      analysis_years = analysis_years,
      washout_months = washout_months,
      allowed_gap_months = allowed_gap_months,
      ...
    )
    res$public |>
      dplyr::mutate(sensitivity_dimension = dimension, sensitivity_value = as.character(value),
                    .before = 1L)
  }

  runs <- list(
    run_one("primary", 24L, 0L, "primary", year_variants[["primary"]],
            "primary", "primary")
  )

  for (w in setdiff(washout_variants, 24L)) {
    runs <- c(runs, list(run_one("primary", w, 0L, "primary", year_variants[["primary"]],
                                  "washout_months", w)))
  }

  for (g in setdiff(gap_variants, 0L)) {
    runs <- c(runs, list(run_one("primary", 24L, g, "primary", year_variants[["primary"]],
                                  "allowed_gap_months", g)))
  }

  for (yl in setdiff(names(year_variants), "primary")) {
    runs <- c(runs, list(run_one("primary", 24L, 0L, yl, year_variants[[yl]],
                                  "analysis_years", yl)))
  }

  for (cv in setdiff(names(claims_variants), "primary")) {
    runs <- c(runs, list(run_one(cv, 24L, 0L, "primary", year_variants[["primary"]],
                                  "claims_variant", cv)))
  }

  dplyr::bind_rows(runs)
}
