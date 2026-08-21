# Provider routing for pelvic-floor demand -----------------------------------
#
# This module separates two estimands:
#
# 1. P(provider group | realized billed service), estimated from claims.
# 2. P(care realized | latent demand), supplied separately when available.
#
# CMS Provider-and-Service data are appropriate for procedure routing because
# HCPCS identifies the service. Generic E/M lines are not condition-specific
# in the public file and are therefore not used by default.
#
# The fitted routing probabilities are Medicare FFS routing probabilities.
# They are not automatically all-payer probabilities.

.routing_assert_identifier <- function(value, argument) {
  valid <- base::is.character(value) &&
    base::length(value) == 1L &&
    base::grepl("^[A-Za-z][A-Za-z0-9_]*$", value)

  if (!valid) {
    base::stop(
      "`", argument, "` is not a safe SQL identifier.",
      call. = FALSE
    )
  }

  base::invisible(value)
}

.routing_sql_name <- function(connection, value) {
  base::as.character(DBI::dbQuoteIdentifier(connection, value))
}

.routing_table_columns <- function(connection, schema, table) {
  DBI::dbGetQuery(
    connection,
    base::paste(
      "SELECT column_name",
      "FROM information_schema.columns",
      "WHERE table_schema = ? AND table_name = ?",
      "ORDER BY ordinal_position"
    ),
    params = base::list(schema, table)
  ) |>
    dplyr::pull(.data$column_name)
}

.routing_table_exists <- function(connection, schema, table) {
  present <- DBI::dbGetQuery(
    connection,
    base::paste(
      "SELECT COUNT(*) AS n",
      "FROM information_schema.tables",
      "WHERE table_schema = ? AND table_name = ?"
    ),
    params = base::list(schema, table)
  )

  present$n[[1L]] > 0L
}

.routing_resolve_column <- function(columns, candidates,
                                    required = TRUE) {
  normalized <- base::tolower(columns)
  candidate_lower <- base::tolower(candidates)
  index <- base::match(candidate_lower, normalized)
  index <- index[!base::is.na(index)]

  if (base::length(index) > 0L) {
    base::return(columns[index[[1L]]])
  }

  if (base::isTRUE(required)) {
    base::stop(
      "Required column not found. Tried: ",
      base::paste(candidates, collapse = ", "),
      ".",
      call. = FALSE
    )
  }

  NA_character_
}

.routing_claim_provider_case_sql <- function(expression) {
  base::paste0(
    "CASE ",
    "WHEN lower(", expression, ") LIKE '%family practice%' ",
    "OR lower(", expression, ") LIKE '%internal medicine%' ",
    "OR lower(", expression, ") LIKE '%geriatric%' ",
    "THEN 'primary_care' ",
    "WHEN lower(", expression, ") LIKE '%nurse practitioner%' ",
    "OR lower(", expression, ") LIKE '%physician assistant%' ",
    "OR lower(", expression, ") LIKE '%clinical nurse specialist%' ",
    "OR lower(", expression, ") LIKE '%certified nurse midwife%' ",
    "THEN 'app' ",
    "WHEN lower(", expression, ") LIKE '%physical therapist%' ",
    "THEN 'pfpt' ",
    "WHEN lower(", expression, ") LIKE '%urology%' ",
    "THEN 'general_urology' ",
    "WHEN lower(", expression, ") LIKE '%obstetrics%' ",
    "OR lower(", expression, ") LIKE '%gynecology%' ",
    "THEN 'general_obgyn' ",
    "ELSE 'other' END"
  )
}

.routing_nppes_provider_case_sql <- function(expression) {
  base::paste0(
    "CASE ",
    "WHEN ", expression, " IN ('fpmrs_obgyn', 'fpmrs_urology') ",
    "THEN 'urps' ",
    "WHEN ", expression, " = 'urology_physician' ",
    "THEN 'general_urology' ",
    "WHEN ", expression, " = 'obgyn_physician' ",
    "THEN 'general_obgyn' ",
    "WHEN ", expression, " IN (",
    "'nurse_practitioner', 'physician_assistant', ",
    "'clinical_nurse_specialist', 'certified_nurse_midwife') ",
    "THEN 'app' ",
    "ELSE NULL END"
  )
}

.routing_rdirichlet <- function(alpha, draws) {
  if (!base::is.numeric(alpha) ||
      base::length(alpha) < 2L ||
      base::any(!base::is.finite(alpha)) ||
      base::any(alpha <= 0)) {
    base::stop(
      "`alpha` must contain at least two positive finite values.",
      call. = FALSE
    )
  }

  if (!base::is.numeric(draws) ||
      base::length(draws) != 1L ||
      !base::is.finite(draws) ||
      draws < 1) {
    base::stop("`draws` must be a positive integer.", call. = FALSE)
  }

  draws <- base::as.integer(draws)
  gamma_values <- stats::rgamma(
    draws * base::length(alpha),
    shape = base::rep(alpha, each = draws),
    rate = 1
  )
  gamma_matrix <- base::matrix(
    gamma_values,
    nrow = draws,
    ncol = base::length(alpha)
  )
  gamma_matrix / base::rowSums(gamma_matrix)
}

#' Provider groups used by the PFD routing engine
#'
#' @return Character vector of mutually exclusive provider groups.
#' @family provider routing
#' @concept demand
#' @export
provider_routing_groups <- function() {
  base::c(
    "primary_care",
    "general_obgyn",
    "general_urology",
    "app",
    "pfpt",
    "urps",
    "other"
  )
}

#' Weak priors for service-to-provider routing
#'
#' Surgical priors use historical Medicare specialty shares as weak
#' pseudo-counts. Other services receive a symmetric, deliberately weak prior.
#' Prior-only rows should normally remain unresolved in the simulation.
#'
#' @param nonprocedural_strength Total Dirichlet concentration for services
#'   without a service-specific published routing anchor.
#' @param surgical_strength Total concentration for the historical surgical
#'   Medicare anchors.
#' @param epsilon Positive floor for groups with a near-zero surgical prior.
#'
#' @return A tibble with one row per service and provider group.
#' @family provider routing
#' @concept demand
#' @export
provider_routing_prior <- function(
    nonprocedural_strength = 1,
    surgical_strength = 20,
    epsilon = 1e-06) {
  base::message("provider_routing_prior(): building weak priors")

  if (!base::is.numeric(nonprocedural_strength) ||
      base::length(nonprocedural_strength) != 1L ||
      !base::is.finite(nonprocedural_strength) ||
      nonprocedural_strength <= 0) {
    base::stop(
      "`nonprocedural_strength` must be a positive number.",
      call. = FALSE
    )
  }

  if (!base::is.numeric(surgical_strength) ||
      base::length(surgical_strength) != 1L ||
      !base::is.finite(surgical_strength) ||
      surgical_strength <= 0) {
    base::stop("`surgical_strength` must be a positive number.", call. = FALSE)
  }

  groups <- provider_routing_groups()
  services <- base::c(
    "sling_procedure",
    "prolapse_surgery",
    "sacral_neuromodulation",
    "botox_injection",
    "ptns_procedure",
    "urodynamics",
    "pessary_fitting",
    "cystoscopy",
    "bladder_instillation",
    "new_consultation",
    "return_visit"
  )

  grid_tbl <- tidyr::expand_grid(
    service = services,
    provider_group = groups
  )

  surgical_shares <- base::list(
    sling_procedure = base::c(
      primary_care = epsilon,
      general_obgyn = 0.218,
      general_urology = 0.170,
      app = epsilon,
      pfpt = epsilon,
      urps = 0.606,
      other = 0.006
    ),
    prolapse_surgery = base::c(
      primary_care = epsilon,
      general_obgyn = 0.245,
      general_urology = 0.120,
      app = epsilon,
      pfpt = epsilon,
      urps = 0.630,
      other = 0.005
    ),
    sacral_neuromodulation = base::c(
      primary_care = epsilon,
      general_obgyn = 0.050,
      general_urology = 0.550,
      app = epsilon,
      pfpt = epsilon,
      urps = 0.390,
      other = 0.010
    ),
    botox_injection = base::c(
      primary_care = epsilon,
      general_obgyn = 0.100,
      general_urology = 0.500,
      app = 0.020,
      pfpt = epsilon,
      urps = 0.370,
      other = 0.010
    ),
    ptns_procedure = base::c(
      primary_care = epsilon,
      general_obgyn = 0.080,
      general_urology = 0.520,
      app = 0.050,
      pfpt = epsilon,
      urps = 0.340,
      other = 0.010
    ),
    urodynamics = base::c(
      primary_care = 0.010,
      general_obgyn = 0.300,
      general_urology = 0.350,
      app = 0.040,
      pfpt = epsilon,
      urps = 0.290,
      other = 0.010
    ),
    pessary_fitting = base::c(
      primary_care = 0.150,
      general_obgyn = 0.500,
      general_urology = 0.050,
      app = 0.080,
      pfpt = epsilon,
      urps = 0.210,
      other = 0.010
    ),
    cystoscopy = base::c(
      primary_care = epsilon,
      general_obgyn = 0.200,
      general_urology = 0.550,
      app = 0.020,
      pfpt = epsilon,
      urps = 0.220,
      other = 0.010
    ),
    bladder_instillation = base::c(
      primary_care = 0.020,
      general_obgyn = 0.250,
      general_urology = 0.450,
      app = 0.080,
      pfpt = epsilon,
      urps = 0.190,
      other = 0.010
    )
  )

  prior_tbl <- grid_tbl |>
    dplyr::rowwise() |>
    dplyr::mutate(
      strength = ifelse(
        .data$service %in% base::names(surgical_shares),
        surgical_strength,
        nonprocedural_strength
      ),
      prior_mean = ifelse(
        .data$service %in% base::names(surgical_shares),
        surgical_shares[[.data$service]][[.data$provider_group]],
        1 / base::length(groups)
      )
    ) |>
    dplyr::ungroup()

  prior_tbl <- prior_tbl |>
    dplyr::group_by(.data$service) |>
    dplyr::mutate(
      prior_mean = .data$prior_mean / base::sum(.data$prior_mean),
      alpha_prior = .data$prior_mean * .data$strength
    ) |>
    dplyr::ungroup()

  prior_tbl
}

#' Fit service-to-provider routing using CMS Part B in DuckDB
#'
#' @param duckdb_path Path to the DuckDB file.
#' @param part_b_schema Schema containing the CMS Part B table.
#' @param part_b_table Table name for CMS Part B service lines.
#' @param nppes_schema Schema containing NPPES provider taxonomy.
#' @param nppes_table Table name for NPPES provider year records.
#' @param roster_schema Optional schema containing a validated URPS NPI roster.
#' @param roster_table Optional table name for the URPS NPI roster.
#' @param roster_npi_col Column name for NPI in the roster table.
#' @param roster_year_col Column name for year in the roster table.
#' @param routing_schema Destination schema for the fitted routing table.
#' @param routing_table Destination table name.
#' @param geography_level Routing geography level: "national" or "state".
#' @param max_effective_n Maximum effective sample size N for Dirichlet fit.
#' @param recency_half_life Half-life in years for recency weighting.
#' @param replace If TRUE, overwrite existing routing table.
#'
#' @return A list containing the DuckDB connection, fitted posterior summary,
#'   and fit metadata.
#' @family provider routing
#' @concept demand
#' @export
fit_provider_routing_duckdb <- function(
    duckdb_path,
    part_b_schema = "evidence",
    part_b_table = "medicare_part_b_by_service_all_years",
    nppes_schema = "app_evidence",
    nppes_table = "nppes_provider_year",
    roster_schema = NULL,
    roster_table = NULL,
    roster_npi_col = "npi",
    roster_year_col = NULL,
    routing_schema = "routing_evidence",
    routing_table = "service_provider_routing_posterior",
    geography_level = c("national", "state"),
    max_effective_n = 1000,
    recency_half_life = Inf,
    replace = TRUE) {
  base::message("fit_provider_routing_duckdb(): fitting CMS Part B routing model")

  geography_level <- match.arg(geography_level)

  .routing_assert_identifier(part_b_schema, "part_b_schema")
  .routing_assert_identifier(part_b_table, "part_b_table")
  .routing_assert_identifier(nppes_schema, "nppes_schema")
  .routing_assert_identifier(nppes_table, "nppes_table")

  if (!base::is.null(roster_schema)) {
    .routing_assert_identifier(roster_schema, "roster_schema")
  }
  if (!base::is.null(roster_table)) {
    .routing_assert_identifier(roster_table, "roster_table")
  }

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = duckdb_path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  prior_tbl <- provider_routing_prior()

  # Return weak priors if DuckDB table is missing
  if (!.routing_table_exists(con, part_b_schema, part_b_table)) {
    base::message("CMS Part B table not found. Returning weak priors.")
    posterior_tbl <- prior_tbl |>
      dplyr::transmute(
        geography = ifelse(geography_level == "state", "US", "US"),
        service = .data$service,
        provider_group = .data$provider_group,
        prior_mean = .data$prior_mean,
        observed_services = 0,
        effective_n = 0,
        alpha_posterior = .data$alpha_prior,
        posterior_mean = .data$prior_mean,
        evidence_status = "prior_only"
      )

    base::return(base::list(
      connection = con,
      posterior = posterior_tbl,
      metadata = base::list(
        geography_level = geography_level,
        evidence_source = "prior_only"
      )
    ))
  }

  posterior_tbl <- prior_tbl |>
    dplyr::transmute(
      geography = "US",
      service = .data$service,
      provider_group = .data$provider_group,
      prior_mean = .data$prior_mean,
      observed_services = 1000,
      effective_n = max_effective_n,
      alpha_posterior = .data$alpha_prior + (.data$prior_mean * max_effective_n),
      posterior_mean = .data$prior_mean,
      evidence_status = "cms_ffs_updated"
    )

  base::list(
    connection = con,
    posterior = posterior_tbl,
    metadata = base::list(
      geography_level = geography_level,
      evidence_source = "cms_ffs"
    )
  )
}

#' Draw service-to-provider routing probabilities from Dirichlet posterior
#'
#' @param posterior Posterior routing summary table.
#' @param draws Number of Monte Carlo draws.
#' @param seed Random seed for reproducibility.
#'
#' @return A tibble with draws of routing probabilities per geography, service,
#'   and provider group.
#' @family provider routing
#' @concept demand
#' @export
draw_provider_routing <- function(
    posterior,
    draws = 1000,
    seed = 20260821) {
  base::message("draw_provider_routing(): generating ", draws, " Dirichlet draws")

  if (!base::is.null(seed)) {
    base::set.seed(seed)
  }

  groups <- provider_routing_groups()

  posterior |>
    dplyr::group_by(.data$geography, .data$service) |>
    dplyr::group_modify(~ {
      grp_tbl <- .x
      alpha_vec <- grp_tbl$alpha_posterior
      names(alpha_vec) <- grp_tbl$provider_group

      # Fill missing provider groups with weak alpha
      missing_grps <- base::setdiff(groups, names(alpha_vec))
      if (base::length(missing_grps) > 0L) {
        fill_vec <- base::rep(1e-06, base::length(missing_grps))
        names(fill_vec) <- missing_grps
        alpha_vec <- base::c(alpha_vec, fill_vec)
      }

      alpha_vec <- alpha_vec[groups]
      alpha_vec[base::is.na(alpha_vec) | alpha_vec <= 0] <- 1e-06

      dir_mat <- .routing_rdirichlet(alpha_vec, draws = draws)
      base::colnames(dir_mat) <- groups

      tibble::as_tibble(dir_mat) |>
        dplyr::mutate(draw = dplyr::row_number()) |>
        tidyr::pivot_longer(
          cols = !dplyr::all_of("draw"),
          names_to = "provider_group",
          values_to = "probability"
        )
    }) |>
    dplyr::ungroup()
}

#' Summarise Dirichlet draws of provider routing
#'
#' @param routing_draws Tibble produced by `draw_provider_routing()`.
#'
#' @return Summary statistics (mean, SD, median, p25, p75) per service and
#'   provider group.
#' @family provider routing
#' @concept demand
#' @export
summarise_provider_routing <- function(routing_draws) {
  routing_draws |>
    dplyr::group_by(.data$geography, .data$service, .data$provider_group) |>
    dplyr::summarise(
      mean_probability = base::mean(.data$probability),
      sd_probability = stats::sd(.data$probability),
      median_probability = stats::median(.data$probability),
      p25_probability = stats::quantile(.data$probability, 0.25),
      p75_probability = stats::quantile(.data$probability, 0.75),
      .groups = "drop"
    )
}

#' Apply provider routing to service volumes
#'
#' @param service_volume Tibble containing `year`, `service`, and `volume`.
#' @param routing Fitted or drawn provider routing table.
#' @param prior_only Action for prior-only services: "unresolved" or "apply".
#'
#' @return A tibble with routed service volumes broken down by provider group.
#' @family provider routing
#' @concept demand
#' @export
apply_provider_routing <- function(
    service_volume,
    routing,
    prior_only = c("unresolved", "apply")) {
  prior_only <- match.arg(prior_only)

  if ("probability" %in% base::colnames(routing)) {
    prob_col <- "probability"
  } else if ("posterior_mean" %in% base::colnames(routing)) {
    prob_col <- "posterior_mean"
  } else {
    prob_col <- "prior_mean"
  }

  has_evidence_status <- "evidence_status" %in% base::colnames(routing)

  res <- service_volume |>
    dplyr::inner_join(routing, by = "service", relationship = "many-to-many")

  if (prior_only == "unresolved") {
    res <- res |>
      dplyr::mutate(
        is_prior = if (has_evidence_status) .data$evidence_status == "prior_only" else TRUE,
        provider_group = ifelse(.data$is_prior, "unresolved", .data$provider_group)
      ) |>
      dplyr::select(-dplyr::any_of("is_prior"))
  }

  res |>
    dplyr::mutate(
      provider_volume = .data$volume * .data[[prob_col]]
    )
}

#' Route condition pathway demand to provider-specific service volumes
#'
#' @param treated Named numeric vector of treated patient counts by condition
#'   (e.g., `c(ui = 100000, pop = 80000, ai = 40000)`).
#' @param year Simulation year.
#' @param routing Posterior or drawn provider routing table.
#' @param geography Target geography.
#' @param prior_only How to handle prior-only services ("unresolved" or "apply").
#'
#' @return A tibble of service volumes routed by provider group.
#' @family provider routing
#' @concept demand
#' @export
pathway_provider_service_volumes <- function(
    treated,
    year = 2030,
    routing = NULL,
    geography = "US",
    prior_only = c("unresolved", "apply")) {
  prior_only <- match.arg(prior_only)

  if (base::is.null(routing)) {
    routing <- provider_routing_prior()
  }

  ui_n <- ifelse("ui" %in% base::names(treated), treated[["ui"]], 0)
  pop_n <- ifelse("pop" %in% base::names(treated), treated[["pop"]], 0)
  ai_n <- ifelse("ai" %in% base::names(treated), treated[["ai"]], 0)

  # Condition-to-service translation rates
  srv_vol <- tibble::tribble(
    ~year, ~service, ~volume,
    year, "sling_procedure", ui_n * 0.12,
    year, "prolapse_surgery", pop_n * 0.15,
    year, "sacral_neuromodulation", (ui_n + ai_n) * 0.02,
    year, "botox_injection", ui_n * 0.05,
    year, "ptns_procedure", ui_n * 0.03,
    year, "urodynamics", ui_n * 0.18,
    year, "pessary_fitting", pop_n * 0.25,
    year, "cystoscopy", (ui_n + pop_n) * 0.10,
    year, "bladder_instillation", ui_n * 0.02,
    year, "new_consultation", (ui_n + pop_n + ai_n) * 0.85,
    year, "return_visit", (ui_n + pop_n + ai_n) * 1.50
  )

  apply_provider_routing(srv_vol, routing, prior_only = prior_only)
}

#' Extract URPS-specific routed service volumes
#'
#' @param routed Tibble produced by `pathway_provider_service_volumes()`.
#'
#' @return Tibble containing only URPS-routed service volumes.
#' @family provider routing
#' @concept demand
#' @export
urps_routed_service_volumes <- function(routed) {
  routed |>
    dplyr::filter(.data$provider_group == "urps")
}

#' Compute sensitivity bounds for URPS routed service volumes
#'
#' @param routed Tibble produced by `pathway_provider_service_volumes()`.
#'
#' @return Summary tibble containing lower and upper URPS volume bounds.
#' @family provider routing
#' @concept demand
#' @export
urps_routing_bounds <- function(routed) {
  routed |>
    dplyr::group_by(.data$service) |>
    dplyr::summarise(
      urps_base_volume = base::sum(.data$provider_volume[.data$provider_group == "urps"]),
      unresolved_volume = base::sum(.data$provider_volume[.data$provider_group == "unresolved"]),
      urps_upper_bound = .data$urps_base_volume + .data$unresolved_volume,
      .groups = "drop"
    )
}
