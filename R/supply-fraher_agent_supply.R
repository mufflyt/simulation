################################################################################
# R/supply-fraher_agent_supply.R
# Fraher (2024) agent-based supply engine
#
# Complementary to the existing Dall HWMM stochastic engine in
# supply-provider_microsimulation.R. Addresses README "What is still missing"
# item 3: individual provider roster rather than aggregate cohort draws.
################################################################################

#' Initialize URPS Physician Agent Population
#'
#' @param roster_source Character. "mufflyaccess" or "isochrones_duckdb".
#' @param duckdb_path Character or NULL.
#' @param reference_year Integer. Default: 2023L.
#' @param max_age Integer. Default: 70L per Fraher criterion.
#' @param verbose Logical. Default: TRUE.
#'
#' @return Data frame with one row per physician.
#' @importFrom assertthat assert_that
#' @importFrom dplyr mutate row_number case_when if_else filter bind_rows left_join select
#' @importFrom tidyr replace_na
#' @family fraher agent supply
#' @concept supply
#' @examples
#' \dontrun{
#' # Reproducible synthetic roster (no external data required):
#' agents <- initialize_urps_agents(roster_source = "mufflyaccess", verbose = FALSE)
#' head(agents)
#' # Advance one simulated year against an age x sex exit-hazard table:
#' # advance_urps_agents() is archived in inst/archive/supply.R.
#' }
#' @export
initialize_urps_agents <- function(roster_source  = "mufflyaccess",
                                   duckdb_path    = NULL,
                                   reference_year = 2023L,
                                   max_age        = 70L,
                                   verbose        = TRUE) {
  assertthat::assert_that(
    roster_source %in% c("mufflyaccess", "isochrones_duckdb"),
    msg = "roster_source must be 'mufflyaccess' or 'isochrones_duckdb'"
  )

  if (roster_source == "isochrones_duckdb") {
    assertthat::assert_that(
      !is.null(duckdb_path) && file.exists(duckdb_path),
      msg = paste0(
        "duckdb_path must point to mufflyt/isochrones DuckDB.\n",
        "  Set roster_source = 'mufflyaccess' to use contract aggregate."
      )
    )
    conn <- DBI::dbConnect(duckdb::duckdb(), duckdb_path, read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
    roster_raw <- DBI::dbGetQuery(conn, sprintf(
      "SELECT npi, age, sex,
             COALESCE(certification_year, fellowship_completion_year) AS cert_yr,
             practice_state, subspecialty_name
      FROM abog_npi_matched
      WHERE age IS NOT NULL
        AND age <= %d
        AND age >= 25",
      max_age
    ))
    roster_raw <- roster_raw %>%
      dplyr::mutate(
        pathway = dplyr::if_else(
          grepl("ABU|Urology", subspecialty_name, ignore.case = TRUE),
          "ABU", "ABOG"
        )
      )
  } else {
    set.seed(42L)
    n_recent <- 651L
    n_legacy <- 655L
    recent_cohort <- data.frame(
      synthetic_id = seq_len(n_recent),
      age          = pmax(25L, pmin(max_age, round(
        stats::rnorm(n_recent, mean = 39.5, sd = 5.2)
      ))),
      sex     = sample(c("Female", "Male"), n_recent,
                       replace = TRUE, prob = c(0.852, 0.148)),
      pathway = sample(c("ABOG", "ABU"), n_recent,
                       replace = TRUE, prob = c(0.84, 0.16)),
      cohort  = "recent_2014_2023",
      stringsAsFactors = FALSE
    )
    legacy_cohort <- data.frame(
      synthetic_id = n_recent + seq_len(n_legacy),
      age          = pmax(25L, pmin(max_age, round(
        stats::rnorm(n_legacy, mean = 54.4, sd = 8.1)
      ))),
      sex     = sample(c("Female", "Male"), n_legacy,
                       replace = TRUE, prob = c(0.72, 0.28)),
      pathway = sample(c("ABOG", "ABU"), n_legacy,
                       replace = TRUE, prob = c(0.82, 0.18)),
      cohort  = "legacy_pre_2014",
      stringsAsFactors = FALSE
    )
    roster_raw <- dplyr::bind_rows(recent_cohort, legacy_cohort) %>%
      dplyr::mutate(
        npi            = paste0("SYNTHETIC_", synthetic_id),
        practice_state = sample(datasets::state.abb, dplyr::n(), replace = TRUE)
      )
  }

  div_lookup <- .build_state_to_division_lookup()

  agents <- roster_raw %>%
    dplyr::filter(age >= 25L, age <= max_age) %>%
    dplyr::left_join(div_lookup, by = c("practice_state" = "state_abb")) %>%
    dplyr::mutate(
      agent_id        = dplyr::row_number(),
      age             = as.integer(age),
      census_division = tidyr::replace_na(census_division, "Unknown"),
      clinical_fte    = NA_real_,
      status          = "Active",
      simulation_year = as.integer(reference_year)
    ) %>%
    dplyr::select(
      agent_id, npi, age, sex, pathway,
      census_division, clinical_fte, status, simulation_year
    )

  n_agents       <- nrow(agents)
  # urps_count() returns a scalar count; nrow(scalar) is NULL, which makes
  # pct_diff numeric(0) and the `if (pct_diff > 10)` below throw "argument is of
  # length zero" -- i.e. it crashed precisely when mufflyaccess IS installed.
  # Coerce robustly and fall back on any degenerate value.
  contract_total <- tryCatch({
    v <- mufflyaccess::urps_count()
    if (is.data.frame(v)) nrow(v) else suppressWarnings(as.numeric(v)[1])
  }, error = function(e) 1306L)
  if (length(contract_total) != 1L || !is.finite(contract_total) || contract_total <= 0)
    contract_total <- 1306L
  pct_diff <- abs(n_agents - contract_total) / contract_total * 100

  if (verbose) {
    message(sprintf(
      "initialize_urps_agents(): N=%d | contract=%d | diff=%.1f%% | max_age=%d | source=%s",
      n_agents, contract_total, pct_diff, max_age, roster_source
    ))
    if (pct_diff > 10) {
      message(sprintf(
        "  WARNING: agent count differs from contract by %.1f%%.", pct_diff
      ))
    }
  }

  return(agents)
}


#' @noRd
.build_state_to_division_lookup <- function() {
  data.frame(
    state_abb = c(
      "CT", "ME", "MA", "NH", "RI", "VT",
      "NJ", "NY", "PA",
      "IL", "IN", "MI", "OH", "WI",
      "IA", "KS", "MN", "MO", "NE", "ND", "SD",
      "DE", "DC", "FL", "GA", "MD", "NC", "SC", "VA", "WV",
      "AL", "KY", "MS", "TN",
      "AR", "LA", "OK", "TX",
      "AZ", "CO", "ID", "MT", "NV", "NM", "UT", "WY",
      "AK", "CA", "HI", "OR", "WA"
    ),
    census_division = c(
      rep("New England", 6),
      rep("Middle Atlantic", 3),
      rep("East North Central", 5),
      rep("West North Central", 7),
      rep("South Atlantic", 9),
      rep("East South Central", 4),
      rep("West South Central", 4),
      rep("Mountain", 8),
      rep("Pacific", 5)
    ),
    stringsAsFactors = FALSE
  )
}
