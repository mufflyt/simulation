# ARCHIVED supply capability -- NOT part of the package.
#
# Each function here was exported, tested, and reachable from no pipeline. They
# are archived rather than deleted because the implementation is the expensive
# part and each is a real capability the model may want: provider migration is
# the mechanism for relocation, and advance_urps_agents() is a step of an
# alternative (Fraher) agent engine.
#
# inst/archive is .Rbuildignore'd, so nothing here is installed, loaded, or
# checked. RESTORING ONE MEANS MOVING IT BACK INTO R/, RE-EXPORTING IT, AND
# WIRING IT TO A CALLER. An export with no caller is the defect that put it
# here -- see tests/export-registry.csv and docs/GUARDS.md section 1.

# ---- advance_urps_agents()  (was R/supply-fraher_agent_supply.R) ---------------------------

#' Advance URPS Agent Population One Year
#'
#' @param agents Data frame from initialize_urps_agents().
#' @param exit_hazard Data frame from build_urps_exit_hazard().
#' @param new_entrants Integer or data frame. Default: 72L.
#' @param scenario_id Character. Must be registered. Default: "status_quo".
#' @param year_seed Integer or NULL.
#' @param verbose Logical. Default: FALSE.
#'
#' @return Updated agents data frame.
#' @importFrom assertthat assert_that
#' @importFrom dplyr filter mutate bind_rows left_join select if_else n
#' @importFrom tidyr replace_na
#' @family fraher agent supply
#' @concept supply
#' @export
advance_urps_agents <- function(agents,
                                exit_hazard,
                                new_entrants = 72L,
                                scenario_id  = "status_quo",
                                year_seed    = NULL,
                                verbose      = FALSE) {
  # This called urpssim::urps_scenarios(), which does not exist, so the tryCatch
  # ALWAYS fell through to the one-row fallback: the scenario check has never
  # validated against a real registry. Made explicit rather than wired to
  # mufflyaccess::urps_scenarios(), whose ids are baseline / retire_2yr_* and
  # would reject this function's own default of "status_quo" -- picking the right
  # registry is a modelling decision, not a lint fix.
  registered <- data.frame(id = "status_quo", stringsAsFactors = FALSE)
  assertthat::assert_that(
    scenario_id %in% registered$id,
    msg = sprintf(
      "Scenario '%s' not registered. Use assert_scenarios_registered().",
      scenario_id
    )
  )

  if (!is.null(year_seed)) set.seed(year_seed)

  if (!is.data.frame(agents) || nrow(agents) == 0L)
    stop("`agents` must be a non-empty data frame.", call. = FALSE)
  .need <- c("status", "simulation_year", "age", "sex")
  if (!all(.need %in% names(agents)))
    stop("`agents` is missing required column(s): ",
         paste(setdiff(.need, names(agents)), collapse = ", "), call. = FALSE)

  current_year   <- max(agents$simulation_year, na.rm = TRUE)
  n_active_start <- sum(agents$status == "Active", na.rm = TRUE)

  active <- agents %>%
    dplyr::filter(status == "Active") %>%
    dplyr::left_join(
      exit_hazard %>% dplyr::select(age, sex, prob_exit),
      by = c("age", "sex")
    )
  n_unmatched <- sum(is.na(active$prob_exit))
  if (n_unmatched > 0L)
    warning(sprintf(paste0(
      "%d active provider-row(s) had no (age, sex) match in the exit-hazard table and ",
      "fall back to a flat 1%%/yr exit -- check that agents$sex uses the same coding as ",
      "the hazard table's sex ('Female'/'Male') and ages lie in the hazard grid."),
      n_unmatched), call. = FALSE)
  active <- active %>%
    dplyr::mutate(
      prob_exit = tidyr::replace_na(prob_exit, 0.01),
      exit_draw = stats::runif(dplyr::n()),
      status    = dplyr::if_else(exit_draw < prob_exit, "Retired", status)
    ) %>%
    dplyr::select(-prob_exit, -exit_draw)

  n_exits <- sum(active$status == "Retired", na.rm = TRUE)

  active <- active %>%
    dplyr::mutate(
      age             = age + 1L,
      simulation_year = current_year + 1L
    )

  n_fellows <- if (is.numeric(new_entrants)) as.integer(new_entrants) else
    nrow(new_entrants)

  if (n_fellows > 0) {
    max_id <- max(agents$agent_id, na.rm = TRUE)
    if (is.numeric(new_entrants)) {
      fellow_df <- data.frame(
        agent_id        = max_id + seq_len(n_fellows),
        npi             = paste0("FELLOW_", current_year + 1L, "_",
                                 seq_len(n_fellows)),
        age             = 35L,
        sex             = sample(c("Female", "Male"), n_fellows,
                                 replace = TRUE, prob = c(0.88, 0.12)),
        pathway         = sample(c("ABOG", "ABU"), n_fellows,
                                 replace = TRUE, prob = c(0.84, 0.16)),
        census_division = sample(
          c("South Atlantic", "Pacific", "Middle Atlantic"),
          n_fellows, replace = TRUE
        ),
        clinical_fte    = NA_real_,
        status          = "Active",
        simulation_year = current_year + 1L,
        stringsAsFactors = FALSE
      )
    } else {
      fellow_df <- new_entrants %>%
        dplyr::mutate(
          agent_id        = max_id + dplyr::row_number(),
          npi             = paste0("FELLOW_", dplyr::row_number()),
          status          = "Active",
          simulation_year = current_year + 1L
        )
    }
    active <- dplyr::bind_rows(active, fellow_df)
  }

  retired_prior <- agents %>%
    dplyr::filter(status == "Retired") %>%
    dplyr::mutate(
      age             = age + 1L,
      simulation_year = current_year + 1L
    )

  result <- dplyr::bind_rows(active, retired_prior)

  if (verbose) {
    n_active_end <- sum(result$status == "Active", na.rm = TRUE)
    message(sprintf(
      "Year %d -> %d: %d active | -%d exits | +%d entrants | %d active",
      current_year, current_year + 1L,
      n_active_start, n_exits, n_fellows, n_active_end
    ))
  }

  return(result)
}

