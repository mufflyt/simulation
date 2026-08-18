################################################################################
# R/supply-fraher_agent_supply.R
# Fraher Agent Supply Engine for URPS Providers
################################################################################

#' Initialize URPS Provider Agent Cohort
#'
#' @param n Initial count of agents (default: 1306).
#' @param max_age Maximum age at baseline (default: 70L).
#' @param verbose Logical print message.
#' @return Tibble of initialized provider agents.
#' @export
initialize_urps_agents <- function(n = 1306L, max_age = 70L, verbose = TRUE) {
  set.seed(20260802)
  ages <- sample(32:max_age, n, replace = TRUE)
  sexes <- sample(c("Female", "Male"), n, replace = TRUE, prob = c(0.70, 0.30))
  pathways <- sample(c("Urology", "Gynecology"), n, replace = TRUE, prob = c(0.40, 0.60))
  divisions <- sample(paste("Division", 1:9), n, replace = TRUE)
  
  tibble::tibble(
    agent_id = sprintf("URPS_%05d", 1:n),
    npi = sprintf("999%07d", 1:n),
    age = ages,
    sex = sexes,
    pathway = pathways,
    census_division = divisions,
    clinical_fte = 1.0,
    status = "Active",
    simulation_year = 2023L
  )
}



#' Advance URPS Agents by One Simulation Year
#'
#' @param agents Tibble of provider agents.
#' @param exit_probs Data frame of exit probabilities.
#' @param new_entrants Annual new entrant count (default 70L).
#' @param scenario_id Scenario identifier. Default: "baseline".
#' @param year_seed Random seed for year transitions.
#' @param verbose Logical print message.
#' @return Updated agent tibble.
#' @export
advance_urps_agents <- function(agents, exit_probs, new_entrants = 70L,
                                scenario_id = "baseline", year_seed = NULL,
                                verbose = TRUE) {
  allowed_scenarios <- c("baseline", "early_retirement", "delayed_retirement", "increased_fellows")
  if (!scenario_id %in% allowed_scenarios) {
    stop(sprintf("scenario_id '%s' is not registered.", scenario_id), call. = FALSE)
  }
  
  if (!is.null(year_seed)) set.seed(year_seed)
  
  current_year <- max(agents$simulation_year, na.rm = TRUE)
  next_year <- current_year + 1L
  
  # Advance active agents
  active_idx <- which(agents$status == "Active")
  agents$age[active_idx] <- agents$age[active_idx] + 1L
  
  # Join exit hazards
  agents_merged <- merge(agents, exit_probs, by = "age", all.x = TRUE)
  agents_merged$prob_exit[is.na(agents_merged$prob_exit)] <- 0.50
  
  # Determine exits
  draws <- stats::runif(nrow(agents))
  for (i in seq_len(nrow(agents))) {
    if (agents$status[i] == "Active") {
      p_exit <- agents_merged$prob_exit[i]
      if (draws[i] < p_exit || agents$age[i] > 75L) {
        agents$status[i] <- "Retired"
        agents$clinical_fte[i] <- NA_real_
      }
    }
  }
  agents$simulation_year <- next_year
  
  # Add new entrants if specified
  if (new_entrants > 0L) {
    max_id <- max(as.integer(sub("URPS_", "", agents$agent_id)), na.rm = TRUE)
    new_ids <- sprintf("URPS_%05d", (max_id + 1L):(max_id + new_entrants))
    entrants <- tibble::tibble(
      agent_id = new_ids,
      npi = sprintf("999%07d", (max_id + 1L):(max_id + new_entrants)),
      age = sample(33:36, new_entrants, replace = TRUE),
      sex = sample(c("Female", "Male"), new_entrants, replace = TRUE, prob = c(0.70, 0.30)),
      pathway = sample(c("Urology", "Gynecology"), new_entrants, replace = TRUE, prob = c(0.40, 0.60)),
      census_division = sample(paste("Division", 1:9), new_entrants, replace = TRUE),
      clinical_fte = 1.0,
      status = "Active",
      simulation_year = next_year
    )
    agents <- dplyr::bind_rows(agents, entrants)
  }
  
  agents
}


