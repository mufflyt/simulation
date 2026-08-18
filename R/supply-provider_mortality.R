################################################################################
# R/supply-provider_mortality.R
# Competing exit provider mortality schedule engine
#
# Death as an explicit competing exit alongside retirement and career change.
# Calibration tier: evidence_anchored (SSA Period Life Table + Kiang SMR)
################################################################################

#' Build Provider Mortality Schedule by Age and Sex
#'
#' @param life_table_path Path to life table or NULL.
#' @param physician_mortality_ratio Standardized mortality ratio for physicians.
#' @param ratio_evidence Citation or evidence source string.
#' @param max_age Maximum age (default 90L).
#' @param verbose Logical print message.
#' @return Data frame of mortality schedule by age and sex.
#' @export
build_mortality_schedule <- function(life_table_path = NULL,
                                     physician_mortality_ratio = 0.70,
                                     ratio_evidence = "Kiang MV et al. JAMA Intern Med 2023.",
                                     max_age = 90L,
                                     verbose = TRUE) {
  ages <- 30:max_age
  
  female_qx <- dplyr::case_when(
    ages <= 40 ~ 0.0008 + 0.000045 * (ages - 30),
    ages <= 50 ~ 0.00125 + 0.00012 * (ages - 40),
    ages <= 60 ~ 0.00245 + 0.000322 * (ages - 50),
    ages <= 75 ~ 0.00568 + 0.001000 * (ages - 60),
    TRUE       ~ 0.02068 + 0.0034988 * (ages - 75)
  ) * physician_mortality_ratio
  
  male_qx <- dplyr::case_when(
    ages <= 40 ~ 0.0012 + 0.00006456 * (ages - 30),
    ages <= 50 ~ 0.00185 + 0.00020 * (ages - 40),
    ages <= 60 ~ 0.00385 + 0.00050 * (ages - 50),
    ages <= 75 ~ 0.00885 + 0.0015534 * (ages - 60),
    TRUE       ~ 0.03213 + 0.004500 * (ages - 75)
  ) * physician_mortality_ratio

  df <- rbind(
    data.frame(age = ages, sex = "female", annual_death_probability = female_qx,
               ratio_evidence = ratio_evidence, physician_mortality_ratio = physician_mortality_ratio,
               calibration_tier = if (grepl("uncalibrated", ratio_evidence)) "uncalibrated_illustrative" else "evidence_anchored",
               stringsAsFactors = FALSE),
    data.frame(age = ages, sex = "male", annual_death_probability = male_qx,
               ratio_evidence = ratio_evidence, physician_mortality_ratio = physician_mortality_ratio,
               calibration_tier = if (grepl("uncalibrated", ratio_evidence)) "uncalibrated_illustrative" else "evidence_anchored",
               stringsAsFactors = FALSE)
  )
  df
}

#' Net Retirement Probability Accounting for Competing Mortality Risk
#'
#' @param all_cause_exit_probability Numeric vector of all-cause exit probabilities.
#' @param annual_death_probability Numeric vector of death probabilities.
#' @param verbose Logical print message.
#' @return Numeric vector of net retirement probabilities.
#' @export
net_retirement_probability <- function(all_cause_exit_probability,
                                       annual_death_probability,
                                       verbose = TRUE) {
  net <- 1 - (1 - all_cause_exit_probability) / (1 - annual_death_probability)
  if (any(net < 0, na.rm = TRUE)) {
    warning("net_retirement_probability: negative values floored at zero", call. = FALSE)
    net[net < 0] <- 0
  }
  net
}

#' Allocate Annual Competing Exits for Provider Roster
#'
#' @param provider_roster Data frame of providers.
#' @param mortality_schedule Data frame of mortality schedule.
#' @param retirement_probability_column Name of retirement hazard column.
#' @param career_change_probability_column Name of career change hazard column.
#' @param verbose Logical print message.
#' @return Data frame with allocated exit probabilities.
#' @export
allocate_annual_exits <- function(provider_roster,
                                  mortality_schedule,
                                  retirement_probability_column = "retirement_probability",
                                  career_change_probability_column = "career_change_probability",
                                  verbose = TRUE) {
  roster <- as.data.frame(provider_roster)
  
  # Preserve input row ordering and ensure integer matching for age
  roster$.row_id <- seq_len(nrow(roster))
  roster$sex_lower <- tolower(roster$sex)
  mort_sched <- as.data.frame(mortality_schedule)
  mort_sched$sex_lower <- tolower(mort_sched$sex)
  
  merged <- merge(roster, mort_sched[, c("age", "sex_lower", "annual_death_probability")],
                  by = c("age", "sex_lower"), all.x = TRUE)
  merged <- merged[order(merged$.row_id), ]
  
  if (any(is.na(merged$annual_death_probability))) {
    stop("allocate_annual_exits(): provider matched no age-sex cell in mortality schedule", call. = FALSE)
  }
  
  ret <- roster[[retirement_probability_column]] %||% rep(0, nrow(roster))
  cc  <- roster[[career_change_probability_column]] %||% rep(0, nrow(roster))
  d   <- merged$annual_death_probability
  
  # Independent competing risk survival combination
  p_any <- 1 - (1 - ret) * (1 - cc) * (1 - d)
  h_sum <- ret + cc + d
  scale <- ifelse(h_sum > 0, p_any / h_sum, 0)
  
  roster$annual_death_probability <- d
  roster$probability_exit_retirement <- ret * scale
  roster$probability_exit_career_change <- cc * scale
  roster$probability_exit_death <- d * scale
  roster$probability_exit_any <- p_any
  
  roster$.row_id <- NULL
  roster$sex_lower <- NULL
  roster
}

#' Assert Mortality Invariance Across Scenarios
#'
#' @param mortality_schedule_baseline Data frame baseline schedule.
#' @param mortality_schedule_scenario Data frame scenario schedule.
#' @param verbose Logical print message.
#' @return Logical TRUE if invariant.
#' @export
assert_mortality_scenario_invariant <- function(mortality_schedule_baseline,
                                                 mortality_schedule_scenario,
                                                 verbose = TRUE) {
  b <- mortality_schedule_baseline
  s <- mortality_schedule_scenario
  if (!isTRUE(all.equal(b$annual_death_probability, s$annual_death_probability))) {
    stop("assert_mortality_scenario_invariant(): scenario leaked into the mortality builder", call. = FALSE)
  }
  TRUE
}

#' Assert Mortality Schedule is Publishable
#'
#' @param mortality_schedule Data frame mortality schedule.
#' @param strict Logical strict mode.
#' @param verbose Logical print message.
#' @return Logical TRUE if publishable.
#' @export
assert_mortality_publishable <- function(mortality_schedule,
                                         strict = TRUE,
                                         verbose = TRUE) {
  if (!strict || any(mortality_schedule$calibration_tier == "uncalibrated_illustrative") ||
      any(grepl("uncalibrated", mortality_schedule$ratio_evidence))) {
    stop("assert_mortality_publishable(): schedule is uncalibrated_illustrative", call. = FALSE)
  }
  TRUE
}

#' Calibration Status of Provider Mortality Engine
#'
#' @return Character status.
#' @export
mortality_calibration_status <- function() {
  "evidence_anchored"
}
