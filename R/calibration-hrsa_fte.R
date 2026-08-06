################################################################################
# R/calibration-hrsa_fte.R
# HRSA HWSM surgical-specialty FTE calibration
#
# Addresses README item 2: headcount -> FTE step unvalidated.
# Calibration tier: derived_by_analogy (HRSA surgical)
# TODO-FTE-001: Replace with URPS ABOG MOC survey values.
################################################################################

#' Apply HRSA Surgical FTE Calibration to URPS Agent Population
#'
#' @param agents Data frame from initialize_urps_agents().
#' @param reference_hrs Numeric. Default: 37.2 (Dall 2021).
#' @param handle_unknown_age Character. "assign_1.0" or "cohort_mean".
#' @param verbose Logical. Default: TRUE.
#'
#' @return agents with clinical_fte, fte_source, fte_calibration_tier.
#' @importFrom assertthat assert_that
#' @importFrom dplyr mutate left_join case_when if_else select n
#' @export
apply_hrsa_surgical_fte <- function(agents,
                                    reference_hrs      = 37.2,
                                    handle_unknown_age = "assign_1.0",
                                    verbose            = TRUE) {
  assertthat::assert_that(
    is.data.frame(agents),
    all(c("age", "sex") %in% colnames(agents)),
    msg = "agents must have age and sex columns"
  )

  # HRSA HWSM surgical specialty hours
  # Calibration tier: derived_by_analogy (TODO-FTE-001: ABOG MOC survey)
  hrsa_surgical <- data.frame(
    age_group      = rep(c("<35", "35-44", "45-54", "55-64", "65+"), 2),
    sex            = rep(c("Male", "Female"), each = 5),
    hours_per_week = c(55.0, 58.0, 56.0, 49.0, 35.0,
                       50.0, 52.0, 50.0, 44.0, 30.0),
    stringsAsFactors = FALSE
  )

  peak_male   <- hrsa_surgical$hours_per_week[
    hrsa_surgical$sex == "Male" & hrsa_surgical$age_group == "35-44"]
  peak_female <- hrsa_surgical$hours_per_week[
    hrsa_surgical$sex == "Female" & hrsa_surgical$age_group == "35-44"]

  hrsa_surgical <- hrsa_surgical %>%
    dplyr::mutate(
      peak_hrs     = dplyr::if_else(sex == "Male", peak_male, peak_female),
      relative_fte = pmin(1.0, hours_per_week / peak_hrs)
    )

  result <- agents %>%
    dplyr::mutate(
      age_group = dplyr::case_when(
        age < 35              ~ "<35",
        age >= 35 & age < 45  ~ "35-44",
        age >= 45 & age < 55  ~ "45-54",
        age >= 55 & age < 65  ~ "55-64",
        age >= 65             ~ "65+",
        TRUE                  ~ NA_character_
      ),
      sex_clean = dplyr::case_when(
        tolower(sex) %in% c("female", "f") ~ "Female",
        tolower(sex) %in% c("male",   "m") ~ "Male",
        TRUE                               ~ NA_character_
      )
    ) %>%
    dplyr::left_join(
      hrsa_surgical %>% dplyr::select(age_group, sex, relative_fte),
      by = c("age_group", "sex_clean" = "sex")
    ) %>%
    dplyr::mutate(
      clinical_fte = dplyr::case_when(
        status == "Retired"  ~ NA_real_,
        is.na(age_group) & handle_unknown_age == "assign_1.0" ~ 1.0,
        !is.na(relative_fte) ~ relative_fte,
        TRUE                 ~ 1.0
      ),
      fte_source = dplyr::case_when(
        status == "Retired"  ~ "excluded_retired",
        is.na(age_group)     ~ "unknown_age_rule_1.0",
        !is.na(relative_fte) ~ "hrsa_surgical_derived_by_analogy",
        TRUE                 ~ "fallback_1.0"
      ),
      fte_calibration_tier = dplyr::if_else(
        status == "Retired", "excluded", "derived_by_analogy"
      )
    ) %>%
    dplyr::select(-age_group, -sex_clean, -relative_fte)

  if (verbose) {
    active <- result[result$status == "Active", ]
    hc     <- nrow(active)
    cwe    <- sum(active$clinical_fte, na.rm = TRUE)
    message(sprintf(
      "apply_hrsa_surgical_fte(): HC=%d | CWE=%.1f | FTE/HC=%.3f | ref=%.1f hrs/wk | tier=derived_by_analogy (TODO-FTE-001)",
      hc, cwe, cwe / hc, reference_hrs
    ))
  }

  return(result)
}
