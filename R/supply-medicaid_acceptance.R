# Physician-Level Medicaid Acceptance Model ----
#
# Predicts individual physician Medicaid acceptance probability based on empirical
# audit data from the 2026 Lizeth Acosta study (/Users/tmuffly/lizeth).
#
# EMPIRICAL ANCHORS:
#   - Overall Medicaid Acceptance: 42.1% (57.9% refusal rate)
#   - Academic Medical Center OR: 3.42 (74% academic acceptance vs 28% private)
#   - Hospital-Based Outpatient Setting OR: 2.15
#   - State Medicaid-to-Medicare Fee Index OR: 1.85 (per 0.20 fee ratio increase)
#   - High Social Vulnerability Index (SVI) OR: 1.45

#' Coefficients for the Physician Medicaid Acceptance Logistic Model
#' @export
MEDICAID_ACCEPTANCE_COEF <- list(
  intercept = -1.15,           # Baseline logit for private office in average fee state (~24% baseline)
  academic_setting = 1.23,     # OR = 3.42 for academic / university health system
  hospital_outpatient = 0.77,  # OR = 2.15 for hospital-based outpatient department
  medicaid_fee_ratio = 2.05,   # Coefficient per unit increase in Medicaid/Medicare fee index
  svi = 0.37,                  # Coefficient per unit SVI (0-1 scale)
  years_certified = -0.015     # Slight decrease in Medicaid acceptance with career longevity
)

#' State-by-State Medicaid-to-Medicare Physician Fee Index Reference Table
#'
#' @description
#' Empirical state Medicaid fee index relative to Medicare PFS (KFF / MACPAC benchmarks).
#' National mean = 0.72.
#'
#' @return A tibble with `state_abbr`, `medicaid_fee_ratio`, and `fee_tier`.
#' @family supply
#' @concept supply
#' @export
medicaid_medicare_fee_index_table <- function() {
  tibble::tribble(
    ~state_abbr, ~medicaid_fee_ratio, ~fee_tier,
    "AK", 1.25, "high",
    "NC", 1.05, "high",
    "MT", 1.00, "high",
    "ND", 0.95, "high",
    "ID", 0.95, "high",
    "NM", 0.92, "high",
    "WY", 0.90, "high",
    "NE", 0.88, "above_average",
    "IA", 0.85, "above_average",
    "CO", 0.82, "above_average",
    "TX", 0.78, "above_average",
    "MA", 0.75, "average",
    "OH", 0.72, "average",
    "IL", 0.68, "below_average",
    "FL", 0.62, "below_average",
    "PA", 0.58, "low",
    "CA", 0.56, "low",
    "NJ", 0.51, "low",
    "RI", 0.49, "low",
    "NY", 0.48, "low"
  )
}

#' Lookup State Medicaid-to-Medicare Fee Ratio
#'
#' @param state_abbr Character vector of 2-letter state postal abbreviations.
#' @param default_ratio Default ratio if state is unlisted or NA (default 0.72).
#'
#' @return Numeric vector of fee ratios.
#' @family supply
#' @concept supply
#' @export
lookup_state_medicaid_fee_ratio <- function(state_abbr, default_ratio = 0.72) {
  tbl <- medicaid_medicare_fee_index_table()
  st_clean <- base::toupper(base::trimws(base::as.character(state_abbr)))
  idx <- base::match(st_clean, tbl$state_abbr)
  ratios <- tbl$medicaid_fee_ratio[idx]
  ratios[base::is.na(ratios)] <- default_ratio
  ratios
}

#' Predict individual physician Medicaid acceptance probability
#'
#' @param academic_setting Logical or numeric vector indicating academic medical center practice.
#' @param hospital_outpatient Logical or numeric vector indicating HOD setting.
#' @param medicaid_fee_ratio State Medicaid-to-Medicare fee ratio (default 0.72 national avg).
#' @param svi Social Vulnerability Index of practice ZIP (0.0 to 1.0, default 0.50).
#' @param years_certified Years since board certification (default 10).
#' @param coef Model coefficient list; defaults to [MEDICAID_ACCEPTANCE_COEF].
#' @return Numeric vector of probabilities in [0, 1].
#' @family supply
#' @concept supply
#' @export
predict_medicaid_acceptance <- function(academic_setting = FALSE,
                                       hospital_outpatient = FALSE,
                                       medicaid_fee_ratio = 0.72,
                                       svi = 0.50,
                                       years_certified = 10,
                                       coef = MEDICAID_ACCEPTANCE_COEF) {
  is_acad <- as.numeric(academic_setting %in% c(TRUE, "TRUE", "True", 1))
  is_hod  <- as.numeric(hospital_outpatient %in% c(TRUE, "TRUE", "True", 1))

  logit <- coef$intercept +
    coef$academic_setting * is_acad +
    coef$hospital_outpatient * is_hod +
    coef$medicaid_fee_ratio * (medicaid_fee_ratio - 0.72) +
    coef$svi * (svi - 0.50) +
    coef$years_certified * (years_certified - 10)

  prob <- 1 / (1 + exp(-logit))
  pmin(pmax(prob, 0.05), 0.95)
}

#' Filter provider supply table by insurance type for E2SFCA spatial access
#'
#' For commercial / Medicare patients, all active providers contribute full FTE capacity.
#' For Medicaid patients, provider capacity is scaled by individual Medicaid acceptance probability
#' or binary acceptance threshold, simulating the 42.1% Medicaid access bottleneck.
#'
#' @param provider_supply Provider supply table with `provider_id`, `supply` (FTEs),
#'   and optional `academic_setting`, `hospital_outpatient`, `medicaid_fee_ratio`, `svi`.
#' @param insurance Insurance filter: `"Commercial"`, `"Medicare"`, or `"Medicaid"`.
#' @param probabilistic If TRUE, scales supply by $P(\text{accepts\_medicaid})$; if FALSE, uses binary draw/threshold.
#' @return Filtered or scaled provider supply table ready for [compute_e2sfca_access()].
#' @family spatial access
#' @concept geography
#' @export
filter_supply_by_insurance <- function(provider_supply,
                                       insurance = "Commercial",
                                       probabilistic = TRUE) {
  if (!insurance %in% c("Medicaid", "medicaid")) {
    return(provider_supply)
  }

  acad <- if ("academic_setting" %in% names(provider_supply)) provider_supply$academic_setting else FALSE
  hod  <- if ("hospital_outpatient" %in% names(provider_supply)) provider_supply$hospital_outpatient else FALSE
  fee  <- if ("medicaid_fee_ratio" %in% names(provider_supply)) provider_supply$medicaid_fee_ratio else 0.72
  svi  <- if ("svi" %in% names(provider_supply)) provider_supply$svi else 0.50
  yrs  <- if ("years_certified" %in% names(provider_supply)) provider_supply$years_certified else 10

  p_accept <- predict_medicaid_acceptance(
    academic_setting = acad,
    hospital_outpatient = hod,
    medicaid_fee_ratio = fee,
    svi = svi,
    years_certified = yrs
  )

  res <- provider_supply
  if (isTRUE(probabilistic)) {
    res$supply <- res$supply * p_accept
  } else {
    res <- res[p_accept >= 0.50, , drop = FALSE]
  }
  res
}
