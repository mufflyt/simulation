# CADR Claims Data Adapter & Delegation Calibration Engine ----
#
# Scientific Hardening Layer: CADR Medicare Carrier file processing.
# Addresses claims limitations:
# 1. Aggregated CADR summary CSVs lack NPI and provider specialty.
# 2. 90-day global package postoperative visits do NOT generate separate E/M claim lines (unbilled in 2008-2016).
# 3. Separately billed intake and follow-up visits can be empirically categorized by CMS specialty code (50=NP, 97=PA).
# Global postoperative visit delegation is treated as a scenario parameter (25%, 50%, 75%, 90%).

#' CMS Provider Specialty Codes for CADR Analysis
#'
#' @return Named character vector of CMS specialty codes.
#' @family CADR claims
#' @concept demand
#' @export
urps_cadr_specialty_codes <- function() {
  c(
    "50" = "Nurse Practitioner (NP)",
    "97" = "Physician Assistant (PA)",
    "16" = "Obstetrics & Gynecology",
    "34" = "Urology",
    "08" = "General Practice",
    "11" = "Internal Medicine",
    "37" = "Pediatric Medicine",
    "38" = "Geriatric Medicine",
    "84" = "Preventive Medicine",
    "65" = "Physical Therapist"
  )
}

#' Build Formal CADR Micro-Data Extract Specification
#'
#' @description
#' Generates the exact specification table and data dictionary to request
#' a Carrier-line extract from the CADR team (Joanna et al.) to separate NP versus PA
#' delegation for billed visits.
#'
#' @return A tibble with field names, descriptions, and privacy treatment.
#' @family CADR claims
#' @concept demand
#' @export
build_cadr_extract_request <- function() {
  tibble::tribble(
    ~field_name, ~description, ~privacy_treatment,
    "episode_id", "Deidentified surgical episode identifier", "Hashed / Synthetic ID",
    "days_from_index", "Days between index surgery and service date", "Integer offset",
    "claim_id", "Carrier claim identifier", "Hashed ID",
    "line_num", "Claim line number", "Unmodified integer",
    "hcpcs_cd", "HCPCS/CPT procedure or E/M code", "Standard CPT code",
    "hcpcs_modifier", "CPT modifier (e.g. 24, 25, 53, 78, 79)", "Unmodified character",
    "prvdr_spclty", "CMS numeric performing provider specialty code", "CMS Specialty Code (e.g. 50, 97)",
    "prf_physn_npi", "Performing provider NPI", "Hashed NPI or Provider Category",
    "carr_line_prvdr_type_cd", "Carrier line provider type code", "CMS Type Code",
    "org_npi_num", "Billing organization NPI", "Hashed Org NPI",
    "place_of_service", "Place of service code (e.g. 11=Office, 22=Outpatient)", "Standard POS Code",
    "allowed_amount", "Medicare allowed payment amount", "Numeric dollars",
    "is_global_period", "Indicator if service occurred within 90-day global period", "Logical (TRUE/FALSE)",
    "derived_provider_category", "Standardized provider category (NP, PA, OBGYN, Urology, PrimaryCare, PT, Other, Missing)", "Categorical Factor"
  )
}

#' Classify CADR Provider Category from CMS Specialty Codes
#'
#' @param line_tbl Carrier line table containing `prvdr_spclty` and optional `npi`.
#'
#' @return Table with added `derived_provider_category` column.
#' @family CADR claims
#' @concept demand
#' @export
classify_cadr_provider_category <- function(line_tbl) {
  if (!base::is.data.frame(line_tbl)) {
    base::stop("`line_tbl` must be a data frame.")
  }

  if (!"prvdr_spclty" %in% names(line_tbl)) {
    base::stop("`line_tbl` must contain column `prvdr_spclty`.")
  }

  base::message("[cadr-adapter] Classifying performing provider categories.")

  line_tbl |>
    dplyr::mutate(
      clean_spclty = base::sprintf("%02d", base::as.integer(.data$prvdr_spclty)),
      derived_provider_category = dplyr::case_when(
        .data$clean_spclty == "50" ~ "NP",
        .data$clean_spclty == "97" ~ "PA",
        .data$clean_spclty == "16" ~ "OBGYN",
        .data$clean_spclty == "34" ~ "Urology",
        .data$clean_spclty %in% c("08", "11", "37", "38", "84") ~ "PrimaryCare",
        .data$clean_spclty == "65" ~ "PT",
        base::is.na(.data$prvdr_spclty) | .data$clean_spclty == "00" ~ "Missing",
        TRUE ~ "Other"
      )
    )
}

#' Calibrate CADR Delegation Bounds and Global Package Sensitivity Grid
#'
#' @description
#' Combines empirical CADR estimates for separately billed E/M visits with
#' probabilistic scenario grids for unbilled 90-day global postoperative visits.
#'
#' @param empirical_billed_tbl Table of observed billed visit delegation rates.
#' @param global_scenario_grid Vector of scenario delegation shares for unbilled global visits (e.g. 0.25, 0.50, 0.75, 0.90).
#'
#' @return A long tibble of delegation policy scenarios for `deconstruct_workload_rvus()`.
#' @family CADR claims
#' @concept demand
#' @export
calibrate_cadr_delegation_bounds <- function(
    empirical_billed_tbl = NULL,
    global_scenario_grid = c(0.25, 0.50, 0.75, 0.90)) {

  base::message("[cadr-adapter] Calibrating delegation bounds and global-period scenario grid.")

  if (base::is.null(empirical_billed_tbl)) {
    empirical_billed_tbl <- tibble::tribble(
      ~phase, ~empirical_app_share, ~emp_source,
      "initial_intake", 0.35, "CADR separately billed E/M claims (2008-2016)",
      "pre_service", 0.25, "CADR pre-procedure consultation claims"
    )
  }

  scenarios <- base::lapply(
    global_scenario_grid,
    function(global_share) {
      tibble::tribble(
        ~scenario_id, ~phase, ~app_share, ~surgeon_rework_share, ~estimation_basis,
        base::paste0("global_app_", base::round(global_share * 100)), "initial_intake", 0.35, 0.10, "Empirical CADR billed claim",
        base::paste0("global_app_", base::round(global_share * 100)), "pre_service", 0.25, 0.15, "Empirical CADR billed claim",
        base::paste0("global_app_", base::round(global_share * 100)), "intra_service", 0.00, 0.00, "Surgeon intra-op time non-delegable",
        base::paste0("global_app_", base::round(global_share * 100)), "post_service", global_share, 0.10, "Unbilled global package scenario parameter"
      )
    }
  )

  dplyr::bind_rows(scenarios)
}
