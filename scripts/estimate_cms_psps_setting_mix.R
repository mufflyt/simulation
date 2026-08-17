#!/usr/bin/env Rscript
# =============================================================================
# Estimate URPS CPT Setting Mix (Facility vs Office) from CMS PSPS 2024
# =============================================================================
#
# PURPOSE:
#   Parses CMS Physician/Supplier Procedure Summary (PSPS) 2024 data to compute
#   national facility (Hospital OR / ASC) vs non-facility (Office) service volume
#   ratios for URPS surgical procedures, injections, and diagnostic urodynamics.
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
})

cat("=================================================================\n")
cat("CMS PSPS 2024 CPT SETTING MIX ESTIMATION PIPELINE\n")
cat("=================================================================\n\n")

psps_path <- "data-raw/cms_psps/MUP_PHY_R26_P05_V10_D24_Geo.csv"
if (!file.exists(psps_path)) stop("CMS PSPS 2024 file not found at ", psps_path)

psps <- read.csv(psps_path, stringsAsFactors = FALSE)

# Filter National level records
nat <- psps |>
  dplyr::filter(Rndrng_Prvdr_Geo_Lvl == "National")

cpts <- c("57288", "51840", "57280", "57425", "57240", "57250", "57260", "57265",
          "64590", "64561", "51715", "53885", "51726", "51729", "52000")

setting_summary <- nat |>
  dplyr::filter(HCPCS_Cd %in% cpts) |>
  dplyr::group_by(HCPCS_Cd, HCPCS_Desc) |>
  dplyr::summarize(
    services_facility = sum(Tot_Srvcs[Place_Of_Srvc == "F"], na.rm = TRUE),
    services_office   = sum(Tot_Srvcs[Place_Of_Srvc %in% c("O", "N")], na.rm = TRUE),
    total_services    = services_facility + services_office,
    p_facility        = ifelse(total_services > 0, services_facility / total_services, 1.0),
    p_office          = ifelse(total_services > 0, services_office / total_services, 0.0),
    avg_allowed_fac   = ifelse(any(Place_Of_Srvc == "F"), mean(Avg_Mdcr_Alowd_Amt[Place_Of_Srvc == "F"], na.rm = TRUE), NA),
    avg_allowed_off   = ifelse(any(Place_Of_Srvc %in% c("O", "N")), mean(Avg_Mdcr_Alowd_Amt[Place_Of_Srvc %in% c("O", "N")], na.rm = TRUE), NA),
    .groups = "drop"
  ) |>
  dplyr::arrange(dplyr::desc(total_services))

cat("--- CMS PSPS 2024 URPS CPT Setting Mix Ratios ---\n")
print(as.data.frame(setting_summary[, c("HCPCS_Cd", "total_services", "services_facility", "services_office", "p_facility", "p_office")]), digits = 4)

# -----------------------------------------------------------------------------
# Summary by Clinical Category
# -----------------------------------------------------------------------------
cat("\n=================================================================\n")
cat("URPS CLINICAL CATEGORY SETTING MIX SUMMARY\n")
cat("=================================================================\n\n")

cat_summary <- setting_summary |>
  dplyr::mutate(
    category = dplyr::case_when(
      HCPCS_Cd %in% c("57288", "51840") ~ "Sling / Incontinence Surgery",
      HCPCS_Cd %in% c("57280", "57425", "57240", "57250", "57260", "57265") ~ "Prolapse Repair (Sacrocolpopexy/Vaginal)",
      HCPCS_Cd %in% c("64590", "64561", "51715", "53885") ~ "Neuromodulation / Injections",
      TRUE ~ "Diagnostic Urodynamics & Cystoscopy"
    )
  ) |>
  dplyr::group_by(category) |>
  dplyr::summarize(
    tot_fac = sum(services_facility),
    tot_off = sum(services_office),
    tot_all = sum(total_services),
    pct_facility = round(100 * tot_fac / tot_all, 2),
    pct_office   = round(100 * tot_off / tot_all, 2),
    .groups = "drop"
  )

print(as.data.frame(cat_summary))

cat("\nDone. CMS PSPS setting mix estimation complete.\n")
