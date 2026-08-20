# HRSA & Timothy Dall Sub-National HRR Spatial Aggregation ----
#
# Implements HRSA 5-step spatial balance aggregation across 306 Hospital Referral Regions (HRRs).

#' Aggregate Sub-National Workforce Balance by Hospital Referral Region (HRR)
#'
#' @description
#' Implements HRSA 5-step spatial balance aggregation across 306 Hospital Referral Regions (HRRs).
#'
#' @param provider_roster Data frame of providers with `hrr_code`, `fte`.
#' @param hrr_demand_tbl Data frame of HRRs with `hrr_code`, `hrr_name`, `demand_fte`.
#'
#' @return Regional HRR workforce supply, demand, balance gap, and shortage status.
#' @family HRSA simulation
#' @concept geography
#' @export
aggregate_hrr_workforce_balance <- function(
    provider_roster,
    hrr_demand_tbl) {

  if (!base::is.data.frame(provider_roster) || !base::is.data.frame(hrr_demand_tbl)) {
    base::stop("Both provider_roster and hrr_demand_tbl must be data frames.")
  }

  supply_by_hrr <- provider_roster |>
    dplyr::group_by(.data$hrr_code) |>
    dplyr::summarise(
      supply_fte = base::sum(.data$fte, na.rm = TRUE),
      provider_headcount = dplyr::n(),
      .groups = "drop"
    )

  balance_tbl <- hrr_demand_tbl |>
    dplyr::left_join(supply_by_hrr, by = "hrr_code") |>
    dplyr::mutate(
      supply_fte = dplyr::coalesce(.data$supply_fte, 0.0),
      provider_headcount = dplyr::coalesce(.data$provider_headcount, 0L),
      gap_fte = .data$demand_fte - .data$supply_fte,
      deficit_pct = (.data$demand_fte - .data$supply_fte) / base::pmax(.data$demand_fte, 1e-6),
      hrsa_shortage_area = .data$deficit_pct >= 0.20
    )

  base::message("[hrsa-hrr] Total HRRs evaluated: ", base::nrow(balance_tbl))
  base::message("[hrsa-hrr] HRR shortage areas (deficit >= 20%): ", base::sum(balance_tbl$hrsa_shortage_area))

  balance_tbl
}
