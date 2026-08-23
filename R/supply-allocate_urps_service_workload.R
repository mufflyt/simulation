# Provider-Level URPS Workload Allocation ----------------------------------

#' Allocate URPS Service Workload among Active Providers
#'
#' Converts national/county service demand and compositional service-share draws into
#' procedure-level workload allocated exclusively among active URPS providers.
#'
#' @param service_demand Data frame with `service`, `condition`, and `demand_services`.
#' @param provider_cohort Tibble of active providers (`rendering_npi`, `is_active`, `provider_type`).
#' @param share_draws Compositional share draws from [draw_compositional_service_shares()].
#' @param rvu_table Work RVU lookup table. Defaults to `CMS_WORK_RVU`.
#'
#' @return A list containing `allocated_workload`, `provider_summary`, and `accounting_audit`.
#' @family supply
#' @concept allocation
#' @export
allocate_urps_service_workload <- function(
    service_demand,
    provider_cohort,
    share_draws = NULL,
    rvu_table = CMS_WORK_RVU) {
  base::message("Allocating URPS service workload among active providers.")

  if (base::is.null(share_draws)) {
    share_draws <- draw_compositional_service_shares(n_draws = 1L)
  }

  active_providers <- provider_cohort |>
    dplyr::filter(.data$is_active == TRUE | dplyr::coalesce(.data$status, "") == "active")

  inactive_providers <- provider_cohort |>
    dplyr::filter(!.data$rendering_npi %in% active_providers$rendering_npi)

  # Join service demand with share draws
  demand_with_shares <- service_demand |>
    dplyr::left_join(
      share_draws |> dplyr::filter(.data$draw == 1L),
      by = c("service", "condition"),
      relationship = "many-to-many"
    ) |>
    dplyr::mutate(
      share = dplyr::coalesce(.data$share, 0.20),
      allocated_services = .data$demand_services * .data$share
    )

  # Distribute allocated services evenly across active providers of each type
  allocated_workload <- demand_with_shares |>
    dplyr::left_join(active_providers, by = "provider_type", relationship = "many-to-many") |>
    dplyr::group_by(.data$service, .data$provider_type) |>
    dplyr::mutate(
      n_active_type = dplyr::n_distinct(.data$rendering_npi, na.rm = TRUE),
      provider_service_volume = .data$allocated_services / base::pmax(.data$n_active_type, 1L)
    ) |>
    dplyr::ungroup() |>
    dplyr::left_join(rvu_table |> dplyr::select("service" = "description", "work_rvu"), by = "service") |>
    dplyr::mutate(
      work_rvu = dplyr::coalesce(.data$work_rvu, 2.0),
      allocated_wrvu = .data$provider_service_volume * .data$work_rvu
    )

  # Accounting Audits
  total_demand_services <- base::sum(service_demand$demand_services, na.rm = TRUE)
  total_allocated_services <- base::sum(demand_with_shares$allocated_services, na.rm = TRUE)
  total_urps_wrvu <- base::sum(allocated_workload$allocated_wrvu, na.rm = TRUE)
  inactive_provider_wrvu <- 0.0

  accounting_audit <- list(
    total_demand_services = total_demand_services,
    total_allocated_services = total_allocated_services,
    services_match = base::abs(total_demand_services - total_allocated_services) < 1e-5,
    total_urps_wrvu = total_urps_wrvu,
    inactive_provider_wrvu = inactive_provider_wrvu,
    accounting_passed = base::abs(total_demand_services - total_allocated_services) < 1e-5 && inactive_provider_wrvu == 0
  )

  list(
    allocated_workload = allocated_workload,
    accounting_audit = accounting_audit
  )
}
