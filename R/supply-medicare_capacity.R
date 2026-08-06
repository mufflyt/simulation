# Medicare workload-indexed capacity -----------------------------------------
#
# Medicare fee-for-service claims measure billed activity, not clinical hours or
# all-payer production. These helpers therefore produce a relative workload
# index, useful for sensitivity analysis while an URPS-specific hours survey is
# unavailable. They do not estimate observed URPS FTE.

#' Build annual Medicare work-RVU totals for a known URPS roster
#'
#' @param claims Claim-line data with provider identifier, year, HCPCS, and
#'   either `services` or `work_rvu`.
#' @param roster Active URPS roster containing the provider identifier. Keep both
#'   fellowship pathways in this roster; specialty cannot be inferred from claims.
#' @param provider_id Shared provider-identifier column.
#' @param services Number of billed services when claims lacks `work_rvu`.
#' @param hcpcs HCPCS column used to join to `rvu`.
#' @param rvu CMS work-RVU table or a study-specific expanded crosswalk.
#' @param minimum_wrvu Annual observed-Medicare activity threshold for the
#'   reference mean. Low-activity rostered clinicians remain in output.
#' @param coverage_col Optional roster column giving estimated Medicare work-RVU
#'   coverage fraction (>0 and <=1); a sensitivity input, not estimated here.
#' @return One row per rostered provider-year with work RVUs and inclusion flags.
#' @details Use only after a roster establishes active URPS status. A zero-claim
#' clinician is never considered non-URPS or inactive: payer mix, Medicare
#' Advantage, and non-billing roles can all yield low FFS activity.
#' @export
medicare_work_rvu_by_provider <- function(claims, roster, provider_id = "npi",
                                          services = "services", hcpcs = "hcpcs",
                                          rvu = CMS_WORK_RVU, minimum_wrvu = 100,
                                          coverage_col = NULL) {
  required_claims <- c(provider_id, "year")
  if (!"work_rvu" %in% names(claims)) required_claims <- c(required_claims, hcpcs)
  missing_claims <- setdiff(required_claims, names(claims))
  missing_roster <- setdiff(c(provider_id, "year"), names(roster))
  if (length(missing_claims) || length(missing_roster)) {
    stop(sprintf("medicare_work_rvu_by_provider: missing claims column(s): %s; roster column(s): %s",
      paste(missing_claims, collapse = ", "), paste(missing_roster, collapse = ", ")), call. = FALSE)
  }
  if (!"work_rvu" %in% names(claims) && !services %in% names(claims)) {
    stop("medicare_work_rvu_by_provider: claims need `work_rvu` or a services column", call. = FALSE)
  }
  assertthat::assert_that(is.numeric(minimum_wrvu), length(minimum_wrvu) == 1L, minimum_wrvu >= 0)
  if (!is.null(coverage_col) && !coverage_col %in% names(roster)) {
    stop(sprintf("medicare_work_rvu_by_provider: coverage column absent from roster: %s", coverage_col), call. = FALSE)
  }

  roster_ids <- roster
  if (anyDuplicated(roster_ids[c(provider_id, "year")])) {
    stop("medicare_work_rvu_by_provider: roster must contain one row per provider-year", call. = FALSE)
  }
  c <- claims
  if (!"work_rvu" %in% names(c)) {
    lookup <- rvu[, c("hcpcs", "work_rvu"), drop = FALSE]
    names(lookup)[1] <- hcpcs
    c <- dplyr::left_join(c, lookup, by = hcpcs)
    if (anyNA(c$work_rvu)) {
      bad <- unique(c[[hcpcs]][is.na(c$work_rvu)])
      stop(sprintf("medicare_work_rvu_by_provider: no work RVU for HCPCS: %s", paste(utils::head(bad, 10), collapse = ", ")), call. = FALSE)
    }
    c$work_rvu <- c$work_rvu * c[[services]]
  }
  if (any(!is.finite(c$work_rvu) | c$work_rvu < 0)) {
    stop("medicare_work_rvu_by_provider: work_rvu must be finite and non-negative", call. = FALSE)
  }
  totals <- dplyr::summarise(dplyr::group_by(c, .data[[provider_id]], .data$year),
    medicare_work_rvu = sum(.data$work_rvu), .groups = "drop")
  out <- dplyr::left_join(roster_ids, totals, by = c(provider_id, "year"))
  out$medicare_work_rvu[is.na(out$medicare_work_rvu)] <- 0
  out$medicare_coverage <- if (is.null(coverage_col)) 1 else roster_ids[[coverage_col]]
  if (any(!is.finite(out$medicare_coverage) | out$medicare_coverage <= 0 | out$medicare_coverage > 1)) {
    stop("medicare_work_rvu_by_provider: coverage must be finite, >0, and <=1", call. = FALSE)
  }
  out$coverage_adjusted_work_rvu <- out$medicare_work_rvu / out$medicare_coverage
  out$included_in_reference <- out$medicare_work_rvu >= minimum_wrvu
  dplyr::as_tibble(out)
}

#' Convert roster-linked Medicare activity into a relative URPS capacity index
#'
#' @param provider_workload Output of [medicare_work_rvu_by_provider()].
#' @param direct_care_hours Annual OB/GYN direct-patient-care-hours anchor used
#'   only to show illustrative implied hours. The default 2,063 is not URPS data.
#' @param reference_weight Optional numeric column for a weighted cohort mean.
#' @return Input with workload index and illustrative hours; attributes document
#'   the reference mean and estimand.
#' @details The included clinicians have a weighted mean index of one. The index
#' is relative Medicare-workload-indexed capacity, not clinical-hours FTE.
#' @export
medicare_workload_index <- function(provider_workload, direct_care_hours = 2063,
                                    reference_weight = NULL) {
  needed <- c("coverage_adjusted_work_rvu", "included_in_reference")
  missing <- setdiff(needed, names(provider_workload))
  if (length(missing)) stop(sprintf("medicare_workload_index: missing column(s): %s", paste(missing, collapse = ", ")), call. = FALSE)
  assertthat::assert_that(is.numeric(direct_care_hours), length(direct_care_hours) == 1L,
    is.finite(direct_care_hours), direct_care_hours > 0)
  w <- rep(1, nrow(provider_workload))
  if (!is.null(reference_weight)) {
    if (!reference_weight %in% names(provider_workload)) stop(sprintf("medicare_workload_index: reference-weight column absent: %s", reference_weight), call. = FALSE)
    w <- provider_workload[[reference_weight]]
  }
  keep <- provider_workload$included_in_reference
  if (!any(keep) || any(!is.finite(w[keep]) | w[keep] <= 0)) {
    stop("medicare_workload_index: need at least one included provider with a positive finite weight", call. = FALSE)
  }
  reference_mean <- stats::weighted.mean(provider_workload$coverage_adjusted_work_rvu[keep], w[keep])
  if (!is.finite(reference_mean) || reference_mean <= 0) stop("medicare_workload_index: reference mean must be positive", call. = FALSE)
  out <- provider_workload
  out$medicare_workload_index <- out$coverage_adjusted_work_rvu / reference_mean
  out$implied_direct_care_hours <- out$medicare_workload_index * direct_care_hours
  attr(out, "reference_mean_work_rvu") <- reference_mean
  attr(out, "direct_care_hours_anchor") <- direct_care_hours
  attr(out, "estimand") <- "relative Medicare-workload-indexed URPS capacity; not observed clinical-hours FTE"
  dplyr::as_tibble(out)
}

#' Summarise Medicare-workload-indexed capacity by provider characteristics
#'
#' @param workload_index Output of [medicare_workload_index()].
#' @param by Grouping variables, e.g. age band, sex, and practice setting.
#' @return Roster count, reference-eligible count, and relative capacity total.
#' @export
summarise_medicare_capacity <- function(workload_index, by = character()) {
  needed <- c("medicare_workload_index", "included_in_reference", by)
  missing <- setdiff(needed, names(workload_index))
  if (length(missing)) stop(sprintf("summarise_medicare_capacity: missing column(s): %s", paste(missing, collapse = ", ")), call. = FALSE)
  grouped <- if (length(by)) dplyr::group_by(workload_index, dplyr::across(dplyr::all_of(by))) else workload_index
  dplyr::summarise(grouped, rostered_providers = dplyr::n(),
    reference_eligible_providers = sum(.data$included_in_reference),
    relative_capacity = sum(.data$medicare_workload_index),
    mean_workload_index = mean(.data$medicare_workload_index), .groups = "drop")
}

#' Crosswalk the URPS CPT basket to Medicare realized-care services
#'
#' @param basket Service-to-HCPCS basket, defaulting to `URPS_CPT_BASKET` (internal).
#' @param include_non_specific Include generic evaluation-and-management codes.
#'   Defaults to FALSE because Provider-and-Service PUF files do not contain a
#'   diagnosis or a patient-level link: a 99213 line is not identifiable as a
#'   pelvic-floor visit without a roster- or diagnosis-linked claims source.
#' @return A unique `hcpcs` to `service` crosswalk.
#' @export
urps_medicare_service_crosswalk <- function(basket = URPS_CPT_BASKET,
                                            include_non_specific = FALSE) {
  needed <- c("service", "hcpcs")
  if (!all(needed %in% names(basket))) {
    stop("basket must contain service and hcpcs columns", call. = FALSE)
  }
  out <- dplyr::distinct(basket[, needed, drop = FALSE])
  if (!isTRUE(include_non_specific)) out <- out[!grepl("^99", out$hcpcs), , drop = FALSE]
  if (anyDuplicated(out$hcpcs)) {
    stop("An HCPCS code maps to more than one URPS service", call. = FALSE)
  }
  dplyr::as_tibble(out)
}

#' Aggregate Medicare FFS claims as realized URPS care
#'
#' This is deliberately an *observed-use* estimand. It measures billed Medicare
#' fee-for-service services after access, referral, payer mix, and capacity have
#' already shaped care. It must not be substituted for latent all-payer demand.
#'
#' @param claims Claim lines, such as the CMS Provider-and-Service PUF.
#' @param crosswalk HCPCS-to-service crosswalk; defaults to the procedure-only
#'   URPS basket. Do not add generic E/M codes unless claims are linked to a
#'   validated URPS roster or diagnosis definition.
#' @param year,hcpcs,services Column names in `claims`.
#' @param state,provider_type,place_of_service,npi Optional claims columns to
#'   retain as dimensions when present. Set any to NULL to omit it.
#' @return Tibble of annual service totals with explicit `estimand` and
#'   `payer_scope` labels. The `unmapped_service_fraction` attribute records
#'   claims excluded because their HCPCS code is outside the study basket.
#' @export
aggregate_medicare_realized_care <- function(
    claims,
    crosswalk = urps_medicare_service_crosswalk(),
    year = "year", hcpcs = "HCPCS_Cd", services = "Tot_Srvcs",
    state = "Rndrng_Prvdr_State_Abrvtn", provider_type = "Rndrng_Prvdr_Type",
    place_of_service = "Place_Of_Srvc", npi = "Rndrng_NPI") {
  required <- c(year, hcpcs, services)
  missing <- setdiff(required, names(claims))
  if (length(missing)) {
    stop("aggregate_medicare_realized_care: claims missing column(s): ",
         paste(missing, collapse = ", "), call. = FALSE)
  }
  if (!all(c("hcpcs", "service") %in% names(crosswalk))) {
    stop("aggregate_medicare_realized_care: crosswalk needs hcpcs and service", call. = FALSE)
  }
  if (anyDuplicated(crosswalk$hcpcs)) {
    stop("aggregate_medicare_realized_care: crosswalk HCPCS values must be unique", call. = FALSE)
  }
  c <- as.data.frame(claims)
  raw_services <- suppressWarnings(as.numeric(c[[services]]))
  if (any(!is.finite(raw_services) | raw_services < 0, na.rm = TRUE)) {
    stop("aggregate_medicare_realized_care: services must be finite and non-negative", call. = FALSE)
  }
  c$.services <- raw_services
  names(c)[names(c) == hcpcs] <- ".hcpcs"
  c <- dplyr::left_join(c, crosswalk, by = c(".hcpcs" = "hcpcs"))
  unmapped <- is.na(c$service)
  total <- sum(c$.services, na.rm = TRUE)
  excluded <- sum(c$.services[unmapped], na.rm = TRUE)
  c <- c[!unmapped & !is.na(c$.services), , drop = FALSE]
  if (!nrow(c)) stop("aggregate_medicare_realized_care: no claim lines match the service crosswalk", call. = FALSE)

  dimensions <- c(year = year, state = state, provider_type = provider_type,
                  place_of_service = place_of_service)
  dimensions <- dimensions[!is.na(dimensions) & nzchar(dimensions) & dimensions %in% names(c)]
  for (nm in names(dimensions)) c[[nm]] <- c[[dimensions[[nm]]]]
  group_cols <- c(names(dimensions), "service")
  out <- dplyr::summarise(dplyr::group_by(c, dplyr::across(dplyr::all_of(group_cols))),
    billed_services = sum(.data$.services), claim_lines = dplyr::n(),
    billing_npis = if (!is.null(npi) && npi %in% names(c)) dplyr::n_distinct(.data[[npi]]) else NA_integer_,
    .groups = "drop")
  out$estimand <- "realized Medicare FFS URPS services; not latent all-payer demand"
  out$payer_scope <- "Medicare fee-for-service"
  attr(out, "unmapped_service_fraction") <- if (total > 0) excluded / total else NA_real_
  attr(out, "caveat") <- "Provider-and-Service PUF suppresses low-volume lines and does not identify beneficiary age; use for older-population realized-use validation, not prevalence or all-payer demand."
  dplyr::as_tibble(out)
}

#' Compare modeled realized care with Medicare FFS service totals
#'
#' @param predicted Tibble with the grouping columns and `predicted_services`.
#' @param observed Output of [aggregate_medicare_realized_care()].
#' @param by Grouping columns; normally `"service"`, or `c("year", "service")`
#'   for a trajectory comparison.
#' @return Joined totals, calibration ratio, and residual. This ratio calibrates
#'   *realized Medicare care* only; it must not be applied to latent demand.
#' @export
compare_medicare_realized_care <- function(predicted, observed, by = c("service")) {
  needed_pred <- c(by, "predicted_services")
  needed_obs <- c(by, "billed_services")
  if (!all(needed_pred %in% names(predicted)) || !all(needed_obs %in% names(observed))) {
    stop("compare_medicare_realized_care: predicted needs grouping columns plus predicted_services; observed needs grouping columns plus billed_services", call. = FALSE)
  }
  p <- dplyr::summarise(dplyr::group_by(predicted, dplyr::across(dplyr::all_of(by))),
                        predicted_services = sum(.data$predicted_services), .groups = "drop")
  o <- dplyr::summarise(dplyr::group_by(observed, dplyr::across(dplyr::all_of(by))),
                        billed_services = sum(.data$billed_services), .groups = "drop")
  out <- dplyr::full_join(p, o, by = by)
  out$calibration_ratio <- ifelse(out$predicted_services > 0,
                                  out$billed_services / out$predicted_services, NA_real_)
  out$residual_services <- out$billed_services - out$predicted_services
  out$comparison_estimand <- "Medicare FFS realized-care validation; not a latent all-payer demand scalar"
  dplyr::as_tibble(out)
}
