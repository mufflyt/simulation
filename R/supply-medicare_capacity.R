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
