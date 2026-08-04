# Delegation Matrix: Evidence and Sensitivity ----
#
# `URPS_DELEGATION_MATRIX` is `derived_by_analogy` -- Forte et al.'s physiatry
# shares, rescaled. This module answers the question that status invites: can it
# be MEASURED instead?
#
# THE ANSWER IS NO, AND THE REASON IS STRUCTURAL. Medicare's provider taxonomy
# has no urogynaecology code. URPS subspecialists bill as "Urology" (82.3% of
# URPS-basket volume) or "Obstetrics/Gynecology" (9.1%), indistinguishably from
# generalists in those specialties. The `urps_share` column -- the one the FTE
# calculation actually turns on -- is therefore not identified by claims alone.
#
# WHAT CAN BE MEASURED IS THE APP SHARE, and it corroborates the borrowed matrix
# in shape while contradicting it in level -- the same split the capacity
# rescale already found for the subspecialist column. See
# `medicare_delegation_corroboration()`.
#
# WHAT WOULD BE NEEDED TO MEASURE IT PROPERLY
#
#   1. NPPES taxonomy linkage. Medicare's Provider & Service PUF carries NPIs;
#      joining NPPES taxonomy 207VX0201X (Female Pelvic Medicine and
#      Reconstructive Surgery) would separate subspecialists from the Urology
#      and OB/GYN pools and identify `urps_share` directly. This is the single
#      highest-value missing input and it is obtainable.
#   2. A fielded URPS practice survey. The only way to observe who performs each
#      service INCLUDING work billed incident-to.
#   3. All-payer claims. Medicare FFS is ~65+, and a large share of
#      pelvic-floor care is delivered under 65.
#
# INCIDENT-TO BILLING IS THE KEY BIAS. Services delivered by an NP or PA under
# physician supervision are billed under the SUPERVISING PHYSICIAN's NPI at 100%
# of the fee schedule. Every APP share measured from claims is therefore a LOWER
# BOUND, and the gap is largest exactly where delegation is most common -- office
# visits and device care. This is why the measured values below are not adopted
# as the matrix.

#' Provider types that are advanced practice providers in the Medicare taxonomy
#' @export
MEDICARE_APP_PROVIDER_TYPES <- c(
  "Nurse Practitioner", "Physician Assistant", "Clinical Nurse Specialist",
  "Certified Nurse Midwife"
)

#' Measured APP share by service from Medicare, against the borrowed matrix
#'
#' Reads the realized-care artifacts and computes, for each URPS service, the
#' share of billed services rendered by an APP. Compares with the shipped
#' delegation matrix.
#'
#' @param realized Tibble of realized Medicare care with `service`,
#'   `provider_type`, `billed_services`; read from `artifacts/` when NULL.
#' @param matrix Delegation matrix to compare against.
#' @return Tibble with `service`, `billed_services`, `measured_app_share`,
#'   `matrix_app_share`, `ratio`; carries a `caveats` attribute and a
#'   `rank_correlation` attribute.
#' @export
medicare_delegation_corroboration <- function(realized = NULL,
                                              matrix = URPS_DELEGATION_MATRIX) {
  if (is.null(realized)) realized <- .load_realized_medicare()
  if (is.null(realized)) {
    stop("medicare_delegation_corroboration(): no realized-care artifact found. ",
         "Run scripts/plot_medicare_realized_care.R, or pass `realized`.",
         call. = FALSE)
  }
  assertthat::assert_that(
    all(c("service", "provider_type", "billed_services") %in% names(realized)))

  is_app <- realized$provider_type %in% MEDICARE_APP_PROVIDER_TYPES
  agg <- stats::aggregate(
    cbind(billed_services = realized$billed_services,
          app_services = realized$billed_services * is_app) ~ realized$service,
    FUN = sum)
  names(agg)[1] <- "service"
  agg$measured_app_share <- agg$app_services / agg$billed_services

  m <- matrix[, c("service", "app_share")]
  names(m)[2] <- "matrix_app_share"
  out <- merge(agg, m, by = "service")
  # A measured share of exactly zero makes the ratio undefined, not infinite:
  # reporting Inf invites it to be read as "infinitely overstated" when it means
  # "no APP-billed service appeared in this cell".
  out$ratio <- ifelse(out$measured_app_share > 0,
                      out$matrix_app_share / out$measured_app_share, NA_real_)
  out <- out[order(-out$billed_services), ]

  rho <- suppressWarnings(stats::cor(out$measured_app_share, out$matrix_app_share,
                                     method = "spearman"))

  structure(
    tibble::as_tibble(out),
    rank_correlation = rho,
    services_not_observed = setdiff(matrix$service, agg$service),
    caveats = c(
      "Medicare FFS only: predominantly age 65+, so under-65 care is absent.",
      paste("Incident-to billing attributes APP work to the supervising",
            "physician's NPI, so every measured APP share is a LOWER BOUND."),
      paste("Evaluation-and-management services (new_consultation,",
            "return_visit, postoperative_care) are absent from the artifact, and",
            "those are exactly where delegation is highest."),
      paste("No urogynaecology provider type exists in the Medicare taxonomy,",
            "so urps_share cannot be measured this way at all.")
    )
  )
}

# Internal: read whichever realized-care artifacts are present.
.load_realized_medicare <- function(dir = "artifacts") {
  files <- list.files(dir, pattern = "^medicare_realized_care_.*\\.rds$",
                      full.names = TRUE)
  files <- files[!grepl("provenance", files)]
  if (length(files) == 0) return(NULL)
  parts <- lapply(files, function(f) tryCatch(readRDS(f), error = function(e) NULL))
  parts <- Filter(function(p) is.data.frame(p) && "provider_type" %in% names(p), parts)
  if (length(parts) == 0) return(NULL)
  do.call(rbind, parts)
}

#' Sensitivity to the delegation capacity factor
#'
#' The subspecialist LEVEL in the matrix is a single rescaling constant
#' (`URPS_DELEGATION_CAPACITY_FACTOR`, 0.434) chosen so base-year productivity is
#' physically plausible.
#'
#' IT DOES NOT MOVE REQUIRED FTE, AND THAT IS THE POINT OF REPORTING IT.
#' `calibrate_wrvu_per_fte()` SOLVES productivity so base-year required FTE
#' equals the base-year demand anchor. Raising the subspecialist share raises
#' base-year URPS work RVUs and the solved wRVU/FTE denominator in exactly the
#' same proportion, so the ratio -- required FTE -- is invariant in every year.
#' The gap is therefore robust to this constant, which is a genuinely reassuring
#' result and the opposite of what a work-RVU-only sweep suggests.
#'
#' What the constant DOES move is the implied productivity, and that is a
#' falsifiable quantity: at 0.434 the model implies 8,147 wRVU per clinical FTE
#' (inside the 3,500-12,000 benchmark), while the Medicare-measured sling share
#' of 0.473 implies a factor near 0.69 and 12,934 wRVU/FTE -- ABOVE the
#' benchmark ceiling. That tension is evidence about the service-volume basket,
#' not about the delegation matrix.
#'
#' @param volumes Service-volume tibble with `year`, `service`, `volume`.
#' @param year Base year for the productivity solve; defaults to the earliest.
#' @param target_year Year at which required FTE is evaluated; defaults to the
#'   latest.
#' @param anchor_fte Base-year required-FTE anchor the solve targets.
#' @param factors Capacity factors to sweep.
#' @return Tibble with `capacity_factor`, `urps_wrvu`, `solved_wrvu_per_fte`,
#'   `required_fte_base`, `required_fte_target`, `productivity_plausible`.
#' @export
delegation_capacity_sensitivity <- function(volumes,
                                            year = NULL,
                                            target_year = NULL,
                                            anchor_fte = 1377.3,
                                            factors = c(0.30, 0.434, 0.50, 0.60, 0.689)) {
  assertthat::assert_that(is.data.frame(volumes),
                          all(c("year", "service", "volume") %in% names(volumes)))
  if (is.null(year)) year <- min(volumes$year, na.rm = TRUE)
  if (is.null(target_year)) target_year <- max(volumes$year, na.rm = TRUE)
  v0 <- volumes[volumes$year == year, , drop = FALSE]
  vT <- volumes[volumes$year == target_year, , drop = FALSE]
  lo <- min(WRVU_PER_FTE_BENCHMARK); hi <- max(WRVU_PER_FTE_BENCHMARK)

  rows <- lapply(factors, function(f) {
    mat <- rescale_delegation_to_capacity(URPS_DELEGATION_FORTE_RAW, f)
    # service_volume_to_wrvu() apportions by the matrix, keeps the `urps`
    # provider rows, and returns a `work_rvu` column (one row per year).
    w0 <- sum(service_volume_to_wrvu(v0, delegation = mat)$work_rvu, na.rm = TRUE)
    wT <- sum(service_volume_to_wrvu(vT, delegation = mat)$work_rvu, na.rm = TRUE)
    wpf <- suppressMessages(calibrate_wrvu_per_fte(w0, anchor_fte))
    tibble::tibble(capacity_factor = f,
                   urps_wrvu = w0,
                   solved_wrvu_per_fte = wpf,
                   required_fte_base = w0 / wpf,
                   required_fte_target = wT / wpf,
                   productivity_plausible = wpf >= lo & wpf <= hi)
  })
  dplyr::bind_rows(rows)
}
