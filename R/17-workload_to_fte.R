# Workload -> Provider FTE Conversion ----
#
# The step every Dall-family demand model performs and the repo previously
# skipped: healthcare ACTIVITY is estimated first, then translated into provider
# FTE through staffing patterns and productivity. Nothing in this repo may divide
# provider FTE by a count of patients, cases, or procedures -- those are not FTE
# units and the ratio is dimensionally meaningless.
#
# Three published implementations, all supported here:
#
#   method = "wrvu"     Dall TM et al. Neurology 2013;81:470-478. Work RVUs per
#                       encounter type are aggregated and CALIBRATED against
#                       observed provider counts. Their result: 2,860 ambulatory
#                       visits ~ 1 clinical FTE and 1,580 hospital consults ~ 1
#                       clinical FTE, at 42.3 patient-care hrs/wk.
#   method = "staffing" Zarek P et al. Phys Ther 2025;105:pzaf014. Staffing ratio
#                       = national service volume / providers employed in that
#                       care-delivery setting; provider-to-visit for ambulatory,
#                       provider-to-day for inpatient, provider-to-resident for
#                       nursing facilities. Held constant across states and years.
#   method = "time"     Dall TM et al. Am J Phys Med Rehabil 2021;100:877-884.
#                       Survey time-share by setting (57% inpatient / 4% SNF /
#                       39% ambulatory) allocates FTE across settings.
#
# A minutes-per-service basket is deliberately NOT the default. No national data
# source carries service durations for pessary care, urodynamics, PTNS, Botox or
# cystoscopy; all three papers avoid needing them by calibrating to observed
# provider counts or to published RVUs. Work RVUs are public (CMS Physician Fee
# Schedule RVU file), so "wrvu" is the defensible default.

# ---- Service basket -------------------------------------------------------

# Work RVUs per unit of service. THESE ARE PLACEHOLDERS pending verification
# against the CMS Physician Fee Schedule RVU file for the modelled year; the
# registry entry `service_workload` in config/canonical_sources.yml is where the
# real file belongs. Everything downstream carries the calibration status, and
# `assert_publishable_workload()` refuses to let an uncalibrated basket be
# labelled publishable.
URPS_SERVICE_WORKLOAD <- tibble::tribble(
  ~service,                  ~setting,      ~unit,        ~work_rvu, ~label,
  "new_consultation",        "ambulatory",  "encounter",       2.60, "New patient office visit (99204 level)",
  "return_visit",            "ambulatory",  "encounter",       1.30, "Established patient visit (99213 level)",
  "pessary_care",            "ambulatory",  "encounter",       0.90, "Pessary fitting / maintenance",
  "urodynamics",             "ambulatory",  "study",           1.60, "Complex urodynamic study",
  "cystoscopy",              "ambulatory",  "procedure",       2.20, "Diagnostic cystourethroscopy",
  "botox_bladder",           "ambulatory",  "procedure",       3.30, "Intradetrusor onabotulinumtoxinA",
  "ptns",                    "ambulatory",  "session",         0.45, "Percutaneous tibial nerve stimulation",
  "bladder_instillation",    "ambulatory",  "session",         0.40, "Bladder instillation",
  "sling_procedure",         "operative",   "procedure",      12.70, "Mid-urethral sling (57288 level)",
  "prolapse_procedure",      "operative",   "procedure",      18.50, "Pelvic organ prolapse repair",
  "postoperative_care",      "ambulatory",  "encounter",       0.00, "Global-period postoperative visit",
  "indirect_clinical_work",  "indirect",    "fte_share",         NA, "Administration, documentation, teaching"
)

URPS_SERVICE_WORKLOAD_STATUS <- "uncalibrated_illustrative"

# Share of a provider's professional time that is NOT billable direct patient
# care. The 2010 AAN Practice Profile (n=910) reported 72.9% of professional time
# in patient care, 9.7% administration, 9.1% research, 5.2% teaching, 3% other;
# the physiatry survey reported 37.4 of 48.5 weekly hours in patient care.
INDIRECT_TIME_SHARE <- 0.271

# ---- Provider-type delegation matrix --------------------------------------

# Share of each service delivered by each provider type. Structure and magnitudes
# follow Forte GJ et al. Am J Phys Med Rehabil 2021;100:866-876, Table 4, which
# measured this by service rather than assuming a blanket substitution ratio.
# Their central finding: NPs and PAs perform 1-3% of injection and diagnostic
# procedures but 15-20% of outpatient services and ~18% of care management. A
# scalar "one APP = 0.5 physicians" cannot represent that, and the physiatry
# model found APP growth offset only 240 of 2,390 FTE of demand growth.
#
# Columns must sum to 1 within each service; `validate_delegation_matrix()`
# enforces it.
URPS_DELEGATION_MATRIX <- tibble::tribble(
  ~service,                 ~urps_share, ~app_share, ~other_clinician_share,
  "new_consultation",              0.70,       0.08,                   0.22,
  "return_visit",                  0.58,       0.22,                   0.20,
  "pessary_care",                  0.45,       0.40,                   0.15,
  "urodynamics",                   0.62,       0.28,                   0.10,
  "cystoscopy",                    0.80,       0.05,                   0.15,
  "botox_bladder",                 0.78,       0.07,                   0.15,
  "ptns",                          0.30,       0.55,                   0.15,
  "bladder_instillation",          0.25,       0.60,                   0.15,
  "sling_procedure",               0.72,       0.01,                   0.27,
  "prolapse_procedure",            0.68,       0.01,                   0.31,
  "postoperative_care",            0.55,       0.35,                   0.10,
  "indirect_clinical_work",        1.00,       0.00,                   0.00
)

#' Validate a provider-type delegation matrix
#'
#' @param matrix Delegation tibble with `service` and share columns.
#' @param tol Tolerance on the row sums.
#' @return (Invisibly) the matrix.
#' @export
validate_delegation_matrix <- function(matrix = URPS_DELEGATION_MATRIX, tol = 1e-8) {
  share_cols <- setdiff(names(matrix), "service")
  if (length(share_cols) == 0) {
    stop("validate_delegation_matrix: no share columns found", call. = FALSE)
  }
  sums <- rowSums(as.matrix(matrix[share_cols]), na.rm = TRUE)
  bad <- abs(sums - 1) > tol
  if (any(bad)) {
    stop(sprintf("validate_delegation_matrix: share columns must sum to 1 per service; offending: %s",
                 paste(matrix$service[bad], collapse = ", ")), call. = FALSE)
  }
  if (any(as.matrix(matrix[share_cols]) < 0, na.rm = TRUE)) {
    stop("validate_delegation_matrix: negative shares", call. = FALSE)
  }
  invisible(matrix)
}

#' Apportion service volume across provider types
#'
#' @param volumes Tibble with `service` and `volume` (and optionally `year`).
#' @param matrix Delegation matrix.
#' @return Long tibble: `year` (if present), `service`, `provider_type`, `volume`.
#' @export
apportion_service_volume <- function(volumes, matrix = URPS_DELEGATION_MATRIX) {
  assertthat::assert_that(all(c("service", "volume") %in% names(volumes)))
  validate_delegation_matrix(matrix)

  joined <- safe_left_join(volumes, matrix, by = "service", min_match_rate = 1.0)
  share_cols <- setdiff(names(matrix), "service")

  dplyr::bind_rows(lapply(share_cols, function(cl) {
    out <- joined
    out$provider_type <- sub("_share$", "", cl)
    out$volume <- joined$volume * joined[[cl]]
    out[, intersect(c("year", "service", "provider_type", "volume"), names(out)), drop = FALSE]
  }))
}

# ---- Work-RVU conversion --------------------------------------------------

#' Total annual work RVUs implied by a service-volume schedule
#'
#' @param volumes Tibble with `service`, `volume`, optionally `year`.
#' @param workload Service basket carrying `work_rvu`.
#' @param provider_type Provider type to total (default the URPS share).
#' @param delegation Delegation matrix; NULL uses the full volume.
#' @return Tibble with `year` (if present) and `work_rvu`.
#' @export
service_volume_to_wrvu <- function(volumes,
                                   workload = URPS_SERVICE_WORKLOAD,
                                   provider_type = "urps",
                                   delegation = URPS_DELEGATION_MATRIX) {
  assertthat::assert_that(all(c("service", "volume") %in% names(volumes)))

  vol <- if (is.null(delegation)) {
    dplyr::mutate(volumes, provider_type = provider_type)
  } else {
    apportion_service_volume(volumes, delegation)
  }
  vol <- dplyr::filter(vol, .data$provider_type == !!provider_type)

  rvu <- dplyr::select(workload, "service", "work_rvu")
  out <- safe_left_join(vol, rvu, by = "service", min_match_rate = 1.0)
  out <- dplyr::mutate(out, work_rvu_total = .data$volume * dplyr::coalesce(.data$work_rvu, 0))

  if ("year" %in% names(out)) {
    dplyr::summarise(dplyr::group_by(out, .data$year),
                     work_rvu = sum(.data$work_rvu_total, na.rm = TRUE), .groups = "drop")
  } else {
    tibble::tibble(work_rvu = sum(out$work_rvu_total, na.rm = TRUE))
  }
}

#' Calibrate annual work RVUs per clinical FTE to a base-year anchor
#'
#' The counterpart of Dall 2013's "after model calibration": the RVU-per-FTE
#' denominator is not asserted from a productivity survey, it is SOLVED so that
#' base-year required FTE equals the base-year demand anchor (base-year supply
#' plus the estimated starting shortfall). Every later year then inherits a
#' denominator that reproduces a known quantity in the base year.
#'
#' @param base_year_wrvu Total URPS work RVUs implied by base-year volumes.
#' @param base_year_required_fte Base-year required FTE (supply + shortfall).
#' @param indirect_share Indirect-time share used by [convert_workload_to_fte()].
#'   It MUST be the same value, or the conversion grosses up a denominator that
#'   was calibrated without the gross-up and base-year required FTE misses its
#'   own anchor by 1/(1 - indirect_share).
#' @return Numeric annual work RVUs per clinical FTE.
#' @export
calibrate_wrvu_per_fte <- function(base_year_wrvu, base_year_required_fte,
                                   indirect_share = INDIRECT_TIME_SHARE) {
  assertthat::assert_that(is.numeric(base_year_wrvu), base_year_wrvu > 0)
  assertthat::assert_that(is.numeric(base_year_required_fte), base_year_required_fte > 0)
  assertthat::assert_that(indirect_share >= 0, indirect_share < 1)
  gross_up <- 1 / (1 - indirect_share)
  wrvu_per_fte <- base_year_wrvu * gross_up / base_year_required_fte
  .msg_info(sprintf(
    "Calibrated productivity: %s work RVUs per clinical FTE per year (anchor %s FTE, %.1f%% indirect time)",
    format(round(wrvu_per_fte), big.mark = ","), format(round(base_year_required_fte)),
    100 * indirect_share
  ))
  wrvu_per_fte
}

#' Convert a service-volume schedule to required provider FTE
#'
#' @param volumes Tibble with `service`, `volume`, optionally `year`.
#' @param wrvu_per_fte Annual work RVUs per clinical FTE
#'   ([calibrate_wrvu_per_fte()]).
#' @param workload Service basket.
#' @param delegation Delegation matrix (NULL = attribute all volume to URPS).
#' @param provider_type Provider type to convert.
#' @param indirect_share Share of professional time that is indirect; required
#'   FTE is grossed up by 1/(1 - indirect_share) so administration, documentation
#'   and teaching are not silently free.
#' @param method One of "wrvu", "staffing", "time".
#' @param staffing_ratios For method = "staffing": tibble with `service` and
#'   `volume_per_fte` (national volume divided by providers in that setting).
#' @return Tibble with `year` (if present), `required_fte`, and the method used.
#' @export
convert_workload_to_fte <- function(volumes,
                                    wrvu_per_fte = NULL,
                                    workload = URPS_SERVICE_WORKLOAD,
                                    delegation = URPS_DELEGATION_MATRIX,
                                    provider_type = "urps",
                                    indirect_share = INDIRECT_TIME_SHARE,
                                    method = c("wrvu", "staffing", "time"),
                                    staffing_ratios = NULL) {
  method <- match.arg(method)
  assertthat::assert_that(indirect_share >= 0, indirect_share < 1)
  gross_up <- 1 / (1 - indirect_share)

  if (method == "wrvu") {
    if (is.null(wrvu_per_fte)) {
      stop("convert_workload_to_fte(method = 'wrvu'): wrvu_per_fte is required. ",
           "Derive it with calibrate_wrvu_per_fte() from a base-year anchor; ",
           "do not assume a productivity level.", call. = FALSE)
    }
    rv <- service_volume_to_wrvu(volumes, workload, provider_type, delegation)
    out <- dplyr::mutate(rv, required_fte = .data$work_rvu / wrvu_per_fte * gross_up)
  } else if (method == "staffing") {
    if (is.null(staffing_ratios) || !all(c("service", "volume_per_fte") %in% names(staffing_ratios))) {
      stop("convert_workload_to_fte(method = 'staffing'): staffing_ratios needs ",
           "columns `service` and `volume_per_fte`.", call. = FALSE)
    }
    vol <- if (is.null(delegation)) {
      dplyr::mutate(volumes, provider_type = provider_type)
    } else {
      apportion_service_volume(volumes, delegation)
    }
    vol <- dplyr::filter(vol, .data$provider_type == !!provider_type)
    vol <- safe_left_join(vol, staffing_ratios, by = "service", min_match_rate = 1.0)
    vol <- dplyr::mutate(vol, fte = .data$volume / .data$volume_per_fte)
    out <- if ("year" %in% names(vol)) {
      dplyr::summarise(dplyr::group_by(vol, .data$year),
                       required_fte = sum(.data$fte, na.rm = TRUE) * gross_up, .groups = "drop")
    } else {
      tibble::tibble(required_fte = sum(vol$fte, na.rm = TRUE) * gross_up)
    }
  } else {
    stop("convert_workload_to_fte(method = 'time'): supply a setting time-share ",
         "allocation via allocate_fte_by_setting() instead.", call. = FALSE)
  }

  dplyr::mutate(out,
                method = method,
                calibration_status = URPS_SERVICE_WORKLOAD_STATUS)
}

#' Allocate total required FTE across care settings by survey time share
#'
#' The physiatry model's route: survey time by delivery setting (57% inpatient,
#' 4% skilled nursing, 39% ambulatory) partitions total required FTE.
#'
#' @param total_fte Total required FTE.
#' @param time_shares Named numeric vector of setting shares summing to 1.
#' @return Tibble with `setting` and `required_fte`.
#' @export
allocate_fte_by_setting <- function(total_fte,
                                    time_shares = c(ambulatory = 0.82, operative = 0.15, inpatient = 0.03)) {
  s <- sum(time_shares)
  if (abs(s - 1) > 1e-8) {
    stop(sprintf("allocate_fte_by_setting: time shares sum to %.4f, not 1", s), call. = FALSE)
  }
  tibble::tibble(setting = names(time_shares),
                 required_fte = as.numeric(total_fte) * unname(time_shares))
}

# ---- Supply-vs-demand gap, FTE on both sides ------------------------------

#' Compare supplied FTE with required FTE
#'
#' The ONLY sanctioned supply/demand comparison in this repo. Both arguments must
#' be in provider-FTE units; a count of prevalent cases, consultations or
#' procedures is rejected. Sign convention follows the published tables: a
#' negative gap is a shortfall.
#'
#' @param supply Tibble with `year` and a supplied-FTE column.
#' @param required Tibble with `year` and `required_fte`.
#' @param supply_col Name of the supplied-FTE column.
#' @return Tibble: `year`, `supplied_fte`, `required_fte`, `gap_fte`, `gap_pct`,
#'   `pct_supply_to_demand`.
#' @export
compute_fte_gap <- function(supply, required, supply_col = "effective_fte_median") {
  assertthat::assert_that(supply_col %in% names(supply),
                          "year" %in% names(supply))
  assertthat::assert_that(all(c("year", "required_fte") %in% names(required)))

  s <- dplyr::transmute(supply, year = .data$year, supplied_fte = .data[[supply_col]])
  r <- dplyr::select(required, "year", "required_fte")

  safe_left_join(s, r, by = "year", min_match_rate = 1.0) %>%
    dplyr::mutate(
      gap_fte = .data$supplied_fte - .data$required_fte,
      gap_pct = 100 * .data$gap_fte / .data$required_fte,
      pct_supply_to_demand = 100 * .data$supplied_fte / .data$required_fte
    )
}

#' Refuse to publish numbers built on an uncalibrated workload basket
#'
#' @param status Calibration status string to check.
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @return (Invisibly) TRUE when publishable.
#' @export
assert_publishable_workload <- function(status = URPS_SERVICE_WORKLOAD_STATUS,
                                        mode = resolve_reproducibility_mode()) {
  if (identical(status, "calibrated")) return(invisible(TRUE))
  msg <- sprintf(
    "Workload basket calibration_status is '%s': work RVUs and delegation shares are placeholders. Verify against the CMS PFS RVU file and a URPS practice survey before publishing any FTE number.",
    status
  )
  if (identical(mode, "strict")) stop(msg, call. = FALSE)
  .msg_warn(msg)
  invisible(FALSE)
}
