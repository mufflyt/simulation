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

# Work RVUs per unit of service, built from the CMS Physician Fee Schedule
# Relative Value File. See R/23-cms_rvu.R for the CPT basket, the mix weights,
# and the re-derivation helpers; the values are no longer placeholders.
#
# Constructed lazily on first use so the package can be collated before the
# CMS tables are available in the namespace.
.urps_workload_cache <- new.env(parent = emptyenv())

#' The URPS service workload basket
#'
#' @return Tibble of `service`, `setting`, `unit`, `work_rvu`, `label`,
#'   `calibration_status`, `source`.
#' @export
urps_service_workload <- function() {
  if (is.null(.urps_workload_cache$tbl)) {
    .urps_workload_cache$tbl <- build_workload_from_cms()
  }
  .urps_workload_cache$tbl
}

#' Calibration status of the workload basket
#' @return Character status.
#' @export
urps_service_workload_status <- function() {
  unique(urps_service_workload()$calibration_status)[1]
}

# Share of a provider's professional time that is NOT billable direct patient
# care. The 2010 AAN Practice Profile (n=910) reported 72.9% of professional
# time in patient care, 9.7% administration, 9.1% research, 5.2% teaching, 3%
# other; the physiatry survey reported 37.4 of 48.5 weekly hours in patient
# care (77.1%). We adopt the AAN figure.
INDIRECT_TIME_SHARE <- 0.271
INDIRECT_TIME_SHARE_SOURCE <- "AAN 2010 Practice Profile (n=910): 72.9% of professional time in patient care"

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
# Mapping used, from Forte Table 4 (mean % of work performed by physiatrists /
# NPs / PAs / PTs, by service):
#
#   URPS service            Forte analogue                 physiatrist share
#   new_consultation        new patient visits/encounters              70.6%
#   return_visit            follow-up patient visits                   62.0%
#   pessary_care            patient care management/follow-up          56.1%
#   urodynamics             diagnostic procedures                      67.4%
#   cystoscopy              diagnostic procedures                      67.4%
#   botox_bladder           injection procedures                       74.1%
#   ptns                    patient care management/follow-up          56.1%
#   bladder_instillation    patient care management/follow-up          56.1%
#   sling / prolapse        interventional procedures and imaging      68.7%
#   postoperative_care      follow-up patient visits                   62.0%
#
# Forte reports the NP and PA shares separately; they are summed into a single
# APP share here. The residual (physiatrist + NP + PA + PT does not reach 100%)
# is assigned to "other_clinician", which for urogynaecology means generalist
# OB/GYN, urology and primary care -- a substantial share of pelvic-floor care
# is delivered outside the subspecialty, which is exactly why the apportionment
# step exists.
#
# For the two OPERATIVE services the Forte procedural analogue understates how
# concentrated surgery is in the subspecialist: NPs and PAs perform 1-3% of
# procedures in their data, so the APP share is set to the floor of that range
# and the balance moved to other_clinician (generalist gynaecologists perform a
# large share of prolapse and sling surgery).
URPS_DELEGATION_FORTE_RAW <- tibble::tribble(
  ~service,                 ~urps_share, ~app_share, ~other_clinician_share, ~forte_analogue,
  "new_consultation",              0.706,      0.159,                  0.135, "new patient visits/encounters",
  "return_visit",                  0.620,      0.325,                  0.055, "follow-up patient visits",
  "pessary_care",                  0.561,      0.347,                  0.092, "patient care management/follow-up",
  "urodynamics",                   0.674,      0.017,                  0.309, "diagnostic procedures",
  "cystoscopy",                    0.674,      0.017,                  0.309, "diagnostic procedures",
  "botox_bladder",                 0.741,      0.083,                  0.176, "injection procedures",
  "ptns",                          0.561,      0.347,                  0.092, "patient care management/follow-up",
  "bladder_instillation",          0.561,      0.347,                  0.092, "patient care management/follow-up",
  "sling_procedure",               0.687,      0.021,                  0.292, "interventional procedures and imaging",
  "prolapse_procedure",            0.687,      0.021,                  0.292, "interventional procedures and imaging",
  "postoperative_care",            0.620,      0.325,                  0.055, "follow-up patient visits",
  "indirect_clinical_work",        1.000,      0.000,                  0.000, "not delegated"
)

#' Rescale the subspecialist share of a delegation matrix to a capacity level
#'
#' Forte's physiatry shares transfer as a SHAPE -- procedures concentrated in the
#' subspecialist, routine follow-up and device care delegated -- but not as a
#' LEVEL. Physiatry is the primary specialty for its conditions; urogynaecology
#' is a small subspecialty inside a much larger system, and most population-level
#' urinary-incontinence care is delivered by generalist obstetrician-
#' gynaecologists, urologists and primary care.
#'
#' Applying the raw shares implies 1,306 subspecialists delivering 64.5% of all
#' modelled national service volume, which solves to about 17,700 work RVUs per
#' clinical FTE per year -- roughly 2.4x any published productivity benchmark.
#' `implied_urps_share()` puts the consistent level near 28%.
#'
#' This rescales every URPS share by a common factor and moves the difference to
#' `other_clinician_share`, leaving `app_share` untouched (the APP-vs-physician
#' split within delegated work is what Forte actually measured) and preserving
#' the relative ordering across services.
#'
#' @param matrix Delegation matrix to rescale.
#' @param factor Multiplier on the URPS shares.
#' @return The rescaled matrix.
#' @export
rescale_delegation_to_capacity <- function(matrix = URPS_DELEGATION_FORTE_RAW,
                                           factor = URPS_DELEGATION_CAPACITY_FACTOR) {
  assertthat::assert_that(is.numeric(factor), factor > 0, factor <= 1)
  keep <- matrix$service != "indirect_clinical_work"
  out <- matrix
  new_urps <- out$urps_share
  new_urps[keep] <- out$urps_share[keep] * factor
  out$other_clinician_share <- out$other_clinician_share +
    (out$urps_share - new_urps)
  out$urps_share <- new_urps
  out
}

# Level correction implied by base-year capacity: see rescale_delegation_to_capacity().
# Derived, not assumed -- implied_urps_share() reports 28.0% against a 64.5%
# raw mean, giving 0.434.
URPS_DELEGATION_CAPACITY_FACTOR <- 0.434

#' Provider-type delegation matrix used by the model
#'
#' Forte's cross-service pattern with the subspecialist level rescaled to what a
#' subspecialty of this size can actually deliver.
#' @export
URPS_DELEGATION_MATRIX <- rescale_delegation_to_capacity(
  URPS_DELEGATION_FORTE_RAW, 0.434
)

URPS_DELEGATION_STATUS <- "derived_by_analogy"
URPS_DELEGATION_SOURCE <- paste(
  "Cross-service SHAPE from Forte GJ et al. Am J Phys Med Rehabil",
  "2021;100:866-876 Table 4 (physiatry); subspecialist LEVEL rescaled by",
  "0.434 so base-year productivity is physically plausible (see",
  "rescale_delegation_to_capacity). NOT a urogynaecology survey:",
  "field one to move this to \"calibrated\".",
  "Partially corroborated: Medicare realized care ranks the APP share across",
  "procedures in the same order (Spearman rho 0.72) but 2-4x lower in level,",
  "and claims understate delegation because incident-to work bills under the",
  "physician. urps_share is NOT identifiable from claims -- Medicare has no",
  "urogynaecology provider type. See medicare_delegation_corroboration()."
)

# Only numeric columns named *_share are shares; the matrix also carries
# documentation columns such as `forte_analogue`.
.share_cols <- function(matrix) {
  cols <- grep("_share$", names(matrix), value = TRUE)
  cols[vapply(matrix[cols], is.numeric, logical(1))]
}

#' Validate a provider-type delegation matrix
#'
#' @param matrix Delegation tibble with `service` and share columns.
#' @param tol Tolerance on the row sums.
#' @return (Invisibly) the matrix.
#' @export
validate_delegation_matrix <- function(matrix = URPS_DELEGATION_MATRIX, tol = 1e-8) {
  share_cols <- .share_cols(matrix)
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
  share_cols <- .share_cols(matrix)

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
                                   workload = urps_service_workload(),
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

#' Plausible annual work RVUs per clinical FTE
#'
#' Benchmark range for a full-time clinical physician in an OB/GYN-family
#' specialty. Productivity surveys (MGMA, AMGA) put the median in the high
#' thousands, not the tens of thousands. REPLACE with the specific survey
#' edition you have licensed before publishing.
#' @export
WRVU_PER_FTE_BENCHMARK <- c(low = 3500, median = 7500, high = 12000)

#' Check that a solved productivity denominator is physically plausible
#'
#' `calibrate_wrvu_per_fte()` SOLVES the denominator from the base-year service
#' volumes and the demand anchor, so it absorbs any error in the volumes. A
#' solved value far outside the benchmark range does not mean the workforce is
#' unusually productive -- it means the service volumes are wrong. This turns
#' that into a visible signal instead of a silently inflated denominator that
#' would suppress projected demand.
#'
#' @param wrvu_per_fte Solved annual work RVUs per clinical FTE.
#' @param benchmark Named low/median/high benchmark.
#' @param mode Reproducibility mode; strict errors outside the range.
#' @return (Invisibly) TRUE when within range.
#' @export
check_productivity_plausible <- function(wrvu_per_fte,
                                         benchmark = WRVU_PER_FTE_BENCHMARK,
                                         mode = resolve_reproducibility_mode()) {
  ok <- wrvu_per_fte >= benchmark[["low"]] && wrvu_per_fte <= benchmark[["high"]]
  if (ok) {
    .msg_info(sprintf("Solved productivity %s wRVU/FTE is within the %s-%s benchmark range.",
                      format(round(wrvu_per_fte), big.mark = ","),
                      format(benchmark[["low"]], big.mark = ","),
                      format(benchmark[["high"]], big.mark = ",")))
    return(invisible(TRUE))
  }
  msg <- sprintf(paste(
    "Solved productivity is %s work RVUs per clinical FTE per year, outside the",
    "plausible %s-%s range (median %s). The denominator is SOLVED from the",
    "base-year service volumes, so this is evidence the VOLUMES are wrong, not",
    "that productivity is unusual. A denominator that is too high suppresses",
    "projected demand. Fix the service-volume inputs before trusting any gap."),
    format(round(wrvu_per_fte), big.mark = ","),
    format(benchmark[["low"]], big.mark = ","),
    format(benchmark[["high"]], big.mark = ","),
    format(benchmark[["median"]], big.mark = ","))
  if (identical(mode, "strict")) stop(msg, call. = FALSE)
  .msg_warn(msg)
  invisible(FALSE)
}

#' Uniform URPS share implied by a productivity target
#'
#' Diagnostic for the case the plausibility check flags. Given base-year service
#' volumes, a demand anchor, and a plausible productivity level, this reports the
#' uniform share of that volume the subspecialty would have to deliver for the
#' three to be mutually consistent.
#'
#' It matters for urogynaecology specifically. Forte's physiatry delegation
#' shares put the subspecialist at 60-75% of most services, but physiatry is the
#' PRIMARY specialty for its conditions. Urogynaecology is a small subspecialty
#' inside a much larger system: most population-level urinary-incontinence care
#' is delivered by generalist obstetrician-gynaecologists, urologists and
#' primary care. Transferring the physiatry shares across therefore biases the
#' URPS share upward, and this function says by how much.
#'
#' @param volumes Base-year service volumes (`service`, `volume`).
#' @param required_fte Base-year required FTE anchor.
#' @param wrvu_per_fte Plausible annual work RVUs per clinical FTE.
#' @param workload Service workload basket.
#' @param indirect_share Indirect-time share.
#' @return Numeric implied uniform URPS share.
#' @export
implied_urps_share <- function(volumes, required_fte,
                               wrvu_per_fte = WRVU_PER_FTE_BENCHMARK[["median"]],
                               workload = urps_service_workload(),
                               indirect_share = INDIRECT_TIME_SHARE) {
  total <- service_volume_to_wrvu(volumes, workload, delegation = NULL)
  gross_up <- 1 / (1 - indirect_share)
  denom <- total$work_rvu * gross_up
  if (!is.finite(denom) || denom <= 0) {
    stop("implied_urps_share: the modelled service basket has zero total work ",
         "RVUs, so no share is defined. Check the service volumes.", call. = FALSE)
  }
  share <- required_fte * wrvu_per_fte / denom
  .msg_info(sprintf(
    "For %s FTE at %s wRVU/FTE, URPS would deliver %.1f%% of the modelled service volume.",
    format(round(required_fte)), format(wrvu_per_fte, big.mark = ","), 100 * share))
  share
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
#' @param by_setting Logical. When `TRUE`, the returned tibble includes a
#'   `setting` column and reports required FTE separately for each care delivery
#'   setting (ambulatory, operative, indirect). Requires the `setting` column to
#'   be present in `volumes` — attach it first with [volumes_with_setting()].
#'   When `FALSE` (default), behaviour is unchanged: a single aggregate FTE is
#'   returned.
#' @return Tibble with `year` (if present), `required_fte`, and the method used.
#'   When `by_setting = TRUE`, also includes a `setting` column.
#' @export
convert_workload_to_fte <- function(volumes,
                                    wrvu_per_fte = NULL,
                                    workload = urps_service_workload(),
                                    delegation = URPS_DELEGATION_MATRIX,
                                    provider_type = "urps",
                                    indirect_share = INDIRECT_TIME_SHARE,
                                    method = c("wrvu", "staffing", "time"),
                                    staffing_ratios = NULL,
                                    by_setting = FALSE) {
  method <- match.arg(method)
  assertthat::assert_that(indirect_share >= 0, indirect_share < 1)
  gross_up <- 1 / (1 - indirect_share)

  if (isTRUE(by_setting)) {
    if (!"setting" %in% names(volumes)) {
      volumes <- volumes_with_setting(volumes, workload)
    }
    settings <- sort(unique(na.omit(volumes$setting)))
    out_list <- lapply(settings, function(s) {
      v_s <- dplyr::filter(volumes, .data$setting == s)
      # Strip setting column before passing to sub-call to avoid recursion
      v_s <- dplyr::select(v_s, -"setting")
      sub <- convert_workload_to_fte(
        v_s, wrvu_per_fte = wrvu_per_fte, workload = workload,
        delegation = delegation, provider_type = provider_type,
        indirect_share = indirect_share, method = method,
        staffing_ratios = staffing_ratios, by_setting = FALSE
      )
      dplyr::mutate(sub, setting = s)
    })
    out <- dplyr::bind_rows(out_list)
    col_order <- c(if ("year" %in% names(out)) "year",
                   "setting", "required_fte", "method", "calibration_status")
    return(dplyr::select(out, dplyr::any_of(col_order)))
  }

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
                calibration_status = urps_service_workload_status())
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
      # Required FTE of zero is degenerate, not infinite: a percentage of nothing
      # is undefined, and returning Inf would propagate into every downstream
      # summary as a plausible-looking number.
      gap_pct = ssot_safe_divide(100 * .data$gap_fte, .data$required_fte),
      pct_supply_to_demand = ssot_safe_divide(100 * .data$supplied_fte,
                                              .data$required_fte)
    )
}

#' Gate publication on the calibration tier of the model inputs
#'
#' Accepts inputs that are anchored to a published source ("calibrated") or
#' determined by an internal constraint rather than assumed ("solved").
#' "derived_by_analogy" inputs -- structure borrowed from a published study in a
#' DIFFERENT specialty -- require explicit opt-in, because transferring a
#' physiatry delegation pattern onto urogynaecology is a modelling choice a
#' reader must be told about. Placeholders are always refused.
#'
#' @param status Calibration status to check.
#' @param allow_analogy Permit "derived_by_analogy" inputs.
#' @param what Label used in the message.
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @return (Invisibly) TRUE when publishable.
#' @export
assert_publishable_workload <- function(status = urps_service_workload_status(),
                                        allow_analogy = FALSE,
                                        what = "workload basket",
                                        mode = resolve_reproducibility_mode()) {
  if (!status %in% CALIBRATION_TIERS) {
    stop(sprintf("Unknown calibration tier '%s'. Expected one of: %s",
                 status, paste(CALIBRATION_TIERS, collapse = ", ")), call. = FALSE)
  }
  if (status %in% c("calibrated", "solved")) return(invisible(TRUE))

  if (status == "derived_by_analogy") {
    msg <- sprintf(paste(
      "The %s is derived by analogy from a published study in another specialty,",
      "not measured in urogynaecology. Report it as an assumption, or pass",
      "allow_analogy = TRUE to proceed knowingly."), what)
    if (isTRUE(allow_analogy)) {
      .msg_info(sprintf("Proceeding with an analogy-derived %s (declared).", what))
      return(invisible(TRUE))
    }
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
    return(invisible(FALSE))
  }

  msg <- sprintf(
    "The %s calibration_status is '%s': the values are placeholders and no number built on them is publishable.",
    what, status)
  if (identical(mode, "strict")) stop(msg, call. = FALSE)
  .msg_warn(msg)
  invisible(FALSE)
}

#' Calibration status of every model input, in one table
#'
#' @return Tibble of `input`, `status`, `source`.
#' @export
calibration_status_report <- function() {
  tibble::tibble(
    input = c("work RVUs", "service case mix", "delegation shares",
              "indirect time share", "clinical hours schedule",
              "hours intercept", "base-year supply"),
    status = c("calibrated", "derived_by_analogy", URPS_DELEGATION_STATUS,
               "calibrated", "derived_by_analogy", "solved", "calibrated"),
    source = c(
      CMS_RVU_RELEASE,
      "Case-mix weights in URPS_CPT_BASKET are a declared assumption; replace with claims-derived shares",
      URPS_DELEGATION_SOURCE,
      INDIRECT_TIME_SHARE_SOURCE,
      "HWSM Exhibit 14 age x sex specification (general internal medicine levels)",
      "Solved so the base-year cohort mean equals the FTE definition (37.2 clinical hrs/wk)",
      "mufflyaccess URPS contract"
    )
  )
}
