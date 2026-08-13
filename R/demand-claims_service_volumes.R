# Claims-derived URPS service volumes ---------------------------------------
#
# example_service_volumes() (R/core-run_workforce_microsimulation.R) builds the
# base-year service basket from hardcoded per-service ratios on the demand
# estimands -- 2.4 return visits per consult, 0.55 slings per surgery case, and
# so on. Those ratios are an ASSUMPTION (calibration tier
# "uncalibrated_illustrative"), and because the productivity denominator is
# SOLVED from them (calibrate_wrvu_per_fte()), a wrong basket shows up as a
# solved wRVU/FTE outside the 3,500-12,000 benchmark -- the "Fix the
# service-volume inputs before trusting any gap" warning.
#
# claims_service_volumes() is the calibrated replacement path. It merges a REAL
# claims-backed volume table (CADR Medicare procedure counts, NAMCS ambulatory
# visits, SASD operative counts) over the illustrative fallback and stamps EACH
# service with its own calibration tier. The overall status is the WEAKEST tier
# present (CALIBRATION_STATUS_RANK, min-wins), so the basket only reports
# "calibrated" once EVERY service is claims-backed -- it never overclaims a
# fully calibrated basket while some services still ride the fallback ratios.
#
# This is the pure combiner; the file I/O + productivity check live in the
# fail-closed runner scripts/calibration/build_claims_service_volumes.R, exactly
# as build_and_run_real_supply_calibration() wraps calibrate_urps_supply_dynamics().

# Weakest (lowest-rank) tier in a vector of statuses. Unknown strings rank 0 --
# an unrecognised status is never a promotion.
.claims_weakest_status <- function(statuses) {
  statuses <- statuses[!is.na(statuses)]
  if (length(statuses) == 0L) return(NA_character_)
  rank <- CALIBRATION_STATUS_RANK[statuses]
  rank[is.na(rank)] <- 0L
  statuses[which.min(rank)]
}

#' Assemble URPS service volumes from real claims, with per-service tiers
#'
#' @description
#' The calibrated counterpart of `example_service_volumes()`. Merges a real,
#' claims-backed volume table over an illustrative fallback and stamps each
#' service with its own calibration tier. The overall status is the WEAKEST tier
#' present, so the basket reports `"calibrated"` only when every service is
#' claims-backed -- it never overclaims a fully calibrated basket while some
#' services still ride the fallback ratios.
#'
#' @details
#' `claims` is source-agnostic: CADR Medicare procedure counts, a NAMCS-derived
#' visit level, and SASD operative counts all funnel into the same
#' `service`/`year`/`volume` shape, each carrying its own `calibration_status`.
#' Services (or years) absent from `claims` are filled from `fallback` -- e.g.
#' `example_service_volumes(demand_long)` -- and tagged `fallback_status`.
#'
#' Fail-closed: with `fallback = NULL` the function does not invent the missing
#' rows; with `require_complete = TRUE` any reliance on the fallback is a hard
#' stop in `strict` mode (a warning in `relaxed`), so a run cannot silently ship
#' a half-claims basket as if it were fully calibrated.
#'
#' @param claims A data frame with `service`, `year`, `volume`, and optionally
#'   `calibration_status` and `source`. Missing `calibration_status` defaults to
#'   `claims_status_default`; missing `source` to `"claims"`.
#' @param fallback Optional data frame with `service`, `year`, `volume` for the
#'   services/years not covered by `claims` (e.g. the illustrative basket).
#'   `NULL` uses `claims` alone.
#' @param fallback_status Calibration tier stamped on fallback rows.
#' @param claims_status_default Calibration tier for claims rows that carry no
#'   `calibration_status` of their own.
#' @param require_complete When `TRUE`, using any fallback row is treated as an
#'   incomplete calibration: `strict` mode errors, `relaxed` warns.
#' @param mode Reproducibility mode.
#'
#' @return A tibble `year`, `service`, `volume`, `calibration_status`, `source`,
#'   with the weakest overall tier recorded in `attr(., "overall_status")`.
#'
#' @seealso `example_service_volumes()`, [calibrate_wrvu_per_fte()],
#'   [check_productivity_plausible()]
#' @family workload to fte
#' @concept demand
#' @export
claims_service_volumes <- function(claims,
                                   fallback = NULL,
                                   fallback_status = "uncalibrated_illustrative",
                                   claims_status_default = "calibrated",
                                   require_complete = FALSE,
                                   mode = resolve_reproducibility_mode()) {
  required <- c("service", "year", "volume")
  if (!all(required %in% names(claims))) {
    stop("claims_service_volumes(): `claims` must have columns ",
         paste(required, collapse = ", "), ".", call. = FALSE)
  }
  if (!fallback_status %in% CALIBRATION_TIERS) {
    stop("claims_service_volumes(): fallback_status must be one of ",
         paste(CALIBRATION_TIERS, collapse = ", "), ".", call. = FALSE)
  }

  claims_std <- claims |>
    dplyr::transmute(
      service = as.character(.data$service),
      year = as.integer(.data$year),
      volume = as.numeric(.data$volume),
      calibration_status = if ("calibration_status" %in% names(claims)) {
        as.character(.data$calibration_status)
      } else {
        claims_status_default
      },
      source = if ("source" %in% names(claims)) as.character(.data$source) else "claims"
    ) |>
    dplyr::filter(!is.na(.data$service), !is.na(.data$year), !is.na(.data$volume),
                  .data$volume >= 0)

  if (any(is.na(CALIBRATION_STATUS_RANK[claims_std$calibration_status]))) {
    bad <- unique(claims_std$calibration_status[
      is.na(CALIBRATION_STATUS_RANK[claims_std$calibration_status])])
    stop("claims_service_volumes(): unknown calibration_status in claims: ",
         paste(bad, collapse = ", "), ".", call. = FALSE)
  }

  used_fallback <- FALSE
  if (!is.null(fallback)) {
    if (!all(required %in% names(fallback))) {
      stop("claims_service_volumes(): `fallback` must have columns ",
           paste(required, collapse = ", "), ".", call. = FALSE)
    }
    fb_std <- fallback |>
      dplyr::transmute(
        service = as.character(.data$service),
        year = as.integer(.data$year),
        volume = as.numeric(.data$volume),
        calibration_status = fallback_status,
        source = "illustrative_fallback"
      ) |>
      dplyr::filter(!is.na(.data$service), !is.na(.data$year), !is.na(.data$volume))
    # Only the (service, year) cells the claims table does not already cover.
    fb_needed <- dplyr::anti_join(fb_std, claims_std, by = c("service", "year"))
    used_fallback <- nrow(fb_needed) > 0L
    out <- dplyr::bind_rows(claims_std, fb_needed)
  } else {
    out <- claims_std
  }

  out <- dplyr::arrange(out, .data$service, .data$year)
  overall <- .claims_weakest_status(out$calibration_status)

  n_claims <- sum(out$source != "illustrative_fallback")
  n_fb <- sum(out$source == "illustrative_fallback")
  .msg_info(sprintf(
    "claims_service_volumes(): %d claims-backed rows, %d fallback rows; overall tier '%s'.",
    n_claims, n_fb, overall))

  if (isTRUE(require_complete) && used_fallback) {
    msg <- paste0(
      "claims_service_volumes(): ", n_fb, " service-year cells fell back to the ",
      "illustrative ratios, so the basket is not fully claims-calibrated (overall '",
      overall, "'). Supply claims for every service before treating the basket as calibrated.")
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
  }

  attr(out, "overall_status") <- overall
  out
}

# Resolve the service-volume basket for a run: prefer the claims-calibrated
# basket that scripts/calibration/build_claims_service_volumes.R writes to the
# repo-local data-raw/demand/, falling back to the illustrative
# example_service_volumes() when it is absent or malformed. The default path is
# repo-relative (matching where the builder writes) rather than data_raw_path(),
# which resolves to the EXTERNAL data root. Absent file -> unchanged behaviour,
# so every run and test without the CSV keeps the illustrative basket.
resolve_service_volumes <- function(demand_long,
                                    path = Sys.getenv(
                                      "SIMULATION_SERVICE_VOLUMES",
                                      file.path("data-raw", "demand", "urps_service_volumes.csv")),
                                    mode = resolve_reproducibility_mode()) {
  fallback <- example_service_volumes(demand_long)
  if (!nzchar(path) || !file.exists(path)) return(fallback)
  claimed <- readr::read_csv(path, show_col_types = FALSE, progress = FALSE)
  if (!all(c("year", "service", "volume") %in% names(claimed))) {
    msg <- sprintf(paste0("resolve_service_volumes(): %s lacks year/service/volume; ",
                          "using the illustrative basket."), path)
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
    return(fallback)
  }
  # claims override the illustrative fallback per (service, year); uncovered
  # cells stay illustrative, so a partial basket is honestly the weakest tier.
  out <- claims_service_volumes(claims = claimed, fallback = fallback, mode = mode)
  .msg_info(sprintf("Service volumes: calibrated basket from %s (overall tier '%s').",
                    path, attr(out, "overall_status")))
  dplyr::select(out, "year", "service", "volume")
}
