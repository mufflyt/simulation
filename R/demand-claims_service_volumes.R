# Claims-derived URPS service volume calibration ------------------------------

#' Calibrate URPS service volumes from claims data with fallback
#'
#' @param claims Data frame of claims-derived service volumes.
#' @param fallback Data frame of fallback service volumes.
#' @param require_complete Require complete claims coverage.
#' @param mode Mode when fallback is required: "strict" (error) or "relaxed" (warning).
#'
#' @return Service volumes table with overall_status attribute.
#' @family demand
#' @concept claims
#' @export
claims_service_volumes <- function(
    claims,
    fallback = NULL,
    require_complete = FALSE,
    mode = base::c("strict", "relaxed")) {

  mode <- base::match.arg(mode)

  allowed_statuses <- c("calibrated", "uncalibrated_illustrative", "modeled", "survey")
  if ("calibration_status" %in% base::names(claims)) {
    invalid_status <- base::setdiff(claims$calibration_status, allowed_statuses)
    if (base::length(invalid_status) > 0L) {
      base::stop("unknown calibration_status: ", base::paste(invalid_status, collapse = ", "), call. = FALSE)
    }
  } else {
    claims$calibration_status <- "calibrated"
  }

  if (!"source" %in% base::names(claims)) {
    claims$source <- "CADR"
  }

  if (base::is.null(fallback)) {
    out <- claims |>
      dplyr::select(dplyr::any_of(c("year", "service", "volume", "calibration_status", "source")))
    overall <- if (base::all(out$calibration_status == "calibrated")) "calibrated" else "uncalibrated_illustrative"
    base::attr(out, "overall_status") <- overall
    base::return(out)
  }

  fallback_prep <- fallback |>
    dplyr::mutate(
      fallback_source = "illustrative_fallback",
      fallback_status = "uncalibrated_illustrative"
    )

  merged <- fallback_prep |>
    dplyr::left_join(
      claims,
      by = c("service", "year"),
      suffix = c("_fb", "_claims")
    )

  used_fallback <- base::any(base::is.na(merged$volume_claims))

  if (require_complete && used_fallback) {
    msg <- "Basket is not fully claims-calibrated."
    if (mode == "strict") {
      base::stop(msg, call. = FALSE)
    } else {
      base::warning(msg, call. = FALSE)
    }
  }

  out <- merged |>
    dplyr::transmute(
      year = .data$year,
      service = .data$service,
      volume = dplyr::coalesce(.data$volume_claims, .data$volume_fb),
      calibration_status = dplyr::coalesce(.data$calibration_status, .data$fallback_status),
      source = dplyr::coalesce(.data$source, .data$fallback_source)
    )

  overall <- if (!used_fallback && base::all(out$calibration_status == "calibrated")) {
    "calibrated"
  } else {
    "uncalibrated_illustrative"
  }

  base::attr(out, "overall_status") <- overall
  out
}


#' Resolve service volumes using calibrated claims file or fallback
#'
#' @param demand_long Demand table.
#' @param path Path to claims service volume CSV file.
#' @param mode Reproducibility mode (`"strict"` or `"relaxed"`), threaded to
#'   [claims_service_volumes()] so a strict run fails on an incomplete basket
#'   rather than silently warning. Defaults to [resolve_reproducibility_mode()].
#'
#' @return Table of service volumes.
#' @keywords internal
resolve_service_volumes <- function(demand_long, path = NULL,
                                    mode = resolve_reproducibility_mode()) {
  fallback <- example_service_volumes(demand_long)
  if (!base::is.null(path) && base::file.exists(path)) {
    claims_csv <- readr::read_csv(path, show_col_types = FALSE)
    claims_service_volumes(claims_csv, fallback = fallback, mode = mode)
  } else {
    fallback
  }
}

