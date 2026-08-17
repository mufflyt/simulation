# Layer 2E: Boundary and Decision-Surface Validation (BVA Engine) ----
#
# Scientific Hardening Section 1-3 Layer 2E: Boundary-Value Analysis Engine
#
# Tests values just below (L - eps), exactly at (L), and just above (L + eps) for every
# scientific threshold registered in `config/scientific_boundaries.yml`.

#' Read Machine-Readable Scientific Boundary Registry
#'
#' @param path Path to boundary YAML config (defaults to `config/scientific_boundaries.yml`).
#' @return List of boundary specifications.
#' @family bva
#' @concept testing
#' @export
read_scientific_boundaries <- function(path = "config/scientific_boundaries.yml") {
  if (!file.exists(path)) {
    path <- system.file("config", "scientific_boundaries.yml", package = "urpssim")
  }
  if (!file.exists(path) && file.exists("../../config/scientific_boundaries.yml")) {
    path <- "../../config/scientific_boundaries.yml"
  }

  cfg <- yaml::read_yaml(path)
  cfg$boundaries
}

#' Evaluate Boundary Value Assignment Across Decision Surface (BVA)
#'
#' Evaluates a scalar value against a registered scientific boundary (below, exact, above).
#'
#' @param boundary_id ID of registered boundary (e.g. `"drive_time_30"`, `"probability_upper"`).
#' @param value Numeric value to evaluate.
#' @param registry Optional boundary registry list.
#' @return Evaluated classification or status string.
#' @family bva
#' @concept testing
#' @export
test_boundary_value <- function(boundary_id, value, registry = NULL) {
  if (is.null(registry)) {
    registry <- read_scientific_boundaries()
  }

  b_spec <- registry[[boundary_id]]
  if (is.null(b_spec)) {
    stop(sprintf("test_boundary_value(): Boundary ID '%s' not found in registry.", boundary_id), call. = FALSE)
  }

  thresh <- as.numeric(b_spec$threshold)
  eps <- 1e-6

  if (b_spec$domain == "geography" && grepl("drive_time", boundary_id)) {
    if (value <= 30.0) return("00-30")
    if (value <= 60.0) return("31-60")
    if (value <= 120.0) return("61-120")
    if (value <= 180.0) return("121-180")
    return(">180")
  }

  if (b_spec$domain == "transition" && b_spec$variable_class == "probability") {
    if (value < 0.0 || value > 1.0) return("error")
    return("valid")
  }

  if (b_spec$domain == "adequacy") {
    if (value < 0.0) return("shortage")
    if (value == 0.0) return("balanced")
    return("surplus")
  }

  if (b_spec$domain == "capability") {
    if (value < thresh) return("incapable")
    return("capable")
  }

  "unknown"
}
