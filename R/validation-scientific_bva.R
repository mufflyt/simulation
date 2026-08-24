# Layer 2E: Boundary and Decision-Surface Validation (BVA Engine) ----
#
# Scientific Hardening Section 1-3 Layer 2E: Boundary-Value Analysis Engine
#
# Tests values just below (L - eps), exactly at (L), and just above (L + eps) for every
# scientific threshold registered in `config/scientific_boundaries.yml`.

#' Read Machine-Readable Scientific Boundary Registry
#'
#' @param path Path to boundary YAML config (defaults to `config/scientific_boundaries.yml`).
#'   When that is absent -- `config/` is `.Rbuildignore`d, so it never exists in
#'   an installed package -- the copy shipped at `extdata/scientific_boundaries.yml`
#'   is read instead.
#' @return List of boundary specifications.
#' @family bva
#' @concept testing
#' @export
read_scientific_boundaries <- function(path = "config/scientific_boundaries.yml") {
  # RESOLUTION ORDER: caller's path -> source tree -> INSTALLED COPY.
  #
  # The installed copy is the one that matters and the one that was missing.
  # config/ is .Rbuildignore'd, so under R CMD check -- where tests run from
  # inside <pkg>.Rcheck/ with no source tree -- neither the relative default
  # nor the ../../ probe below resolves, and every caller died with
  # "Boundary ID 'drive_time_30' not found in registry", which reads like a
  # registry defect rather than a missing file.
  #
  # This function already reached for system.file(), so shipping it was always
  # the intent; it looked in "config", a directory inst/ does not have. The
  # registry now ships in inst/extdata alongside the other config files this
  # package installs (ai_claims_basket.yml, recurrence_evidence.csv, ...),
  # and tests/testthat/test-config-extdata-sync.R asserts the shipped copy
  # still matches config/.
  if (!file.exists(path)) {
    root <- .repo_source_root()
    if (!is.na(root)) {
      candidate <- file.path(root, "config", "scientific_boundaries.yml")
      if (file.exists(candidate)) path <- candidate
    }
  }
  if (!file.exists(path)) {
    installed <- system.file("extdata", "scientific_boundaries.yml",
                             package = "urpssim")
    if (nzchar(installed) && file.exists(installed)) path <- installed
  }
  if (!file.exists(path)) {
    path <- "inst/extdata/scientific_boundaries.yml"   # dev (load_all)
  }
  if (!file.exists(path)) {
    stop(
      "read_scientific_boundaries(): boundary registry not found. Looked for ",
      "config/scientific_boundaries.yml in the source tree and ",
      "extdata/scientific_boundaries.yml in the installed package.",
      call. = FALSE
    )
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
