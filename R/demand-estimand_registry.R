# Machine-Readable Estimand Registry & Semantic Contract Enforcement ----
#
# Scientific Hardening Section 2 Layer 2B: Semantic Correctness
#
# Enforces exact semantic boundaries across all scientific outputs.
# Prevents semantic substitution errors (e.g. D6 inpatient volume -> D3 total surgery).

#' Read Machine-Readable Estimand Registry YAML
#'
#' @param path Path to estimands YAML config (defaults to `config/estimands.yml`).
#' @return List of estimand contracts.
#' @family estimands
#' @concept demand
#' @export
read_estimand_registry <- function(path = "config/estimands.yml") {
  if (!file.exists(path)) {
    # Traverse upwards or check 00_pkg_src under R CMD check
    curr <- getwd()
    found <- FALSE
    for (i in 1:6) {
      cand <- file.path(curr, "config", "estimands.yml")
      if (file.exists(cand)) {
        path <- cand
        found <- TRUE
        break
      }
      src_cand <- file.path(curr, "00_pkg_src", "urpssim", "config", "estimands.yml")
      if (file.exists(src_cand)) {
        path <- src_cand
        found <- TRUE
        break
      }
      curr <- dirname(curr)
    }
    if (!found) {
      path <- system.file("config", "estimands.yml", package = "urpssim")
    }
  }

  if (!file.exists(path)) {
    stop(sprintf("read_estimand_registry(): Cannot locate estimands config file at '%s'.", path), call. = FALSE)
  }

  cfg <- yaml::read_yaml(path)
  cfg$estimands
}

#' Assert Estimand Compatibility and Protect Semantic Boundaries
#'
#' Evaluates whether a source estimand (e.g. `D6`) can be legally used for a target operation.
#'
#' @param source_estimand Name of the source estimand (e.g. `"D6"` or `"D3"`).
#' @param target_use Proposed usage role string (e.g. `"total_surgical_demand_calibration"`).
#' @param registry Optional estimand registry list.
#' @return (Invisibly) TRUE if compatible; throws a hard error if forbidden or incompatible.
#' @family estimands
#' @concept demand
#' @export
assert_estimand_compatible <- function(source_estimand, target_use, registry = NULL) {
  if (is.null(registry)) {
    registry <- read_estimand_registry()
  }

  contract <- registry[[source_estimand]]
  if (is.null(contract)) {
    stop(sprintf("assert_estimand_compatible(): Source estimand '%s' not found in registry.", source_estimand), call. = FALSE)
  }

  forbidden <- unlist(contract$forbidden_uses)
  allowed <- unlist(contract$allowed_uses)

  if (target_use %in% forbidden) {
    stop(sprintf(
      "assert_estimand_compatible(): SEMANTIC FAILURE! Estimand '%s' (%s) is FORBIDDEN for target use '%s'. %s",
      source_estimand, contract$name, target_use,
      "Inpatient-only or restricted estimands cannot be substituted for total national demand or FTE capacity."
    ), call. = FALSE)
  }

  if (length(allowed) > 0 && !target_use %in% allowed) {
    stop(sprintf(
      "assert_estimand_compatible(): Target use '%s' is not in allowed uses for Estimand '%s'.",
      target_use, source_estimand
    ), call. = FALSE)
  }

  invisible(TRUE)
}
