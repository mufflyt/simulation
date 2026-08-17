# Cross-Repository simulation <-> cliff Access-Surface Contract ----
#
# Scientific Hardening Section 12 P1: Cross-Repository Contract CI
#
# Enforces strict contract validation for access surface exported from `simulation`
# and consumed by downstream package `cliff`.

#' Validate Access Surface Export Contract for cliff Consumption
#'
#' @param access_df Access surface data frame (e.g. tract-level spatial access scores).
#' @return List containing `valid` (logical), `missing_columns` (character), and `checksum_sha256`.
#' @family contract
#' @concept geography
#' @export
validate_simulation_cliff_contract <- function(access_df) {
  required_cols <- c("geoid", "spatial_access_score", "provider_count", "calibration_status")
  missing <- setdiff(required_cols, names(access_df))

  if (length(missing) > 0) {
    stop("validate_simulation_cliff_contract(): missing required schema column(s): ",
         paste(missing, collapse = ", "), call. = FALSE)
  }

  if (nrow(access_df) == 0 || any(is.na(access_df$spatial_access_score))) {
    stop("validate_simulation_cliff_contract(): access surface contains NA or zero rows.", call. = FALSE)
  }

  checksum <- digest::digest(access_df, algo = "sha256")

  list(
    valid = TRUE,
    missing_columns = character(0),
    checksum_sha256 = checksum,
    row_count = nrow(access_df)
  )
}
