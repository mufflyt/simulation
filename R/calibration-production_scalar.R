################################################################################
# R/calibration-production_scalar.R
# A production calibration scalar requires provenance for BOTH SIDES of the
# division.
#
# The external target side was hardened first: anchors carry a sha256, a clinical
# review with a named reviewer, and an estimand contract. The prediction side had
# no such requirement, so a bare numeric typed into a smoke test produced
# arithmetic indistinguishable from a calibration result -- and it was reported
# as one. 0.963, 1.408 and 0.790 were computed against illustrative predictions
# of 5,000,000 / 100,000 / 0.30 that no model ever emitted.
#
# The sequencing this enforces matters beyond bookkeeping: the model must expose
# its UNCALIBRATED base-year predictions before it is allowed to see the target.
# Otherwise the calibration target leaks into the prediction-generating path and
# the scalar stops being a check on anything.
################################################################################

#' Assert a model prediction may be used for a production calibration scalar
#'
#' @param model_prediction Named list carrying `estimand_id`, `prediction`,
#'   `model_run_id`, `model_version`, `artifact_path`, `artifact_sha256`,
#'   `generated_utc` and `prediction_status`. `prediction_status` must be
#'   `"production"`.
#' @return Invisibly, TRUE. Stops otherwise.
#' @export
assert_production_prediction <- function(model_prediction) {
  required_names <- c("estimand_id", "prediction", "model_run_id",
                      "model_version", "artifact_path", "artifact_sha256",
                      "generated_utc", "prediction_status")
  missing_names <- base::setdiff(required_names, base::names(model_prediction))
  if (base::length(missing_names) > 0L) {
    base::stop("Model prediction is missing production provenance: ",
               base::paste(missing_names, collapse = ", "), call. = FALSE)
  }
  if (!base::identical(model_prediction$prediction_status, "production")) {
    base::stop("Calibration scalar cannot use a non-production prediction.",
               call. = FALSE)
  }
  if (!base::is.finite(model_prediction$prediction) ||
      model_prediction$prediction <= 0) {
    base::stop("Production prediction must be finite and > 0.", call. = FALSE)
  }
  if (!base::nzchar(base::as.character(model_prediction$artifact_sha256)) ||
      base::is.na(model_prediction$artifact_sha256)) {
    base::stop("Production prediction has no artifact checksum; the run that ",
               "produced it cannot be identified.", call. = FALSE)
  }
  base::invisible(TRUE)
}

#' Compute a production calibration scalar
#'
#' @param external_target Numeric anchor value.
#' @param model_prediction Provenance-carrying prediction; see
#'   [assert_production_prediction].
#' @return One-row tibble carrying the scalar and the provenance of both sides.
#' @export
compute_production_scalar <- function(external_target, model_prediction) {
  assert_production_prediction(model_prediction)
  if (!base::is.finite(external_target) || external_target <= 0) {
    base::stop("External target must be finite and > 0.", call. = FALSE)
  }
  scalar <- external_target / model_prediction$prediction
  tibble::tibble(
    estimand_id           = model_prediction$estimand_id,
    external_target       = external_target,
    raw_model_prediction  = model_prediction$prediction,
    calibration_scalar    = scalar,
    calibrated_prediction = model_prediction$prediction * scalar,
    model_run_id          = model_prediction$model_run_id,
    model_version         = model_prediction$model_version,
    model_artifact_sha256 = model_prediction$artifact_sha256,
    generated_utc         = model_prediction$generated_utc)
}

#' Current calibration state, stated without overclaiming
#'
#' Distinguishes three things the previous reporting collapsed: whether the
#' external anchor is frozen, whether the clinical estimand is approved, and
#' whether a production scalar exists. Only the third requires model output.
#'
#' @param config_path Calibration YAML.
#' @return Tibble, one row per anchor.
#' @export
calibration_state <- function(config_path = "config/calibration_targets.yml") {
  cfg <- yaml::read_yaml(config_path)
  rows <- base::lapply(base::names(cfg$anchors), function(nm) {
    a <- cfg$anchors[[nm]]
    review <- a$clinical_review %||% base::list()
    present <- base::file.exists(a$path)
    hashed  <- base::nzchar(base::as.character(a$sha256 %||% ""))
    tibble::tibble(
      anchor              = nm,
      clinical_review     = base::as.character(review$status %||% "not_recorded"),
      anchor_status       = if (present && hashed) "production_ready"
                            else if (base::identical(a$status, "missing")) "blocked"
                            else "incomplete",
      production_scalar   = "pending_real_model_prediction")
  })
  out <- dplyr::bind_rows(rows)
  base::message(base::sprintf(
    "%d of %d anchors production-ready; %d production scalars estimated from ",
    base::sum(out$anchor_status == "production_ready"), base::nrow(out), 0L),
    "actual model output.")
  out
}

