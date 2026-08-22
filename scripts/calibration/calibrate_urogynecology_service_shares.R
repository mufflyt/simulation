#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  if (!requireNamespace("urpssim", quietly = TRUE)) {
    pkgload::load_all(".", quiet = TRUE)
  } else {
    library(urpssim)
  }
})

events_path <- Sys.getenv("URPS_SERVICE_SHARE_EVENTS", "")
cms_path <- Sys.getenv("URPS_CMS_SERVICE_SHARE_EVIDENCE", "")
chia_path <- Sys.getenv("URPS_CHIA_SERVICE_SHARE_EVIDENCE", "")
output_dir <- Sys.getenv(
  "URPS_SERVICE_SHARE_OUTPUT_DIR",
  file.path("artifacts", "service_shares")
)
seed <- base::as.integer(Sys.getenv("URPS_SERVICE_SHARE_SEED", "20260822"))
draws <- base::as.integer(Sys.getenv("URPS_SERVICE_SHARE_DRAWS", "1000"))

if (!base::nzchar(events_path) || !base::file.exists(events_path)) {
  base::stop(
    "Set URPS_SERVICE_SHARE_EVENTS to a provider-group event-count CSV/RDS.",
    call. = FALSE
  )
}

read_input <- function(path) {
  if (base::grepl("\\.rds$", path, ignore.case = TRUE)) {
    base::readRDS(path)
  } else {
    readr::read_csv(path, show_col_types = FALSE, progress = interactive())
  }
}

events <- read_input(events_path)
if (!"condition" %in% base::names(events)) {
  base::stop(
    "Production calibration events must include `condition`; do not collapse ",
    "condition-specific provider routing into an unspecified aggregate.",
    call. = FALSE
  )
}

cms_evidence <- NULL
if (base::nzchar(cms_path)) {
  if (!base::file.exists(cms_path)) {
    base::stop("CMS evidence file does not exist: ", cms_path, call. = FALSE)
  }
  cms_evidence <- base::readRDS(cms_path)
}

chia_evidence <- NULL
if (base::nzchar(chia_path)) {
  if (!base::file.exists(chia_path)) {
    base::stop("CHIA evidence file does not exist: ", chia_path, call. = FALSE)
  }
  chia_evidence <- base::readRDS(chia_path)
}

bundle <- calibrate_service_share_model(
  events = events,
  cms_evidence = cms_evidence,
  chia_evidence = chia_evidence,
  draws = draws,
  seed = seed
)
bundle$provenance$event_file_sha256 <- digest::digest(
  file = events_path,
  algo = "sha256"
)
if (!base::is.null(cms_evidence)) {
  bundle$provenance$cms_file_sha256 <- digest::digest(
    file = cms_path,
    algo = "sha256"
  )
}
if (!base::is.null(chia_evidence)) {
  bundle$provenance$chia_file_sha256 <- digest::digest(
    file = chia_path,
    algo = "sha256"
  )
}
validate_service_share_bundle(bundle)

base::dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
bundle_path <- base::file.path(
  output_dir,
  base::paste0("calibrated_service_share_bundle_", timestamp, ".rds")
)
draw_path <- base::file.path(
  output_dir,
  base::paste0("calibrated_service_share_draws_", timestamp, ".csv")
)
alpha_path <- base::file.path(
  output_dir,
  base::paste0("calibrated_service_share_alpha_", timestamp, ".csv")
)
score_path <- base::file.path(
  output_dir,
  base::paste0("calibrated_service_share_holdout_scores_", timestamp, ".csv")
)

base::saveRDS(bundle, bundle_path)
readr::write_csv(bundle$share_draws, draw_path)
readr::write_csv(bundle$selected_alpha, alpha_path)
readr::write_csv(bundle$holdout_scores, score_path)

base::message("Saved calibrated bundle: ", base::normalizePath(bundle_path))
base::message("Saved share draws: ", base::normalizePath(draw_path))
base::message("Saved selected alpha values: ", base::normalizePath(alpha_path))
base::message("Saved held-out scores: ", base::normalizePath(score_path))
