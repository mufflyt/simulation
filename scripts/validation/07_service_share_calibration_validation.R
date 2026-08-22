#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  pkgload::load_all(".", quiet = TRUE)
})

required_env <- c(
  events = "URPS_SERVICE_SHARE_EVENTS",
  bundle = "URPS_CALIBRATED_SERVICE_SHARE_BUNDLE",
  cms = "URPS_CMS_SERVICE_SHARE_EVIDENCE",
  chia = "URPS_CHIA_SERVICE_SHARE_EVIDENCE"
)
paths <- stats::setNames(
  base::vapply(
    required_env,
    base::Sys.getenv,
    FUN.VALUE = base::character(1)
  ),
  base::names(required_env)
)
missing <- paths[!base::nzchar(paths) | !base::file.exists(paths)]
if (base::length(missing) > 0L) {
  base::stop(
    "Real-data service-share validation requires mounted inputs for:\n  ",
    base::paste(
      base::paste0(base::names(missing), " = ", missing),
      collapse = "\n  "
    ),
    "\nNo fixture or legacy fallback is permitted.",
    call. = FALSE
  )
}

read_events <- function(path) {
  if (base::grepl("\\.rds$", path, ignore.case = TRUE)) {
    base::readRDS(path)
  } else {
    readr::read_csv(
      path,
      show_col_types = FALSE,
      progress = interactive()
    )
  }
}

events <- read_events(paths[["events"]])
bundle <- base::readRDS(paths[["bundle"]])
cms <- base::readRDS(paths[["cms"]])
chia <- base::readRDS(paths[["chia"]])

base::message("Validating calibrated service-share bundle.")
validate_service_share_bundle(bundle)
cms_checks <- validate_cms_service_share_accounting(cms)

base::message("Running source-dropout validation.")
dropout <- evaluate_service_share_source_dropout(
  events = events,
  cms_evidence = cms,
  chia_evidence = chia,
  draws = 500L,
  seed = bundle$config$seed
)

base::message("Running one-year coupled accounting validation.")
simulation <- run_end_to_end_simulation(
  start_year = 2025L,
  end_year = 2025L,
  initial_provider_count = 200L,
  fellowship_entrants = 10L,
  service_share_engine = "calibrated",
  service_share_bundle = bundle,
  service_share_draw = base::min(bundle$share_draws$draw_id),
  run_practice_economics = TRUE,
  practice_economics_draws = 25L,
  seed = bundle$config$seed,
  save_outputs = FALSE
)
accounting_checks <- validate_service_share_accounting(
  simulation,
  cms_evidence = cms
)
manifest <- service_share_provenance_manifest(bundle)
manifest <- manifest |>
  dplyr::bind_rows(tibble::tibble(
    key = base::paste0("file_sha256_", base::names(paths)),
    value = base::vapply(
      paths,
      function(path) digest::digest(file = path, algo = "sha256"),
      FUN.VALUE = base::character(1)
    )
  ))

output_dir <- base::Sys.getenv(
  "URPS_SERVICE_SHARE_VALIDATION_DIR",
  unset = base::file.path("artifacts", "service_share_validation")
)
base::dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")

outputs <- base::list(
  cms_accounting = cms_checks,
  source_dropout = dropout,
  coupled_accounting = accounting_checks,
  provenance_manifest = manifest,
  coupled_diagnostics = simulation$service_share_diagnostics
)
purrr::iwalk(outputs, function(data, name) {
  path <- base::file.path(
    output_dir,
    base::paste0(name, "_", timestamp, ".csv")
  )
  readr::write_csv(data, path)
  base::message("Saved ", name, ": ", base::normalizePath(path))
})

bundle_copy <- base::file.path(
  output_dir,
  base::paste0("validated_service_share_bundle_", timestamp, ".rds")
)
base::saveRDS(bundle, bundle_copy)
base::message("Saved validated bundle copy: ", base::normalizePath(bundle_copy))
base::message(
  "Reproducibility digest: ",
  service_share_reproducibility_digest(bundle)
)
base::message("Real-data service-share validation completed.")
