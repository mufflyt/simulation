# Load free adequacy evidence into the research DuckDB.
#
# Existing BRFSS and ACS PUMS artifacts are reused. Sources without stable
# national bulk endpoints are accepted through a two-column manifest with
# `source_id` and `local_path`; this prevents a changing state PDF or dashboard
# from being silently treated as a canonical table.

run_adequacy_source_load <- function(
    db_path = base::Sys.getenv(
      "URPS_ADEQUACY_DB",
      base::file.path("data-raw", "adequacy_sources", "adequacy.duckdb")
    ),
    manifest_path = base::Sys.getenv(
      "URPS_ADEQUACY_SOURCE_MANIFEST",
      base::file.path(
        "data-raw", "adequacy_sources", "source_manifest.csv"
      )
    ),
    download_catalog_sources = TRUE,
    strict = FALSE) {
  base::message("Starting free adequacy-source DuckDB load.")
  base::message("DuckDB path: ", db_path)
  base::dir.create(base::dirname(db_path), recursive = TRUE,
                   showWarnings = FALSE)

  pkgload::load_all(".", quiet = TRUE)
  source_manifest <- if (base::file.exists(manifest_path)) {
    base::message("Reading source manifest: ", manifest_path)
    readr::read_csv(manifest_path, show_col_types = FALSE, progress = FALSE)
  } else {
    base::message("No source manifest found; using existing canonical files.")
    tibble::tibble(
      source_id = base::character(),
      local_path = base::character()
    )
  }

  existing_candidates <- tibble::tribble(
    ~source_id, ~local_path,
    "brfss_access",
    base::file.path(
      "data-raw", "brfss", "brfss_2024_women18plus.rds"
    ),
    "acs_pums",
    base::file.path(
      "data-raw", "acs", "acs5_2023_pums_women18plus.rds"
    )
  ) |>
    dplyr::filter(base::file.exists(.data$local_path))

  source_manifest <- dplyr::bind_rows(
    source_manifest,
    existing_candidates
  ) |>
    dplyr::distinct(.data$source_id, .keep_all = TRUE)

  loading <- load_adequacy_sources_duckdb(
    db_path = db_path,
    source_manifest = source_manifest,
    download_catalog_sources = download_catalog_sources,
    overwrite = FALSE,
    strict = strict
  )

  stamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
  audit_path <- base::file.path(
    base::dirname(db_path),
    base::paste0("adequacy_source_ingest_audit_", stamp, ".csv")
  )
  readr::write_csv(loading$audit, audit_path)
  available_n <- base::sum(loading$audit$status != "missing")
  missing_n <- base::sum(loading$audit$status == "missing")
  base::message(
    "Adequacy sources available: ", available_n,
    "; missing: ", missing_n, "."
  )
  base::message("Saved ingestion audit: ",
                base::normalizePath(audit_path, mustWork = TRUE))
  base::message("Completed free adequacy-source DuckDB load.")
  base::invisible(loading)
}

if (base::sys.nframe() == 0L) {
  run_adequacy_source_load()
}
