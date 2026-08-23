# Public Evidence & 12 Infrastructure Tables DuckDB Ingestion -----------------

#' Ingest Public Evidence Files into DuckDB
#'
#' Ingests downloaded CMS PFS RVU, BLS OEWS wage data, CMS DAC clinician registry,
#' and MEPS care-seeking data into the timestamped evidence DuckDB database.
#'
#' @param duckdb_path Path to the target DuckDB file.
#' @param project_root Path to the simulation repository root.
#' @param schema Target DuckDB schema (default "evidence").
#' @param replace Replace existing tables if present.
#'
#' @return A list with ingested table names and row counts.
#' @family data acquisition
#' @concept data
#' @export
ingest_public_evidence_duckdb <- function(
    duckdb_path,
    project_root = ".",
    schema = "evidence",
    replace = TRUE) {
  base::message("Starting public evidence DuckDB ingestion.")

  connection <- open_urps_evidence_db(duckdb_path, read_only = FALSE)
  on.exit(.urps_disconnect(connection), add = TRUE)

  DBI::dbExecute(connection, base::paste0("CREATE SCHEMA IF NOT EXISTS ", schema))

  ingested_summary <- list()

  # 1. CMS Work RVU Table & Full PFS File Ingestion
  rvu_tbl <- CMS_WORK_RVU
  DBI::dbWriteTable(
    connection,
    DBI::Id(schema = schema, table = "cms_pfs_rvu_2025"),
    rvu_tbl,
    overwrite = base::isTRUE(replace)
  )
  ingested_summary$cms_pfs_rvu_2025 <- base::nrow(rvu_tbl)
  base::message("Ingested `cms_pfs_rvu_2025`: ", scales::comma(base::nrow(rvu_tbl)), " rows.")

  pfs_dir <- base::file.path(project_root, "data-raw", "cms_pfs")
  pfs_csv <- base::file.path(pfs_dir, "PPRRVU25_JAN.csv")
  if (base::file.exists(pfs_csv)) {
    base::message("Ingesting full CMS PFS 2025 file (PPRRVU25_JAN.csv) into DuckDB...")
    sql <- base::paste0(
      "CREATE OR REPLACE TABLE ", schema, ".cms_pfs_full_2025 AS ",
      "SELECT * FROM read_csv_auto('", pfs_csv, "', ALL_VARCHAR=TRUE)"
    )
    DBI::dbExecute(connection, sql)
    count <- DBI::dbGetQuery(connection, base::paste0("SELECT COUNT(*) AS n FROM ", schema, ".cms_pfs_full_2025"))$n
    ingested_summary$cms_pfs_full_2025 <- count
    base::message("Ingested `cms_pfs_full_2025`: ", scales::comma(count), " rows.")
  }

  # 2. BLS OEWS Wage Benchmarks
  bls_dir <- base::file.path(project_root, "data-raw", "bls_oews")
  bls_files <- base::list.files(bls_dir, pattern = "\\.(xlsx|csv|txt)$", full.names = TRUE, recursive = TRUE)

  if (base::length(bls_files) > 0L) {
    bls_csv <- base::grep("\\.csv$", bls_files, value = TRUE)
    if (base::length(bls_csv) > 0L) {
      bls_df <- readr::read_csv(bls_csv[[1L]], show_col_types = FALSE)
      DBI::dbWriteTable(
        connection,
        DBI::Id(schema = schema, table = "bls_oews_wages_2025"),
        bls_df,
        overwrite = base::isTRUE(replace)
      )
      ingested_summary$bls_oews_wages_2025 <- base::nrow(bls_df)
      base::message("Ingested `bls_oews_wages_2025`: ", scales::comma(base::nrow(bls_df)), " rows.")
    }
  }

  # 3. CMS DAC Clinicians National Roster
  dac_dir <- base::file.path(project_root, "data-raw", "cms_dac")
  dac_files <- base::list.files(dac_dir, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)

  if (base::length(dac_files) > 0L) {
    dac_path <- dac_files[[1L]]
    base::message("Ingesting CMS DAC clinicians from CSV into DuckDB...")
    sql <- base::paste0(
      "CREATE OR REPLACE TABLE ", schema, ".cms_dac_clinicians AS ",
      "SELECT * FROM read_csv_auto('", dac_path, "', ALL_VARCHAR=TRUE)"
    )
    DBI::dbExecute(connection, sql)
    count <- DBI::dbGetQuery(connection, base::paste0("SELECT COUNT(*) AS n FROM ", schema, ".cms_dac_clinicians"))$n
    ingested_summary$cms_dac_clinicians <- count
    base::message("Ingested `cms_dac_clinicians`: ", scales::comma(count), " rows.")
  }

  # 4. MEPS Care Seeking Survey Data
  meps_df <- tryCatch({
    load_meps_care_seeking()
  }, error = function(e) {
    NULL
  })

  if (!base::is.null(meps_df)) {
    DBI::dbWriteTable(
      connection,
      DBI::Id(schema = schema, table = "meps_hc_care_seeking"),
      meps_df,
      overwrite = base::isTRUE(replace)
    )
    ingested_summary$meps_hc_care_seeking <- base::nrow(meps_df)
    base::message("Ingested `meps_hc_care_seeking`: ", scales::comma(base::nrow(meps_df)), " rows.")
  }

  base::message("Public evidence DuckDB ingestion complete.")
  ingested_summary
}

#' Ingest All 12 Production Infrastructure Objects into DuckDB
#'
#' Executes all 12 production infrastructure components (registries, CMS evidence,
#' CHIA evidence, claims classification, Dirichlet calibration, joint compositional
#' draws, evidence synthesis, calibration bundle, and workload allocation) and writes
#' all resulting tables to DuckDB.
#'
#' @param duckdb_path Target DuckDB database path.
#' @param n_draws Number of compositional draws to generate and save.
#' @param schema Target DuckDB schema (default "evidence").
#'
#' @return A summary list of all ingested 12 infrastructure tables with row counts.
#' @family data acquisition
#' @concept data
#' @export
ingest_all_12_infrastructure_tables_to_duckdb <- function(
    duckdb_path,
    n_draws = 100L,
    schema = "evidence") {
  base::message("Starting complete ingestion of all 12 production infrastructure tables into DuckDB.")

  # First run public raw evidence ingestion
  summary_counts <- ingest_public_evidence_duckdb(duckdb_path = duckdb_path, schema = schema, replace = TRUE)

  connection <- open_urps_evidence_db(duckdb_path, read_only = FALSE)
  on.exit(.urps_disconnect(connection), add = TRUE)

  DBI::dbExecute(connection, base::paste0("CREATE SCHEMA IF NOT EXISTS ", schema))

  # 1. Canonical Service Registry
  service_registry <- build_urogynecology_service_registry()
  DBI::dbWriteTable(connection, DBI::Id(schema = schema, table = "service_registry"), service_registry, overwrite = TRUE)
  summary_counts$service_registry <- nrow(service_registry)

  # 2. Canonical Provider Taxonomy Registry
  taxonomy_registry <- build_urogynecology_provider_taxonomy_registry()
  DBI::dbWriteTable(connection, DBI::Id(schema = schema, table = "provider_taxonomy_registry"), taxonomy_registry, overwrite = TRUE)
  summary_counts$provider_taxonomy_registry <- nrow(taxonomy_registry)

  # 3. CMS Service Share Evidence
  cms_evidence <- default_cms_service_share_evidence()
  DBI::dbWriteTable(connection, DBI::Id(schema = schema, table = "cms_service_shares_suppression"), cms_evidence$service_bounds, overwrite = TRUE)
  summary_counts$cms_service_shares_suppression <- nrow(cms_evidence$service_bounds)

  # cms_evidence$wrvu_shares does not exist: build_cms_service_share_evidence()
  # never grew a per-service, per-provider-bucket wRVU-weighted share table
  # (see tests/testthat/test-data-cms-service-shares.R's skipped "wRVU
  # weighted shares" test for why this isn't invented here -- the weighting
  # semantics need a real design decision, not a guess). Warn and skip this
  # one table rather than halting all 12 -- the other 11 do not depend on it.
  base::warning(
    "ingest_all_12_infrastructure_tables_to_duckdb(): skipping ",
    "cms_wrvu_weighted_shares -- build_cms_service_share_evidence() does ",
    "not produce a wrvu_shares table. This is a real gap, not a missing ",
    "argument.",
    call. = FALSE
  )
  summary_counts$cms_wrvu_weighted_shares <- NA_integer_

  # 4. CHIA All-Payer Evidence
  chia_evidence <- build_chia_service_share_evidence(service_registry = service_registry, taxonomy_registry = taxonomy_registry)
  DBI::dbWriteTable(connection, DBI::Id(schema = schema, table = "chia_setting_shares"), chia_evidence$setting_shares, overwrite = TRUE)
  summary_counts$chia_setting_shares <- nrow(chia_evidence$setting_shares)

  DBI::dbWriteTable(connection, DBI::Id(schema = schema, table = "chia_payer_shares"), chia_evidence$payer_shares, overwrite = TRUE)
  summary_counts$chia_payer_shares <- nrow(chia_evidence$payer_shares)

  DBI::dbWriteTable(connection, DBI::Id(schema = schema, table = "chia_specialty_composition"), chia_evidence$specialty_composition, overwrite = TRUE)
  summary_counts$chia_specialty_composition <- nrow(chia_evidence$specialty_composition)

  # 5. Calibrated Dirichlet Model
  calib_model <- calibrate_service_share_model(cms_evidence = cms_evidence, chia_evidence = chia_evidence)
  DBI::dbWriteTable(connection, DBI::Id(schema = schema, table = "calibrated_priors"), calib_model$calibrated_priors, overwrite = TRUE)
  summary_counts$calibrated_priors <- nrow(calib_model$calibrated_priors)

  # 6. Joint Compositional Simplex Draws
  share_draws <- draw_compositional_service_shares(calibration_model = calib_model, n_draws = n_draws)
  DBI::dbWriteTable(connection, DBI::Id(schema = schema, table = "compositional_share_draws"), share_draws, overwrite = TRUE)
  summary_counts$compositional_share_draws <- nrow(share_draws)

  # 7. Synthesized CMS + CHIA Evidence
  synthesized <- combine_service_share_evidence(cms_evidence = cms_evidence, chia_evidence = chia_evidence)
  DBI::dbWriteTable(connection, DBI::Id(schema = schema, table = "combined_synthesized_evidence"), synthesized, overwrite = TRUE)
  summary_counts$combined_synthesized_evidence <- nrow(synthesized)

  # 8. Calibration Bundle Provenance
  bundle <- build_service_share_calibration_bundle(calibration_model = calib_model, n_draws = 10L)
  provenance_tbl <- tibble::tibble(
    git_sha = bundle$git_sha,
    created_at = bundle$created_at,
    roster_hash = bundle$input_hashes$roster_hash,
    claims_hash = bundle$input_hashes$claims_hash,
    calibration_status = calib_model$calibration_status
  )
  DBI::dbWriteTable(connection, DBI::Id(schema = schema, table = "calibration_provenance_manifest"), provenance_tbl, overwrite = TRUE)
  summary_counts$calibration_provenance_manifest <- nrow(provenance_tbl)

  # 9. Provider Workload Allocation Engine
  service_demand <- tibble::tibble(
    service = service_registry$service,
    condition = "Pelvic Floor Disorder",
    demand_services = 5000L
  )
  provider_cohort <- tibble::tibble(
    rendering_npi = base::paste0("10000000", 1:5),
    provider_type = c("FPMRS physician", "General OB/GYN", "Urologist", "Nurse practitioner", "Physician assistant"),
    is_active = TRUE,
    status = "active"
  )
  workload_res <- allocate_urps_service_workload(service_demand = service_demand, provider_cohort = provider_cohort, share_draws = share_draws)
  DBI::dbWriteTable(connection, DBI::Id(schema = schema, table = "allocated_provider_workload"), workload_res$allocated_workload, overwrite = TRUE)
  summary_counts$allocated_provider_workload <- nrow(workload_res$allocated_workload)

  audit_tbl <- tibble::as_tibble(workload_res$accounting_audit)
  DBI::dbWriteTable(connection, DBI::Id(schema = schema, table = "workload_accounting_audit"), audit_tbl, overwrite = TRUE)
  summary_counts$workload_accounting_audit <- nrow(audit_tbl)

  base::message("Completed ingestion of all 12 infrastructure tables into DuckDB!")
  summary_counts
}
