# Build the empirical five-source DuckDB ---------------------------------

manifest_path <- base::Sys.getenv(
  "URPS_FIVE_SOURCE_MANIFEST",
  unset = "config/five_source_manifest.csv"
)
duckdb_path <- base::Sys.getenv(
  "URPS_FIVE_SOURCE_DUCKDB",
  unset = "artifacts/data/urps_five_source.duckdb"
)
base::message("Manifest input: ", manifest_path)
base::message("DuckDB output: ", duckdb_path)

if (!base::file.exists(manifest_path)) {
  base::stop("Manifest does not exist: ", manifest_path, call. = FALSE)
}

pkgload::load_all(export_all = FALSE, helpers = FALSE, quiet = TRUE)
source_manifest <- readr::read_csv(
  manifest_path,
  show_col_types = FALSE,
  progress = FALSE
)
ingestion_summary <- build_five_source_duckdb(
  source_manifest = source_manifest,
  duckdb_path = duckdb_path,
  overwrite = base::identical(
    base::Sys.getenv("URPS_OVERWRITE_DUCKDB", unset = "false"),
    "true"
  )
)
base::message("Ingestion summary:")
base::print(ingestion_summary, n = base::nrow(ingestion_summary))
base::message("Saved DuckDB: ", base::normalizePath(duckdb_path))
