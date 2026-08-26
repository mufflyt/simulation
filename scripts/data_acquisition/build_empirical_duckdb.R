source_manifest <- urpssim::empirical_source_manifest()

database_path <- urpssim::build_empirical_simulation_database(
  manifest = source_manifest,
  database_path = "data-raw/empirical/urps_empirical.duckdb",
  raw_directory = "data-raw/empirical",
  download_missing = TRUE,
  overwrite = FALSE
)

base::message("Empirical database ready: ", database_path)
