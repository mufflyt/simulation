# Build the national URPS empirical evidence DuckDB ------------------------

if (requireNamespace("devtools", quietly = TRUE)) {
  devtools::load_all(".")
} else {
  base::stop(
    "Run this script from the repository or install `urpssim` first.",
    call. = FALSE
  )
}

timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
evidence_dir <- base::file.path("artifacts", "evidence")
if (!base::dir.exists(evidence_dir)) {
  base::dir.create(evidence_dir, recursive = TRUE)
}
duckdb_path <- base::file.path(
  evidence_dir,
  base::paste0("urps_national_evidence_", timestamp, ".duckdb")
)

base::message("Starting timestamped national evidence build.")
base::message("Repository root: ", base::normalizePath("."))
base::message("Requested DuckDB path: ", duckdb_path)

evidence_bundle <- build_urps_national_evidence_lake(
  duckdb_path = duckdb_path,
  project_root = ".",
  overwrite = TRUE
)

base::message("Parameter estimates:")
base::print(evidence_bundle$parameter_estimates)
base::message("Exact saved file path: ", evidence_bundle$duckdb_path)
