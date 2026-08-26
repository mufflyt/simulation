#!/usr/bin/env Rscript
# =============================================================================
# Build CHIA physician-attributed surgical volume (supply-side)
# =============================================================================
# Thin driver: loads the provider crosswalk CSVs, then calls the exported
# view-building functions in R/data-chia_physician_attribution.R (the
# classification logic itself is there, not here, so it's unit-testable
# against a small synthetic fixture -- see
# tests/testthat/test-chia-physician-attribution.R -- without needing the
# real 10GB external CHIA database). See that file's header comments for the
# empirical findings behind the classification choices (raw-vs-encrypted
# physician IDs, the ICD-9/ICD-10-PCS operative definition, the specialty-
# share refinement, the newborn-exclusion-from-signal fix, and the DuckDB
# UNPIVOT persistence bug worked around with UNION ALL).

suppressPackageStartupMessages({
  library(DBI)
  library(readr)
})
pkgload::load_all(".", quiet = TRUE)

pull_dir <- "/Volumes/MufflySamsung/chia_dropbox_pull"

db_path <- .chia_duckdb_default()
if (is.na(db_path) || !file.exists(db_path)) {
  stop("build_physician_attribution.R: CHIA DuckDB not found (resolved to '",
       db_path, "'). Set URPS_CHIA_DUCKDB or mount the drive.", call. = FALSE)
}

con <- DBI::dbConnect(duckdb::duckdb(), db_path, read_only = FALSE)
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

cat("=================================================================\n")
cat("BUILDING CHIA PHYSICIAN-ATTRIBUTED SURGICAL VOLUME\n")
cat("=================================================================\n\n")

# ---- Phase 1: provider crosswalk load into chia_provider -------------------
cat("=== Phase 1: provider crosswalk CSVs -> chia_provider ===\n")
DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS chia_provider")

crosswalk_files <- c(
  borim_stdrel_npi_straight_from_cd   = file.path(pull_dir, "2004 - 2014 data", "BORIM_STDREL_NPI_straight_from_CD.csv"),
  fipa_prelds_leghashphysician_master = file.path(pull_dir, "2004 - 2014 data", "FIPA_PreLDS_LegHashPhysician_Master.csv"),
  goba_unique                         = file.path(pull_dir, "2004 - 2014 data", "GOBA_unique.csv"),
  borim_flagged                       = file.path(pull_dir, "BORIM_Flagged.csv"),
  borim_flagged_copy                  = file.path(pull_dir, "BORIM_Flagged - Copy.csv"),
  borim_specialties                   = file.path(pull_dir, "Borim_Specialties.csv"),
  nomatchborim                        = file.path(pull_dir, "nomatchborim.csv")
)

for (tbl in names(crosswalk_files)) {
  path <- crosswalk_files[[tbl]]
  if (!file.exists(path)) {
    cat(sprintf("  [skip] %s -- not found: %s\n", tbl, path))
    next
  }
  # Everything read as character, matching load_chia_mdb.py's "Text stays
  # VARCHAR" discipline: these files carry zip codes, license numbers with
  # leading structure, and encrypted IDs where numeric coercion would be
  # actively harmful, not just imprecise.
  #
  # ENCODING: per scripts/chia/repair_encoding.py's own finding, 12 of these
  # source CSVs are Windows-1252, not UTF-8 (accented physician names, curly
  # quotes). Reading as cp1252 is safe for pure-ASCII files too -- ASCII
  # bytes 0-127 decode identically -- so this is applied universally rather
  # than per-file-detected, matching that script's documented fix (transcode
  # cp1252 -> UTF-8) applied at read time instead of as a post-hoc repair.
  df <- readr::read_csv(path, col_types = readr::cols(.default = "c"),
                        locale = readr::locale(encoding = "windows-1252"),
                        show_col_types = FALSE, progress = FALSE)
  names(df) <- make.unique(names(df))
  DBI::dbWriteTable(con, DBI::Id(schema = "chia_provider", table = tbl), df,
                    overwrite = TRUE)
  cat(sprintf("  [ok] chia_provider.%s: %s rows, %d cols\n",
              tbl, format(nrow(df), big.mark = ","), ncol(df)))
}

# ---- Phase 2-5: view-building logic (R/data-chia_physician_attribution.R) --
cat("\n=== Phase 2: chia_casemix.v_hdd_discharge_physician ===\n")
build_chia_hdd_discharge_physician_view(con)
n <- DBI::dbGetQuery(con, "SELECT count(*) n FROM chia_casemix.v_hdd_discharge_physician")$n
cat(sprintf("  [ok] %s (discharge, physician) pairs\n", format(n, big.mark = ",")))

cat("\n=== Phase 3: chia_casemix.v_hdd_discharge_procedure (raw + specialty-share refined) ===\n")
build_chia_hdd_discharge_procedure_views(con)
tab <- DBI::dbGetQuery(con, "SELECT _data_year, procedure_class, count(*) n FROM chia_casemix.v_hdd_discharge_procedure GROUP BY 1,2 ORDER BY 1,2")
print(tab)
downgraded <- DBI::dbGetQuery(con, "SELECT code, n, round(pct_surgical,1) pct_surgical FROM chia_casemix.v_procedure_specialty_share WHERE n>=20 AND pct_surgical<50.0 ORDER BY n DESC LIMIT 15")
cat("  codes downgraded to non_operative by the specialty-share refinement:\n")
print(downgraded)

cat("\n=== Phase 3a: chia_casemix.hdd_diagnosis_long ===\n")
build_chia_hdd_diagnosis_long_view(con)
n_dx <- DBI::dbGetQuery(con, "SELECT count(*) n FROM chia_casemix.hdd_diagnosis_long")$n
cat(sprintf("  [ok] %s diagnosis rows\n", format(n_dx, big.mark = ",")))

cat("\n=== Phase 3a2: chia_casemix.v_hdd_discharge_procedure_family ===\n")
build_chia_hdd_procedure_family_view(con)
fam_counts <- DBI::dbGetQuery(con, "SELECT procedure_family, count(*) n FROM chia_casemix.v_hdd_discharge_procedure_family WHERE procedure_family IS NOT NULL GROUP BY 1 ORDER BY 2 DESC")
print(fam_counts)

cat("\n=== Phase 3b: chia_casemix.v_hdd_discharge_canonical ===\n")
build_chia_hdd_discharge_canonical_view(con)
n2 <- DBI::dbGetQuery(con, "SELECT count(*) n FROM chia_casemix.v_hdd_discharge_canonical")$n
cat(sprintf("  [ok] %s discharge rows\n", format(n2, big.mark = ",")))

cat("\n=== Phase 4-5: chia_casemix.surgeon_year_volume + v_surgeon_year_completeness ===\n")
build_chia_surgeon_year_volume_views(con)
top <- DBI::dbGetQuery(con, "SELECT * FROM chia_casemix.surgeon_year_volume ORDER BY operative_cases DESC LIMIT 10")
print(top)
print(DBI::dbGetQuery(con, "SELECT * FROM chia_casemix.v_surgeon_year_completeness"))

cat("\n=================================================================\n")
cat("DONE. Run scripts/chia/test_chia.py against this database next.\n")
cat("=================================================================\n")
