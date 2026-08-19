#!/usr/bin/env Rscript
# =============================================================================
# Build CHIA physician-attributed surgical volume (supply-side)
# =============================================================================
# Implements the plan at .claude/plans/cryptic-gathering-waffle.md (session of
# 2026-08-19): turns the raw CHIA HDD (inpatient discharge) tables already
# loaded by load_chia_mdb.py + finalize_db.py into chia_casemix.surgeon_year_volume
# -- physician-identified surgical case counts by year -- for calibrating
# supply-side FTE/productivity parameters. scripts/chia/test_chia.py already
# specifies the target shape and invariants (gates W1-W5, D5, D10); this
# script is what makes those gates runnable instead of erroring on missing
# views.
#
# EMPIRICAL FINDING THAT CHANGED THE PLAN: the legacy R scripts and the
# FIPA_PreLDS_LegHashPhysician_Master.csv crosswalk assume physician IDs on
# the discharge record are ENCRYPTED and need de-hashing via FIPA. That is
# true for the 2004-2014 era files (which we have not loaded -- no raw HDD
# discharge data for those years). For the 2015-2018 files actually loaded
# here, OperatingPhysician*/AttendingPhysicianNumber are ALREADY RAW BORIM
# LICENSE NUMBERS: checked empirically (91.6% of distinct
# AttendingPhysicianNumber values in FY2018 match a BORIM license directly).
# This matches test_chia.py's own D6 gate ("OOD SurgeonAssociatedProcedure1
# is a raw BORIM number, not an encrypted hash"). So Phase 2 below skips the
# FIPA hop entirely for the years we have.
#
# OPERATIVE CLASSIFICATION: FY2015 procedure codes are ICD-9-CM (3-4 digit
# numeric, no decimal stored); FY2016-2018 are pure ICD-10-PCS (7-char
# alphanumeric) -- confirmed empirically per file, not assumed. Operative is
# defined generally (ICD-9-CM category 01-86; ICD-10-PCS section 0 or 1 --
# Medical/Surgical and Obstetrics), NOT via a URPS-specific code list: this
# view feeds workforce-wide surgeon volume (test_chia.py W1 expects the
# top-volume physicians across ALL specialties to be surgical ones), not a
# procedure_family classification (separate, out-of-scope-for-this-pass task).

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

# ---- Phase 2: physician attribution (RAW BORIM numbers, 2015-2018) --------
cat("\n=== Phase 2: chia_casemix.v_hdd_discharge_physician ===\n")
# NOTE: originally written with UNPIVOT, which is more concise -- but DuckDB
# v1.5.2 has a real bug where an UNPIVOT-based view loses its column list on
# catalog reload (works within the creating session, fails with "UNPIVOT name
# count mismatch - got 1 names but 0 expressions" on a fresh connection,
# confirmed empirically). UNION ALL is more verbose but portable/persistent.
physician_cols <- c("OperatingPhysicianPrincipal", paste0("OperatingPhysicianSignificant", 1:14))
union_sql <- paste(
  sprintf("SELECT RecordType20ID, _data_year, TRY_CAST(%s AS BIGINT) AS borim_license FROM chia_casemix.v_hdd_discharge_all_years",
          physician_cols),
  collapse = "\nUNION ALL\n"
)
DBI::dbExecute(con, sprintf("
  CREATE OR REPLACE VIEW chia_casemix.v_hdd_discharge_physician AS
  SELECT DISTINCT RecordType20ID, _data_year, borim_license
  FROM (%s)
  WHERE borim_license IS NOT NULL
", union_sql))
n <- DBI::dbGetQuery(con, "SELECT count(*) n FROM chia_casemix.v_hdd_discharge_physician")$n
cat(sprintf("  [ok] %s (discharge, physician) pairs\n", format(n, big.mark = ",")))

# ---- Phase 3: procedure classification --------------------------------------
cat("\n=== Phase 3: chia_casemix.v_hdd_discharge_procedure ===\n")
DBI::dbExecute(con, "
  CREATE OR REPLACE VIEW chia_casemix.v_hdd_discharge_procedure AS
  SELECT
    RecordType20ID,
    _data_year,
    NULLIF(NULLIF(trim(ProcedureCode), '-'), '') AS principal_procedure,
    CASE
      WHEN ProcedureCode IS NULL OR trim(ProcedureCode) IN ('-', '') THEN NULL
      -- FY2015: ICD-9-CM, numeric, no decimal point stored; surgical = category 01-86.
      WHEN _data_year <= 2015 THEN
        CASE WHEN TRY_CAST(LEFT(ProcedureCode, 2) AS INTEGER) BETWEEN 1 AND 86
             THEN 'operative' ELSE 'non_operative' END
      -- FY2016+: ICD-10-PCS, 7-char; section 0 (Medical/Surgical) or 1 (Obstetrics) = operative.
      ELSE
        CASE WHEN LEFT(ProcedureCode, 1) IN ('0', '1')
             THEN 'operative' ELSE 'non_operative' END
    END AS procedure_class
  FROM chia_casemix.v_hdd_procedurecode_all_years
  WHERE AssociatedIndicator = 1
")
tab <- DBI::dbGetQuery(con, "SELECT _data_year, procedure_class, count(*) n FROM chia_casemix.v_hdd_discharge_procedure GROUP BY 1,2 ORDER BY 1,2")
print(tab)

# ---- Phase 3b: canonical discharge view (demographics + is_surgical) -------
cat("\n=== Phase 3b: chia_casemix.v_hdd_discharge_canonical ===\n")
DBI::dbExecute(con, "
  CREATE OR REPLACE VIEW chia_casemix.v_hdd_discharge_canonical AS
  SELECT
    c.RecordType20ID,
    c._data_year,
    TRY_CAST(c.AgeLDS AS INTEGER) AS age,
    c.SexLDS AS sex,
    c.Race1 AS race_primary,
    c.IdOrgSite AS org_site,
    c.IdOrgFiler AS org_filer,
    TRY_CAST(c.TotalChargesSpecial AS DOUBLE) AS charges_special,
    c.AdmissionType,
    (p.procedure_class = 'operative') AS is_surgical
  FROM chia_casemix.v_hdd_discharge_all_years c
  LEFT JOIN chia_casemix.v_hdd_discharge_procedure p USING (RecordType20ID, _data_year)
")
n2 <- DBI::dbGetQuery(con, "SELECT count(*) n FROM chia_casemix.v_hdd_discharge_canonical")$n
cat(sprintf("  [ok] %s discharge rows\n", format(n2, big.mark = ",")))

# ---- Phase 4: surgeon_year_volume ------------------------------------------
cat("\n=== Phase 4: chia_casemix.surgeon_year_volume ===\n")
DBI::dbExecute(con, "
  CREATE OR REPLACE VIEW chia_casemix.surgeon_year_volume AS
  SELECT
    b.NPI AS npi,
    c._data_year AS fy,
    MAX(b.specialty_1) AS borim_specialty,
    count(*) FILTER (WHERE p.procedure_class = 'operative' AND c.AdmissionType <> '4') AS operative_cases,
    count(*) FILTER (WHERE p.procedure_class = 'operative' AND c.AdmissionType = '4')  AS newborn_cases
  FROM chia_casemix.v_hdd_discharge_canonical c
  JOIN chia_casemix.v_hdd_discharge_procedure p USING (RecordType20ID, _data_year)
  JOIN chia_casemix.v_hdd_discharge_physician d USING (RecordType20ID, _data_year)
  JOIN chia_provider.borim_stdrel_npi_straight_from_cd b
    ON TRY_CAST(b.license AS BIGINT) = d.borim_license
  WHERE b.NPI IS NOT NULL AND trim(b.NPI) <> ''
  GROUP BY 1, 2
")
top <- DBI::dbGetQuery(con, "SELECT * FROM chia_casemix.surgeon_year_volume ORDER BY operative_cases DESC LIMIT 10")
print(top)

# ---- Phase 5: completeness view ---------------------------------------------
cat("\n=== Phase 5: chia_casemix.v_surgeon_year_completeness ===\n")
DBI::dbExecute(con, "
  CREATE OR REPLACE VIEW chia_casemix.v_surgeon_year_completeness AS
  WITH op AS (
    SELECT c._data_year AS fy, c.RecordType20ID,
           bool_or(d.borim_license IS NOT NULL) AS attributed
    FROM chia_casemix.v_hdd_discharge_canonical c
    LEFT JOIN chia_casemix.v_hdd_discharge_physician d USING (RecordType20ID, _data_year)
    WHERE c.is_surgical
    GROUP BY 1, 2
  )
  SELECT fy,
         round(100.0 * sum(CASE WHEN attributed THEN 1 ELSE 0 END) / count(*), 1) AS pct_attributed,
         NULL AS dark_sites   -- FY2016 OOD physician-reporting cliff detection deferred (OOD out of scope this pass)
  FROM op
  GROUP BY 1
  ORDER BY 1
")
print(DBI::dbGetQuery(con, "SELECT * FROM chia_casemix.v_surgeon_year_completeness"))

cat("\n=================================================================\n")
cat("DONE. Run scripts/chia/test_chia.py against this database next.\n")
cat("=================================================================\n")
