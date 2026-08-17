#!/usr/bin/env Rscript
# Test whether the inpatient hysterectomy decline is setting migration or
# reclassification inside HIDD, using UB-04 revenue-center evidence.
# hdd_service covers FY2015-FY2018 only.
suppressPackageStartupMessages({
  library(DBI); library(duckdb); library(dplyr); library(dbplyr)
  library(readr); library(tibble); library(yaml)
})
source("R/data-chia_revenue_setting.R")

DB <- "/Volumes/MufflySamsung/DuckDB/chia_cadr.duckdb"
con <- dbConnect(duckdb::duckdb(), DB, read_only = TRUE)
cfg <- yaml::read_yaml("config/chia_urps_inpatient_codes.yml")

# Assign procedure families in SQL from the YAML, honouring diagnosis qualifiers.
fam_sql <- function(nm) {
  f <- cfg$families[[nm]]
  i9  <- paste(sprintf("'%s'", f$icd9cm$exact), collapse = ",")
  i10 <- paste(sprintf("principal_procedure LIKE '%s%%'", f$icd10pcs$prefix), collapse = " OR ")
  sprintf("(( _data_year <= 2015 AND principal_procedure IN (%s)) OR (_data_year >= 2016 AND (%s)))", i9, i10)
}
dx_sql <- function(nm) {
  f <- cfg$families[[nm]]
  if (is.null(f$requires_diagnosis)) return("TRUE")
  s <- cfg$diagnosis_qualifiers[[f$requires_diagnosis]]
  pats <- c(s$icd9cm, s$icd10cm)
  cond <- if (identical(s$match, "prefix"))
    paste(sprintf("dx.code LIKE '%s%%'", pats), collapse = " OR ")
  else paste0("dx.code IN (", paste(sprintf("'%s'", pats), collapse = ","), ")")
  sprintf("EXISTS (SELECT 1 FROM chia_casemix.hdd_diagnosis_long dx
           WHERE dx.RecordType20ID = c.RecordType20ID AND dx._data_year = c._data_year
             AND (%s))", cond)
}
fam_names <- names(cfg$families)
cases <- paste(vapply(fam_names, function(fn)
  sprintf("WHEN %s AND %s THEN '%s'", fam_sql(fn), dx_sql(fn), fn),
  character(1)), collapse = "\n    ")

dbExecute(con, sprintf("
CREATE OR REPLACE TEMP VIEW chia_urps_cohort AS
SELECT c.RecordType20ID, c._data_year,
       TRY_CAST(a.LengthOfStay AS INTEGER) AS los_days,
       CASE
    %s
    ELSE NULL END AS procedure_family
FROM chia_casemix.v_cohort_female_adult c
JOIN chia_casemix.v_hdd_discharge_all_years a USING (RecordType20ID, _data_year)
WHERE c._data_year >= 2015", cases))

cohort  <- tbl(con, "chia_urps_cohort") |> filter(!is.na(procedure_family))
service <- tbl(con, sql("SELECT * FROM chia_casemix.v_hdd_service_all_years"))

res <- build_chia_ub04_setting_evidence(
  cohort_tbl           = cohort,
  service_tbl          = service,
  encounter_id_col     = "RecordType20ID",
  year_col             = "_data_year",
  procedure_family_col = "procedure_family",
  revenue_code_col     = "RevenueCode",
  los_col              = "los_days",
  save_dir             = "artifacts/chia_revenue_setting"
)

cat("\n=== THE DECISIVE TABLE: pop_hysterectomy ===\n")
res$annual_trends |>
  filter(procedure_family == "pop_hysterectomy") |>
  select(year, n_encounters, pct_or_036x, pct_anesthesia_037x, pct_recovery_071x,
         pct_inpatient_bed, pct_ambulatory_049x, pct_observation_0762,
         los_median, los_p25, los_p75) |>
  mutate(across(where(is.numeric), ~round(.x, 1))) |> as.data.frame() |> print()

cat("\n=== all families ===\n")
res$annual_trends |>
  select(year, procedure_family, n_encounters, pct_or_036x,
         pct_strong_inpatient_or, pct_ambulatory_049x, los_median) |>
  mutate(across(where(is.numeric), ~round(.x, 1))) |> as.data.frame() |> print()
dbDisconnect(con, shutdown = TRUE)
