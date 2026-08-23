################################################################################
# R/data-chia_procedure_family.R
# Applies config/chia_urps_inpatient_codes.yml's clinically-reviewed ICD-9-CM /
# ICD-10-PCS procedure-family classification to chia_casemix.v_hdd_discharge_*,
# adding a `procedure_family` column to chia_casemix.v_hdd_discharge_canonical.
#
# The SQL-generation pattern (per-family CASE WHEN, honoring the FY2015/2016
# ICD seam and diagnosis-qualifier EXISTS subqueries) is a direct, tested port
# of scripts/chia/run_chia_revenue_setting.R's fam_sql()/dx_sql() -- that
# script is the empirically-verified reference implementation (results
# reported in docs/CHIA_TECHNICAL_APPENDIX.md); this file makes the same logic
# a reusable, testable package function instead of a one-off analysis script,
# and persists the result as a view column rather than a session-scoped TEMP
# VIEW so other callers (build_chia_inpatient_urps_series(),
# build_chia_urogynecology_service_events()) can rely on it.
#
# Requires build_chia_hdd_diagnosis_long_view() (R/data-chia_physician_attribution.R)
# to have been run first.
################################################################################

# One family's ICD match condition, honoring the ICD-9-CM (<=FY2015) /
# ICD-10-PCS (>=FY2016) seam. `principal_procedure IN ()` is a DuckDB PARSER
# ERROR, not a vacuous FALSE (confirmed empirically against the real
# database) -- two families (transvaginal_mesh_pop, revision_removal) are
# ICD-10-only and have an empty icd9cm$exact, so that branch is emitted as a
# literal `FALSE` instead of an empty IN-list. This diverges from
# run_chia_revenue_setting.R::fam_sql(), which does not guard this case and
# would error identically if actually run against a family lacking icd9cm
# codes.
.chia_procedure_family_icd_sql <- function(family) {
  icd9 <- family$icd9cm$exact
  i9 <- if (length(icd9) == 0L) {
    "FALSE"
  } else {
    sprintf("principal_procedure IN (%s)", paste(sprintf("'%s'", icd9), collapse = ","))
  }
  icd10_prefix <- family$icd10pcs$prefix
  i10 <- if (length(icd10_prefix) == 0L) {
    "FALSE"
  } else {
    paste(sprintf("principal_procedure LIKE '%s%%'", icd10_prefix), collapse = " OR ")
  }
  sprintf(
    "((_data_year <= 2015 AND %s) OR (_data_year >= 2016 AND (%s)))",
    i9, i10
  )
}

# One family's diagnosis-qualifier condition, if any. `requires_diagnosis` is
# read at the family's TOP LEVEL only, matching dx_sql()'s existing behavior:
# sui_sling's qualifier is nested under icd10pcs: in the YAML (a documented,
# open item -- see clinical_review_criteria.sui_diagnosis: status: OPEN,
# "sui_sling currently carries NO diagnosis qualifier"), so it is deliberately
# NOT picked up here, matching the reference script and the YAML's own
# documented intent rather than "fixing" an open clinical decision unilaterally.
.chia_procedure_family_dx_sql <- function(family, diagnosis_qualifiers) {
  req <- family$requires_diagnosis
  if (is.null(req)) {
    return("TRUE")
  }
  qualifier <- diagnosis_qualifiers[[req]]
  patterns <- c(qualifier$icd9cm, qualifier$icd10cm)
  cond <- if (identical(qualifier$match, "prefix")) {
    paste(sprintf("dx.code LIKE '%s%%'", patterns), collapse = " OR ")
  } else {
    paste0("dx.code IN (", paste(sprintf("'%s'", patterns), collapse = ","), ")")
  }
  sprintf(
    "EXISTS (SELECT 1 FROM chia_casemix.hdd_diagnosis_long dx
             WHERE dx.RecordType20ID = c.RecordType20ID AND dx._data_year = c._data_year
               AND (%s))",
    cond
  )
}

#' Build the procedure-family classification view
#'
#' Classifies `chia_casemix.v_hdd_discharge_all_years.ProcedureCode` (aliased
#' `principal_procedure` downstream) into the clinically-reviewed procedure
#' families in `config/chia_urps_inpatient_codes.yml` (`pop_hysterectomy`,
#' `apical_abdominal_mesh`, `colpocleisis`, `transvaginal_mesh_pop`,
#' `vaginal_native_tissue_pop_repair`, `sui_sling`, `revision_removal`,
#' `genitourinary_fistula`), honoring the FY2015/FY2016 ICD-9-CM/ICD-10-PCS
#' seam and each family's diagnosis qualifier. Unclassified discharges get
#' `procedure_family = NULL`.
#'
#' Requires [build_chia_hdd_diagnosis_long_view()] to have been run first.
#'
#' @param con Open, writable DuckDB connection.
#' @param config Parsed `config/chia_urps_inpatient_codes.yml` (a list with a
#'   `families` element and a `diagnosis_qualifiers` element). Defaults to
#'   reading that file from the current working directory (the package root).
#' @return `con`, invisibly.
#' @family chia physician attribution
#' @concept supply
#' @export
build_chia_hdd_procedure_family_view <- function(
    con,
    config = yaml::read_yaml("config/chia_urps_inpatient_codes.yml")) {
  families <- config$families
  diagnosis_qualifiers <- config$diagnosis_qualifiers
  family_names <- names(families)

  # A CASE expression requires at least one WHEN; an empty config (e.g. a test
  # fixture that doesn't exercise family content) falls back to a constant
  # NULL rather than emitting invalid SQL.
  case_expr <- if (length(family_names) == 0L) {
    "NULL"
  } else {
    cases <- paste(
      vapply(family_names, function(fn) {
        family <- families[[fn]]
        sprintf(
          "WHEN %s AND %s THEN '%s'",
          .chia_procedure_family_icd_sql(family),
          .chia_procedure_family_dx_sql(family, diagnosis_qualifiers),
          fn
        )
      }, character(1)),
      collapse = "\n      "
    )
    sprintf("CASE\n      %s\n      ELSE NULL\n      END", cases)
  }

  DBI::dbExecute(con, sprintf("
    CREATE OR REPLACE VIEW chia_casemix.v_hdd_discharge_procedure_family AS
    SELECT
      c.RecordType20ID,
      c._data_year,
      %s AS procedure_family
    FROM chia_casemix.v_hdd_discharge_procedure_raw c
  ", case_expr))
  invisible(con)
}
