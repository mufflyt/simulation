################################################################################
# R/data-chia_ood_observation_normalization.R
# Normalizes chia_casemix.ood_observation_YYYY (2004-2018, "Outpatient
# Observation Data" -- hospital-based observation-status billing, NOT general
# ambulatory/office visits, see docs/superpowers/plans/2026-08-28-chia-ood-outpatient-urps-service-events.md
# for the scope decision this implements) and classifies the six URPS
# services CHIA HDD (inpatient discharge data) cannot see.
#
# WHY THIS VIEW EXISTS, NOT JUST chia_casemix.v_ood_observation_all_years:
# that existing view is a bare `UNION ALL BY NAME` across ood_observation_2004
# .. ood_observation_2018. CPT columns are named CPT1-CPT5 in the pre-2015
# tables and CPTCode1-CPTCode5 from 2015 onward (verified directly, 2026-08-28)
# -- UNION ALL BY NAME does not merge those into one column, it creates BOTH
# and leaves each NULL wherever the source table lacks it. Querying
# `.CPTCode1` against v_ood_observation_all_years silently returns NULL for
# every 2004-2014 row; querying `.CPT1` silently returns NULL for every
# 2015-2018 row. This is the exact "technically-successful query that covers
# less than it appears to" failure shape tests/export-registry.csv's own
# header warns about for a different reason -- caught here by testing against
# a synthetic fixture shaped like BOTH column-name eras (see
# tests/testthat/test-data-chia-ood-urogynecology-service-events.R).
################################################################################

#' Build the column-name-normalized OOD observation view
#'
#' `COALESCE(CPTCode1, CPT1) AS cpt_1` (and 2-5), plus a `_cpt_column_era` flag
#' (`"CPTCode1-5"` / `"CPT1-5"`) so downstream code and tests can assert which
#' era supplied a given row rather than assuming.
#'
#' @param con Open, writable DuckDB connection.
#' @return `con`, invisibly.
#' @family chia physician attribution
#' @concept supply
#' @export
build_chia_ood_observation_normalized_view <- function(con) {
  DBI::dbExecute(con, "
    CREATE OR REPLACE VIEW chia_casemix.v_ood_observation_cpt_normalized AS
    SELECT
      *,
      COALESCE(CPTCode1, CPT1) AS cpt_1,
      COALESCE(CPTCode2, CPT2) AS cpt_2,
      COALESCE(CPTCode3, CPT3) AS cpt_3,
      COALESCE(CPTCode4, CPT4) AS cpt_4,
      COALESCE(CPTCode5, CPT5) AS cpt_5,
      CASE WHEN CPTCode1 IS NOT NULL OR CPTCode2 IS NOT NULL
             OR CPTCode3 IS NOT NULL OR CPTCode4 IS NOT NULL
             OR CPTCode5 IS NOT NULL
           THEN 'CPTCode1-5' ELSE 'CPT1-5' END AS _cpt_column_era
    FROM chia_casemix.v_ood_observation_all_years
  ")
  invisible(con)
}

#' Read the outpatient CPT -> URPS service crosswalk
#'
#' @param config Parsed YAML from `config/chia_urps_outpatient_cpt_codes.yml`
#'   (a named list, each element a list with a `cpt` character vector).
#' @return A tibble with columns `cpt_code`, `service`. One row per CPT code
#'   (a code that maps to more than one service is not supported and will
#'   error -- the source config has none).
#' @family chia physician attribution
#' @concept supply
#' @keywords internal
.chia_ood_cpt_service_map <- function(config) {
  rows <- lapply(names(config), function(service) {
    tibble::tibble(cpt_code = config[[service]]$cpt, service = service)
  })
  out <- dplyr::bind_rows(rows)
  dup <- out$cpt_code[duplicated(out$cpt_code)]
  if (length(dup) > 0) {
    stop(
      "CPT code(s) mapped to more than one service in ",
      "config/chia_urps_outpatient_cpt_codes.yml: ",
      paste(unique(dup), collapse = ", "), call. = FALSE
    )
  }
  out
}

#' Build the CPT-classified OOD observation view
#'
#' A row is classified into `service` if ANY of its five CPT slots (normalized
#' across the two column-name eras by
#' [build_chia_ood_observation_normalized_view()]) or its `PrincipalProcedureCode`
#' matches a code in `config/chia_urps_outpatient_cpt_codes.yml`. Requires
#' [build_chia_ood_observation_normalized_view()] to have been run first on
#' the same connection.
#'
#' @param con Open, writable DuckDB connection.
#' @param config Parsed YAML, default
#'   `yaml::read_yaml("config/chia_urps_outpatient_cpt_codes.yml")`.
#' @return `con`, invisibly.
#' @family chia physician attribution
#' @concept supply
#' @export
build_chia_ood_cpt_service_view <- function(
    con,
    config = yaml::read_yaml(
      here::here("config", "chia_urps_outpatient_cpt_codes.yml")
    )) {
  cpt_map <- .chia_ood_cpt_service_map(config)

  case_when_sql <- paste(
    sprintf(
      "WHEN cpt_1='%1$s' OR cpt_2='%1$s' OR cpt_3='%1$s' OR cpt_4='%1$s' OR cpt_5='%1$s' OR PrincipalProcedureCode='%1$s' THEN '%2$s'",
      cpt_map$cpt_code, cpt_map$service
    ),
    collapse = "\n      "
  )

  DBI::dbExecute(con, sprintf("
    CREATE OR REPLACE VIEW chia_casemix.v_ood_observation_service AS
    SELECT *,
      CASE
      %s
      ELSE NULL
      END AS service
    FROM chia_casemix.v_ood_observation_cpt_normalized
  ", case_when_sql))
  invisible(con)
}
