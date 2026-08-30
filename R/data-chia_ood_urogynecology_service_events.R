################################################################################
# R/data-chia_ood_urogynecology_service_events.R
# Assembles the OOD (Outpatient Observation Data) sibling of
# chia_casemix.urogynecology_service_events for the six URPS services HDD
# cannot see. See docs/superpowers/plans/2026-08-28-chia-ood-outpatient-urps-service-events.md
# for the full spec, scope decision, and verified facts this implements.
#
# TWO OUTPUTS, DELIBERATELY, per that plan's physician-attribution finding:
#   - build_chia_ood_urogynecology_service_events(): physician-attributed,
#     FY2015-2018 only (matches the existing HDD table's own validated
#     NPPES-taxonomy window -- not re-litigated here).
#   - build_chia_ood_urogynecology_service_volume(): physician-BLIND, full
#     2004-2018 range. The 11 extra years of volume are still useful for a
#     trend/validation view even with no name attached to each encounter.
#
# NEITHER OUTPUT IS BLENDED into calibrate_service_share_model()'s primary
# evidence for these six services -- register as "cross-check only" wherever
# consumed. "Outpatient Observation Data" is hospital-based observation-status
# billing, not general ambulatory/office visits; it is a partial,
# hospital-selected sample of these six services, not a representative
# estimate. See the plan's Scope Decision section for the full reasoning.
################################################################################

#' Classify a CHIA OOD `source_pay_code` definition into a `payer_group`
#'
#' OOD's `PrimarySourceOfPayment` is NOT the same code space as HDD's
#' `PrimaryPayerType` (verified 2026-08-28: HDD uses ~15 single-character
#' codes with a published lookup, `primary_payer_type.csv`; OOD uses ~150
#' specific-insurer numeric codes, `insurance_table.csv` /
#' `inst/extdata/chia_ood_source_of_payment_lookup.csv`, e.g. `121` =
#' "Medicare", `47` = "Neighborhood Health Plan"). `.chia_resolve_payer_group()`
#' (built for HDD's code space) does not apply here and was NOT extended to
#' cover this -- this is a genuinely separate classification problem with a
#' genuinely separate source table.
#'
#' Classified by keyword rule against the real CHIA-published definition
#' text (never by guessing at what an unfamiliar insurer name "probably" is):
#' `Medicare`/`Medigap`/`AARP` -> Medicare; `Medicaid`/`MassHealth`/`MCD`
#' (the abbreviation used in e.g. "Network Health (Cambridge Health Alliance
#' MCD Program)") -> Medicaid; named
#' commercial insurers and the "Other Commercial/HMO/POS/EPO/Non-Managed
#' Care" catch-all codes -> Commercial; everything else (Worker's
#' Compensation, Auto Insurance, Free Care, Foundation, Grant, Other
#' Government, CHAMPUS, and Massachusetts Commonwealth Care/"CommCare" --
#' a state-subsidized exchange-like program, not federal Medicaid and not
#' standard commercial insurance) -> `Other/Public`.
#'
#' **No `Self-pay` code exists in this lookup table** (unlike HDD's explicit
#' `1 = SP = Self-Pay`). `159` ("None (Valid only for secondary source of
#' payment)") appearing as a PRIMARY value would be a data-quality artifact
#' per CHIA's own note, not evidence of self-pay -- also resolves to
#' `Other/Public`, not guessed as `Self-pay`. This is a real, documented
#' asymmetry with the HDD-side resolver, not an oversight: OOD-derived
#' `payer_group` will structurally never produce a `Self-pay` row.
#'
#' @param definition Character vector of raw `source_of_payment_definitions`
#'   text (from the lookup table, not the numeric code).
#' @return Character vector of the same length, values in `Medicare`,
#'   `Medicaid`, `Commercial`, `Other/Public`. Never `NA`, never `Self-pay`.
#' @family chia physician attribution
#' @concept supply
#' @keywords internal
.chia_ood_classify_source_of_payment <- function(definition) {
  d <- definition
  dplyr::case_when(
    grepl("medicare|medigap|aarp", d, ignore.case = TRUE) ~ "Medicare",
    grepl("medicaid|masshealth|\\bmcd\\b", d, ignore.case = TRUE) ~ "Medicaid",
    grepl(
      paste(
        "worker.?s compensation", "auto insurance", "free care", "foundation",
        "^grant$|, grant|grant ", "other government", "champus",
        "commcare|commonwealth care", "^none ",
        sep = "|"
      ),
      d, ignore.case = TRUE
    ) ~ "Other/Public",
    TRUE ~ "Commercial"
  )
}

#' Load the CHIA OOD source-of-payment lookup and resolve it to `payer_group`
#'
#' @return A tibble: `source_pay_code` (character, matches
#'   `PrimarySourceOfPayment`'s raw string values), `definition`,
#'   `payer_group`.
#' @family chia physician attribution
#' @concept supply
#' @keywords internal
.chia_ood_source_of_payment_table <- function() {
  path <- system.file(
    "extdata", "chia_ood_source_of_payment_lookup.csv",
    package = "urpssim"
  )
  if (!nzchar(path)) {
    stop(
      "inst/extdata/chia_ood_source_of_payment_lookup.csv not found -- ",
      "package not installed/loaded correctly.", call. = FALSE
    )
  }
  raw <- readr::read_csv(path, show_col_types = FALSE)
  raw |>
    dplyr::transmute(
      source_pay_code = as.character(.data$source_pay_code),
      definition = .data$source_of_payment_definitions,
      payer_group = .chia_ood_classify_source_of_payment(.data$definition)
    )
}

#' Resolve CHIA OOD `PrimarySourceOfPayment` codes to a canonical `payer_group`
#'
#' Real bug caught building this (2026-08-28): a first version collapsed
#' `NA`/blank input into `"Other/Public"` via the same fallback used for a
#' genuinely-known-but-unmapped code. Checked against the real database and
#' 77% of the six-service rows have a `NULL` `PrimarySourceOfPayment` --
#' folding those into `"Other/Public"` silently misrepresented "we don't
#' know the payer for this encounter" as "this is a known Worker's-Comp/
#' Free-Care/CommCare-type payer," inflating that bucket to 84.6% of total
#' volume. Fixed: missing/blank input resolves to `"Unknown"`, kept
#' distinct from `"Other/Public"` (a real code that just isn't one of the
#' four named categories).
#'
#' @param primary_source_of_payment Character vector of raw
#'   `PrimarySourceOfPayment` codes (`NA`/blank included).
#' @return Character vector of the same length, values in `Medicare`,
#'   `Medicaid`, `Commercial`, `Other/Public`, `Unknown`. Never `NA`, never
#'   `Self-pay` (see [.chia_ood_classify_source_of_payment()] for why).
#' @family chia physician attribution
#' @concept supply
#' @keywords internal
.chia_ood_resolve_payer_group <- function(primary_source_of_payment) {
  lut <- .chia_ood_source_of_payment_table()
  code <- trimws(primary_source_of_payment)
  resolved <- lut$payer_group[match(code, lut$source_pay_code)]
  resolved[is.na(resolved) & (is.na(code) | code == "")] <- "Unknown"
  resolved[is.na(resolved)] <- "Other/Public"
  resolved
}

# CPT service -> urogynecology_service_share_registry() service. Identical
# names by construction (config/chia_urps_outpatient_cpt_codes.yml's keys ARE
# the service names), kept as an explicit constant so this file documents
# the six-service scope the same way the HDD sibling file documents its two.
.CHIA_OOD_SIX_SERVICES <- c(
  "pessary_care", "urodynamics", "cystoscopy",
  "botox_bladder", "ptns", "bladder_instillation"
)

#' Build `chia_casemix.ood_urogynecology_service_events` (physician-attributed)
#'
#' FY2015-2018 only, matching the existing HDD-side table's own validated
#' NPPES-taxonomy window. Requires
#' [build_chia_ood_observation_normalized_view()] and
#' [build_chia_ood_cpt_service_view()] to have been run first on the same
#' connection. `setting` is fixed `"outpatient_observation"` -- see this
#' file's header for why this is not simply `"outpatient"`.
#'
#' @param con Open, writable DuckDB connection.
#' @param years Integer vector of `_data_year` values to include. Default
#'   `2015:2018` (the physician-attribution-validated window).
#' @param min_cell_size Passed to `.chia_suppress_small_cells()`
#'   (`R/data-chia_urogynecology_service_events.R`; reused as-is, not
#'   duplicated -- it is already generic over any events tibble in the
#'   `encounter_id, year, rendering_npi, service, payer_group, setting`
#'   shape).
#' @return `con`, invisibly.
#' @family chia physician attribution
#' @concept supply
#' @export
build_chia_ood_urogynecology_service_events <- function(
    con, years = 2015:2018, min_cell_size = 11L) {
  services_sql <- paste(sprintf("'%s'", .CHIA_OOD_SIX_SERVICES), collapse = ",")
  years_sql <- paste(years, collapse = ",")

  raw <- DBI::dbGetQuery(con, sprintf("
    SELECT
      RecordType01ID || '-' || _data_year AS encounter_id,
      _data_year AS year,
      b.NPI AS rendering_npi,
      o.service,
      o.PrimarySourceOfPayment AS primary_source_of_payment
    FROM chia_casemix.v_ood_observation_service o
    LEFT JOIN chia_provider.borim_stdrel_npi_straight_from_cd b
      ON TRY_CAST(b.license AS BIGINT) = TRY_CAST(o.PhysicianNumber AS BIGINT)
    WHERE o.service IN (%s) AND o._data_year IN (%s)
  ", services_sql, years_sql)) |>
    tibble::as_tibble()

  events <- raw |>
    dplyr::mutate(
      payer_group = .chia_ood_resolve_payer_group(.data$primary_source_of_payment),
      setting = "outpatient_observation",
      rendering_npi = dplyr::na_if(trimws(.data$rendering_npi), "")
    ) |>
    dplyr::select(
      "encounter_id", "year", "rendering_npi", "service",
      "payer_group", "setting"
    )

  out <- .chia_suppress_small_cells(events, min_cell_size = min_cell_size)

  DBI::dbWriteTable(
    con,
    DBI::Id(schema = "chia_casemix", table = "ood_urogynecology_service_events"),
    out,
    overwrite = TRUE
  )
  invisible(con)
}

#' Build `chia_casemix.ood_urogynecology_service_volume_2004_2018` (physician-blind)
#'
#' Full 2004-2018 range (no physician-attribution join, so not limited to the
#' FY2015-2018 attribution window). No `rendering_npi` column and no
#' small-cell floor -- there is no physician identity in this table to
#' protect, only aggregate counts. Requires
#' [build_chia_ood_observation_normalized_view()] and
#' [build_chia_ood_cpt_service_view()] to have been run first on the same
#' connection.
#'
#' @param con Open, writable DuckDB connection.
#' @param years Integer vector of `_data_year` values to include. Default
#'   `2004:2018` (the full OOD range).
#' @return `con`, invisibly.
#' @family chia physician attribution
#' @concept supply
#' @export
build_chia_ood_urogynecology_service_volume <- function(con, years = 2004:2018) {
  services_sql <- paste(sprintf("'%s'", .CHIA_OOD_SIX_SERVICES), collapse = ",")
  years_sql <- paste(years, collapse = ",")

  raw <- DBI::dbGetQuery(con, sprintf("
    SELECT _data_year AS year, service, PrimarySourceOfPayment AS primary_source_of_payment
    FROM chia_casemix.v_ood_observation_service
    WHERE service IN (%s) AND _data_year IN (%s)
  ", services_sql, years_sql)) |>
    tibble::as_tibble()

  out <- raw |>
    dplyr::mutate(
      payer_group = .chia_ood_resolve_payer_group(.data$primary_source_of_payment),
      setting = "outpatient_observation"
    ) |>
    dplyr::count(
      .data$year, .data$service, .data$payer_group, .data$setting,
      name = "service_events"
    )

  DBI::dbWriteTable(
    con,
    DBI::Id(
      schema = "chia_casemix",
      table = "ood_urogynecology_service_volume_2004_2018"
    ),
    out,
    overwrite = TRUE
  )
  invisible(con)
}
