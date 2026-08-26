################################################################################
# R/data-chia_urogynecology_service_events.R
# Assembles chia_casemix.urogynecology_service_events -- the table
# read_chia_service_share_events() (R/data-chia_urogynecology_service_shares.R)
# requires and, until now, nothing in this repo built.
#
# SCOPE, DECIDED DELIBERATELY, NOT A GAP TO CLOSE LATER: CHIA HDD is inpatient
# hospital discharge data. Of the 8 `service` values in
# urogynecology_service_share_registry(), only sling_procedure and
# prolapse_procedure are plausibly observable as inpatient discharges.
# pessary_care, urodynamics, cystoscopy, botox_bladder, ptns and
# bladder_instillation are essentially always office/ASC/outpatient-hospital
# procedures -- docs/CHIA_TECHNICAL_APPENDIX.md Section 6 already states this
# class of utilization is "Invisible" to CHIA HDD, in the repo's own words.
# There is no honest way to manufacture inpatient discharge records for
# procedures that structurally don't generate them, so this builder never
# emits rows for those six services. calibrate_service_share_model() already
# tolerates partial per-service source coverage by design (mirroring the
# CMS-side M_suppressed_missing_services accounting): CHIA's role is to
# corroborate CMS on the two services it can see, not replace it everywhere.
#
# Requires build_chia_physician_attribution(con) (R/data-chia_physician_attribution.R)
# to have been run first (procedure_family, v_hdd_discharge_physician).
################################################################################

# procedure_family -> service. Deliberately excludes revision_removal (the
# YAML itself marks incident_sling_eligible: false -- a sling removal is not
# another sling placement) and genitourinary_fistula (clinically distinct
# from POP, no `service` correspondent -- forcing it into prolapse_procedure
# would misrepresent the estimand, not just mislabel it).
.CHIA_PROCEDURE_FAMILY_TO_SERVICE <- c(
  sui_sling                        = "sling_procedure",
  pop_hysterectomy                 = "prolapse_procedure",
  apical_abdominal_mesh            = "prolapse_procedure",
  colpocleisis                     = "prolapse_procedure",
  transvaginal_mesh_pop            = "prolapse_procedure",
  vaginal_native_tissue_pop_repair = "prolapse_procedure"
)

# PrimaryPayerType -> payer_group. Reconciles the three existing, mutually
# incomplete inline mappings (scripts/chia/build_payer_specific_access.R,
# build_within_zip_payer_gap.R, build_chia_urogyn_travel_kernel.R's payer_lab)
# plus docs/CHIA_TECHNICAL_APPENDIX.md's documented reform-era codes (Q, H, 9),
# against every code actually observed in FY2015-2018 (verified directly
# against the live database). Unmapped/rare codes (0, 2, 5, N, T, U, Z, blank,
# "-", and a single stray lowercase "c" data-entry artifact) resolve to
# "Other/Public" -- a documented catch-all, never a silent NA/drop.
.CHIA_PAYER_TYPE_TO_GROUP <- c(
  "1" = "Self-pay",
  "3" = "Medicare", "F" = "Medicare",
  "4" = "Medicaid", "B" = "Medicaid",
  "6" = "Commercial", "7" = "Commercial", "8" = "Commercial",
  "C" = "Commercial", "D" = "Commercial", "E" = "Commercial",
  "J" = "Commercial", "K" = "Commercial",
  "9" = "Other/Public", "H" = "Other/Public", "Q" = "Other/Public"
)

#' Resolve CHIA `PrimaryPayerType` codes to a canonical `payer_group`
#'
#' @param primary_payer_type Character vector of raw `PrimaryPayerType` codes.
#' @return Character vector of the same length, values in `Self-pay`,
#'   `Medicare`, `Medicaid`, `Commercial`, `Other/Public`. Never `NA`.
#' @family chia physician attribution
#' @concept supply
#' @keywords internal
.chia_resolve_payer_group <- function(primary_payer_type) {
  resolved <- unname(.CHIA_PAYER_TYPE_TO_GROUP[primary_payer_type])
  resolved[is.na(resolved)] <- "Other/Public"
  resolved
}

#' Apply a minimum-cell-size floor to CHIA service events
#'
#' `service_events` is an aggregate count, not row-level PHI, but a
#' single-provider cell in a thin `(service, year, payer_group, setting)`
#' stratum can still reveal an individual physician's practice pattern. Cells
#' below `min_cell_size` have `rendering_npi` nulled (not dropped) and are
#' re-aggregated, so the volume is preserved for the accounting the
#' calibration model needs but the physician identity is not.
#'
#' @param events A tibble with columns `encounter_id`, `year`, `rendering_npi`,
#'   `service`, `payer_group`, `setting`, one row per encounter.
#' @param min_cell_size Minimum distinct-encounter count per
#'   `(service, year, payer_group, setting, rendering_npi)` cell before
#'   `rendering_npi` is nulled. Default 11 (a common small-cell-suppression
#'   convention).
#' @return A tibble in the `urogynecology_service_events` schema:
#'   `encounter_id, year, rendering_npi, service, payer_group, setting,
#'   service_events`.
#' @family chia physician attribution
#' @concept supply
#' @keywords internal
.chia_suppress_small_cells <- function(events, min_cell_size = 11L) {
  cell_n <- events |>
    dplyr::group_by(
      .data$service, .data$year, .data$payer_group,
      .data$setting, .data$rendering_npi
    ) |>
    dplyr::mutate(cell_n = dplyr::n()) |>
    dplyr::ungroup()

  cell_n |>
    dplyr::mutate(
      rendering_npi = dplyr::if_else(
        .data$cell_n < min_cell_size, NA_character_, .data$rendering_npi
      )
    ) |>
    dplyr::group_by(
      .data$year, .data$rendering_npi, .data$service,
      .data$payer_group, .data$setting
    ) |>
    dplyr::summarise(
      encounter_id = paste(sort(.data$encounter_id), collapse = ";"),
      service_events = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::select(
      "encounter_id", "year", "rendering_npi", "service",
      "payer_group", "setting", "service_events"
    )
}

#' Build `chia_casemix.urogynecology_service_events`
#'
#' Assembles classified CHIA inpatient encounters for `sling_procedure` and
#' `prolapse_procedure` (see this file's header for why the other six
#' `service` values are deliberately never populated), writing the result to
#' `chia_casemix.urogynecology_service_events` so
#' [read_chia_service_share_events()] can read it.
#'
#' Requires [build_chia_physician_attribution()] to have been run first on
#' the same connection.
#'
#' @param con Open, writable DuckDB connection to the CHIA database.
#' @param min_cell_size Passed to [.chia_suppress_small_cells()].
#' @return `con`, invisibly.
#' @family chia physician attribution
#' @concept supply
#' @export
build_chia_urogynecology_service_events <- function(con, min_cell_size = 11L) {
  families_sql <- paste(
    sprintf("'%s'", names(.CHIA_PROCEDURE_FAMILY_TO_SERVICE)),
    collapse = ","
  )

  # ONE physician per discharge, not one row per listed physician. A discharge
  # can carry more than one attributed physician (a principal operator plus
  # "significant" co-surgeons, all flattened into v_hdd_discharge_physician
  # with no role preserved -- verified against the real data: 95 of 818
  # classified discharges list 2-4 distinct physicians). Fanning out to every
  # listed physician would multiply that discharge's service_events by however
  # many physicians it lists, inflating volume beyond the true discharge
  # count. QUALIFY picks one deterministically (lowest borim_license) so the
  # grain stays one row per discharge; which specific co-surgeon is credited
  # on a multi-physician case is arbitrary, but the volume is not.
  raw <- DBI::dbGetQuery(con, sprintf("
    SELECT
      c.RecordType20ID || '-' || c._data_year AS encounter_id,
      c._data_year AS year,
      b.NPI AS rendering_npi,
      c.procedure_family,
      a.PrimaryPayerType AS primary_payer_type
    FROM chia_casemix.v_hdd_discharge_canonical c
    JOIN chia_casemix.v_hdd_discharge_all_years a USING (RecordType20ID, _data_year)
    LEFT JOIN chia_casemix.v_hdd_discharge_physician d USING (RecordType20ID, _data_year)
    LEFT JOIN chia_provider.borim_stdrel_npi_straight_from_cd b
      ON TRY_CAST(b.license AS BIGINT) = d.borim_license
    WHERE c.procedure_family IN (%s)
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY c.RecordType20ID, c._data_year
      ORDER BY d.borim_license NULLS LAST
    ) = 1
  ", families_sql)) |>
    tibble::as_tibble()

  events <- raw |>
    dplyr::mutate(
      service = unname(.CHIA_PROCEDURE_FAMILY_TO_SERVICE[.data$procedure_family]),
      payer_group = .chia_resolve_payer_group(.data$primary_payer_type),
      setting = "inpatient",
      rendering_npi = dplyr::na_if(trimws(.data$rendering_npi), "")
    ) |>
    dplyr::select(
      "encounter_id", "year", "rendering_npi", "service",
      "payer_group", "setting"
    )

  out <- .chia_suppress_small_cells(events, min_cell_size = min_cell_size)

  DBI::dbWriteTable(
    con,
    DBI::Id(schema = "chia_casemix", table = "urogynecology_service_events"),
    out,
    overwrite = TRUE
  )
  invisible(con)
}
