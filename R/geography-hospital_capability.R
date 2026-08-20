# URPS Hospital Capability & Infrastructure Tiering (CMS HCRIS) ----
#
# Source: CMS HCRIS Hospital Cost Report, Form CMS-2552-10 (2010-2026).
#
# Interpretation:
# A reported nonzero cost-center value on Worksheet A000000 (Column 00700) is affirmative
# evidence that the capability was present during that fiscal reporting period.
# Absence of a reported value is "not confirmed", not proof of absence.
#
# Sterile processing:
# HCRIS does not contain a direct sterile-processing variable. Central Services & Supply (01400)
# together with an active Operating Room (05000) serves as a conservative proxy.
#
# Ultrasound:
# HCRIS does not contain a separate ultrasound cost center. Diagnostic Radiology (05400) is
# retained as an imaging proxy, with optional outpatient HCPCS claims confirmation.

HCRIS_HOSPITAL_REPORT_COLUMNS <- c(
  "rpt_rec_num",
  "prvdr_ctrl_type_cd",
  "prvdr_num",
  "npi",
  "rpt_stus_cd",
  "fy_bgn_dt",
  "fy_end_dt",
  "proc_dt",
  "initl_rpt_sw",
  "last_rpt_sw",
  "trnsmtl_num",
  "fi_num",
  "adr_vndr_cd",
  "fi_creat_dt",
  "util_cd",
  "npr_dt",
  "spec_ind"
)

HCRIS_HOSPITAL_NUMERIC_COLUMNS <- c(
  "rpt_rec_num",
  "wksht_cd",
  "line_num",
  "clmn_num",
  "itm_val_num"
)


#' HCRIS cost centers relevant to operative urogynecology
#'
#' Worksheet A, column 7 contains net expenses used for cost allocation.
#'
#' @return Tibble describing relevant CMS-2552-10 cost centers.
#' @family provider geography
#' @concept geography
#' @export
hcris_urps_capability_spec <- function() {

  tibble::tribble(
    ~signal, ~line_num, ~measurement_role,
    "central_services", "01400", "sterile_processing_proxy",
    "pharmacy", "01500", "support",
    "nonphysician_anesthetists", "01900", "anesthesia_support",
    "operating_room", "05000", "core",
    "recovery_room", "05100", "core",
    "anesthesiology", "05300", "core",
    "diagnostic_radiology", "05400", "imaging_proxy",
    "ct", "05700", "imaging_direct",
    "mri", "05800", "imaging_optional",
    "laboratory", "06000", "core",
    "blood_products", "06200", "blood_support",
    "blood_storage_transfusion", "06300", "core",
    "drugs_charged", "07300", "pharmacy_support"
  ) |>
    dplyr::mutate(
      wksht_cd = "A000000",
      clmn_num = "00700"
    )
}


#' Normalize CMS Certification Numbers
#'
#' @param value Character or numeric CCN.
#' @return Six-character CCN.
#' @keywords internal
.normalize_hcris_ccn <- function(value) {

  value_chr <- base::as.character(value)

  value_chr <- stringr::str_trim(
    value_chr
  )

  value_chr[value_chr == ""] <- NA_character_

  stringr::str_pad(
    value_chr,
    width = 6L,
    side = "left",
    pad = "0"
  )
}


#' Parse HCRIS dates
#'
#' @param value Character vector.
#' @return Date vector.
#' @keywords internal
.parse_hcris_date <- function(value) {

  value_chr <- base::as.character(value)

  value_chr[value_chr == ""] <- NA_character_

  parsed_date <- base::as.Date(
    value_chr,
    format = "%m/%d/%Y"
  )

  iso_index <- base::which(
    base::is.na(parsed_date) &
      !base::is.na(value_chr)
  )

  if (base::length(iso_index) > 0L) {
    parsed_date[iso_index] <- base::as.Date(
      value_chr[iso_index],
      format = "%Y-%m-%d"
    )
  }

  parsed_date
}


#' Read raw HCRIS hospital report files
#'
#' @description
#' Reads one or more raw CMS HCRIS RPT files through DuckDB. The large
#' annual files remain lazy until the required columns are selected.
#'
#' @param report_paths Character vector of raw HCRIS RPT CSV paths.
#'
#' @return Hospital cost-report identity table.
#' @family provider geography
#' @concept geography
#' @export
read_hcris_hospital_reports <- function(report_paths) {

  if (base::length(report_paths) < 1L) {
    base::stop(
      "At least one HCRIS report file is required.",
      call. = FALSE
    )
  }

  missing_paths <- report_paths[
    !base::file.exists(report_paths)
  ]

  if (base::length(missing_paths) > 0L) {
    base::stop(
      "Missing HCRIS report file(s): ",
      base::paste(missing_paths, collapse = ", "),
      call. = FALSE
    )
  }

  base::message(
    "[hospital-capability] Reading ",
    base::length(report_paths),
    " HCRIS report file(s)."
  )

  report_lazy <- duckplyr::read_file_duckdb(
    path = report_paths,
    table_function = "read_csv",
    prudence = "stingy",
    options = base::list(
      header = FALSE,
      all_varchar = TRUE
    )
  )

  expected_n <- base::length(
    HCRIS_HOSPITAL_REPORT_COLUMNS
  )

  if (base::ncol(report_lazy) < expected_n) {
    base::stop(
      "Unexpected HCRIS report-file structure.",
      call. = FALSE
    )
  }

  report_tbl <- report_lazy |>
    dplyr::select(
      .data$column0,
      .data$column2,
      .data$column3,
      .data$column4,
      .data$column5,
      .data$column6,
      .data$column7
    ) |>
    dplyr::collect() |>
    dplyr::transmute(
      rpt_rec_num = base::as.character(.data$column0),
      ccn = .normalize_hcris_ccn(.data$column2),
      npi = base::as.character(.data$column3),
      report_status = base::as.character(.data$column4),
      fy_bgn_dt = .parse_hcris_date(.data$column5),
      fy_end_dt = .parse_hcris_date(.data$column6),
      proc_dt = .parse_hcris_date(.data$column7)
    ) |>
    dplyr::mutate(
      fiscal_year = base::as.integer(
        base::format(
          .data$fy_end_dt,
          "%Y"
        )
      )
    )

  base::message(
    "[hospital-capability] Reports retained: ",
    base::format(
      base::nrow(report_tbl),
      big.mark = ","
    )
  )

  base::message(
    "[hospital-capability] Hospitals represented: ",
    base::format(
      dplyr::n_distinct(report_tbl$ccn),
      big.mark = ","
    )
  )

  report_tbl
}


#' Read relevant HCRIS numeric cost-center cells
#'
#' @description
#' Reads only the small subset of Worksheet A cells needed to identify
#' hospital capabilities. This avoids materializing the entire HCRIS
#' numeric file in R.
#'
#' @param numeric_paths Character vector of HCRIS NMRC CSV paths.
#' @param capability_spec Cost-center specification.
#'
#' @return Selected HCRIS cost-center cells.
#' @family provider geography
#' @concept geography
#' @export
read_hcris_urps_cost_centers <- function(
    numeric_paths,
    capability_spec = hcris_urps_capability_spec()) {

  if (base::length(numeric_paths) < 1L) {
    base::stop(
      "At least one HCRIS numeric file is required.",
      call. = FALSE
    )
  }

  missing_paths <- numeric_paths[
    !base::file.exists(numeric_paths)
  ]

  if (base::length(missing_paths) > 0L) {
    base::stop(
      "Missing HCRIS numeric file(s): ",
      base::paste(missing_paths, collapse = ", "),
      call. = FALSE
    )
  }

  base::message(
    "[hospital-capability] Reading targeted HCRIS cost centers."
  )

  numeric_lazy <- duckplyr::read_file_duckdb(
    path = numeric_paths,
    table_function = "read_csv",
    prudence = "stingy",
    options = base::list(
      header = FALSE,
      all_varchar = TRUE
    )
  )

  line_keys <- base::unique(
    capability_spec$line_num
  )

  cell_tbl <- numeric_lazy |>
    dplyr::transmute(
      rpt_rec_num = .data$column0,
      wksht_cd = .data$column1,
      line_num = .data$column2,
      clmn_num = .data$column3,
      itm_val_num = .data$column4
    ) |>
    dplyr::filter(
      .data$wksht_cd == "A000000",
      .data$clmn_num == "00700",
      .data$line_num %in% line_keys
    ) |>
    dplyr::collect() |>
    dplyr::mutate(
      rpt_rec_num = base::as.character(
        .data$rpt_rec_num
      ),
      itm_val_num = readr::parse_double(
        .data$itm_val_num,
        na = c("", "NA", "NULL")
      )
    )

  base::message(
    "[hospital-capability] Relevant HCRIS cells retained: ",
    base::format(
      base::nrow(cell_tbl),
      big.mark = ","
    )
  )

  cell_tbl
}


#' Collapse HCRIS cost centers into hospital capability evidence
#'
#' @param report_tbl HCRIS report table.
#' @param cell_tbl Selected HCRIS numeric cells.
#' @param capability_spec HCRIS capability specification.
#' @param zero_tolerance Absolute threshold for a nonzero cost.
#'
#' @return Hospital-year capability panel.
#' @family provider geography
#' @concept geography
#' @export
build_hcris_hospital_capability_panel <- function(
    report_tbl,
    cell_tbl,
    capability_spec = hcris_urps_capability_spec(),
    zero_tolerance = 1e-8) {

  base::message(
    "[hospital-capability] Building hospital-year capability panel."
  )

  evidence_tbl <- cell_tbl |>
    dplyr::inner_join(
      capability_spec |>
        dplyr::select(
          .data$signal,
          .data$line_num
        ),
      by = "line_num"
    ) |>
    dplyr::group_by(
      .data$rpt_rec_num,
      .data$signal
    ) |>
    dplyr::summarise(
      reported_value = base::sum(
        base::abs(.data$itm_val_num),
        na.rm = TRUE
      ),
      evidence = base::any(
        base::abs(.data$itm_val_num) >
          zero_tolerance,
        na.rm = TRUE
      ),
      .groups = "drop"
    )

  report_evidence_tbl <- evidence_tbl |>
    dplyr::select(
      .data$rpt_rec_num,
      .data$signal,
      .data$evidence
    ) |>
    tidyr::pivot_wider(
      names_from = .data$signal,
      values_from = .data$evidence,
      names_prefix = "evidence_",
      values_fill = FALSE
    )

  expected_evidence_cols <- base::paste0(
    "evidence_",
    capability_spec$signal
  )

  absent_cols <- base::setdiff(
    expected_evidence_cols,
    base::names(report_evidence_tbl)
  )

  for (column_name in absent_cols) {
    report_evidence_tbl[[column_name]] <- FALSE
  }

  linked_tbl <- report_tbl |>
    dplyr::left_join(
      report_evidence_tbl,
      by = "rpt_rec_num"
    )

  evidence_cols <- base::intersect(
    expected_evidence_cols,
    base::names(linked_tbl)
  )

  for (column_name in evidence_cols) {
    linked_tbl[[column_name]][
      base::is.na(linked_tbl[[column_name]])
    ] <- FALSE
  }

  hospital_year_tbl <- linked_tbl |>
    dplyr::filter(
      !base::is.na(.data$ccn),
      !base::is.na(.data$fiscal_year)
    ) |>
    dplyr::group_by(
      .data$ccn,
      .data$fiscal_year
    ) |>
    dplyr::summarise(
      dplyr::across(
        dplyr::all_of(evidence_cols),
        ~ base::any(.x, na.rm = TRUE)
      ),
      report_n = dplyr::n_distinct(
        .data$rpt_rec_num
      ),
      .groups = "drop"
    ) |>
    dplyr::mutate(

      # Direct anesthesia evidence.
      anesthesia_evidence =
        .data$evidence_anesthesiology |
        .data$evidence_nonphysician_anesthetists,

      # HCRIS does not explicitly identify sterile processing.
      # OR + central supply is our conservative proxy.
      sterile_processing_proxy =
        .data$evidence_operating_room &
        .data$evidence_central_services,

      basic_lab_evidence =
        .data$evidence_laboratory,

      diagnostic_radiology_evidence =
        .data$evidence_diagnostic_radiology,

      ct_evidence =
        .data$evidence_ct,

      # HCRIS imaging evidence. Ultrasound is handled separately below.
      imaging_evidence =
        .data$evidence_diagnostic_radiology |
        .data$evidence_ct,

      pharmacy_evidence =
        .data$evidence_pharmacy |
        .data$evidence_drugs_charged,

      blood_bank_evidence =
        .data$evidence_blood_storage_transfusion |
        .data$evidence_blood_products,

      pacu_evidence =
        .data$evidence_recovery_room,

      operating_room_evidence =
        .data$evidence_operating_room
    )

  hospital_year_tbl <- hospital_year_tbl |>
    dplyr::mutate(

      # High-specificity full operative infrastructure.
      urps_core_operative_confirmed =
        .data$operating_room_evidence &
        .data$pacu_evidence &
        .data$anesthesia_evidence &
        .data$basic_lab_evidence &
        .data$blood_bank_evidence,

      # Additional support infrastructure requested for this analysis.
      urps_support_confirmed =
        .data$anesthesia_evidence &
        .data$sterile_processing_proxy &
        .data$basic_lab_evidence &
        .data$imaging_evidence &
        .data$pharmacy_evidence,

      # Most conservative definition.
      urps_full_scope_confirmed =
        .data$urps_core_operative_confirmed &
        .data$sterile_processing_proxy &
        .data$imaging_evidence &
        .data$pharmacy_evidence,

      capability_count =
        base::as.integer(.data$anesthesia_evidence) +
        base::as.integer(.data$sterile_processing_proxy) +
        base::as.integer(.data$basic_lab_evidence) +
        base::as.integer(.data$imaging_evidence) +
        base::as.integer(.data$pharmacy_evidence)
    )

  base::message(
    "[hospital-capability] Hospital-years built: ",
    base::format(
      base::nrow(hospital_year_tbl),
      big.mark = ","
    )
  )

  base::message(
    "[hospital-capability] Full-scope confirmed hospital-years: ",
    base::format(
      base::sum(
        hospital_year_tbl$urps_full_scope_confirmed,
        na.rm = TRUE
      ),
      big.mark = ","
    )
  )

  hospital_year_tbl
}


# Common diagnostic ultrasound procedures useful for confirming that a
# hospital actually bills diagnostic ultrasound services.
URPS_ULTRASOUND_CONFIRMATION_HCPCS <- c(
  "76536",  # soft tissue head/neck
  "76604",  # chest
  "76700",  # complete abdominal
  "76705",  # limited abdominal
  "76770",  # complete retroperitoneal
  "76775",  # limited retroperitoneal
  "76830",  # transvaginal
  "76856",  # complete pelvic
  "76857"   # limited pelvic
)


#' Identify hospital-year ultrasound confirmation from claims
#'
#' @description
#' Searches hospital outpatient provider-service records for common
#' diagnostic ultrasound HCPCS codes.
#'
#' @param service_tbl Hospital-provider-service table.
#' @param ccn_col Hospital CCN column.
#' @param year_col Calendar-year column.
#' @param hcpcs_col HCPCS/CPT column.
#' @param services_col Optional service-volume column.
#' @param ultrasound_codes Confirmation HCPCS codes.
#'
#' @return One row per hospital-year with ultrasound confirmation.
#' @family provider geography
#' @concept geography
#' @export
identify_hospital_ultrasound_claims <- function(
    service_tbl,
    ccn_col,
    year_col,
    hcpcs_col,
    services_col = NULL,
    ultrasound_codes =
      URPS_ULTRASOUND_CONFIRMATION_HCPCS) {

  required_cols <- c(
    ccn_col,
    year_col,
    hcpcs_col
  )

  missing_cols <- base::setdiff(
    required_cols,
    base::names(service_tbl)
  )

  if (base::length(missing_cols) > 0L) {
    base::stop(
      "service_tbl is missing: ",
      base::paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }

  base::message(
    "[hospital-capability] Identifying ultrasound claims."
  )

  ultrasound_tbl <- service_tbl |>
    dplyr::transmute(
      ccn = .normalize_hcris_ccn(
        .data[[ccn_col]]
      ),
      fiscal_year = base::as.integer(
        .data[[year_col]]
      ),
      hcpcs_code = base::as.character(
        .data[[hcpcs_col]]
      ),
      service_count = if (
        base::is.null(services_col)
      ) {
        1
      } else {
        base::as.numeric(
          .data[[services_col]]
        )
      }
    ) |>
    dplyr::filter(
      .data$hcpcs_code %in% ultrasound_codes
    ) |>
    dplyr::group_by(
      .data$ccn,
      .data$fiscal_year
    ) |>
    dplyr::summarise(
      ultrasound_claim_services = base::sum(
        .data$service_count,
        na.rm = TRUE
      ),
      ultrasound_claim_code_n =
        dplyr::n_distinct(.data$hcpcs_code),
      ultrasound_claims_confirmed = TRUE,
      .groups = "drop"
    )

  base::message(
    "[hospital-capability] Hospital-years with ultrasound evidence: ",
    base::format(
      base::nrow(ultrasound_tbl),
      big.mark = ","
    )
  )

  ultrasound_tbl
}


#' Add claims-based ultrasound confirmation
#'
#' @param capability_tbl HCRIS hospital capability panel.
#' @param ultrasound_tbl Claims-based ultrasound confirmation.
#'
#' @return Enhanced hospital capability panel.
#' @family provider geography
#' @concept geography
#' @export
add_ultrasound_confirmation <- function(
    capability_tbl,
    ultrasound_tbl) {

  enhanced_tbl <- capability_tbl |>
    dplyr::left_join(
      ultrasound_tbl,
      by = c(
        "ccn",
        "fiscal_year"
      )
    ) |>
    dplyr::mutate(
      ultrasound_claims_confirmed =
        dplyr::coalesce(
          .data$ultrasound_claims_confirmed,
          FALSE
        ),

      # Three useful imaging definitions.
      imaging_hcris_confirmed =
        .data$diagnostic_radiology_evidence |
        .data$ct_evidence,

      imaging_specific_confirmed =
        .data$ct_evidence |
        .data$ultrasound_claims_confirmed,

      imaging_any_confirmed =
        .data$diagnostic_radiology_evidence |
        .data$ct_evidence |
        .data$ultrasound_claims_confirmed
    )

  enhanced_tbl
}


#' Attach hospital identity and geography
#'
#' @param capability_tbl Hospital-year capability panel.
#' @param hospital_master_tbl Hospital master table.
#' @param master_ccn_col CCN column in hospital master.
#'
#' @return Hospital-year capability table with hospital metadata.
#' @family provider geography
#' @concept geography
#' @export
attach_hospital_capability_master <- function(
    capability_tbl,
    hospital_master_tbl,
    master_ccn_col = "facility_id") {

  if (!master_ccn_col %in%
      base::names(hospital_master_tbl)) {
    base::stop(
      "Hospital master does not contain ",
      master_ccn_col,
      ".",
      call. = FALSE
    )
  }

  hospital_lookup_tbl <- hospital_master_tbl |>
    dplyr::mutate(
      ccn = .normalize_hcris_ccn(
        .data[[master_ccn_col]]
      )
    )

  duplicate_ccn_tbl <- hospital_lookup_tbl |>
    dplyr::count(
      .data$ccn,
      name = "hospital_n"
    ) |>
    dplyr::filter(
      .data$hospital_n > 1L,
      !base::is.na(.data$ccn)
    )

  if (base::nrow(duplicate_ccn_tbl) > 0L) {
    base::stop(
      "Hospital master has duplicated CCNs.",
      call. = FALSE
    )
  }

  linked_tbl <- capability_tbl |>
    dplyr::left_join(
      hospital_lookup_tbl,
      by = "ccn"
    )

  match_rate <- base::mean(
    capability_tbl$ccn %in%
      hospital_lookup_tbl$ccn
  )

  base::message(
    "[hospital-capability] Hospital-master match rate: ",
    scales::percent(
      match_rate,
      accuracy = 0.1
    )
  )

  linked_tbl
}


#' Classify hospital suitability for operative urogynecology
#'
#' @param hospital_tbl Hospital capability panel.
#'
#' @return Hospital capability panel with analytic tiers.
#' @family provider geography
#' @concept geography
#' @export
classify_urps_hospital_capability <- function(
    hospital_tbl) {

  hospital_tbl |>
    dplyr::mutate(

      # Tier 0: no confirmed OR.
      urps_site_tier = dplyr::case_when(

        !.data$operating_room_evidence ~
          "not_confirmed",

        # Tier 3: strongest evidence for full operative practice.
        .data$operating_room_evidence &
          .data$pacu_evidence &
          .data$anesthesia_evidence &
          .data$basic_lab_evidence &
          .data$blood_bank_evidence &
          .data$sterile_processing_proxy &
          .data$pharmacy_evidence &
          .data$imaging_evidence ~
          "full_scope_confirmed",

        # Tier 2: core operative hospital infrastructure.
        .data$operating_room_evidence &
          .data$pacu_evidence &
          .data$anesthesia_evidence &
          .data$basic_lab_evidence &
          .data$blood_bank_evidence ~
          "core_operative_confirmed",

        # Tier 1: some surgical evidence, but incomplete confirmation.
        TRUE ~
          "partial_evidence"
      ),

      urps_site_tier = base::factor(
        .data$urps_site_tier,
        levels = c(
          "not_confirmed",
          "partial_evidence",
          "core_operative_confirmed",
          "full_scope_confirmed"
        ),
        ordered = TRUE
      )
    )
}
