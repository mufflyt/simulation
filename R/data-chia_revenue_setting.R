################################################################################
# R/data-chia_revenue_setting.R
# CHIA encounter-level UB-04 / NUBC revenue-center setting evidence
#
# Calibration tier: observed_regional (Massachusetts, FY2015-FY2018 only)
#
# WHY
# ---
# Inpatient hysterectomy in the CHIA cohort falls 10,984 (FY2004) -> 2,269
# (FY2018) while median length of stay never moves off 2.0 days. Two hypotheses
# fit that: the cases migrated out of the inpatient setting entirely, or they
# were reclassified inside HIDD. Revenue-center codes distinguish them, because
# they name the hospital cost centres that participated in the encounter.
#
# The decisive pattern would be: volume collapses while 036x operating room,
# 037x anaesthesia, 071x recovery, inpatient-bed evidence and LOS all hold
# steady -- i.e. the encounters that remain are the same inpatient surgical
# pathway, and the missing volume genuinely left the dataset.
#
# THE KEY DESIGN CHOICE
# ---------------------
# A 049x ambulatory-surgery or 0762 observation revenue code on an HIDD record
# is evidence about which hospital COST CENTRE was involved. It is not proof
# the encounter itself was ambulatory or observation. This function therefore
# preserves the raw flags and builds a conservative encounter-level
# classification on top of them, rather than reclassifying encounters.
#
# COVERAGE LIMIT
# --------------
# chia_casemix.hdd_service exists for FY2015-FY2018 only (33.9M service lines).
# The revenue-code evidence cannot reach the FY2004-2014 portion of the
# hysterectomy decline. Within its window, inpatient hysterectomy still falls
# 3,976 -> 2,269 (43%), which is testable.
#
# Runs against lazy DuckDB tables. Do not collect() hdd_service -- it is 33.9M
# rows; the joins and aggregations belong in the database.
################################################################################

#' Build CHIA encounter-level UB-04 revenue-center setting evidence
#'
#' Uses service-line UB-04/NUBC revenue-center codes from `hdd_service` to
#' characterize which hospital departments participated in each HIDD encounter.
#'
#' This does NOT infer that an HIDD encounter was ambulatory merely because a
#' 049x ambulatory-surgery or 0762 observation revenue code is present.
#'
#' @param cohort_tbl Lazy or in-memory table containing the analytic cohort.
#' @param service_tbl Lazy or in-memory `hdd_service` table.
#' @param encounter_id_col Encounter identifier in both tables.
#' @param year_col Fiscal/discharge year in `cohort_tbl`.
#' @param procedure_family_col Harmonized procedure-family column.
#' @param revenue_code_col Revenue-center code column in `service_tbl`.
#' @param los_col Optional length-of-stay column in `cohort_tbl`.
#' @param save_dir Directory for timestamped artifacts.
#' @param save_encounter_level Save the encounter-level evidence as RDS.
#'
#' @return A named list containing encounter evidence, annual trends,
#'   setting-pattern counts, the analytic revenue-family dictionary,
#'   an audit table, and a dynamic summary sentence.
#'
#' @export
build_chia_ub04_setting_evidence <- function(
    cohort_tbl,
    service_tbl,
    encounter_id_col,
    year_col,
    procedure_family_col,
    revenue_code_col,
    los_col = NULL,
    save_dir = "artifacts/chia_revenue_setting",
    save_encounter_level = TRUE) {

  base::message("build_chia_ub04_setting_evidence(): starting.")
  base::message("Encounter ID column: ", encounter_id_col)
  base::message("Year column: ", year_col)
  base::message("Procedure-family column: ", procedure_family_col)
  base::message("Revenue-code column: ", revenue_code_col)

  timestamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
  base::dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)

  safe_mean <- function(x) {
    x_valid <- x[base::is.finite(x)]
    if (base::length(x_valid) == 0L) return(NA_real_)
    base::mean(x_valid)
  }
  safe_sd <- function(x) {
    x_valid <- x[base::is.finite(x)]
    if (base::length(x_valid) < 2L) return(NA_real_)
    stats::sd(x_valid)
  }
  safe_quantile <- function(x, probability) {
    x_valid <- x[base::is.finite(x)]
    if (base::length(x_valid) == 0L) return(NA_real_)
    base::unname(stats::quantile(x_valid, probs = probability,
                                 names = FALSE, na.rm = TRUE))
  }
  format_integer <- function(x) {
    base::format(base::round(x), big.mark = ",", scientific = FALSE, trim = TRUE)
  }
  format_p_value <- function(p_value) {
    if (!base::is.finite(p_value)) return("p=NA")
    if (p_value < 0.001) return("p<0.001")
    base::sprintf("p=%.3f", p_value)
  }

  base::message("Standardizing cohort columns.")

  if (base::is.null(los_col)) {
    cohort_standard_lazy <- cohort_tbl |>
      dplyr::transmute(
        encounter_id = .data[[encounter_id_col]],
        year = base::as.integer(.data[[year_col]]),
        procedure_family = base::as.character(.data[[procedure_family_col]]),
        los_days = NA_real_
      )
  } else {
    cohort_standard_lazy <- cohort_tbl |>
      dplyr::transmute(
        encounter_id = .data[[encounter_id_col]],
        year = base::as.integer(.data[[year_col]]),
        procedure_family = base::as.character(.data[[procedure_family_col]]),
        los_days = base::as.numeric(.data[[los_col]])
      )
  }

  cohort_keys_lazy <- cohort_standard_lazy |> dplyr::distinct(.data$encounter_id)

  base::message("Restricting hdd_service to encounters in the analytic cohort.")

  service_standard_lazy <- service_tbl |>
    dplyr::transmute(
      encounter_id = .data[[encounter_id_col]],
      revenue_code_num = base::as.integer(.data[[revenue_code_col]])
    ) |>
    dplyr::semi_join(cohort_keys_lazy, by = "encounter_id")

  base::message("Auditing revenue-code parsing.")

  service_parse_audit <- service_standard_lazy |>
    dplyr::summarise(
      n_service_lines = dplyr::n(),
      n_unparsed_codes = base::sum(base::as.integer(base::is.na(.data$revenue_code_num)))
    ) |>
    dplyr::collect() |>
    dplyr::mutate(pct_unparsed = 100 * .data$n_unparsed_codes / .data$n_service_lines)

  base::message("Classifying UB-04/NUBC revenue-center families.")

  service_flagged_lazy <- service_standard_lazy |>
    dplyr::mutate(
      has_room_board_01xx          = dplyr::between(.data$revenue_code_num, 100L, 199L),
      has_special_care_020x_021x   = dplyr::between(.data$revenue_code_num, 200L, 219L),
      has_lab_030x_031x            = dplyr::between(.data$revenue_code_num, 300L, 319L),
      has_imaging_032x_035x        = dplyr::between(.data$revenue_code_num, 320L, 359L),
      has_or_036x                  = dplyr::between(.data$revenue_code_num, 360L, 369L),
      has_anesthesia_037x          = dplyr::between(.data$revenue_code_num, 370L, 379L),
      has_ed_045x                  = dplyr::between(.data$revenue_code_num, 450L, 459L),
      has_ambulatory_surgery_049x  = dplyr::between(.data$revenue_code_num, 490L, 499L),
      has_outpatient_services_050x = dplyr::between(.data$revenue_code_num, 500L, 509L),
      has_clinic_051x              = dplyr::between(.data$revenue_code_num, 510L, 519L),
      has_freestanding_clinic_052x = dplyr::between(.data$revenue_code_num, 520L, 529L),
      has_recovery_071x            = dplyr::between(.data$revenue_code_num, 710L, 719L),
      has_treatment_room_0761      = .data$revenue_code_num == 761L,
      has_observation_0762         = .data$revenue_code_num == 762L
    )

  base::message("Collapsing service lines to one row per encounter.")

  encounter_flags_lazy <- service_flagged_lazy |>
    dplyr::group_by(.data$encounter_id) |>
    dplyr::summarise(
      has_any_revenue_line         = 1L,
      has_room_board_01xx          = base::max(base::as.integer(.data$has_room_board_01xx), na.rm = TRUE),
      has_special_care_020x_021x   = base::max(base::as.integer(.data$has_special_care_020x_021x), na.rm = TRUE),
      has_lab_030x_031x            = base::max(base::as.integer(.data$has_lab_030x_031x), na.rm = TRUE),
      has_imaging_032x_035x        = base::max(base::as.integer(.data$has_imaging_032x_035x), na.rm = TRUE),
      has_or_036x                  = base::max(base::as.integer(.data$has_or_036x), na.rm = TRUE),
      has_anesthesia_037x          = base::max(base::as.integer(.data$has_anesthesia_037x), na.rm = TRUE),
      has_ed_045x                  = base::max(base::as.integer(.data$has_ed_045x), na.rm = TRUE),
      has_ambulatory_surgery_049x  = base::max(base::as.integer(.data$has_ambulatory_surgery_049x), na.rm = TRUE),
      has_outpatient_services_050x = base::max(base::as.integer(.data$has_outpatient_services_050x), na.rm = TRUE),
      has_clinic_051x              = base::max(base::as.integer(.data$has_clinic_051x), na.rm = TRUE),
      has_freestanding_clinic_052x = base::max(base::as.integer(.data$has_freestanding_clinic_052x), na.rm = TRUE),
      has_recovery_071x            = base::max(base::as.integer(.data$has_recovery_071x), na.rm = TRUE),
      has_treatment_room_0761      = base::max(base::as.integer(.data$has_treatment_room_0761), na.rm = TRUE),
      has_observation_0762         = base::max(base::as.integer(.data$has_observation_0762), na.rm = TRUE),
      .groups = "drop"
    )

  base::message("Joining revenue-center evidence to the analytic cohort.")

  encounter_evidence_tbl <- cohort_standard_lazy |>
    dplyr::left_join(encounter_flags_lazy, by = "encounter_id") |>
    dplyr::collect()

  flag_names <- c(
    "has_any_revenue_line", "has_room_board_01xx", "has_special_care_020x_021x",
    "has_lab_030x_031x", "has_imaging_032x_035x", "has_or_036x",
    "has_anesthesia_037x", "has_ed_045x", "has_ambulatory_surgery_049x",
    "has_outpatient_services_050x", "has_clinic_051x",
    "has_freestanding_clinic_052x", "has_recovery_071x",
    "has_treatment_room_0761", "has_observation_0762"
  )

  base::message("Converting missing service evidence to explicit zero flags.")

  encounter_evidence_tbl <- encounter_evidence_tbl |>
    dplyr::mutate(dplyr::across(dplyr::all_of(flag_names), ~ dplyr::coalesce(.x, 0L))) |>
    dplyr::mutate(
      has_inpatient_bed_evidence =
        .data$has_room_board_01xx == 1L | .data$has_special_care_020x_021x == 1L,
      has_or_corrob =
        .data$has_or_036x == 1L &
        (.data$has_anesthesia_037x == 1L | .data$has_recovery_071x == 1L),
      has_strong_inpatient_or_evidence =
        .data$has_or_036x == 1L & .data$has_inpatient_bed_evidence,
      care_setting_evidence = dplyr::case_when(
        .data$has_strong_inpatient_or_evidence ~ "hospital_or_plus_inpatient_bed",
        .data$has_or_corrob                    ~ "hospital_or_plus_anesthesia_or_recovery",
        .data$has_or_036x == 1L                ~ "hospital_or_revenue_only",
        .data$has_ambulatory_surgery_049x == 1L ~ "ambulatory_surgery_revenue_on_hidd",
        .data$has_observation_0762 == 1L       ~ "observation_revenue_on_hidd",
        .data$has_inpatient_bed_evidence       ~ "inpatient_bed_without_or_revenue",
        .data$has_any_revenue_line == 1L       ~ "other_hospital_revenue",
        TRUE                                   ~ "no_hdd_service_match"
      )
    )

  base::message("Building year-by-procedure revenue-center trend table.")

  trend_tbl <- encounter_evidence_tbl |>
    dplyr::group_by(.data$year, .data$procedure_family) |>
    dplyr::summarise(
      n_encounters            = dplyr::n(),
      n_with_revenue          = base::sum(.data$has_any_revenue_line),
      pct_with_revenue        = 100 * base::mean(.data$has_any_revenue_line),
      n_or_036x               = base::sum(.data$has_or_036x),
      pct_or_036x             = 100 * base::mean(.data$has_or_036x),
      n_strong_inpatient_or   = base::sum(.data$has_strong_inpatient_or_evidence),
      pct_strong_inpatient_or = 100 * base::mean(.data$has_strong_inpatient_or_evidence),
      n_anesthesia_037x       = base::sum(.data$has_anesthesia_037x),
      pct_anesthesia_037x     = 100 * base::mean(.data$has_anesthesia_037x),
      n_recovery_071x         = base::sum(.data$has_recovery_071x),
      pct_recovery_071x       = 100 * base::mean(.data$has_recovery_071x),
      n_inpatient_bed         = base::sum(.data$has_inpatient_bed_evidence),
      pct_inpatient_bed       = 100 * base::mean(.data$has_inpatient_bed_evidence),
      n_ambulatory_049x       = base::sum(.data$has_ambulatory_surgery_049x),
      pct_ambulatory_049x     = 100 * base::mean(.data$has_ambulatory_surgery_049x),
      n_observation_0762      = base::sum(.data$has_observation_0762),
      pct_observation_0762    = 100 * base::mean(.data$has_observation_0762),
      n_ed_045x               = base::sum(.data$has_ed_045x),
      pct_ed_045x             = 100 * base::mean(.data$has_ed_045x),
      los_mean   = safe_mean(.data$los_days),
      los_sd     = safe_sd(.data$los_days),
      los_median = safe_quantile(.data$los_days, 0.50),
      los_p25    = safe_quantile(.data$los_days, 0.25),
      los_p75    = safe_quantile(.data$los_days, 0.75),
      .groups = "drop"
    ) |>
    dplyr::arrange(.data$procedure_family, .data$year)

  base::message("Building mutually exclusive care-setting pattern table.")

  setting_pattern_tbl <- encounter_evidence_tbl |>
    dplyr::count(.data$year, .data$procedure_family, .data$care_setting_evidence,
                 name = "n_encounters") |>
    dplyr::group_by(.data$year, .data$procedure_family) |>
    dplyr::mutate(pct_encounters = 100 * .data$n_encounters / base::sum(.data$n_encounters)) |>
    dplyr::ungroup() |>
    dplyr::arrange(.data$procedure_family, .data$year, dplyr::desc(.data$n_encounters))

  base::message("Creating analytic UB-04 revenue-family dictionary.")

  revenue_family_tbl <- tibble::tribble(
    ~code_family, ~analytic_label,
    "010x-019x", "room_board",
    "020x-021x", "special_care",
    "030x-031x", "laboratory",
    "032x-035x", "imaging",
    "036x",      "operating_room",
    "037x",      "anesthesia",
    "045x",      "emergency_department",
    "049x",      "ambulatory_surgery_cost_center",
    "050x",      "outpatient_services",
    "051x",      "clinic",
    "052x",      "freestanding_clinic",
    "071x",      "recovery_room",
    "0761",      "treatment_room",
    "0762",      "observation"
  ) |>
    dplyr::mutate(
      interpretation = dplyr::case_when(
        .data$code_family == "049x" ~ base::paste(
          "Ambulatory-surgery cost-center evidence on an HIDD",
          "encounter; not proof the encounter was outpatient."),
        .data$code_family == "0762" ~ base::paste(
          "Observation-service evidence on an HIDD encounter;",
          "not proof the encounter remained observation-only."),
        TRUE ~ "Hospital revenue-center evidence."
      )
    )

  base::message("Calculating overall 036x operating-room trend.")

  annual_or_tbl <- encounter_evidence_tbl |>
    dplyr::filter(base::is.finite(.data$year)) |>
    dplyr::group_by(.data$year) |>
    dplyr::summarise(
      n_encounters = dplyr::n(),
      n_or_036x    = base::sum(.data$has_or_036x),
      pct_or_036x  = 100 * base::mean(.data$has_or_036x),
      .groups = "drop"
    ) |>
    dplyr::arrange(.data$year)

  trend_p_value <- NA_real_

  if (base::nrow(annual_or_tbl) >= 2L) {
    or_trend_fit <- stats::glm(
      base::cbind(n_or_036x, n_encounters - n_or_036x) ~ year,
      family = stats::binomial(), data = annual_or_tbl)
    coefficient_tbl <- stats::coef(base::summary(or_trend_fit))
    if ("year" %in% base::rownames(coefficient_tbl)) {
      trend_p_value <- coefficient_tbl["year", "Pr(>|z|)"]
    }
  }

  first_year_row <- annual_or_tbl |> dplyr::slice_head(n = 1L)
  last_year_row  <- annual_or_tbl |> dplyr::slice_tail(n = 1L)

  direction <- dplyr::case_when(
    last_year_row$pct_or_036x > first_year_row$pct_or_036x ~ "increased",
    last_year_row$pct_or_036x < first_year_row$pct_or_036x ~ "decreased",
    TRUE ~ "was unchanged"
  )

  summary_sentence <- base::sprintf(
    base::paste0("From %d to %d, 036x operating-room revenue evidence %s ",
                 "from %.1f%% (%s of %s encounters) to %.1f%% ",
                 "(%s of %s encounters; %s)."),
    first_year_row$year, last_year_row$year, direction,
    first_year_row$pct_or_036x,
    format_integer(first_year_row$n_or_036x),
    format_integer(first_year_row$n_encounters),
    last_year_row$pct_or_036x,
    format_integer(last_year_row$n_or_036x),
    format_integer(last_year_row$n_encounters),
    format_p_value(trend_p_value)
  )

  base::message("Dynamic summary: ", summary_sentence)

  cohort_audit_tbl <- tibble::tibble(
    metric = c("cohort_encounters", "encounters_with_hdd_service",
               "encounters_without_hdd_service", "service_lines",
               "unparsed_revenue_codes"),
    value = c(
      base::nrow(encounter_evidence_tbl),
      base::sum(encounter_evidence_tbl$has_any_revenue_line),
      base::sum(encounter_evidence_tbl$has_any_revenue_line == 0L),
      service_parse_audit$n_service_lines,
      service_parse_audit$n_unparsed_codes
    )
  )

  mk <- function(stem, ext) base::file.path(
    save_dir, base::paste0("chia_ub04_", stem, "_", timestamp, ".", ext))
  trend_path      <- mk("setting_trends",   "csv")
  pattern_path    <- mk("setting_patterns", "csv")
  dictionary_path <- mk("revenue_families", "csv")
  audit_path      <- mk("setting_audit",    "csv")
  summary_path    <- mk("setting_summary",  "txt")

  base::message("Saving annual trend table.");      readr::write_csv(trend_tbl, trend_path)
  base::message("Saving setting-pattern table.");   readr::write_csv(setting_pattern_tbl, pattern_path)
  base::message("Saving revenue-family dictionary."); readr::write_csv(revenue_family_tbl, dictionary_path)
  base::message("Saving audit table.");             readr::write_csv(cohort_audit_tbl, audit_path)
  base::message("Saving dynamic summary sentence."); base::writeLines(summary_sentence, summary_path)

  encounter_path <- NA_character_
  if (base::isTRUE(save_encounter_level)) {
    encounter_path <- mk("encounter_setting", "rds")
    base::message("Saving encounter-level setting evidence.")
    base::saveRDS(encounter_evidence_tbl, encounter_path, compress = "gzip")
  }

  base::message("Trend artifact: ", trend_path)
  base::message("Pattern artifact: ", pattern_path)
  base::message("Dictionary artifact: ", dictionary_path)
  base::message("Audit artifact: ", audit_path)
  base::message("Summary artifact: ", summary_path)
  if (base::isTRUE(save_encounter_level)) {
    base::message("Encounter artifact: ", encounter_path)
  }
  base::message("build_chia_ub04_setting_evidence(): complete.")

  base::list(
    encounter_evidence = encounter_evidence_tbl,
    annual_trends      = trend_tbl,
    setting_patterns   = setting_pattern_tbl,
    revenue_families   = revenue_family_tbl,
    audit              = cohort_audit_tbl,
    summary_sentence   = summary_sentence,
    paths = base::list(trends = trend_path, patterns = pattern_path,
                       dictionary = dictionary_path, audit = audit_path,
                       summary = summary_path, encounter = encounter_path)
  )
}
