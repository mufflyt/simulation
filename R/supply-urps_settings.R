# Care Delivery Setting Differentiation ----
#
# HWSM v5.19.20 models demand separately by care setting (hospital inpatient,
# ED, ambulatory office, outpatient clinic, post-acute, home health, school,
# academia, public). The URPS model previously collapsed everything to a single
# wRVU aggregate with no setting split, making two analyses impossible:
#
#   (a) Care-delivery shift scenarios: office → telehealth, hospital → ASC.
#   (b) Per-setting staffing ratios that differ from the aggregate.
#
# This module adds two layers on top of the existing workload machinery in
# R/supply-workload_to_fte.R:
#
#   Layer 1 — Setting-aware FTE dispatch:
#     `volumes_with_setting()` joins the service-volume tibble with the workload
#     basket's `setting` column. `convert_workload_to_fte(by_setting = TRUE)`
#     returns per-setting FTE rather than a single total.
#
#   Layer 2 — Setting-shift scenarios:
#     `split_volumes_by_setting()` distributes undifferentiated volumes across
#     settings using a mix matrix. `apply_setting_scenario()` applies a named
#     scenario that shifts fractions of volume between settings (e.g. 20% of
#     consultations move from office to telehealth), with per-setting
#     productivity adjustments scaling effective wRVU output.
#
# Wiring: pass `setting_scenario = "telehealth_10pct"` to
# `run_workforce_microsimulation()` to apply the shift before
# `convert_workload_to_fte()`.
#
# ---- Setting taxonomy --------------------------------------------------------
#
# Six URPS-relevant settings (collapses the HWSM's nine into those where URPS
# providers actually practise):
#
#   office              Provider's own office; non-facility RVU applies.
#   telehealth          Synchronous video/audio visit; same work RVU as office.
#   hospital_outpatient HOD / clinic; facility RVU applies.
#   asc                 Ambulatory surgery centre for minor procedures.
#   operative           Major OR procedures (hospital-based or ASC).
#   postacute           Skilled nursing facility rounds (rare for URPS).
#
# ---- Default setting mix -----------------------------------------------------
#
# Illustrative baseline fractions pending the PSPS place-of-service data
# (see data-raw/cms_psps/DOWNLOAD.md). Mix values are calibration_status =
# "uncalibrated_illustrative" until `load_psps_pos_shares()` is run and the
# PSPS-derived values replace these.
#
# Sources for the illustrative mix:
#   Office / telehealth split: AMA 2022 Physician Practice Benchmark Survey
#     (telehealth adoption post-COVID: ~8% of visits for surgical subspecialties).
#   Hospital outpatient vs ASC: Ambulatory Surgery Center Association 2022
#     (diagnostic urodynamics: ~20% at HOD; cystoscopy: ~30% ASC).
#   Operative setting split: HCUP ASC data 2020 (sling: ~65% ASC/HOD vs 35%
#     inpatient; prolapse repair: ~45% inpatient due to complexity).
#
# Each row must sum to 1 across settings; `validate_setting_mix()` enforces it.

URPS_SETTING_NAMES <- c("office", "telehealth", "hospital_outpatient",
                         "asc", "operative", "postacute")

#' Default care-delivery setting mix for each URPS service
#'
#' A tibble with columns `service`, `setting`, `share`. Each service's shares
#' sum to 1. Illustrative until replaced by PSPS place-of-service data.
#'
#' @export
URPS_DEFAULT_SETTING_MIX <- tibble::tribble(
  ~service,                ~setting,               ~share,
  # Office/facility split from CMS MUP_PHY 2024 (PSPS geo-service file).
  # Telehealth sub-split: illustrative AMA 2022 ratio scaled to PSPS office share.
  # ASC sub-split: illustrative ASCA 2022 ratio scaled to PSPS facility share.
  # Surgical CPTs: PSPS F share sub-split into operative/asc at illustrative ratio.
  # postoperative_care and indirect_clinical_work: not billable HCPCS, kept illustrative.
  "new_consultation",      "office",                0.8240,
  "new_consultation",      "telehealth",            0.0758,
  "new_consultation",      "hospital_outpatient",   0.1002,

  "return_visit",          "office",                0.7524,
  "return_visit",          "telehealth",            0.1758,
  "return_visit",          "hospital_outpatient",   0.0718,

  "pessary_care",          "office",                0.8635,
  "pessary_care",          "telehealth",            0.0469,
  "pessary_care",          "hospital_outpatient",   0.0896,

  "urodynamics",           "office",                0.9137,
  "urodynamics",           "hospital_outpatient",   0.0636,
  "urodynamics",           "asc",                   0.0227,

  "cystoscopy",            "office",                0.6666,
  "cystoscopy",            "hospital_outpatient",   0.1516,
  "cystoscopy",            "asc",                   0.1818,

  "botox_bladder",         "office",                0.4909,
  "botox_bladder",         "hospital_outpatient",   0.3524,
  "botox_bladder",         "asc",                   0.1567,

  "ptns",                  "office",                0.9214,
  "ptns",                  "telehealth",            0.0196,
  "ptns",                  "hospital_outpatient",   0.0590,

  "bladder_instillation",  "office",                0.8396,
  "bladder_instillation",  "hospital_outpatient",   0.1604,

  "sling_procedure",       "office",                0.0178,
  "sling_procedure",       "operative",             0.3438,
  "sling_procedure",       "asc",                   0.6384,

  "prolapse_procedure",    "office",                0.0137,
  "prolapse_procedure",    "operative",             0.4438,
  "prolapse_procedure",    "asc",                   0.5425,

  "postoperative_care",    "office",                0.80,
  "postoperative_care",    "telehealth",            0.15,
  "postoperative_care",    "hospital_outpatient",   0.05,

  "indirect_clinical_work","office",                1.00
)

URPS_DEFAULT_SETTING_MIX_STATUS <- "calibrated_psps_2024"
URPS_DEFAULT_SETTING_MIX_SOURCE <- paste(
  "Office/facility split from CMS MUP_PHY 2024 (MUP_PHY_R26_P05_V10_D24_Geo.csv).",
  "Telehealth sub-split of office share: AMA 2022 Benchmark Survey ratio (scaled).",
  "ASC sub-split of facility share: ASCA 2022 ratio (scaled).",
  "Surgical CPT operative/ASC sub-split: HCUP ASC 2020 ratio (scaled).",
  "postoperative_care and indirect_clinical_work: illustrative (not HCPCS-billed)."
)

#' Per-setting productivity multipliers relative to office baseline
#'
#' Scales the effective wRVU output per unit of volume. A multiplier < 1 means
#' the same service takes proportionally more physician time in that setting.
#'
#' Rationale:
#'   office = 1.00 (reference)
#'   telehealth = 0.92: AMA survey 2022 — synchronous video visits average
#'     ~15 min vs ~20 min for office, but documentation overhead is higher;
#'     net productivity ~8% below office.
#'   hospital_outpatient = 0.88: HOPD scheduling gaps, longer room turnover,
#'     and institutional documentation reduce throughput (Dall 2013 analogy).
#'   asc = 1.05: streamlined scheduling and room turnover raise throughput
#'     vs hospital for ambulatory procedures (ASCA 2022).
#'   operative = 1.00: OR procedures — wRVU is the unit; no throughput
#'     adjustment warranted.
#'   postacute = 0.80: travel time and multi-patient rounds reduce billable
#'     concentration (physiatry analogy, Zarek 2025).
#'
#' @export
URPS_SETTING_PRODUCTIVITY <- c(
  office              = 1.00,
  telehealth          = 0.92,
  hospital_outpatient = 0.88,
  asc                 = 1.05,
  operative           = 1.00,
  postacute           = 0.80
)

# ---- Pre-defined scenarios ---------------------------------------------------

#' Pre-defined care-delivery setting shift scenarios
#'
#' Each entry is a list with:
#' \describe{
#'   \item{label}{Human-readable name.}
#'   \item{shifts}{Named list of per-service shift specs. Each spec names the
#'     `from` setting, the `to` setting, and the `fraction` of volume to move.}
#' }
#'
#' @export
URPS_SETTING_SCENARIOS <- list(
  baseline = list(
    label  = "Baseline (no setting shift)",
    shifts = list()
  ),

  telehealth_10pct = list(
    label  = "10% of E/M visits move to telehealth",
    shifts = list(
      list(service = c("new_consultation", "return_visit", "postoperative_care"),
           from = "office", to = "telehealth", fraction = 0.10)
    )
  ),

  telehealth_25pct = list(
    label  = "25% of E/M visits move to telehealth",
    shifts = list(
      list(service = c("new_consultation", "return_visit", "postoperative_care"),
           from = "office", to = "telehealth", fraction = 0.25)
    )
  ),

  asc_migration_30pct = list(
    label  = "30% of operative procedures shift from hospital to ASC",
    shifts = list(
      list(service = c("sling_procedure", "prolapse_procedure"),
           from = "operative", to = "asc", fraction = 0.30)
    )
  ),

  combined_shift = list(
    label  = "Telehealth 15% + ASC 20% combined shift",
    shifts = list(
      list(service = c("new_consultation", "return_visit", "postoperative_care"),
           from = "office", to = "telehealth", fraction = 0.15),
      list(service = c("sling_procedure", "prolapse_procedure"),
           from = "operative", to = "asc", fraction = 0.20)
    )
  )
)

# ---- Validation --------------------------------------------------------------

#' Validate a setting mix table
#'
#' @param mix Tibble with `service`, `setting`, `share`.
#' @param tol Tolerance on row sums.
#' @return (Invisibly) `mix`, or error naming offending services.
#' @export
validate_setting_mix <- function(mix, tol = 1e-8) {
  assertthat::assert_that(is.data.frame(mix),
                          all(c("service", "setting", "share") %in% names(mix)))
  if (any(mix$share < 0 | mix$share > 1, na.rm = TRUE)) {
    stop("validate_setting_mix: shares must be in [0, 1].", call. = FALSE)
  }
  if (any(!mix$setting %in% URPS_SETTING_NAMES)) {
    bad <- unique(mix$setting[!mix$setting %in% URPS_SETTING_NAMES])
    stop(sprintf("validate_setting_mix: unknown setting(s): %s",
                 paste(bad, collapse = ", ")), call. = FALSE)
  }
  sums <- tapply(mix$share, mix$service, sum)
  bad  <- names(sums)[abs(sums - 1) > tol]
  if (length(bad)) {
    stop(sprintf("validate_setting_mix: shares do not sum to 1 for service(s): %s",
                 paste(bad, collapse = ", ")), call. = FALSE)
  }
  invisible(mix)
}

# ---- Layer 1: attach setting to volumes ------------------------------------

#' Join service volumes with their care-delivery setting
#'
#' Attaches the `setting` column from the workload basket to a service-volume
#' tibble. Services absent from the workload basket receive `NA_character_` for
#' setting, with a warning.
#'
#' This is a prerequisite for `convert_workload_to_fte(by_setting = TRUE)`.
#'
#' @param volumes Tibble with at least `service` and `volume`.
#' @param workload Service basket from [urps_service_workload()].
#' @return `volumes` with an added `setting` column.
#' @export
volumes_with_setting <- function(volumes,
                                  workload = urps_service_workload()) {
  assertthat::assert_that(all(c("service", "volume") %in% names(volumes)))
  assertthat::assert_that("setting" %in% names(workload))
  setting_map <- workload[, c("service", "setting")]
  out <- dplyr::left_join(volumes, setting_map, by = "service")
  missing <- is.na(out$setting) & !is.na(out$service)
  if (any(missing)) {
    warning(sprintf(
      "volumes_with_setting: %d service(s) absent from workload basket: %s",
      sum(missing), paste(sort(unique(out$service[missing])), collapse = ", ")
    ), call. = FALSE)
  }
  out
}

# ---- Layer 2: setting-mix split ---------------------------------------------

#' Distribute service volumes across care-delivery settings
#'
#' Takes an undifferentiated service-volume tibble (`year`, `service`, `volume`)
#' and returns a longer tibble (`year`, `service`, `setting`, `volume`) where
#' each service's volume is split across settings according to `mix`.
#'
#' @param volumes Tibble with `service` and `volume` (optionally `year`).
#' @param mix Setting-mix table; see [URPS_DEFAULT_SETTING_MIX].
#' @return Longer tibble with `service`, `setting`, `volume` (and `year` if
#'   present in `volumes`).
#' @export
#'
#' @examples
#' vols <- tibble::tibble(year = 2025L, service = "new_consultation", volume = 3e6)
#' split_volumes_by_setting(vols)
split_volumes_by_setting <- function(volumes,
                                      mix = URPS_DEFAULT_SETTING_MIX) {
  assertthat::assert_that(all(c("service", "volume") %in% names(volumes)))
  validate_setting_mix(mix)

  year_col <- "year" %in% names(volumes)
  out <- dplyr::left_join(volumes, mix, by = "service",
                           relationship = "many-to-many")
  out <- dplyr::mutate(out, volume = .data$volume * .data$share)
  out <- dplyr::select(out, dplyr::any_of(c("year", "service", "setting", "volume")))

  missing_svc <- setdiff(volumes$service, mix$service)
  if (length(missing_svc)) {
    warning(sprintf(
      "split_volumes_by_setting: %d service(s) absent from setting mix: %s",
      length(missing_svc), paste(missing_svc, collapse = ", ")
    ), call. = FALSE)
  }
  out
}

# ---- Layer 2: setting-shift scenarios ---------------------------------------

#' Apply a single setting-shift specification to a split-volume tibble
#'
#' Moves `fraction` of `from`-setting volume to `to`-setting for the named
#' services. The operation is: multiply `from` volume by `(1 - fraction)` and
#' add the difference to `to`. If `to` does not yet exist for a service it is
#' created.
#'
#' @param volumes_split Tibble with `service`, `setting`, `volume` (and
#'   optionally `year`) from [split_volumes_by_setting()].
#' @param service Character vector of service names to shift.
#' @param from Character setting name to shift volume away from.
#' @param to Character setting name to shift volume toward.
#' @param fraction Fraction of `from` volume to move to `to`. In \[0, 1\].
#' @return Updated volumes_split tibble.
#' @export
apply_setting_shift <- function(volumes_split, service, from, to, fraction) {
  if (!is.numeric(fraction) || length(fraction) != 1 ||
      fraction < 0 || fraction > 1) {
    stop("apply_setting_shift: fraction must be a single numeric in [0, 1].",
         call. = FALSE)
  }
  if (!from %in% URPS_SETTING_NAMES || !to %in% URPS_SETTING_NAMES) {
    stop(sprintf("apply_setting_shift: 'from' and 'to' must be in URPS_SETTING_NAMES."),
         call. = FALSE)
  }
  if (fraction == 0) return(volumes_split)

  affected <- volumes_split$service %in% service &
              volumes_split$setting == from

  if (!any(affected)) return(volumes_split)

  shifted_volume <- volumes_split$volume[affected] * fraction
  volumes_split$volume[affected] <- volumes_split$volume[affected] - shifted_volume

  # Build rows for the destination setting (may already exist or may be new)
  to_rows <- volumes_split[affected, , drop = FALSE]
  to_rows$setting <- to
  to_rows$volume  <- shifted_volume

  existing_to <- volumes_split$service %in% service & volumes_split$setting == to
  if (any(existing_to)) {
    # Add to existing destination rows (match by service + year if present)
    join_cols <- intersect(c("year", "service"), names(volumes_split))
    merge_key <- lapply(join_cols, function(col) to_rows[[col]])
    for (i in seq_len(nrow(to_rows))) {
      idx <- which(existing_to)
      match_i <- idx[mapply(function(col) {
        volumes_split[[col]][idx] == to_rows[[col]][i]
      }, join_cols) |> (function(m) if (is.matrix(m)) rowSums(m) == length(join_cols) else m)()]
      if (length(match_i) == 1L) {
        volumes_split$volume[match_i] <- volumes_split$volume[match_i] + to_rows$volume[i]
        to_rows <- to_rows[-i, , drop = FALSE]
      }
    }
    if (nrow(to_rows) > 0) {
      volumes_split <- dplyr::bind_rows(volumes_split, to_rows)
    }
  } else {
    volumes_split <- dplyr::bind_rows(volumes_split, to_rows)
  }
  volumes_split
}

#' Apply productivity adjustments by care delivery setting
#'
#' Scales each row's `volume` by the setting-specific productivity multiplier,
#' producing an **effective volume** that accounts for throughput differences
#' between office, telehealth, hospital outpatient, ASC, and operative settings.
#' The scaled volume is then passed to `convert_workload_to_fte()` so that
#' lower productivity in hospital outpatient settings correctly increases the
#' required FTE to deliver the same number of encounters.
#'
#' @param volumes_split Tibble from [split_volumes_by_setting()].
#' @param productivity Named numeric vector of multipliers; see
#'   [URPS_SETTING_PRODUCTIVITY].
#' @return `volumes_split` with `volume` divided by the productivity multiplier
#'   (effective volume = raw volume / productivity, so lower productivity →
#'   larger effective volume → more FTE required).
#' @export
apply_setting_productivity <- function(volumes_split,
                                        productivity = URPS_SETTING_PRODUCTIVITY) {
  assertthat::assert_that(all(c("service", "setting", "volume") %in%
                                 names(volumes_split)))
  if (!is.numeric(productivity) || is.null(names(productivity))) {
    stop("apply_setting_productivity: productivity must be a named numeric vector.",
         call. = FALSE)
  }
  if (any(productivity <= 0)) {
    stop("apply_setting_productivity: all productivity values must be > 0.",
         call. = FALSE)
  }

  idx  <- match(volumes_split$setting, names(productivity))
  mult <- ifelse(is.na(idx), 1, productivity[idx])
  # Divide: low productivity → effective volume increases → more FTE needed
  dplyr::mutate(volumes_split, volume = .data$volume / mult)
}

#' Apply a named setting-shift scenario to a service-volume tibble
#'
#' Convenience function that:
#'   1. Splits undifferentiated volumes across settings via [split_volumes_by_setting()].
#'   2. Applies each shift specification from `URPS_SETTING_SCENARIOS[[scenario_id]]`.
#'   3. Applies per-setting productivity adjustments via [apply_setting_productivity()].
#'   4. Re-aggregates to the (`year`, `service`) level so the output is
#'      compatible with [convert_workload_to_fte()] — or returns the long
#'      setting-level tibble when `aggregate = FALSE`.
#'
#' @param volumes Tibble with `service`, `volume` (and optionally `year`).
#' @param scenario_id Character key in [URPS_SETTING_SCENARIOS].
#' @param mix Setting-mix table for the split step.
#' @param productivity Per-setting productivity multipliers.
#' @param aggregate Logical. When `TRUE` (default), re-aggregate to
#'   (`year`, `service`) level for downstream wRVU conversion. When `FALSE`,
#'   return the long setting-level tibble.
#' @param registry Setting scenario registry; defaults to [URPS_SETTING_SCENARIOS].
#' @return A tibble compatible with [convert_workload_to_fte()].
#' @seealso [split_volumes_by_setting()], [apply_setting_productivity()],
#'   [URPS_SETTING_SCENARIOS]
#' @export
#'
#' @examples
#' demand <- compute_demand_denominators(example_female_population_by_band())
#' vols   <- example_service_volumes(demand)
#' adj    <- apply_setting_scenario(vols, "telehealth_10pct")
#' # adj is in the same shape as vols but with effective volumes adjusted
apply_setting_scenario <- function(volumes,
                                    scenario_id  = "baseline",
                                    mix          = URPS_DEFAULT_SETTING_MIX,
                                    productivity = URPS_SETTING_PRODUCTIVITY,
                                    aggregate    = TRUE,
                                    registry     = URPS_SETTING_SCENARIOS) {
  scen <- registry[[scenario_id]]
  if (is.null(scen)) {
    stop(sprintf("apply_setting_scenario: unknown scenario_id '%s'.", scenario_id),
         call. = FALSE)
  }

  # Step 1: split across settings
  split <- split_volumes_by_setting(volumes, mix = mix)

  # Step 2: apply each shift
  for (sh in scen$shifts) {
    split <- apply_setting_shift(split,
                                  service  = sh$service,
                                  from     = sh$from,
                                  to       = sh$to,
                                  fraction = sh$fraction)
  }

  # Step 3: productivity adjustment
  split <- apply_setting_productivity(split, productivity = productivity)

  if (!aggregate) return(split)

  # Step 4: re-aggregate to (year, service) — sum effective volumes
  grp_cols <- intersect(c("year", "service"), names(split))
  dplyr::summarise(
    dplyr::group_by(split, dplyr::across(dplyr::all_of(grp_cols))),
    volume = sum(.data$volume, na.rm = TRUE),
    .groups = "drop"
  )
}

# ---- PSPS data loader --------------------------------------------------------

#' Load CMS PSPS place-of-service shares for URPS CPT codes
#'
#' Reads the CMS Medicare Physician & Other Practitioners PUF (Provider &
#' Service level file) and computes the fraction of each URPS service delivered
#' in facility vs. non-facility settings.
#'
#' See `data-raw/cms_psps/DOWNLOAD.md` for download instructions.
#'
#' @param path Path to the unzipped MUP_PHY CSV.
#' @param basket CPT basket; defaults to `URPS_CPT_BASKET`.
#' @param file_type `"prov_svc"` (default, Provider & Service level) or
#'   `"geo_svc"` (Geography & Service aggregate).
#' @return Tibble: `service`, `setting`, `share` in the shape of
#'   [URPS_DEFAULT_SETTING_MIX]. Write this to replace the illustrative
#'   defaults.
#' @export
load_psps_pos_shares <- function(path,
                                  basket    = URPS_CPT_BASKET,
                                  file_type = c("prov_svc", "geo_svc")) {
  file_type <- match.arg(file_type)
  if (!file.exists(path)) {
    stop(sprintf(
      "load_psps_pos_shares: file not found: %s\nSee data-raw/cms_psps/DOWNLOAD.md.",
      path), call. = FALSE)
  }

  raw <- utils::read.csv(path, stringsAsFactors = FALSE)

  # Case-insensitive column matching (CMS uses mixed-case in 2024+ releases)
  nm_upper <- toupper(names(raw))
  find_col <- function(candidates) {
    hit <- which(nm_upper %in% toupper(candidates))
    if (length(hit)) names(raw)[hit[1]] else NA_character_
  }
  hcpcs_col <- find_col(c("HCPCS_CD", "HCPCS_CD", "HCPCS"))
  pos_col   <- find_col(c("PLACE_OF_SRVC", "PLACE_OF_SERVICE"))
  svc_col   <- find_col(c("TOT_SRVCS", "TOTAL_SERVICES"))
  if (is.na(hcpcs_col) || is.na(pos_col) || is.na(svc_col)) {
    stop(paste0(
      "load_psps_pos_shares: could not identify HCPCS, place-of-service, or services columns.\n",
      "Found: ", paste(names(raw), collapse = ", ")), call. = FALSE)
  }

  # Filter to URPS CPT codes
  urps_codes <- unique(basket$hcpcs)
  sub <- raw[raw[[hcpcs_col]] %in% urps_codes, ]
  if (nrow(sub) == 0L) {
    stop("load_psps_pos_shares: no URPS CPT codes found in the file.", call. = FALSE)
  }
  sub$hcpcs_cd       <- sub[[hcpcs_col]]
  sub$place_of_srvc  <- toupper(trimws(sub[[pos_col]]))
  sub$tot_srvcs      <- suppressWarnings(as.numeric(sub[[svc_col]]))

  # Aggregate to hcpcs × place_of_service
  agg <- sub |>
    dplyr::filter(!is.na(.data$tot_srvcs)) |>
    dplyr::group_by(.data$hcpcs_cd, .data$place_of_srvc) |>
    dplyr::summarise(n = sum(.data$tot_srvcs, na.rm = TRUE), .groups = "drop")

  # Map F/O to setting names (service-aware: surgical CPTs at F → "operative")
  surgical_cpts <- basket$hcpcs[basket$service %in%
                                   c("sling_procedure", "prolapse_procedure")]
  agg$setting <- dplyr::case_when(
    agg$place_of_srvc == "O"                          ~ "office",
    agg$place_of_srvc == "F" &
      agg$hcpcs_cd %in% surgical_cpts                ~ "operative",
    agg$place_of_srvc == "F"                          ~ "hospital_outpatient",
    TRUE                                              ~ NA_character_
  )
  agg <- dplyr::filter(agg, !is.na(.data$setting))

  # Join CPT → service
  cpt_svc <- dplyr::select(basket, "hcpcs", "service", "mix")
  out <- dplyr::left_join(agg, cpt_svc, by = c("hcpcs_cd" = "hcpcs"),
                           relationship = "many-to-many")
  out <- dplyr::mutate(out, weighted_n = .data$n * .data$mix)

  # Aggregate to service × setting
  svc_setting <- out |>
    dplyr::group_by(.data$service, .data$setting) |>
    dplyr::summarise(total = sum(.data$weighted_n, na.rm = TRUE), .groups = "drop") |>
    dplyr::group_by(.data$service) |>
    dplyr::mutate(share = .data$total / sum(.data$total)) |>
    dplyr::ungroup() |>
    dplyr::select("service", "setting", "share")

  message(sprintf(
    "load_psps_pos_shares: derived setting mix for %d services from %d CPT codes (%d records).",
    dplyr::n_distinct(svc_setting$service), length(urps_codes), nrow(sub)
  ))
  svc_setting
}

# ---- Summary reporting -------------------------------------------------------

#' Compare service volumes before and after a setting-shift scenario
#'
#' @param volumes Tibble with `service`, `volume` (optionally `year`).
#' @param scenario_id Character key in [URPS_SETTING_SCENARIOS].
#' @param year_filter Optional year to subset.
#' @return Tibble: `service`, `volume_baseline`, `volume_adjusted`,
#'   `volume_delta`, `pct_change`.
#' @export
setting_scenario_summary <- function(volumes,
                                      scenario_id = "telehealth_10pct",
                                      year_filter = NULL) {
  base <- volumes
  adj  <- apply_setting_scenario(volumes, scenario_id)

  if (!is.null(year_filter)) {
    base <- dplyr::filter(base, .data$year %in% year_filter)
    adj  <- dplyr::filter(adj,  .data$year %in% year_filter)
  }

  dplyr::left_join(
    dplyr::rename(base, volume_baseline  = "volume"),
    dplyr::rename(adj,  volume_adjusted  = "volume"),
    by = intersect(c("year", "service"), names(base))
  ) |>
    dplyr::mutate(
      volume_delta = .data$volume_adjusted - .data$volume_baseline,
      pct_change   = 100 * .data$volume_delta / .data$volume_baseline
    )
}
