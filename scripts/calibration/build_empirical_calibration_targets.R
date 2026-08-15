# scripts/calibration/build_empirical_calibration_targets.R
#
# Build the three empirical evidence streams, hash every artifact, freeze hashes
# for production anchors that exist, compute HDMM-style scalars ONLY where a
# production target is present and locked, and refuse to quietly turn CHIA or
# CADR into national targets.
#
# ROLES -- deliberate, and the point of the whole file:
#   NAMCS   national office-volume scalar (4,814,760 visits)   PRODUCTION ANCHOR
#   Lizeth  access output validation                           validation only
#   Rabice  access output validation                           validation only
#   CHIA    regional all-payer DELIVERED utilization           validation only
#   CADR    workload/service intensity GIVEN TREATMENT         validation only
#
# CADR's 5,566 sling episodes span the 2008-2016 treated Medicare cohort and are
# workload given treatment, not population utilization. CHIA is Massachusetts
# inpatient. Neither is a national annual volume, and this file will not let
# either be used as one.

#' Require packages used by empirical calibration
#' @return Invisibly, TRUE.
.require_empirical_packages <- function() {
  packages <- c("DBI", "digest", "dplyr", "duckdb", "pkgload", "readr",
                "stringr", "tibble", "tidyr", "yaml")
  missing_packages <- packages[
    !base::vapply(packages, base::requireNamespace, logical(1), quietly = TRUE)]
  if (base::length(missing_packages) > 0L) {
    base::stop("Missing package(s): ",
               base::paste(missing_packages, collapse = ", "), call. = FALSE)
  }
  base::invisible(TRUE)
}

#' Compute a SHA-256 digest for one file
#' @param path Existing file path.
#' @return Character SHA-256 digest.
.sha256_file <- function(path) {
  if (!base::file.exists(path)) {
    base::stop("Cannot hash missing file: ", path, call. = FALSE)
  }
  digest::digest(file = path, algo = "sha256")
}

#' Require specified columns
#' @param records A data frame.
#' @param required_names Required column names.
#' @param label Human-readable source name.
#' @return Invisibly, TRUE.
.require_columns <- function(records, required_names, label) {
  missing_names <- base::setdiff(required_names, base::names(records))
  if (base::length(missing_names) > 0L) {
    base::stop(label, " is missing: ",
               base::paste(missing_names, collapse = ", "), call. = FALSE)
  }
  base::invisible(TRUE)
}

#' Write a timestamped CSV and compute its SHA-256
#' @param records Data frame to save.
#' @param directory Destination directory.
#' @param stem Filename stem.
#' @param stamp Timestamp string.
#' @return Named list with path and sha256.
.write_hashed_csv <- function(records, directory, stem, stamp) {
  base::dir.create(directory, recursive = TRUE, showWarnings = FALSE)
  path <- base::file.path(directory, base::paste0(stem, "_", stamp, ".csv"))
  base::message("Writing ", stem, ": ", path)
  readr::write_csv(records, path, na = "")
  hash <- .sha256_file(path)
  base::message("SHA-256: ", hash)
  base::message("Saved file: ", base::normalizePath(path, mustWork = TRUE))
  base::list(path = path, sha256 = hash)
}

# ---------------------------------------------------------------------------
# CHIA -- regional all-payer delivered utilization
#
# Sourced from the validated chia_cadr DuckDB, not a CSV: the cohort
# (v_cohort_female_adult) and the POP-hysterectomy definition
# (config/chia_urps_inpatient_codes.yml) are both gated by
# scripts/chia/test_chia.py. Reading raw case-mix CSVs would bypass the era
# renames, the operative classification, and the newborn-attribution fix.
# ---------------------------------------------------------------------------

#' Build the CHIA regional all-payer utilization validation anchor
#'
#' @param db Path to chia_cadr.duckdb.
#' @param expected_total Regression guard on the FY2004-2018 total. The
#'   validated value is 17,676 (includes the ICD-9 codes withdrawn in the
#'   October 2006 update; omitting them undercounts FY2004-2006 by ~10%). NOTE: 1,306 is the 2023 board-certified active
#'   URPS PHYSICIAN count (see R/supply-roster.R), not a CHIA encounter count --
#'   using it here would be a number collision that hard-fails the build.
#' @return Named list with annual, summary, and anchor tables.
.build_chia_validation <- function(db, expected_total = 17676L) {
  base::message("========================================")
  base::message("CHIA REGIONAL ALL-PAYER VALIDATION")
  base::message("========================================")

  if (!base::file.exists(db)) {
    base::stop("CHIA database not found: ", db, call. = FALSE)
  }
  base::message("Reading CHIA DuckDB: ", db)

  connection <- DBI::dbConnect(duckdb::duckdb(), db, read_only = TRUE)
  base::on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)

  annual_records <- DBI::dbGetQuery(connection, "
    WITH dx AS (
      SELECT DISTINCT RecordType20ID, _data_year
      FROM chia_casemix.hdd_diagnosis_long
      WHERE code LIKE '618%' OR code LIKE 'N81%')
    SELECT c._data_year AS year,
           count(*)     AS pop_hysterectomy_encounters
    FROM chia_casemix.v_cohort_female_adult c
    JOIN dx USING (RecordType20ID, _data_year)
    WHERE (c._data_year <= 2015 AND c.principal_procedure IN
             ('6831','6839','6841','6849','6851','6859','6861','6869',
              '6871','6879','689','684','686','687'))
       OR (c._data_year >= 2016 AND (c.principal_procedure LIKE '0UT9%'
                                  OR c.principal_procedure LIKE '0UB9%'))
    GROUP BY 1 ORDER BY 1") |>
    tibble::as_tibble()

  total_pop_hysterectomy <- base::sum(
    annual_records$pop_hysterectomy_encounters, na.rm = TRUE)

  base::message("POP-indication hysterectomy encounters: ",
                base::format(total_pop_hysterectomy, big.mark = ","))

  if (!base::is.null(expected_total)) {
    if (!base::identical(base::as.integer(total_pop_hysterectomy),
                         base::as.integer(expected_total))) {
      base::stop("CHIA regression guard failed. Expected ",
                 base::format(expected_total, big.mark = ","),
                 " POP-hysterectomy encounters but found ",
                 base::format(total_pop_hysterectomy, big.mark = ","),
                 ". Inspect the CHIA vintage, the cohort view, or the code ",
                 "families in config/chia_urps_inpatient_codes.yml.",
                 call. = FALSE)
    }
    base::message("CHIA regression guard passed: ",
                  base::format(expected_total, big.mark = ","))
  }

  annual_summary <- annual_records |>
    dplyr::summarise(
      year_start    = base::min(year),
      year_end      = base::max(year),
      n_years       = dplyr::n(),
      mean_annual   = base::mean(pop_hysterectomy_encounters),
      sd_annual     = stats::sd(pop_hysterectomy_encounters),
      median_annual = stats::median(pop_hysterectomy_encounters),
      p25_annual    = stats::quantile(pop_hysterectomy_encounters, 0.25,
                                      names = FALSE),
      p75_annual    = stats::quantile(pop_hysterectomy_encounters, 0.75,
                                      names = FALSE))

  base::message(base::sprintf(
    "Annual range %d-%d: median %.0f, first year %d, last year %d.",
    annual_summary$year_start, annual_summary$year_end,
    annual_summary$median_annual,
    annual_records$pop_hysterectomy_encounters[1L],
    annual_records$pop_hysterectomy_encounters[base::nrow(annual_records)]))
  base::message("The series falls ~78% across the window. That is documented ",
                "setting migration to ambulatory surgery, not falling disease ",
                "(docs/CHIA_TECHNICAL_APPENDIX.md section 1); do not read it ",
                "as a demand trend.")

  anchor <- tibble::tibble(
    anchor_id  = "chia_pop_hysterectomy",
    observed   = total_pop_hysterectomy,
    year_start = annual_summary$year_start,
    year_end   = annual_summary$year_end,
    geography  = "Massachusetts",
    population = "all-payer inpatient encounters, female age 18+",
    estimand   = "POP-indication hysterectomy encounters (inpatient only)",
    source     = "CHIA Massachusetts case-mix (chia_cadr.duckdb)",
    evidence_status            = "direct_empirical_regional",
    calibration_role           = "regional_demand_validation",
    production_scalar_eligible = FALSE,
    notes = base::paste(
      "Regional treated utilization, INPATIENT ONLY. CHIA Case Mix has no",
      "ambulatory-surgery database and 957 CMR 8.00 binds acute care",
      "hospitals, so freestanding ASCs never submit. Do not divide a national",
      "model prediction by this value."))

  base::list(annual = annual_records, annual_summary = annual_summary,
             anchor = anchor)
}

#' Build CADR treated-patient workload validation
#' @param cadr_path CADR workload-per-treated-patient artifact.
#' @param year_start First utilization year.
#' @param year_end Last utilization year.
#' @return CADR pathway validation table.
.build_cadr_validation <- function(cadr_path, year_start = 2008L,
                                   year_end = 2016L) {
  base::message("========================================")
  base::message("CADR MEDICARE WORKLOAD VALIDATION")
  base::message("========================================")

  if (!base::file.exists(cadr_path)) {
    base::stop("CADR file not found: ", cadr_path, call. = FALSE)
  }
  base::message("Reading CADR: ", cadr_path)

  cadr_records <- readr::read_csv(cadr_path, show_col_types = FALSE,
                                  progress = FALSE)
  .require_columns(cadr_records,
    c("pathway", "n_treated_episodes", "post_em_visits_per_pt",
      "post_procedures_per_pt", "subsequent_pessary_visits_per_pt",
      "subsequent_PT_sessions_per_pt", "reoperation_prob",
      "fpmrs_wrvu_per_pt"),
    "CADR workload artifact")

  n_years <- year_end - year_start + 1L

  validation <- cadr_records |>
    dplyr::filter(pathway %in% c("UI Sling", "Pessary", "PT", "Open burch",
                                 "Laparoscopic burch")) |>
    dplyr::mutate(
      year_start        = base::as.integer(year_start),
      year_end          = base::as.integer(year_end),
      n_observed_years  = base::as.integer(n_years),
      cohort_episodes_per_observed_year = n_treated_episodes / n_observed_years,
      geography         = "United States",
      population        = "treated Medicare women age >=65",
      evidence_status   = "direct_empirical_population_limited",
      calibration_role  = "treated_patient_service_intensity",
      production_scalar_eligible = FALSE,
      notes = base::paste(
        "n_treated_episodes is the full observed CADR cohort count across",
        base::paste0(year_start, "-", year_end),
        "-- NOT a one-year national all-payer procedure total."))

  sling_count <- validation |>
    dplyr::filter(pathway == "UI Sling") |>
    dplyr::pull(n_treated_episodes)

  if (base::length(sling_count) == 1L) {
    base::message("CADR sling episodes across ", year_start, "-", year_end,
                  ": ", base::format(sling_count, big.mark = ","),
                  "  (", base::format(base::round(sling_count / n_years),
                                      big.mark = ","),
                  "/yr in the treated Medicare cohort)")
  }
  base::message("CADR is retained as workload-given-treatment validation. It ",
                "is NOT an annual national sling volume and is not eligible ",
                "to produce a calibration scalar.")
  validation
}

#' Build Lizeth and Rabice access validation targets
#' @param lizeth_dir Local Lizeth repository path.
#' @return Named list of access validation tables.
.build_mystery_caller_validation <- function(lizeth_dir = "../lizeth") {
  base::message("========================================")
  base::message("URPS MYSTERY-CALLER ACCESS VALIDATION")
  base::message("========================================")

  find_lizeth     <- base::get("find_lizeth_redcap", mode = "function", inherits = TRUE)
  parse_lizeth    <- base::get("parse_lizeth_physician_information", mode = "function", inherits = TRUE)
  prepare_lizeth  <- base::get("prepare_lizeth_access", mode = "function", inherits = TRUE)
  estimate_lizeth <- base::get("estimate_lizeth_access_anchor", mode = "function", inherits = TRUE)

  lizeth_path <- find_lizeth(lizeth_dir)
  base::message("Reading Lizeth REDCap source: ", lizeth_path)

  lizeth_records  <- readr::read_csv(lizeth_path, show_col_types = FALSE,
                                     progress = FALSE)
  parsed_records  <- parse_lizeth(lizeth_records)
  access_records  <- prepare_lizeth(parsed_records)
  access_anchor   <- estimate_lizeth(access_records)

  valid_years <- access_records$call_year[!base::is.na(access_records$call_year)]
  year_start  <- if (base::length(valid_years) > 0L) base::min(valid_years) else NA_integer_
  year_end    <- if (base::length(valid_years) > 0L) base::max(valid_years) else NA_integer_

  overall <- access_anchor$overall

  lizeth_overall <- tibble::tibble(
    source = "Lizeth", anchor_id = "lizeth_urps_access",
    year_start = year_start, year_end = year_end,
    n_calls = overall$n_calls, n_physicians = overall$n_physicians,
    appointment_n = overall$appointment_n,
    appointment_pct = overall$appointment_pct,
    wait_mean = overall$wait_mean, wait_sd = overall$wait_sd,
    wait_median = overall$wait_median, wait_p25 = overall$wait_p25,
    wait_p75 = overall$wait_p75,
    p_value_insurance = access_anchor$p_value_insurance,
    wait_unit = "business_days",
    evidence_status = "direct_empirical_national_preliminary",
    calibration_role = "access_output_validation",
    production_scalar_eligible = FALSE)

  lizeth_insurance <- access_anchor$by_insurance |>
    dplyr::mutate(source = "Lizeth", year_start = year_start,
                  year_end = year_end,
                  p_value_insurance = access_anchor$p_value_insurance,
                  calibration_role = "payer_access_validation") |>
    dplyr::relocate(source, year_start, year_end)

  rabice_source <- base::get("URPS_WAIT_OBSERVATIONS", inherits = TRUE)
  rabice <- rabice_source |>
    dplyr::filter(study == "Rabice") |>
    dplyr::transmute(
      source = study, anchor_id = "rabice_urps_wait",
      year_start = data_year, year_end = data_year,
      scenario = scenario, insurance = insurance,
      wait_mean_business_days = wait_business_days,
      n_offices = n_offices, evidence_status = status,
      calibration_role = "access_output_validation",
      production_scalar_eligible = FALSE, citation = citation)

  base::message(access_anchor$summary_sentence)

  base::list(lizeth_overall = lizeth_overall,
             lizeth_by_insurance = lizeth_insurance, rabice = rabice,
             summary_sentence = access_anchor$summary_sentence)
}

#' Read the numeric value from a production anchor
#' @param path Anchor CSV.
#' @return Numeric scalar target.
.read_production_target <- function(path) {
  anchor_records <- readr::read_csv(path, show_col_types = FALSE,
                                    progress = FALSE)
  candidate_names <- c("observed", "estimate")
  present <- candidate_names[candidate_names %in% base::names(anchor_records)]
  if (base::length(present) == 0L) {
    base::stop("Anchor has neither `observed` nor `estimate`: ", path,
               call. = FALSE)
  }
  target_value <- base::as.numeric(anchor_records[[present[[1L]]]][[1L]])
  if (!base::is.finite(target_value) || target_value <= 0) {
    base::stop("Anchor target is not positive and finite: ", path, call. = FALSE)
  }
  target_value
}

#' Freeze hashes for production anchors that already exist
#'
#' Missing files remain missing. This function never manufactures an anchor.
#'
#' @param config_path Calibration YAML.
#' @param stamp Timestamp for the backup.
#' @return Invisibly, updated configuration.
.freeze_existing_anchor_hashes <- function(config_path, stamp) {
  base::message("Freezing hashes for existing production anchors.")
  config <- yaml::read_yaml(config_path)

  backup_path <- base::file.path(
    base::dirname(config_path),
    base::paste0("calibration_targets_backup_", stamp, ".yml"))
  base::file.copy(config_path, backup_path, overwrite = FALSE)
  base::message("Config backup: ", backup_path)

  # Surgical text edit, NOT yaml::write_yaml. A full round-trip strips every
  # comment, and this file's comments carry the methodology: why a scalar far
  # from 1 signals a structural mismatch, the published HDMM reference scalars,
  # and which anchors are unacquired and why. Losing them to a script run is a
  # silent documentation regression.
  lines <- base::readLines(config_path)

  for (anchor_name in base::names(config$anchors)) {
    anchor_path <- config$anchors[[anchor_name]]$path
    if (!base::file.exists(anchor_path)) {
      base::message("Still missing: ", anchor_name, " -> ", anchor_path)
      next
    }
    hash <- .sha256_file(anchor_path)

    # locate this anchor's block, then its sha256 line within it
    start <- base::grep(base::paste0("^  ", anchor_name, ":"), lines)
    if (base::length(start) != 1L) {
      base::warning("Could not locate anchor block for ", anchor_name,
                    "; hash not written.", call. = FALSE)
      next
    }
    nxt <- base::grep("^  [A-Za-z_]+:", lines)
    nxt <- nxt[nxt > start]
    stop_at <- if (base::length(nxt) > 0L) base::min(nxt) - 1L else base::length(lines)
    sha_line <- base::grep("^\\s*sha256:", lines[start:stop_at])
    if (base::length(sha_line) != 1L) {
      base::warning("No unique sha256 line for ", anchor_name, call. = FALSE)
      next
    }
    idx <- start + sha_line - 1L
    lines[idx] <- base::sprintf('    sha256: "%s"', hash)
    base::message("Locked ", anchor_name, " -> ", hash)
  }

  base::writeLines(lines, config_path)
  base::message("Updated canonical config: ",
                base::normalizePath(config_path, mustWork = TRUE))
  base::invisible(config)
}

#' Register empirical validation anchors in calibration YAML
#' @param config_path Calibration YAML path.
#' @param registry Validation artifact registry.
#' @return Invisibly, updated configuration.
.register_validation_anchors <- function(config_path, registry) {
  base::message("Registering empirical validation anchors.")
  config <- yaml::read_yaml(config_path)
  # Append, preserving comments (see .freeze_existing_anchor_hashes). The
  # validation_anchors block is rewritten wholesale each run; the anchors block
  # above it is never touched here.
  lines <- base::readLines(config_path)
  # Cut from the sentinel, not from the `validation_anchors:` key -- the comment
  # header sits ABOVE the key, so cutting at the key leaves the old header
  # behind and the block duplicates on every run.
  sentinel <- "# >>> VALIDATION ANCHORS (generated) >>>"
  marker <- base::grep(sentinel, lines, fixed = TRUE)
  if (base::length(marker) == 0L) {
    marker <- base::grep("^validation_anchors:", lines)
  }
  if (base::length(marker) > 0L) {
    lines <- lines[base::seq_len(base::min(marker) - 1L)]
  }
  while (base::length(lines) > 0L &&
         !base::nzchar(lines[base::length(lines)])) {
    lines <- lines[-base::length(lines)]
  }

  block <- c(
    "",
    "# >>> VALIDATION ANCHORS (generated) >>>",
    "# ---------------------------------------------------------------------------",
    "# VALIDATION anchors -- regenerated by",
    "# scripts/calibration/build_empirical_calibration_targets.R.",
    "#",
    "# These are NOT calibration targets. Every entry is",
    "# production_scalar_eligible: false, and the build refuses to divide a",
    "# national model prediction by any of them. CHIA is Massachusetts inpatient;",
    "# CADR is workload GIVEN TREATMENT in a 2008-2016 Medicare cohort, so its",
    "# 5,566 sling episodes are not an annual national sling volume.",
    "# ---------------------------------------------------------------------------",
    "validation_anchors:")
  for (row_index in base::seq_len(base::nrow(registry))) {
    block <- c(block,
      base::sprintf("  %s:", registry$anchor_id[[row_index]]),
      base::sprintf('    source: "%s"', registry$source[[row_index]]),
      base::sprintf('    path: "%s"', registry$path[[row_index]]),
      base::sprintf('    sha256: "%s"', registry$sha256[[row_index]]),
      base::sprintf('    calibration_role: "%s"',
                    registry$calibration_role[[row_index]]),
      "    production_scalar_eligible: false")
  }

  base::writeLines(c(lines, block), config_path)
  base::message("Validation registry written to config.")
  base::invisible(config)
}

#' Build the production-anchor readiness and scalar report
#' @param model_predictions Optional tibble with `anchor_id` and
#'   `model_prediction`. NOTE: any scalar computed here is named
#'   `illustrative_smoke_test_scalar` and is NOT a production calibration
#'   scalar. A production scalar requires provenance on BOTH sides of the
#'   division and must go through
#'   [compute_production_scalar()]; a bare numeric cannot produce one.
#' @param config_path Calibration YAML.
#' @return Anchor-readiness report. Contains no production scalar.
.build_production_calibration_report <- function(
    model_predictions = NULL,
    config_path = "config/calibration_targets.yml") {

  base::message("Building ANCHOR READINESS report.")
  base::message("  Any scalar below is an illustrative smoke test, not a ",
                "production calibration scalar. Production scalars require ",
                "provenance on both sides and go through ",
                "compute_production_scalar().")
  config <- yaml::read_yaml(config_path)

  if (!base::is.null(model_predictions)) {
    .require_columns(model_predictions, c("anchor_id", "model_prediction"),
                     "Model predictions")
  }

  max_scalar <- base::as.numeric(config$max_scalar)

  rows <- base::lapply(base::names(config$anchors), function(anchor_name) {
    specification <- config$anchors[[anchor_name]]
    anchor_path   <- specification$path
    expected_hash <- specification$sha256
    if (base::is.null(expected_hash)) expected_hash <- ""

    exists        <- base::file.exists(anchor_path)
    observed_hash <- if (exists) .sha256_file(anchor_path) else NA_character_
    hash_declared <- base::nzchar(expected_hash)
    hash_matches  <- exists && hash_declared &&
                     base::identical(observed_hash, expected_hash)
    target_value  <- if (exists) .read_production_target(anchor_path) else NA_real_

    prediction <- NA_real_
    if (!base::is.null(model_predictions)) {
      prediction_row <- model_predictions |>
        dplyr::filter(.data$anchor_id == anchor_name)
      if (base::nrow(prediction_row) == 1L) {
        prediction <- prediction_row$model_prediction[[1L]]
      }
    }

    # Clinical review is a PRECONDITION, not metadata. An anchor whose
    # procedure-family definitions have not been reviewed cannot produce a
    # scalar even if its file is present and its hash matches.
    # Anchor-specific: enforces only the dependencies this anchor names.
    review_ok <- !base::inherits(base::try(
      assert_anchor_reviewed(specification), silent = TRUE), "try-error")

    ready  <- exists && hash_declared && hash_matches && review_ok
    scalar <- if (ready && base::is.finite(prediction) && prediction > 0) {
      target_value / prediction
    } else NA_real_

    calibrated_prediction <- if (base::is.finite(scalar)) {
      prediction * scalar
    } else NA_real_

    direction <- dplyr::case_when(
      !base::is.finite(scalar) ~ "not_calculated",
      scalar > 1               ~ "model_prediction_below_anchor",
      scalar < 1               ~ "model_prediction_above_anchor",
      TRUE                     ~ "model_prediction_equals_anchor")

    structural_flag <- base::is.finite(scalar) &&
      (scalar > max_scalar || scalar < 1 / max_scalar)

    tibble::tibble(
      anchor_id = anchor_name, path = anchor_path, present = exists,
      expected_sha256 = expected_hash, observed_sha256 = observed_hash,
      hash_declared = hash_declared, hash_matches = hash_matches,
      target = target_value, model_prediction = prediction,
      illustrative_smoke_test_scalar = scalar,
      calibrated_prediction = calibrated_prediction, direction = direction,
      structural_mismatch_flag = structural_flag,
      clinical_review_ok = review_ok, production_ready = ready)
  })

  dplyr::bind_rows(rows)
}

#' Build empirical URPS calibration targets and validation evidence
#'
#' CHIA, CADR, Lizeth, and Rabice are deliberately kept in their valid empirical
#' roles. Only independently declared production anchors are permitted to
#' generate HDMM-style national calibration scalars.
#'
#' @param chia_db Path to chia_cadr.duckdb.
#' @param lizeth_dir Local Lizeth repository.
#' @param cadr_path CADR workload artifact.
#' @param model_predictions Optional tibble with `anchor_id` and
#'   `model_prediction`.
#' @param anchor_dir Directory for empirical anchor artifacts.
#' @param artifact_dir Directory for calibration reports.
#' @param config_path Calibration YAML.
#' @param expected_chia_pop_hysterectomy CHIA regression guard (17,172).
#' @param freeze_existing_hashes Whether to lock present production anchors.
#' @param strict_production_gate Stop if any production anchor is unresolved.
#' @return Named list of evidence, registry, and calibration report.
#' @export
build_empirical_calibration_targets <- function(
    chia_db = "/Volumes/MufflySamsung/DuckDB/chia_cadr.duckdb",
    lizeth_dir = "../lizeth",
    cadr_path = base::file.path("scripts", "cadr", "outputs",
                                "workload_per_treated_patient.csv"),
    model_predictions = NULL,
    anchor_dir = base::file.path("data", "anchors", "validation"),
    artifact_dir = base::file.path("artifacts", "calibration"),
    config_path = base::file.path("config", "calibration_targets.yml"),
    expected_chia_pop_hysterectomy = 17676L,
    freeze_existing_hashes = TRUE,
    strict_production_gate = FALSE) {

  base::message("========================================")
  base::message("EMPIRICAL URPS CALIBRATION BUILD")
  base::message("========================================")

  .require_empirical_packages()
  if (!base::exists("assert_production_scalar_eligible")) {
    base::source(base::file.path("R", "calibration-clinical_review_gate.R"))
  }
  base::message("Loading current simulation source checkout.")
  pkgload::load_all(".", quiet = TRUE)

  base::message("Lizeth repository: ", lizeth_dir)
  base::message("CADR artifact: ", cadr_path)
  base::message("CHIA database: ", chia_db)

  stamp <- base::format(base::Sys.time(), "%Y%m%d_%H%M%S")
  base::dir.create(anchor_dir, recursive = TRUE, showWarnings = FALSE)
  base::dir.create(artifact_dir, recursive = TRUE, showWarnings = FALSE)

  access_evidence <- .build_mystery_caller_validation(lizeth_dir = lizeth_dir)
  cadr_evidence   <- .build_cadr_validation(cadr_path = cadr_path)
  chia_evidence   <- .build_chia_validation(
    db = chia_db, expected_total = expected_chia_pop_hysterectomy)

  lizeth_file <- .write_hashed_csv(access_evidence$lizeth_overall,
                                   anchor_dir, "lizeth_access", stamp)
  lizeth_insurance_file <- .write_hashed_csv(access_evidence$lizeth_by_insurance,
                                   anchor_dir, "lizeth_access_by_insurance", stamp)
  rabice_file <- .write_hashed_csv(access_evidence$rabice,
                                   anchor_dir, "rabice_wait", stamp)
  cadr_file   <- .write_hashed_csv(cadr_evidence,
                                   anchor_dir, "cadr_service_intensity", stamp)
  chia_file   <- .write_hashed_csv(chia_evidence$anchor,
                                   anchor_dir, "chia_pop_hysterectomy", stamp)
  chia_annual_file <- .write_hashed_csv(chia_evidence$annual,
                                   anchor_dir, "chia_pop_hysterectomy_annual", stamp)

  registry <- tibble::tibble(
    anchor_id = c("lizeth_access", "lizeth_access_by_insurance", "rabice_wait",
                  "cadr_service_intensity", "chia_pop_hysterectomy"),
    source = c("Lizeth national URPS mystery caller",
               "Lizeth national URPS mystery caller",
               "Rabice national URPS mystery caller",
               "CADR Medicare", "CHIA Massachusetts"),
    path = c(lizeth_file$path, lizeth_insurance_file$path, rabice_file$path,
             cadr_file$path, chia_file$path),
    sha256 = c(lizeth_file$sha256, lizeth_insurance_file$sha256,
               rabice_file$sha256, cadr_file$sha256, chia_file$sha256),
    calibration_role = c("access_output_validation", "payer_access_validation",
                         "access_output_validation",
                         "treated_patient_service_intensity",
                         "regional_demand_validation"),
    production_scalar_eligible = FALSE)

  registry_file <- .write_hashed_csv(registry, artifact_dir,
                                     "empirical_validation_anchor_registry", stamp)

  if (base::isTRUE(freeze_existing_hashes)) {
    .freeze_existing_anchor_hashes(config_path = config_path, stamp = stamp)
  }
  .register_validation_anchors(config_path = config_path, registry = registry)

  calibration_report <- .build_production_calibration_report(
    model_predictions = model_predictions, config_path = config_path)
  calibration_file <- .write_hashed_csv(calibration_report, artifact_dir,
                                        "production_calibration_report", stamp)

  unresolved <- calibration_report |> dplyr::filter(!production_ready)
  if (base::nrow(unresolved) > 0L) {
    base::message("UNRESOLVED production calibration anchors: ",
                  base::paste(unresolved$anchor_id, collapse = ", "))
  } else {
    base::message("All production calibration anchors are locked.")
  }

  chia_years <- chia_evidence$annual_summary
  empirical_summary <- base::paste0(
    access_evidence$summary_sentence,
    " CHIA contributed Massachusetts all-payer POP-hysterectomy utilization",
    if (base::is.finite(chia_years$year_start)) {
      base::sprintf(" from %d-%d", chia_years$year_start, chia_years$year_end)
    } else "",
    "; CADR contributed treated-Medicare service intensity from 2008-2016. ",
    "Neither CHIA nor CADR was allowed to masquerade as a national all-payer ",
    "annual volume target.")

  base::message(empirical_summary)
  base::message("Validation registry: ",
                base::normalizePath(registry_file$path, mustWork = TRUE))
  base::message("Calibration report: ",
                base::normalizePath(calibration_file$path, mustWork = TRUE))

  if (base::isTRUE(strict_production_gate) && base::nrow(unresolved) > 0L) {
    base::stop("Production calibration gate failed. Missing, unlocked, or ",
               "hash-mismatched anchor(s): ",
               base::paste(unresolved$anchor_id, collapse = ", "), call. = FALSE)
  }

  base::message("EMPIRICAL URPS CALIBRATION BUILD COMPLETE")

  base::list(
    lizeth = access_evidence, cadr = cadr_evidence, chia = chia_evidence,
    validation_registry = registry, production_calibration = calibration_report,
    summary_sentence = empirical_summary,
    saved_files = c(lizeth = lizeth_file$path,
                    lizeth_insurance = lizeth_insurance_file$path,
                    rabice = rabice_file$path, cadr = cadr_file$path,
                    chia = chia_file$path, chia_annual = chia_annual_file$path,
                    registry = registry_file$path,
                    calibration = calibration_file$path))
}
