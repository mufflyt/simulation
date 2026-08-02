# =============================================================================
# DPMM -> downstream demand-hierarchy contract exporter
# =============================================================================
# Emits the DPMM prevalence microsimulation as a tidy, VERSIONED artifact that
# downstream repositories (cliff, twostep, isochrones) consume instead of
# rebuilding the epidemiology themselves. This implements the target contract in
# the URPS microsimulation improvement plan (2026-07-30): sec 10 (demand-
# denominator hierarchy, tiers 3-4) and sec 16 (versioned outputs + provenance
# manifest).
#
# WHAT THIS IS (and is not):
#   - This transforms an existing DPMM results object into a tidy CSV + JSON
#     manifest. It does NOT run the simulation and does NOT calibrate it.
#   - The current DPMM transition probabilities are placeholders (see README),
#     so every row is stamped calibration_status = "uncalibrated_illustrative".
#     Downstream consumers MUST gate on that flag and keep their validated
#     published-anchor denominators as the default until it flips to "calibrated".
#
# Contract mapping (improvement plan sec 10):
#   tier3_prevalent_pfd  <- incontinence_prevalence      (any-incontinence proxy)
#   tier4_symptomatic    <- moderate_plus_prevalence     (moderate-or-worse)
#   (tier5 care-seeking / tier6 procedural are NOT emitted: the model does not
#    yet produce them; see improvement plan sec 10 and README sec "Integrate
#    healthcare utilization".)
# =============================================================================

DPMM_DEMAND_CONTRACT_VERSION <- "0.1.0"

#' Export the DPMM demand trajectory as a versioned downstream contract
#'
#' @description Turns a DPMM results object into (1) a tidy long CSV of demand
#'   denominators by calendar year and hierarchy tier, indexed to the base year,
#'   and (2) a JSON provenance manifest. Written to `output_directory` as
#'   `dpmm_demand_contract_v<version>.csv` and `..._manifest.json`.
#'
#' @param dpmm_results List returned by the DPMM analysis; must contain
#'   `composite_scores$population_statistics` with columns `year`,
#'   `living_population`, `incontinence_prevalence`, and (optionally)
#'   `moderate_plus_prevalence`, `severe_incontinence_prevalence`.
#' @param output_directory Directory to write into (created if missing).
#' @param model_version Version string stamped on every row and the manifest.
#'   Defaults to `DPMM_DEMAND_CONTRACT_VERSION`.
#' @param base_year Calendar year the index is normalized to (= 100). Default 2025.
#' @param calendar_year_offset `calendar_year = year + offset`. The DPMM encodes
#'   sim `year` starting at 1 for calendar 2025, so offset = 2024. Default 2024.
#' @param us_women_40plus National denominator used to scale prevalence to case
#'   counts (persons). Default 85e6, matching the DPMM national scale-up.
#' @param calibration_status Provenance guard for downstream gating. Keep
#'   "uncalibrated_illustrative" until the transition model is validated.
#' @param verbose Log progress. Default TRUE.
#' @return (invisibly) a list with `csv_path`, `manifest_path`, and the tidy `data`.
export_dpmm_demand_contract <- function(dpmm_results,
                                        output_directory,
                                        model_version = DPMM_DEMAND_CONTRACT_VERSION,
                                        base_year = 2025L,
                                        calendar_year_offset = 2024L,
                                        us_women_40plus = 85e6,
                                        calibration_status = "uncalibrated_illustrative",
                                        verbose = TRUE) {

  ps <- dpmm_results$composite_scores$population_statistics
  if (is.null(ps) || !nrow(ps)) {
    stop("export_dpmm_demand_contract(): composite_scores$population_statistics is empty.",
         call. = FALSE)
  }
  if (!all(c("year", "incontinence_prevalence") %in% names(ps))) {
    stop("export_dpmm_demand_contract(): population_statistics needs `year` and ",
         "`incontinence_prevalence`.", call. = FALSE)
  }
  if (!dir.exists(output_directory)) {
    dir.create(output_directory, recursive = TRUE)
  }

  has_col <- function(nm) nm %in% names(ps)
  # living_population drives the national case scale-up; fall back to prevalence-only.
  living_ok <- has_col("living_population") && any(!is.na(ps$living_population))
  living <- if (living_ok) ps$living_population else NA_real_
  scaling_factor <- if (living_ok) us_women_40plus / max(living, na.rm = TRUE) else NA_real_

  # --- build one tidy block per emitted tier ---------------------------------
  make_tier <- function(tier, prevalence_vec) {
    calendar_year <- ps$year + calendar_year_offset
    base_row <- which(calendar_year == base_year)
    base_prev <- if (length(base_row)) prevalence_vec[base_row[1]] else NA_real_
    data.frame(
      model                = "DPMM",
      model_version        = model_version,
      calibration_status   = calibration_status,
      geography            = "national",
      population_scope     = "us_women_40plus",
      denominator_tier     = tier,
      calendar_year        = calendar_year,
      prevalence           = prevalence_vec,
      # No per-simulation spread is carried in population_statistics yet, so CIs
      # are NA (improvement plan sec 5 - parameter uncertainty - is future work).
      prevalence_lo        = NA_real_,
      prevalence_hi        = NA_real_,
      national_cases       = if (living_ok)
                               living * prevalence_vec * scaling_factor else NA_real_,
      # 2025 = 100 index: this is the column cliff consumes as an alternative D1.
      denominator_index    = if (!is.na(base_prev) && base_prev > 0)
                               100 * prevalence_vec / base_prev else NA_real_,
      stringsAsFactors     = FALSE
    )
  }

  blocks <- list(make_tier("tier3_prevalent_pfd", ps$incontinence_prevalence))
  if (has_col("moderate_plus_prevalence")) {
    blocks[[length(blocks) + 1L]] <- make_tier("tier4_symptomatic", ps$moderate_plus_prevalence)
  }
  tidy <- do.call(rbind, blocks)
  tidy <- tidy[order(tidy$denominator_tier, tidy$calendar_year), , drop = FALSE]

  # --- write CSV -------------------------------------------------------------
  csv_path <- file.path(output_directory,
                        sprintf("dpmm_demand_contract_v%s.csv", model_version))
  utils::write.csv(tidy, csv_path, row.names = FALSE)

  # --- provenance manifest (improvement plan sec 16) -------------------------
  csv_hash <- tryCatch(unname(tools::md5sum(csv_path)), error = function(e) NA_character_)
  manifest <- list(
    artifact              = basename(csv_path),
    model                 = "DPMM",
    model_version         = model_version,
    calibration_status    = calibration_status,
    generated_at          = as.character(Sys.time()),
    base_year             = base_year,
    calendar_years        = range(tidy$calendar_year),
    tiers                 = sort(unique(tidy$denominator_tier)),
    n_rows                = nrow(tidy),
    csv_md5               = csv_hash,
    scaling               = list(us_women_40plus = us_women_40plus,
                                 scaling_factor = scaling_factor),
    source_model_file     = "R/05-dppm_50_year_national_incontinence.R",
    contract_reference    = "URPS microsimulation improvement plan 2026-07-30, sec 10 & 16",
    downstream_consumers  = c("cliff", "twostep", "isochrones"),
    notes = paste("Transition probabilities are placeholders; prevalence is",
                  "illustrative, not validated. Downstream must gate on",
                  "calibration_status and keep published-anchor denominators as",
                  "the default until this reads 'calibrated'.")
  )
  manifest_path <- file.path(output_directory,
                             sprintf("dpmm_demand_contract_v%s_manifest.json", model_version))
  if (requireNamespace("jsonlite", quietly = TRUE)) {
    jsonlite::write_json(manifest, manifest_path, auto_unbox = TRUE, pretty = TRUE, null = "null")
  } else {
    # Minimal fallback so the manifest still exists without jsonlite.
    writeLines(paste(names(unlist(manifest)), unlist(manifest), sep = ": "), manifest_path)
  }

  if (verbose) {
    msg <- sprintf("Wrote demand contract v%s (%d rows, %s) + manifest: %s",
                   model_version, nrow(tidy), calibration_status, csv_path)
    .msg_info(msg)
  }

  invisible(list(csv_path = csv_path, manifest_path = manifest_path, data = tidy))
}
