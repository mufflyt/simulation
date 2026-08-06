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
#' @param allow_uncalibrated Declare an exploratory export. Defaults to FALSE, so
#'   the contract is refused unless `calibration_status` is `"fitted"` or
#'   `"calibrated"`; see [assert_calibrated_transitions()]. The default status
#'   here is `"uncalibrated_illustrative"`, so this path requires the override
#'   until the transition model is validated.
#' @param verbose Log progress. Default TRUE.
#' @return (invisibly) a list with `csv_path`, `manifest_path`, and the tidy `data`.
export_dpmm_demand_contract <- function(dpmm_results,
                                        output_directory,
                                        model_version = DPMM_DEMAND_CONTRACT_VERSION,
                                        base_year = 2025L,
                                        calendar_year_offset = 2024L,
                                        us_women_40plus = 85e6,
                                        calibration_status = "uncalibrated_illustrative",
                                        allow_uncalibrated = FALSE,
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
  # Gated before dir.create() for the same reason as the HDMM/DMDM exporters.
  assert_calibrated_transitions(
    list(status = calibration_status),
    allow_uncalibrated = allow_uncalibrated,
    what = "DPMM demand-contract tiers")

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

# =============================================================================
# HDMM life-course demand contract (tiers 5-6: care-seeking + procedural)
# =============================================================================
# The DPMM exporter above serves tiers 3-4 (prevalence / symptomatic). The
# reproductive life-course demand model (R/demand-lifecourse.R) carries the
# demand hierarchy further down the care pathway and emits tiers 5-6 into the
# SAME contract schema, so a downstream consumer (e.g. cliff) reads any tier
# through one generic seam:
#
#   tier5_care_seeking  <- expected national women seeking pelvic-floor care
#   tier6_procedural    <- expected national annual specialty service units
#
# Fed by lifecourse_demand_trajectory()$demand_summary. Base R (matching the DPMM
# exporter), so the formatting is testable without the tidyverse.

HDMM_DEMAND_CONTRACT_VERSION <- "0.1.0"

#' Export the HDMM life-course demand trajectory as contract tiers 5-6
#'
#' @description Formats a national life-course demand summary into the shared
#'   demand-contract schema (the same columns as [export_dpmm_demand_contract()]),
#'   emitting `tier5_care_seeking` and `tier6_procedural` indexed to the base year,
#'   plus a JSON provenance manifest.
#'
#' @param trajectory Data frame with `year`, `care_seeking_national`, and
#'   `service_units_national` (the `demand_summary` from
#'   [lifecourse_demand_trajectory()]).
#' @param output_directory Directory to write into (created if missing).
#' @param model_version Version string stamped on every row and the manifest.
#'   Defaults to `HDMM_DEMAND_CONTRACT_VERSION`.
#' @param base_year Calendar year the index is normalized to (= 100). Default 2025.
#' @param scenario Scenario label recorded in the manifest.
#' @param calibration_status Provenance guard; keep "placeholder_uncalibrated"
#'   until the obstetric/urogynecologic transition equations are fitted.
#' @param population_scope Population denominator label. Default "us_adult_women".
#' @param allow_uncalibrated Declare an exploratory export. Defaults to FALSE, so
#'   the contract is refused unless `calibration_status` is `"fitted"` or
#'   `"calibrated"`; see [assert_calibrated_transitions()]. The HDMM path carries
#'   one status for the whole artifact rather than per-tier provenance.
#' @param verbose Log progress. Default TRUE.
#' @return (invisibly) a list with `csv_path`, `manifest_path`, and the tidy `data`.
#' @keywords internal
export_hdmm_demand_contract <- function(trajectory,
                                        output_directory,
                                        model_version = HDMM_DEMAND_CONTRACT_VERSION,
                                        base_year = 2025L,
                                        scenario = "baseline",
                                        calibration_status = "placeholder_uncalibrated",
                                        population_scope = "us_adult_women",
                                        allow_uncalibrated = FALSE,
                                        verbose = TRUE) {
  need <- c("year", "care_seeking_national", "service_units_national")
  if (!is.data.frame(trajectory) || !all(need %in% names(trajectory))) {
    stop("export_hdmm_demand_contract(): trajectory needs columns ",
         paste(need, collapse = ", "), ".", call. = FALSE)
  }
  # Same gate as the DMDM exporter, and for the same reason: this writes a
  # publication-facing artifact from a bare data frame. Gated before dir.create()
  # so a refused export leaves nothing behind.
  assert_calibrated_transitions(
    list(status = calibration_status),
    allow_uncalibrated = allow_uncalibrated,
    what = "HDMM demand-contract tiers")

  if (!dir.exists(output_directory)) dir.create(output_directory, recursive = TRUE)
  trajectory <- trajectory[order(trajectory$year), , drop = FALSE]

  # Optional lo/hi columns (from lifecourse_demand_trajectory_ci) become
  # national_cases_lo/hi and denominator_index_lo/hi; absent -> NA. All indices
  # are rebased to the SAME median base-year value so the band is consistent.
  getcol <- function(nm) if (nm %in% names(trajectory)) trajectory[[nm]] else NULL
  make_tier <- function(tier, value_vec, lo_vec = NULL, hi_vec = NULL) {
    base_row <- which(trajectory$year == base_year)
    base_val <- if (length(base_row)) value_vec[base_row[1]] else NA_real_
    nyr <- length(value_vec)
    if (is.null(lo_vec)) lo_vec <- rep(NA_real_, nyr)
    if (is.null(hi_vec)) hi_vec <- rep(NA_real_, nyr)
    idx <- function(v) if (!is.na(base_val) && base_val > 0) 100 * v / base_val else rep(NA_real_, length(v))
    data.frame(
      model                = "HDMM",
      model_version        = model_version,
      calibration_status   = calibration_status,
      geography            = "national",
      population_scope     = population_scope,
      denominator_tier     = tier,
      calendar_year        = trajectory$year,
      prevalence           = NA_real_,
      prevalence_lo        = NA_real_,
      prevalence_hi        = NA_real_,
      national_cases       = value_vec,
      national_cases_lo    = lo_vec,
      national_cases_hi    = hi_vec,
      denominator_index    = idx(value_vec),
      denominator_index_lo = idx(lo_vec),
      denominator_index_hi = idx(hi_vec),
      stringsAsFactors     = FALSE
    )
  }

  tidy <- rbind(
    make_tier("tier5_care_seeking", trajectory$care_seeking_national,
              getcol("care_seeking_national_lo"), getcol("care_seeking_national_hi")),
    make_tier("tier6_procedural",   trajectory$service_units_national,
              getcol("service_units_national_lo"), getcol("service_units_national_hi"))
  )
  tidy <- tidy[order(tidy$denominator_tier, tidy$calendar_year), , drop = FALSE]

  csv_path <- file.path(output_directory,
                        sprintf("hdmm_demand_contract_v%s.csv", model_version))
  utils::write.csv(tidy, csv_path, row.names = FALSE)

  csv_hash <- tryCatch(unname(tools::md5sum(csv_path)), error = function(e) NA_character_)
  manifest <- list(
    artifact             = basename(csv_path),
    model                = "HDMM",
    model_version        = model_version,
    scenario             = scenario,
    calibration_status   = calibration_status,
    generated_at         = as.character(Sys.time()),
    base_year            = base_year,
    calendar_years       = range(tidy$calendar_year),
    tiers                = sort(unique(tidy$denominator_tier)),
    n_rows               = nrow(tidy),
    csv_md5              = csv_hash,
    source_model_file    = "R/demand-lifecourse.R",
    contract_reference   = "URPS improvement plan 2026-07-30, sec 10 & 16; Zarek 2025 architecture",
    downstream_consumers = c("cliff", "twostep", "isochrones"),
    notes = paste("Reproductive life-course demand (vaginal-delivery exposure ->",
                  "pelvic-floor disease -> care pathway -> service use). Coefficient",
                  "tables are placeholders; downstream must gate on calibration_status",
                  "and keep validated published-anchor denominators as the default",
                  "until this reads 'calibrated'.")
  )
  manifest_path <- file.path(output_directory,
                             sprintf("hdmm_demand_contract_v%s_manifest.json", model_version))
  if (requireNamespace("jsonlite", quietly = TRUE)) {
    jsonlite::write_json(manifest, manifest_path, auto_unbox = TRUE, pretty = TRUE, null = "null")
  } else {
    writeLines(paste(names(unlist(manifest)), unlist(manifest), sep = ": "), manifest_path)
  }

  if (verbose) {
    msg <- sprintf("Wrote HDMM demand contract v%s (%d rows, tiers 5-6, %s): %s",
                   model_version, nrow(tidy), calibration_status, csv_path)
    .msg_info(msg)
  }

  invisible(list(csv_path = csv_path, manifest_path = manifest_path, data = tidy))
}

# =============================================================================
# DMDM dynamic-prevalence contract bridge (tiers 3 + per-condition)
# =============================================================================
# The dynamic multistate model (demand-dynamic_multistate / demand-dynamic_open) produces year-by-year population
# prevalence. This bridges that output into the shared demand-contract schema so
# it flows downstream (cliff) exactly like the DPMM/HDMM exporters. It emits:
#   tier3_prevalent_pfd  <- any-PFD prevalence = 1 - (1-ui)(1-pop)(1-ai)
#                           (independence approximation across conditions)
#   dmdm_ui / dmdm_pop / dmdm_ai  <- per-condition population prevalence
# so a consumer can read any tier via read_dpmm_demand_contract() +
# dpmm_alt_d1_index(tier = ...). Base R, matching the other exporters.

DMDM_DEMAND_CONTRACT_VERSION <- "0.1.0"

# The tiers every DMDM contract export writes. Named here so the calibration
# gate below and make_tier() below that cannot drift apart: a tier added to one
# and not the other would ship ungated numbers.
DMDM_CONTRACT_TIERS <- c("tier3_prevalent_pfd", "dmdm_ui", "dmdm_pop", "dmdm_ai")

#' Export a DMDM open-population trajectory as demand-contract tiers
#'
#' @description Formats a dynamic-prevalence trajectory (from
#'   [dmdm_open_prevalence_trajectory()] / [simulate_dmdm_open()]) into the shared
#'   demand-contract schema, emitting `tier3_prevalent_pfd` (any-PFD) plus
#'   per-condition `dmdm_ui`/`dmdm_pop`/`dmdm_ai`, indexed to the base year, with a
#'   JSON provenance manifest.
#'
#' @param trajectory Data frame with `year`, `population`, `prev_ui`, `prev_pop`,
#'   `prev_ai`.
#' @param output_directory Directory to write into (created if missing).
#' @param model_version Version string. Default `DMDM_DEMAND_CONTRACT_VERSION`.
#' @param base_year Calendar year the index is normalized to (= 100). Default 2025.
#' @param calibration_status Provenance guard. Keep "placeholder_uncalibrated"
#'   until the onset/remission hazards are fitted (see [fit_dmdm_transitions()]).
#'   Ignored (and derived from `transitions`) when `transitions` carries its own
#'   `calibration_status`.
#' @param transitions Optional transition object actually used to produce
#'   `trajectory` (e.g. [dmdm_transitions_with_pop_literature()]). When supplied,
#'   its `calibration_status` becomes the object-level status and its per-condition
#'   `provenance` is stamped per tier in a `tier_calibration_status` column, so a
#'   downstream consumer can gate on the provenance of the specific tier it reads
#'   (e.g. `dmdm_pop` = "derived_by_analogy" while `dmdm_ui`/`dmdm_ai` stay
#'   placeholder). The any-PFD `tier3` is stamped with the *weakest* provenance
#'   across the conditions that compose it.
#' @param population_scope Population denominator label. Default "us_adult_women".
#' @param allow_uncalibrated Declare an exploratory export. Defaults to FALSE, so
#'   the contract is refused unless every tier it would write is `"fitted"` or
#'   `"calibrated"`; see [assert_calibrated_transitions()]. This mirrors the gate
#'   on the engines that produce `trajectory` -- without it, a hand-assembled
#'   trajectory could still reach downstream consumers as a demand contract.
#' @param verbose Log progress. Default TRUE.
#' @return (invisibly) a list with `csv_path`, `manifest_path`, and the tidy `data`.
#' @keywords internal
export_dmdm_demand_contract <- function(trajectory,
                                        output_directory,
                                        model_version = DMDM_DEMAND_CONTRACT_VERSION,
                                        base_year = 2025L,
                                        calibration_status = "placeholder_uncalibrated",
                                        transitions = NULL,
                                        population_scope = "us_adult_women",
                                        allow_uncalibrated = FALSE,
                                        verbose = TRUE) {
  need <- c("year", "population", "prev_ui", "prev_pop", "prev_ai")
  if (!is.data.frame(trajectory) || !all(need %in% names(trajectory))) {
    stop("export_dmdm_demand_contract(): trajectory needs columns ",
         paste(need, collapse = ", "), ".", call. = FALSE)
  }
  trajectory <- trajectory[order(trajectory$year), , drop = FALSE]

  # Provenance: object-level status + per-condition status from `transitions`.
  # A tier is only as trustworthy as its weakest input, so tier3 (any-PFD) takes
  # the weakest of the three conditions' statuses.
  status_rank <- c(placeholder_uncalibrated = 1L, uncalibrated_illustrative = 1L,
                   derived_by_analogy = 2L, fitted = 3L, calibrated = 4L)
  weakest <- function(s) {
    r <- status_rank[s]; r[is.na(r)] <- 0L
    s[which.min(r)]
  }
  prov <- if (!is.null(transitions) && !is.null(transitions$provenance))
    transitions$provenance else NULL
  if (!is.null(transitions) && !is.null(transitions$calibration_status))
    calibration_status <- transitions$calibration_status
  or_default <- function(x, d) if (is.null(x)) d else x
  tier_status <- function(tier) {
    if (is.null(prov)) return(calibration_status)
    switch(tier,
           dmdm_ui  = or_default(prov$ui,  calibration_status),
           dmdm_pop = or_default(prov$pop, calibration_status),
           dmdm_ai  = or_default(prov$ai,  calibration_status),
           tier3_prevalent_pfd = weakest(c(prov$ui, prov$pop, prov$ai)),
           calibration_status)
  }

  # Fail closed before writing anything. The engines in R/demand-dynamic_multistate and R/demand-dynamic_open already
  # refuse uncalibrated transitions, but this function takes a bare data frame,
  # so a caller who assembles a trajectory by hand -- or holds one produced
  # before the gate existed -- could still emit a CSV that downstream consumers
  # (cliff, twostep, isochrones) read as a demand contract. Stamping the status
  # into the file is not enough: the numbers still leave the function, and a
  # consumer that does not gate on tier_calibration_status reads them as real.
  #
  # Gate on the WEAKEST status written, not the object-level one: the CSV is a
  # single artifact, and any row in it is a number someone can lift. The
  # per-tier column stays useful for consumers that do discriminate.
  # The directory is created only after this passes, so a refused export leaves
  # no empty artifact directory behind.
  contract_statuses <- c(calibration_status,
                         vapply(DMDM_CONTRACT_TIERS, tier_status, character(1)))
  assert_calibrated_transitions(
    list(status = unname(weakest(contract_statuses))),
    allow_uncalibrated = allow_uncalibrated,
    what = "DMDM demand-contract tiers")

  if (!dir.exists(output_directory)) dir.create(output_directory, recursive = TRUE)

  any_pfd <- 1 - (1 - trajectory$prev_ui) * (1 - trajectory$prev_pop) * (1 - trajectory$prev_ai)
  make_tier <- function(tier, prev_vec) {
    base_row <- which(trajectory$year == base_year)
    base_val <- if (length(base_row)) prev_vec[base_row[1]] else NA_real_
    idx <- if (!is.na(base_val) && base_val > 0) 100 * prev_vec / base_val
           else rep(NA_real_, length(prev_vec))
    data.frame(
      model                  = "DMDM",
      model_version          = model_version,
      calibration_status     = calibration_status,
      tier_calibration_status = tier_status(tier),
      geography              = "national",
      population_scope       = population_scope,
      denominator_tier       = tier,
      calendar_year          = trajectory$year,
      prevalence             = prev_vec,
      prevalence_lo          = NA_real_,
      prevalence_hi          = NA_real_,
      national_cases         = trajectory$population * prev_vec,
      national_cases_lo      = NA_real_,
      national_cases_hi      = NA_real_,
      denominator_index      = idx,
      denominator_index_lo   = NA_real_,
      denominator_index_hi   = NA_real_,
      stringsAsFactors       = FALSE
    )
  }

  tidy <- rbind(
    make_tier("tier3_prevalent_pfd", any_pfd),
    make_tier("dmdm_ui",  trajectory$prev_ui),
    make_tier("dmdm_pop", trajectory$prev_pop),
    make_tier("dmdm_ai",  trajectory$prev_ai)
  )
  tidy <- tidy[order(tidy$denominator_tier, tidy$calendar_year), , drop = FALSE]

  csv_path <- file.path(output_directory,
                        sprintf("dmdm_demand_contract_v%s.csv", model_version))
  utils::write.csv(tidy, csv_path, row.names = FALSE)

  csv_hash <- tryCatch(unname(tools::md5sum(csv_path)), error = function(e) NA_character_)
  manifest <- list(
    artifact             = basename(csv_path),
    model                = "DMDM",
    model_version        = model_version,
    calibration_status   = calibration_status,
    generated_at         = as.character(Sys.time()),
    base_year            = base_year,
    calendar_years       = range(tidy$calendar_year),
    tiers                = sort(unique(tidy$denominator_tier)),
    tier_calibration_status = as.list(stats::setNames(
      tidy$tier_calibration_status[!duplicated(tidy$denominator_tier)],
      tidy$denominator_tier[!duplicated(tidy$denominator_tier)])),
    n_rows               = nrow(tidy),
    csv_md5              = csv_hash,
    source_model_file    = "R/demand-dynamic_open.R",
    contract_reference   = "URPS improvement plan 2026-07-30, sec 10 & 16; DMDM (IP sec 9)",
    downstream_consumers = c("cliff", "twostep", "isochrones"),
    notes = paste("Dynamic multistate prevalence (onset/remission/death over the",
                  "obstetric life course). tier3 any-PFD uses an independence",
                  "approximation across conditions. When produced from the",
                  "literature POP transitions, dmdm_pop is derived_by_analogy",
                  "while dmdm_ui/dmdm_ai remain placeholders; downstream must gate",
                  "on tier_calibration_status for the tier it reads.")
  )
  manifest_path <- file.path(output_directory,
                             sprintf("dmdm_demand_contract_v%s_manifest.json", model_version))
  if (requireNamespace("jsonlite", quietly = TRUE)) {
    jsonlite::write_json(manifest, manifest_path, auto_unbox = TRUE, pretty = TRUE, null = "null")
  } else {
    writeLines(paste(names(unlist(manifest)), unlist(manifest), sep = ": "), manifest_path)
  }

  if (verbose) {
    msg <- sprintf("Wrote DMDM demand contract v%s (%d rows, %s): %s",
                   model_version, nrow(tidy), calibration_status, csv_path)
    if (exists(".msg_info", mode = "function")) .msg_info(msg) else message(msg)
  }

  invisible(list(csv_path = csv_path, manifest_path = manifest_path, data = tidy))
}
