# Practice-economics payer mix ------------------------------------------
#
# simulate_practice_economics() (R/supply-practice_economics.R) needs
# medicare_share/medicaid_share/commercial_share/self_pay_share per
# practice-year, summing to 1. No real payer-mix number existed anywhere in
# this repo -- R/data-practice_survey.R logs payer_mix_constraints as an
# unresolved item requiring a fielded survey. This file supplies a real,
# cited estimate from two secondary sources instead, per a data-acquisition
# review of CHIA, CMS/HRSA, AHRQ 3P-RD, and NAMCS (2026-08-22):
#
# NAMCS is the PRIMARY source for all four shares. It is the only one of the
# candidates that is both real, national, and filterable to URPS diagnosis
# codes (reusing flag_urps_visits()/URPS_ICD10_PREFIXES, the same filter
# namcs_urps_visit_anchor() already uses). AHRQ 3P-RD's Physician Geographic
# file (see data-raw/ahrq_3prd/01-ahrq_3prd_acquire.R) has real Medicare/
# Medicaid claims-volume data, but it cannot be filtered to a specialty --
# its payer columns are ZIP3-level aggregates across every physician in the
# 13-state sample. Comparing the two directly found they disagree
# substantially (3P-RD ~59%/41% Medicare/Medicaid within the government-
# payer bucket, all specialties pooled; NAMCS, URPS-specific, ~92%/8%),
# plausibly because URPS conditions skew heavily toward the Medicare-
# eligible 65+ population in a way a general claims sample does not
# reflect. Per an explicit user decision, 3P-RD is therefore reported ONLY
# as an independent cross-check value (ahrq_3prd_medicare_medicaid_ratio())
# alongside the NAMCS-derived shares -- it is never blended into them.
#
# A second cross-check, chia_medicare_medicaid_ratio(), queries the live CHIA
# Case Mix DuckDB directly (chia_casemix_con(), the connector already
# established in R/data-chia_casemix_duckdb.R) rather than vendoring a copy:
# CHIA discharge data is not public-use like the 3P-RD PUF, so nothing about
# it is committed to this repo beyond the query and the resulting aggregate
# numbers. Filtered to female adults (SexLDS, AgeLDS >= 18), excluding
# newborn admissions (AdmissionType <> '4', the same W5 leak-detection fix
# already applied elsewhere in the CHIA pipeline this session), pooled
# 2015-2018 (the years present in the currently mounted snapshot): Medicare
# 73.1% / Medicaid 26.9% of the combined government-payer discharge count --
# a third, independent data point that sits between NAMCS's URPS-specific
# ~92%/8% and 3P-RD's all-specialty ~59%/41%, consistent with CHIA's
# demographic (though not diagnosis-specific) narrowing to female adults.
# Like the 3P-RD ratio, this is reported as a cross-check only and is never
# blended into the NAMCS-derived shares.

# Read the vendored four-row payer-mix aggregate.
#
# Kept internal deliberately: callers should ask for namcs_urps_payer_mix(),
# which prefers the microdata and reaches this only when the microdata is
# absent. Exporting it would invite code to pin itself to the frozen copy.
#
# `min_records` is re-applied here rather than trusting the CSV's `reliable`
# column, so that changing NAMCS_MIN_RECORDS re-evaluates the flag instead of
# silently disagreeing with the vendored value.
.vendored_urps_payer_mix <- function(min_records = NAMCS_MIN_RECORDS) {
  # SHIPPED IN inst/extdata, NOT data-raw. The first version of this lived in
  # data-raw/ beside the acquisition script, which reads as the natural home
  # and is wrong: data-raw/ is .Rbuildignore'd, so under R CMD check -- where
  # the suite runs from inside the INSTALLED package and the source tree is
  # absent -- the fallback was as unreachable as the microdata it stands in
  # for. It passed under pkgload::load_all() from the repo root and failed the
  # moment it was checked properly. inst/extdata IS installed, which is what
  # makes this reachable everywhere the package is. Same idiom as
  # .pop_transition_extdata() in R/demand-pop_transitions.R.
  csv_path <- base::system.file(
    "extdata", "namcs_urps_payer_mix.csv", package = "urpssim"
  )
  if (!base::nzchar(csv_path)) {
    csv_path <- "inst/extdata/namcs_urps_payer_mix.csv"   # dev (load_all)
  }
  if (!base::file.exists(csv_path)) {
    root <- .repo_source_root()
    if (!base::is.na(root)) {
      csv_path <- base::file.path(root, "inst", "extdata",
                                  "namcs_urps_payer_mix.csv")
    }
  }
  if (!base::file.exists(csv_path)) {
    base::stop(
      "namcs_urps_payer_mix(): neither the pooled NAMCS microdata nor the ",
      "vendored aggregate ('", csv_path, "') is available.\n",
      "Run data-raw/namcs/02-namcs_multiyear_acquire.R to create the former.",
      call. = FALSE
    )
  }

  vendored <- readr::read_csv(
    csv_path,
    col_types = readr::cols(
      payer_tier = readr::col_character(),
      share = readr::col_double(),
      n_unweighted = readr::col_integer(),
      reliable = readr::col_logical()
    )
  )
  result <- vendored |>
    dplyr::mutate(reliable = .data$n_unweighted >= min_records) |>
    dplyr::arrange(.data$payer_tier)

  unreliable <- result$payer_tier[!result$reliable]
  if (base::length(unreliable) > 0L) {
    .msg_warn(base::sprintf(
      base::paste(
        "namcs_urps_payer_mix(): tier(s) %s rest on fewer than %d unweighted",
        "records (NCHS reliability floor). Treat those shares as indicative",
        "only."
      ),
      base::paste(unreliable, collapse = ", "), min_records
    ))
  }

  structure(
    result,
    provenance = list(
      source = "National Ambulatory Medical Care Survey (NAMCS) Public Use File",
      agency = "National Center for Health Statistics, CDC",
      files = "Pooled 2015, 2016, 2018, 2019 (load_namcs_pooled())",
      weight = "PATWT (patient visit weight, annualised national estimate)",
      population = "Female patients aged 18+ with a URPS-flagged diagnosis",
      other_tier_handling = base::paste(
        "NAMCS PAYTYPER categories 4/6/7 (worker's comp, other government,",
        "no-charge/charity) are excluded and the remaining four categories",
        "renormalized to sum to 1."
      ),
      # The one field that differs from the live derivation, so that anything
      # inspecting provenance can tell the two apart rather than assuming the
      # microdata was read.
      derivation = base::paste(
        "Read from the vendored aggregate", csv_path, "because the pooled",
        "NAMCS microdata was not present. See",
        "inst/extdata/namcs_urps_payer_mix_manifest.txt."
      )
    )
  )
}

#' NAMCS-derived URPS payer mix (Medicare/Medicaid/commercial/self-pay)
#'
#' @description
#' Revives `.namcs_insurance_tier()` (defined in
#' R/demand-namcs_visit_equations.R, written but never called before this
#' function) and applies it to the same URPS-filtered, PATWT-weighted visit
#' pool `namcs_urps_visit_anchor()` uses: `is_urps` diagnosis flag, NAMCS
#' `SEX == 1` (female), `AGE >= 18`, non-missing `PATWT`. The five-tier NAMCS
#' classification (`Private`/`Medicare`/`Medicaid`/`Uninsured`/`Other`) is
#' reduced to the four shares [simulate_practice_economics()] needs by
#' dropping `Other` (worker's comp, other government payers, no-charge/
#' charity -- none of which map cleanly onto the four-way split) and
#' renormalizing the rest to sum to 1.
#'
#' @param namcs Tibble from [load_namcs_pooled()]; loaded when `NULL`. When
#'   `NULL` and the pooled microdata is not present (it is `.gitignore`d and
#'   lives under the `.Rbuildignore`d `data-raw/`, so it never exists in CI or
#'   under `R CMD check`), the vendored four-row aggregate
#'   `inst/extdata/namcs_urps_payer_mix.csv` is read instead and the
#'   returned `provenance` attribute carries an extra `derivation` field
#'   saying so.
#' @param min_records NCHS reliability floor (see [namcs_urps_visit_anchor()]);
#'   any tier below this unweighted-record count is flagged `reliable = FALSE`.
#'
#' @return Tibble with `payer_tier` (`Medicare`/`Medicaid`/`Private`/
#'   `Uninsured`), `share` (fraction of URPS visits, `Other` excluded and the
#'   rest renormalized to sum to 1), `n_unweighted`, `reliable`. Carries a
#'   `provenance` attribute.
#' @family practice economics
#' @concept supply
#' @export
namcs_urps_payer_mix <- function(namcs = NULL, min_records = NAMCS_MIN_RECORDS) {
  if (is.null(namcs)) {
    # THE MICRODATA IS SOURCE-TREE-ONLY; THE AGGREGATE IS NOT.
    #
    # The pooled NAMCS RDS lives under data-raw/, which is .Rbuildignore'd,
    # and is itself *.rds, which .gitignore excludes as a PHI/DUA control the
    # nightly leak-guard job asserts is still in place. It therefore cannot
    # reach CI by any route, and thirteen tests that merely need a payer mix
    # as an INPUT -- practice economics, the end-to-end runner -- errored
    # rather than ran.
    #
    # What those tests need is the four-row RESULT, which is an aggregate of
    # a public-use file: no records, no cells below what NCHS already
    # publishes, no DUA. So it is vendored as a CSV (the same way the AHRQ
    # 3P-RD and CHIA extracts in this repo are) and read when the microdata
    # is out of reach. Deriving from the microdata always wins when it IS
    # present, so a re-acquisition silently supersedes the vendored copy.
    #
    # Probed by existence rather than by catching load_namcs_pooled()'s
    # error: only ABSENCE may fall back. A file that is present but
    # unreadable is a real fault and must stay loud.
    if (!file.exists(.namcs_pooled_path())) {
      return(.vendored_urps_payer_mix(min_records))
    }
    namcs <- load_namcs_pooled()
  }
  flagged <- flag_urps_visits(namcs)

  # Mirrors namcs_urps_visit_anchor()'s exact filter convention -- see that
  # function's comment on the NAMCS SEX == 1 == FEMALE coding.
  keep <- flagged$is_urps & flagged$SEX == 1L & flagged$AGE >= 18 &
    !is.na(flagged$PATWT)
  keep[is.na(keep)] <- FALSE
  urps <- flagged[keep, ]

  urps$payer_tier <- .namcs_insurance_tier(urps$PAYTYPER)

  weighted <- urps |>
    dplyr::filter(!is.na(.data$payer_tier)) |>
    dplyr::group_by(.data$payer_tier) |>
    dplyr::summarise(
      weighted_visits = sum(.data$PATWT, na.rm = TRUE),
      n_unweighted = dplyr::n(),
      .groups = "drop"
    )

  core <- weighted |>
    dplyr::filter(.data$payer_tier != "Other")
  total_core <- sum(core$weighted_visits)

  result <- core |>
    dplyr::mutate(
      share = .data$weighted_visits / total_core,
      reliable = .data$n_unweighted >= min_records
    ) |>
    dplyr::select("payer_tier", "share", "n_unweighted", "reliable") |>
    dplyr::arrange(.data$payer_tier)

  unreliable <- result$payer_tier[!result$reliable]
  if (length(unreliable) > 0L) {
    .msg_warn(sprintf(
      paste(
        "namcs_urps_payer_mix(): tier(s) %s rest on fewer than %d unweighted",
        "records (NCHS reliability floor). Treat those shares as indicative",
        "only."
      ),
      paste(unreliable, collapse = ", "), min_records
    ))
  }

  structure(
    result,
    provenance = list(
      source = "National Ambulatory Medical Care Survey (NAMCS) Public Use File",
      agency = "National Center for Health Statistics, CDC",
      files = "Pooled 2015, 2016, 2018, 2019 (load_namcs_pooled())",
      weight = "PATWT (patient visit weight, annualised national estimate)",
      population = "Female patients aged 18+ with a URPS-flagged diagnosis",
      other_tier_handling = paste(
        "NAMCS PAYTYPER categories 4/6/7 (worker's comp, other government,",
        "no-charge/charity) are excluded and the remaining four categories",
        "renormalized to sum to 1."
      )
    )
  )
}

#' AHRQ 3P-RD Medicare-to-Medicaid claims-volume ratio (cross-check only)
#'
#' @description
#' Reads the vendored AHRQ 3P-RD Physician Geographic summary (see
#' `data-raw/ahrq_3prd/01-ahrq_3prd_acquire.R`) and computes the national,
#' claims-volume-weighted share of combined Medicare+Medicaid claims volume
#' that is Medicare, across all 325 ZIP3 areas in the 13-state 3P-RD sample.
#'
#' This is real, cited AHRQ administrative-claims data, but the
#' `physician_geographic` file cannot be filtered to a physician specialty
#' (its payer columns are pre-aggregated across every physician in each
#' ZIP3). It disagrees substantially with the URPS-specific ratio implied
#' by [namcs_urps_payer_mix()] (see that function's file-level comment for
#' the comparison and the likely reason: URPS conditions skew toward the
#' Medicare-eligible population in a way a general, all-specialty claims
#' sample does not reflect). **This function's result is reported only as
#' an independent cross-check value; [practice_payer_mix_defaults()] does
#' not use it to adjust the NAMCS-derived shares.**
#'
#' @param csv_path Path to the vendored per-state summary CSV.
#'
#' @return A one-row tibble: `medicare_share_of_government_claims`,
#'   `n_states`, `n_zip3`, `total_medicare_claims_permonth`,
#'   `total_medicaid_claims_permonth`.
#' @family practice economics
#' @concept supply
#' @export
ahrq_3prd_medicare_medicaid_ratio <- function(
    csv_path = "data-raw/ahrq_3prd/ahrq_3prd_medicare_medicaid_claims_by_state.csv") {
  if (!base::file.exists(csv_path)) {
    root <- .repo_source_root()
    if (!base::is.na(root)) {
      csv_path <- base::file.path(root, csv_path)
    }
  }
  if (!base::file.exists(csv_path)) {
    base::stop(
      "ahrq_3prd_medicare_medicaid_ratio(): file not found: ",
      csv_path, call. = FALSE
    )
  }
  by_state <- readr::read_csv(csv_path, show_col_types = FALSE)
  total_medicare <- base::sum(by_state$total_medicare_claims_permonth)
  total_medicaid <- base::sum(by_state$total_medicaid_claims_permonth)
  tibble::tibble(
    medicare_share_of_government_claims =
      total_medicare / (total_medicare + total_medicaid),
    n_states = base::nrow(by_state),
    n_zip3 = base::sum(by_state$n_zip3),
    total_medicare_claims_permonth = total_medicare,
    total_medicaid_claims_permonth = total_medicaid
  )
}

#' CHIA Medicare-to-Medicaid discharge ratio (cross-check only)
#'
#' @description
#' Queries the live CHIA Case Mix DuckDB (via [chia_casemix_con()], the
#' connector already established in R/data-chia_casemix_duckdb.R -- nothing
#' about CHIA's discharge-level data is vendored into this repo) for the
#' Medicare-to-Medicaid share of combined government-payer inpatient
#' discharges among female adults, excluding newborn admissions.
#'
#' Like [ahrq_3prd_medicare_medicaid_ratio()], this is real, cited
#' administrative data with real construct limitations: it is Massachusetts-
#' only, acute-inpatient-only (URPS practice revenue is mostly office-based),
#' and demographically but not diagnostically narrowed (female adults, not
#' URPS-specific visits/discharges). It sits between the other two sources
#' (see this file's header comment for the three-way comparison) -- **this
#' function's result is reported only as an independent cross-check value;
#' [practice_payer_mix_defaults()] does not use it to adjust the
#' NAMCS-derived shares.**
#'
#' @param connection Open CHIA Case Mix DuckDB connection; opened via
#'   [chia_casemix_con()] and disconnected before returning when `NULL`.
#'
#' @return A tibble with one row per data year plus a `"pooled"` row:
#'   `data_year`, `medicare_share_of_government_discharges`,
#'   `medicare_n`, `medicaid_n`.
#' @family practice economics
#' @concept supply
#' @export
chia_medicare_medicaid_ratio <- function(connection = NULL) {
  owns_connection <- base::is.null(connection)
  if (owns_connection) connection <- chia_casemix_con()
  if (owns_connection) {
    base::on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)
  }

  by_year <- DBI::dbGetQuery(connection, "
    SELECT _data_year AS data_year,
      CASE WHEN \"PrimaryPayerType\" IN ('3','F') THEN 'medicare'
           WHEN \"PrimaryPayerType\" IN ('4','B') THEN 'medicaid' END AS payer_grp,
      COUNT(*) AS n
    FROM chia_casemix.v_hdd_discharge_all_years
    WHERE \"AdmissionType\" <> '4'
      AND \"SexLDS\" IN ('F', '2')
      AND CAST(\"AgeLDS\" AS INTEGER) >= 18
    GROUP BY ALL
  ")
  by_year <- by_year[!base::is.na(by_year$payer_grp), ]

  wide <- by_year |>
    tidyr::pivot_wider(names_from = "payer_grp", values_from = "n") |>
    dplyr::mutate(
      data_year = base::as.character(.data$data_year),
      medicare_share_of_government_discharges =
        .data$medicare / (.data$medicare + .data$medicaid),
      medicare_n = .data$medicare,
      medicaid_n = .data$medicaid
    ) |>
    dplyr::select(
      "data_year", "medicare_share_of_government_discharges",
      "medicare_n", "medicaid_n"
    ) |>
    dplyr::arrange(.data$data_year)

  pooled <- tibble::tibble(
    data_year = "pooled",
    medicare_share_of_government_discharges =
      base::sum(wide$medicare_n) / (base::sum(wide$medicare_n) + base::sum(wide$medicaid_n)),
    medicare_n = base::sum(wide$medicare_n),
    medicaid_n = base::sum(wide$medicaid_n)
  )

  dplyr::bind_rows(wide, pooled)
}

#' Default practice-economics payer mix
#'
#' @description
#' The `medicare_share`/`medicaid_share`/`commercial_share`/`self_pay_share`
#' input [simulate_practice_economics()] needs, built entirely from
#' [namcs_urps_payer_mix()] (`Private` -> `commercial_share`, `Uninsured` ->
#' `self_pay_share`). [ahrq_3prd_medicare_medicaid_ratio()] and
#' [chia_medicare_medicaid_ratio()] are attached as a `crosschecks` attribute
#' for comparison only -- see those functions' docstrings for why neither is
#' blended in.
#'
#' @param namcs_mix Tibble from [namcs_urps_payer_mix()]; computed when `NULL`.
#' @param include_crosscheck Whether to attach the 3P-RD and CHIA cross-check
#'   ratios (3P-RD requires the vendored `data-raw/ahrq_3prd` file; CHIA
#'   requires the live CHIA DuckDB to be reachable -- either or both may be
#'   silently omitted from the attribute when unavailable).
#'
#' @return One-row tibble with `medicare_share`, `medicaid_share`,
#'   `commercial_share`, `self_pay_share` (sums to 1), `self_pay_reliable`
#'   (`FALSE` when the underlying NAMCS Uninsured cell is below the NCHS
#'   reliability floor). Carries a `crosschecks` attribute (a named list,
#'   `ahrq_3prd`/`chia`) when requested and reachable.
#' @family practice economics
#' @concept supply
#' @export
practice_payer_mix_defaults <- function(
    namcs_mix = NULL, include_crosscheck = TRUE) {
  if (base::is.null(namcs_mix)) namcs_mix <- namcs_urps_payer_mix()

  share_for <- function(tier) {
    row <- namcs_mix[namcs_mix$payer_tier == tier, ]
    if (base::nrow(row) == 0L) base::return(base::list(share = 0, reliable = NA))
    base::list(share = row$share[[1]], reliable = row$reliable[[1]])
  }
  medicare <- share_for("Medicare")
  medicaid <- share_for("Medicaid")
  commercial <- share_for("Private")
  self_pay <- share_for("Uninsured")

  out <- tibble::tibble(
    medicare_share = medicare$share,
    medicaid_share = medicaid$share,
    commercial_share = commercial$share,
    self_pay_share = self_pay$share,
    self_pay_reliable = self_pay$reliable
  )

  if (base::isTRUE(include_crosscheck)) {
    ahrq_crosscheck <- base::tryCatch(
      ahrq_3prd_medicare_medicaid_ratio(),
      error = function(e) NULL
    )
    chia_crosscheck <- base::tryCatch(
      chia_medicare_medicaid_ratio(),
      error = function(e) NULL
    )
    crosschecks <- base::list(
      ahrq_3prd = ahrq_crosscheck, chia = chia_crosscheck
    )
    crosschecks <- crosschecks[!base::vapply(crosschecks, base::is.null, logical(1))]
    if (base::length(crosschecks) > 0L) {
      base::attr(out, "crosschecks") <- crosschecks
      namcs_medicare_pct <- 100 * medicare$share / (medicare$share + medicaid$share)
      report_lines <- base::sprintf(
        "NAMCS-derived Medicare share of government-payer visits = %.1f%%.",
        namcs_medicare_pct
      )
      if (!base::is.null(ahrq_crosscheck)) {
        report_lines <- base::c(report_lines, base::sprintf(
          "AHRQ 3P-RD (all-specialty, 13-state, cross-check only) = %.1f%%.",
          100 * ahrq_crosscheck$medicare_share_of_government_claims
        ))
      }
      if (!base::is.null(chia_crosscheck)) {
        pooled_row <- chia_crosscheck[chia_crosscheck$data_year == "pooled", ]
        report_lines <- base::c(report_lines, base::sprintf(
          "CHIA (MA, female-adult inpatient, cross-check only) = %.1f%%.",
          100 * pooled_row$medicare_share_of_government_discharges
        ))
      }
      base::message(
        "practice_payer_mix_defaults(): ",
        base::paste(report_lines, collapse = " ")
      )
    }
  }
  out
}
