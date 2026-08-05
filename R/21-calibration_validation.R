# Calibration and Validation ----
#
# Two mechanics from the HWMM documentation that the repo was missing, plus the
# four-part validation taxonomy the models are evaluated against.
#
# 1. CALIBRATION SCALARS (HDMM, Exhibit 11). Prediction equations estimated on a
#    survey are applied to the representative population, national totals are
#    predicted, and those predictions are compared with independent national
#    estimates from larger sources (NIS for hospitalisations, NHAMCS for ED
#    visits, NAMCS for office visits). A multiplicative scalar is then created
#    per category:
#
#       scalar = national estimate / model-predicted estimate
#
#    "For example, if the model under-predicted ED visits for a particular
#     diagnosis category by 10% then a scalar of 1.1 was added to the prediction
#     equation for that diagnosis category."
#
#    The published office-visit scalars run from 0.243 (orthopaedic surgery) to
#    1.665 (paediatrics); obstetrics & gynaecology was 0.906 and urology 0.728 --
#    i.e. the uncalibrated model over-predicted OB/GYN office visits by about
#    10% and urology visits by about 37%. A model without this step is not
#    anchored to any observed national total.
#
# 2. TWO-METHOD AGREEMENT. Fraher & Knapton (2017) compared the ASN nephrology
#    report against the UNC FutureDocs model. The two used different demand
#    definitions (ESRD patients per nephrologist vs unmet visits by clinical
#    service area) and different geographies (HRRs vs tertiary service areas),
#    yet agreed on the qualitative conclusion: large metropolitan markets
#    saturated, rural markets wide open. Agreement across genuinely independent
#    methods is evidence; agreement between two rescalings of one population
#    series is arithmetic.
#
# 3. VALIDATION TAXONOMY (HWMM, "Validation activities"): conceptual, internal,
#    external and data validation. `validation_report()` runs the internal checks
#    that can be automated and records the status of the rest.

# ---- Calibration scalars --------------------------------------------------

#' Fit multiplicative calibration scalars against independent national anchors
#'
#' @param predicted Tibble with `category` and `predicted` (model output).
#' @param observed Tibble with `category` and `observed` (independent national
#'   estimate from a larger source: NIS, NAMCS, NHAMCS, claims).
#' @param max_scalar Scalars beyond this magnitude (or below its reciprocal)
#'   indicate a structural problem rather than a calibration offset and are
#'   flagged rather than silently applied.
#' @return Tibble with `category`, `predicted`, `observed`, `scalar`, `flagged`.
#' @export
fit_calibration_scalars <- function(predicted, observed, max_scalar = 3) {
  assertthat::assert_that(all(c("category", "predicted") %in% names(predicted)))
  assertthat::assert_that(all(c("category", "observed") %in% names(observed)))

  out <- safe_left_join(predicted, observed, by = "category", min_match_rate = 1.0)
  out <- dplyr::mutate(
    out,
    scalar = dplyr::if_else(.data$predicted > 0, .data$observed / .data$predicted, NA_real_),
    flagged = !is.na(.data$scalar) &
      (.data$scalar > max_scalar | .data$scalar < 1 / max_scalar)
  )
  n_flag <- sum(out$flagged, na.rm = TRUE)
  if (n_flag > 0) {
    .msg_warn(sprintf(
      "%d calibration scalar(s) outside [1/%g, %g]: %s. A scalar this large is a structural mismatch, not a calibration offset.",
      n_flag, max_scalar, max_scalar,
      paste(out$category[out$flagged], collapse = ", ")
    ))
  }
  out
}

#' Apply calibration scalars to model output
#'
#' @param values Tibble with `category` and a value column.
#' @param scalars Tibble from [fit_calibration_scalars()].
#' @param value_col Name of the column to scale.
#' @return `values` with the column scaled and a `calibration_scalar` column.
#' @export
apply_calibration_scalars <- function(values, scalars, value_col = "predicted") {
  assertthat::assert_that(value_col %in% names(values), "category" %in% names(values))
  s <- dplyr::select(scalars, "category", calibration_scalar = "scalar")
  out <- safe_left_join(values, s, by = "category", min_match_rate = 1.0)
  out[[value_col]] <- out[[value_col]] * dplyr::coalesce(out$calibration_scalar, 1)
  out
}

#' Published HDMM office-visit calibration scalars (Exhibit 11)
#'
#' Reference values for sanity-checking the magnitude of a newly fitted scalar.
#' @return Tibble of published scalars.
#' @export
published_calibration_scalars <- function() {
  tibble::tribble(
    ~specialty,                 ~namcs_visits_thousands, ~model_predicted_thousands, ~scalar,
    "Family Medicine",                           202494,                      411955,   0.492,
    "Pediatrics",                                136119,                       81775,   1.665,
    "Internal Medicine",                          81701,                       72292,   1.130,
    "Obstetrics & Gynecology",                    73198,                       80804,   0.906,
    "Orthopedic Surgery",                         30114,                      124001,   0.243,
    "Ophthalmology",                              46289,                      127436,   0.363,
    "Dermatology",                                49947,                       90870,   0.550,
    "Psychiatry",                                 29993,                      110045,   0.273,
    "Cardiovascular Diseases",                    27783,                       32945,   0.843,
    "Otolaryngology",                             28965,                       27495,   1.053,
    "Urology",                                    26153,                       35925,   0.728,
    "General Surgery",                            15685,                       16282,   0.963,
    "Neurology",                                  14407,                       29811,   0.483
  )
}

#' Refuse to publish demand output that was never calibrated to a national anchor
#'
#' @param calibration Tibble from [fit_calibration_scalars()], or NULL.
#' @param mode Reproducibility mode.
#' @return (Invisibly) TRUE when calibration is present.
#' @export
assert_demand_calibrated <- function(calibration, mode = resolve_reproducibility_mode()) {
  msg <- paste(
    "Demand output is not calibrated to an independent national anchor.",
    "Predict base-year national totals, compare with NAMCS/NHAMCS/NIS or claims,",
    "and fit scalars with fit_calibration_scalars() (HDMM Exhibit 11)."
  )
  ok <- is.data.frame(calibration) && nrow(calibration) > 0 && "scalar" %in% names(calibration)
  if (!ok) {
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
    return(invisible(FALSE))
  }
  invisible(TRUE)
}

# ---- Two-method agreement -------------------------------------------------

#' Compare two independent models' geographic adequacy rankings
#'
#' The Fraher & Knapton comparison, generalised. Two estimates count as
#' independent only if they use different demand definitions AND different data
#' sources; rescaling one population series by three constants does not qualify,
#' and `independent = FALSE` records that fact in the output rather than letting
#' a tautological rho be read as robustness.
#'
#' @param a,b Tibbles with `geo` and an adequacy or shortage measure.
#' @param measure_a,measure_b Column names of the measures.
#' @param independent Whether the two methods are genuinely independent.
#' @return List with Spearman rho, agreement on sign, and a verdict.
#' @export
two_method_agreement <- function(a, b, measure_a = "adequacy", measure_b = "adequacy",
                                 independent = TRUE) {
  assertthat::assert_that(all(c("geo", measure_a) %in% names(a)))
  assertthat::assert_that(all(c("geo", measure_b) %in% names(b)))

  joined <- dplyr::inner_join(
    dplyr::select(a, "geo", .a = dplyr::all_of(measure_a)),
    dplyr::select(b, "geo", .b = dplyr::all_of(measure_b)),
    by = "geo"
  )
  if (nrow(joined) < 3) {
    stop("two_method_agreement: fewer than 3 shared geographies", call. = FALSE)
  }

  rho <- suppressWarnings(stats::cor(joined$.a, joined$.b, method = "spearman",
                                     use = "complete.obs"))
  sign_agree <- mean(sign(joined$.a - 1) == sign(joined$.b - 1), na.rm = TRUE)

  if (!independent) {
    .msg_warn("two_method_agreement(): the two measures were declared NOT independent. ",
              "Rank correlation between rescalings of the same series is arithmetic, not evidence.")
  }

  list(
    spearman_rho = rho,
    sign_agreement = sign_agree,
    n_geo = nrow(joined),
    independent = independent,
    verdict = if (!independent) {
      "not_independent"
    } else if (!is.na(rho) && rho >= 0.7 && sign_agree >= 0.7) {
      "concordant"
    } else {
      "discordant"
    },
    detail = joined
  )
}

#' Detect estimands that are proportional rescalings of one another
#'
#' Guard against reintroducing the tautological-concordance defect: if two demand
#' series are constant multiples of each other they carry identical information,
#' and any rank correlation between them is exactly 1 by construction.
#'
#' @param long Long tibble with `year`, `estimand`, and a value column.
#' @param value_col Value column name.
#' @param tol Relative tolerance on the constancy of the ratio.
#' @return Tibble of proportional estimand pairs (empty when all are distinct).
#' @export
detect_proportional_estimands <- function(long, value_col = "demand_cases", tol = 1e-8) {
  assertthat::assert_that(all(c("year", "estimand", value_col) %in% names(long)))
  wide <- tidyr::pivot_wider(
    dplyr::select(long, "year", "estimand", dplyr::all_of(value_col)),
    names_from = "estimand", values_from = dplyr::all_of(value_col)
  )
  cols <- setdiff(names(wide), "year")
  if (length(cols) < 2) return(tibble::tibble(a = character(0), b = character(0)))

  pairs <- utils::combn(cols, 2, simplify = FALSE)
  hits <- lapply(pairs, function(p) {
    x <- wide[[p[1]]]; y <- wide[[p[2]]]
    ok <- is.finite(x) & is.finite(y) & y != 0
    if (sum(ok) < 2) return(NULL)
    r <- x[ok] / y[ok]
    if (diff(range(r)) <= tol * max(abs(r))) {
      tibble::tibble(a = p[1], b = p[2], ratio = r[1])
    } else NULL
  })
  dplyr::bind_rows(hits)
}

#' Refuse a concordance claim built on proportional estimands
#'
#' @param long Long demand tibble.
#' @param value_col Value column.
#' @param mode Reproducibility mode.
#' @return (Invisibly) the proportional pairs found.
#' @export
assert_estimands_independent <- function(long, value_col = "demand_cases",
                                         mode = resolve_reproducibility_mode()) {
  prop <- detect_proportional_estimands(long, value_col)
  if (nrow(prop) > 0) {
    msg <- sprintf(
      paste("Demand estimands %s are exact proportional rescalings of one another.",
            "Their rank correlation is 1 by construction and carries no information",
            "about robustness. Give each estimand its own age profile or data source."),
      paste(sprintf("%s~%s", prop$a, prop$b), collapse = ", ")
    )
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
  }
  invisible(prop)
}

# ---- Internal validation checks -------------------------------------------

#' Run the automatable internal-validation checks
#'
#' HWMM lists four validation types: conceptual (peer review), internal
#' (programming integrity, consistency with published statistics, stress tests),
#' external (subject-matter expert critique, comparison with sources not used as
#' inputs) and data validation. Only internal checks can be automated; the report
#' records the status of the others so they cannot be quietly skipped.
#'
#' @param supply Supply projection tibble.
#' @param required Required-FTE tibble.
#' @param gap Optional [baseline_gap()] object.
#' @param state_totals Optional tibble with `year` and `fte` summed by state.
#' @param gap_projection Optional data frame from [as_urps_gap_projection()] /
#'   [gap_projections_all_scenarios()]. When supplied, two internal checks are
#'   added: (1) all REQUIRED_COLS are present; (2) gap arithmetic is consistent
#'   (`gap_fte == supply_clinical_fte - demand_clinical_fte` to ±0.01 FTE).
#' @return Tibble of checks with pass/fail and detail.
#' @export
validation_report <- function(supply, required = NULL, gap = NULL,
                              state_totals = NULL, gap_projection = NULL) {
  checks <- list()

  add <- function(name, type, passed, detail) {
    checks[[length(checks) + 1L]] <<- tibble::tibble(
      check = name, type = type, passed = passed, detail = detail
    )
  }

  num_cols <- names(supply)[vapply(supply, is.numeric, logical(1))]
  neg <- vapply(num_cols, function(cl) any(supply[[cl]] < 0, na.rm = TRUE), logical(1))
  add("no_negative_supply", "internal", !any(neg),
      if (any(neg)) paste("negative values in", paste(num_cols[neg], collapse = ", ")) else "ok")

  if ("year" %in% names(supply)) {
    dup <- anyDuplicated(supply[, intersect(c("year", "scenario", "subspecialty"), names(supply))]) > 0
    add("no_duplicate_year_rows", "internal", !dup,
        if (dup) "duplicated year/scenario rows" else "ok")
  }

  if (!is.null(required)) {
    ok <- all(required$required_fte > 0, na.rm = TRUE)
    add("required_fte_positive", "internal", ok,
        if (ok) "ok" else "non-positive required FTE")
  }

  # Two separate questions, and reporting only the first is how a borrowed
  # physical-therapy distribution came to be recorded as "capacity_survey: 5.2%
  # shortfall" -- indistinguishable in the artifact from a URPS survey that was
  # actually fielded. `method` names the arithmetic; `calibration_status` says
  # whether the number was measured on urogynaecologists, and both belong here.
  if (!is.null(gap)) {
    is_gap <- inherits(gap, "urps_baseline_gap")
    tier <- if (is_gap) gap$calibration_status %||% NA_character_ else NA_character_
    estimated <- is_gap && !isTRUE(all.equal(gap$adequacy, 1)) &&
      !identical(tier, "uncalibrated_illustrative")
    add("base_year_gap_estimated", "internal", estimated,
        if (estimated) sprintf("%s (%s): %.1f%% shortfall",
                               gap$method, tier, gap$shortfall_pct)
        else if (identical(tier, "uncalibrated_illustrative"))
          "shortfall assumed with no evidence ledger"
        else "base-year equilibrium assumed")

    measured <- identical(tier, "calibrated")
    add("base_year_gap_measured", "external", measured,
        if (measured) sprintf("measured on urogynaecologists: %s%s",
                              gap$method,
                              if (!is.na(gap$source %||% NA_character_))
                                paste0(" -- ", gap$source) else "")
        else sprintf(paste("NOT measured on urogynaecologists (%s). The base-year",
                           "shortfall passes through to the headline gap with a",
                           "coefficient of one: field a URPS capacity survey or",
                           "cite an external anchor."),
                     if (is.na(tier)) "no calibration_status recorded" else tier))
  } else {
    add("base_year_gap_estimated", "internal", FALSE, "no base-year gap supplied")
    add("base_year_gap_measured", "external", FALSE, "no base-year gap supplied")
  }

  if (!is.null(state_totals) && all(c("year", "fte", "national_fte") %in% names(state_totals))) {
    d <- abs(state_totals$fte - state_totals$national_fte)
    ok <- all(d < 1e-6 * pmax(state_totals$national_fte, 1), na.rm = TRUE)
    add("state_totals_equal_national", "internal", ok,
        if (ok) "ok" else sprintf("max discrepancy %.4f", max(d, na.rm = TRUE)))
  }

  if (!is.null(gap_projection)) {
    missing_cols <- setdiff(REQUIRED_COLS, names(gap_projection))
    add("gap_projection_cols", "internal", length(missing_cols) == 0L,
        if (length(missing_cols) == 0L) "ok"
        else sprintf("missing: %s", paste(missing_cols, collapse = ", ")))

    if (all(c("supply_clinical_fte", "demand_clinical_fte", "gap_fte") %in% names(gap_projection))) {
      residual <- abs(gap_projection$gap_fte -
                        (gap_projection$supply_clinical_fte - gap_projection$demand_clinical_fte))
      ok <- all(residual <= 0.01, na.rm = TRUE)
      add("gap_projection_arithmetic", "internal", ok,
          if (ok) "ok"
          else sprintf("max residual %.4f FTE (tolerance 0.01)", max(residual, na.rm = TRUE)))
    }
  }

  add("conceptual_validation", "conceptual", NA,
      "Peer review / advisory-panel critique of the framework: manual")
  add("external_validation", "external", NA,
      "Compare projections with sources not used as inputs (claims volumes, wait times, mystery-caller access): manual")
  add("data_validation", "data", NA,
      "Input quality review against published statistics: manual")

  dplyr::bind_rows(checks)
}

#' Summarise a validation report and fail closed on internal failures
#'
#' @param report Tibble from [validation_report()].
#' @param mode Reproducibility mode.
#' @return (Invisibly) the report.
#' @export
assert_validation_passed <- function(report, mode = resolve_reproducibility_mode()) {
  failed <- report[report$type == "internal" & !is.na(report$passed) & !report$passed, ]
  if (nrow(failed) > 0) {
    msg <- sprintf("Internal validation failed: %s",
                   paste(sprintf("%s (%s)", failed$check, failed$detail), collapse = "; "))
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
  }
  invisible(report)
}
