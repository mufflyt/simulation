################################################################################
# R/supply-retirement_hazard.R
# Empirical URPS retirement hazard from mufflyt/cliff pipeline
#
# Addresses README item 4: retirement uncertainty unquantified.
# Calibration tier: calibrated (with cliff) / derived_by_analogy (fallback)
#
# Retirement model (IHS Markit HWMM Exhibits 17-18):
#   P(still active at age a) = exp(-(a / scale)^shape)
#   Discrete annual exit prob: 1 - S(a+1) / S(a)
#   Scenario levers shift `scale` by ±2 yr, preserving the stochastic spread.
################################################################################

# ---- Weibull survival parameters (HWMM Exhibits 17-18, derived_by_analogy) --
#
# Shape and scale are from HWSM general physician curves re-parameterised for
# URPS sub-specialties. ABOG female exits earlier (scale 68.5) than ABOG male
# (70.2); ABU mixed-practice exits earliest (66.0) because urologists who add
# pelvic floor often shift back to general urology at career midpoint.
# `derived_by_analogy` tier until ABOG departure micro-data are available.

# Scale derivation: for a Weibull S(a) = exp(-(a/scale)^shape),
#   median retirement age = scale × ln(2)^(1/shape)
# Target medians: ABOG female 67, ABOG male 70, ABU 64.
#   Female: scale = 67 / 0.693^(1/2.1) = 67 / 0.836 = 80.1
#   Male:   scale = 70 / 0.693^(1/1.9) = 70 / 0.820 = 85.4
#   ABU:    scale = 64 / 0.693^(1/2.0) = 64 / 0.832 = 76.9
# Shapes from HWSM Exhibits 17–18 general physician curves (derived_by_analogy).

URPS_WEIBULL_PARAMS <- list(
  abog_female = list(shape = 2.1, scale = 80.1,
                     median_retirement_age = 67,
                     tier = "derived_by_analogy",
                     note = "HWSM Exhibit 17 female physician analogy; median 67"),
  abog_male   = list(shape = 1.9, scale = 85.4,
                     median_retirement_age = 70,
                     tier = "derived_by_analogy",
                     note = "HWSM Exhibit 18 male physician analogy; median 70"),
  abu         = list(shape = 2.0, scale = 76.9,
                     median_retirement_age = 64,
                     tier = "derived_by_analogy",
                     note = "HWSM Exhibit 18 male analogy, median 64 (mixed urology practice exits earlier)")
)

#' Weibull discrete annual exit probabilities for URPS providers
#'
#' Computes P(exit in year a to a+1 | active at a) from the Weibull survival
#' function, with an optional scale shift for scenario levers.  Scenarios move
#' `scale` (±2 yr = scale ± 2), which shifts the median retirement age while
#' preserving the stochastic spread — the correct HWMM parameterisation (see
#' Exhibits 17–18 and every published Dall-family scenario table).
#'
#' @param ages Integer vector of ages to evaluate.
#' @param sex "Female" or "Male".
#' @param pathway "ABOG" or "ABU".
#' @param scale_shift Numeric added to the canonical scale parameter.  Default
#'   0 (baseline); +2 = "delayed retirement" scenario; −2 = "early retirement".
#' @return Named numeric vector of annual exit probabilities, length
#'   `length(ages)`.
#' @keywords internal
urps_weibull_exit_probs <- function(ages, sex, pathway = "ABOG",
                                     scale_shift = 0) {
  key <- if (identical(pathway, "ABU")) {
    "abu"
  } else if (tolower(sex) == "female") {
    "abog_female"
  } else {
    "abog_male"
  }
  p <- URPS_WEIBULL_PARAMS[[key]]
  shape <- p$shape
  scale <- p$scale + scale_shift

  S <- function(a) exp(-(a / scale)^shape)
  Sa  <- pmax(1e-12, S(ages))
  Sa1 <- pmax(1e-12, S(ages + 1))
  pmax(0, pmin(0.99, 1 - Sa1 / Sa))
}

#' Weibull survival curve for a URPS provider group
#'
#' Returns the probability of still being active at each age, given active at
#' `entry_age`.  Useful for plotting retirement trajectories and for comparing
#' the baseline vs scenario curves visually.
#'
#' @param ages Integer vector of ages.
#' @param sex "Female" or "Male".
#' @param pathway "ABOG" or "ABU".
#' @param scale_shift Numeric scale shift for scenario (default 0).
#' @param entry_age Age at which to condition (default 30).
#' @return Tibble with columns `age`, `sex`, `pathway`, `scale_shift`,
#'   `p_active`.
#'
#' @section Relationship to the mufflyaccess contract:
#' (Concerns `mufflyaccess::urps_survival_curve()`.)
#'
#' Related, and **intentionally not contract-compatible**. The contract version
#' is thirteen lines; this one adds `pathway` (ABOG/ABU), sex-keyed
#' coefficients, `scale_shift` and `entry_age`, and returns a tibble rather than
#' a vector. Renamed from `urps_survival_curve()` on 2026-08-09 for that reason
#' — see the corresponding note on [supply_p_active()].
#'
#' @keywords internal
supply_survival_curve <- function(ages = 30:85, sex = "Female",
                                 pathway = "ABOG", scale_shift = 0,
                                 entry_age = 30L) {
  key <- if (identical(pathway, "ABU")) {
    "abu"
  } else if (tolower(sex) == "female") {
    "abog_female"
  } else {
    "abog_male"
  }
  p <- URPS_WEIBULL_PARAMS[[key]]
  shape <- p$shape
  scale <- p$scale + scale_shift

  S <- function(a) exp(-(a / scale)^shape)
  S_entry <- S(entry_age)
  p_active <- ifelse(ages < entry_age, NA_real_,
                     pmax(0, S(ages) / S_entry))

  tibble::tibble(
    age         = ages,
    sex         = sex,
    pathway     = pathway,
    scale_shift = scale_shift,
    p_active    = p_active
  )
}

#' Canonical URPS Retirement-Event Query for the NBER DuckDB
#'
#' Emits SQL producing one row per observed URPS departure, with the columns
#' [build_urps_exit_hazard()] expects (`age`, `sex`, `confidence_score`).
#'
#' Cohort is defined by **NPPES taxonomy `207VF0040X`** (ObGyn / Female Pelvic
#' Medicine & Reconstructive Surgery) across the 2013-2024 yearly rosters, not
#' by the procedure-derived ML predictions in
#' `main.obgyn_subspecialty_ml_predictions`. That table is built from billing
#' volume, so retirees are absent by construction: only 16 of its 653 FPMRS
#' NPIs appear in `credentials.retirement_consensus` (2.4%) against a
#' roster-wide rate of 34%. `crosswalk.subspecialty_by_npi` is likewise
#' unusable — its `npi` column holds 4-5 character surrogate keys, not NPIs.
#'
#' Age is `retirement_year_final - year_of_birth`, per the 2026-08-13 decision
#' to use Doximity `year_of_birth` directly rather than a graduation-year
#' proxy; at this sample size the retirement join, not the age join, is the
#' binding constraint.
#'
#' @param min_confidence Minimum `retirement_confidence_final`. Default 0.60.
#' @param age_range Plausible age-at-exit bounds. Default `c(30, 80)`.
#' @return A single SQL string.
#' @family retirement hazard
#' @concept supply
#' @export
urps_cliff_query <- function(min_confidence = 0.60, age_range = c(30, 80)) {
  taxonomy_urps <- "207VF0040X"
  years         <- 2013:2024

  tax_clause <- paste(
    sprintf("taxonomy_%d = '%s'", 1:14, taxonomy_urps), collapse = " OR "
  )
  roster <- paste(sprintf(
    "SELECT npi, gender FROM credentials.temporal_obgyn_only_%d WHERE %s",
    years, tax_clause
  ), collapse = " UNION ")

  sprintf("
    WITH cohort AS (
      SELECT npi, MAX(gender) AS gender FROM (%s) GROUP BY npi
    ),
    consensus AS (
      SELECT npi, retirement_year_final, retirement_confidence_final
      FROM credentials.retirement_consensus
      WHERE is_retired_final AND retirement_year_final IS NOT NULL
    ),
    birth AS (
      SELECT npi, MAX(year_of_birth) AS year_of_birth
      FROM main.doximity_2024_medical_school
      WHERE year_of_birth IS NOT NULL
      GROUP BY npi
    )
    SELECT
      consensus.retirement_year_final - birth.year_of_birth AS age,
      CASE WHEN upper(cohort.gender) = 'F' THEN 'Female'
           WHEN upper(cohort.gender) = 'M' THEN 'Male' END AS sex,
      consensus.retirement_confidence_final AS confidence_score
    FROM cohort
    JOIN consensus USING (npi)
    JOIN birth USING (npi)
    WHERE cohort.gender IS NOT NULL
      AND consensus.retirement_confidence_final >= %f
      AND consensus.retirement_year_final - birth.year_of_birth
          BETWEEN %d AND %d",
    roster, min_confidence, as.integer(age_range[1]), as.integer(age_range[2])
  )
}

#' Build URPS Age-Specific Retirement Hazard
#'
#' Returns a per-age exit probability table. Its consumer,
#' `advance_urps_agents()`, is archived in inst/archive/supply.R.
#' The default fallback uses the Weibull survival curves from
#' [urps_weibull_exit_probs()] (derived-by-analogy from HWSM Exhibits 17–18),
#' replacing the previous coarse step function.  When a cliff DuckDB is
#' available, a Gompertz model is fitted to observed departure events and
#' returned at the `calibrated` tier.
#'
#' @param cliff_duckdb_path Character path to the cliff DuckDB, or NULL.
#' @param cliff_query Optional SQL returning one row per departure with columns
#'   `age`, `sex`, and (optionally) `confidence_score`. When NULL (default) the
#'   legacy `main`-schema table scan runs first; if that finds none of the
#'   expected tables, [urps_cliff_query()] is attempted automatically before
#'   falling back to the analogy tier. Pass this explicitly to override both.
#' @param require_calibrated Logical; if TRUE, error instead of silently
#'   returning the `derived_by_analogy` Weibull fallback. Use this whenever a
#'   caller depends on the result actually being fitted to observed data — a
#'   missing table or a drifted schema otherwise yields a plausible-looking
#'   102-row curve with `n_events = 0`. Default FALSE (back-compatible).
#' @param min_confidence Minimum cliff confidence score to include. Default 0.60.
#' @param smooth Logical; loess-smooth the Gompertz predictions. Default TRUE.
#' @param scale_shift Numeric scale shift applied to the Weibull fallback only
#'   (cliff Gompertz uses its own fitted parameters). Default 0.
#' @param verbose Logical.
#' @return Named list: `exit_probs` (data frame), `source`, `n_events`,
#'   `hazard_cv`, `weibull_params` (the parameters used in the fallback). A
#'   calibrated result also carries `cohort_source`, naming which route
#'   supplied the departures: `"legacy_main_schema"`, `"cliff_query"` (caller
#'   supplied), or `"urps_cliff_query_auto"`.
#' @importFrom assertthat assert_that
#' @importFrom dplyr mutate filter case_when select bind_rows if_else
#' @importFrom purrr map_dfr
#' @family retirement hazard
#' @concept supply
#' @export
build_urps_exit_hazard <- function(cliff_duckdb_path = NULL,
                                   cliff_query        = NULL,
                                   require_calibrated = FALSE,
                                   min_confidence   = 0.60,
                                   smooth           = TRUE,
                                   scale_shift      = 0,
                                   verbose          = TRUE) {
  ages <- 30:80

  # The fallback is a legitimate result when nobody is relying on calibration,
  # and a silent failure when somebody is. `require_calibrated` lets the caller
  # say which case they are in; every fallback path routes through here.
  .abort_if_required <- function(src, detail) {
    if (isTRUE(require_calibrated)) {
      stop(sprintf(
        "build_urps_exit_hazard(require_calibrated = TRUE): no calibrated hazard could be fitted (%s). %s",
        src, detail
      ), call. = FALSE)
    }
  }

  weibull_fallback <- function() {
    dplyr::bind_rows(
      lapply(c("Female", "Male"), function(s) {
        p <- urps_weibull_exit_probs(ages, s, "ABOG", scale_shift)
        data.frame(age = ages, sex = s,
                   prob_exit        = p,
                   se_prob_exit     = p * 0.15,
                   calibration_tier = "derived_by_analogy",
                   stringsAsFactors = FALSE)
      })
    )
  }

  .weibull_return <- function(src) {
    list(
      exit_probs    = weibull_fallback(),
      source        = src,
      n_events      = 0L,
      hazard_cv     = 0.15,
      weibull_params = list(
        abog_female  = URPS_WEIBULL_PARAMS$abog_female,
        abog_male    = URPS_WEIBULL_PARAMS$abog_male,
        abu          = URPS_WEIBULL_PARAMS$abu,
        scale_shift  = scale_shift
      )
    )
  }

  if (is.null(cliff_duckdb_path) || !file.exists(cliff_duckdb_path)) {
    .abort_if_required(
      "hwsm_weibull_analogy",
      sprintf("cliff_duckdb_path is %s.",
              if (is.null(cliff_duckdb_path)) "NULL" else
                sprintf("'%s', which does not exist", cliff_duckdb_path))
    )
    if (verbose) {
      message(sprintf(
        "build_urps_exit_hazard(): cliff DuckDB unavailable. Using Weibull survival curves (HWSM Exhibits 17-18 analogy, scale_shift=%.1f).",
        scale_shift
      ))
    }
    return(.weibull_return("hwsm_weibull_analogy"))
  }

  if (!requireNamespace("flexsurv", quietly = TRUE)) {
    .abort_if_required("hwsm_weibull_analogy_flexsurv_missing",
                       "Install flexsurv to fit the Gompertz model.")
    warning("flexsurv not installed. Using Weibull fallback.", call. = FALSE)
    return(.weibull_return("hwsm_weibull_analogy_flexsurv_missing"))
  }

  conn <- DBI::dbConnect(duckdb::duckdb(), cliff_duckdb_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  # Run SQL that is expected to yield departure rows. Returns NULL (never
  # raises) so the caller decides between falling back and trying the next
  # source; `strict` distinguishes a caller's explicit query, whose failure is
  # an error worth surfacing, from the speculative auto-attempt below.
  .try_query <- function(sql, label, strict) {
    out <- tryCatch(DBI::dbGetQuery(conn, sql), error = function(e) {
      if (strict) {
        .abort_if_required(paste0(label, "_failed"), conditionMessage(e))
        warning(sprintf("%s failed (%s). Using fallback.", label,
                        conditionMessage(e)), call. = FALSE)
      }
      NULL
    })
    if (is.null(out)) return(NULL)

    missing_cols <- setdiff(c("age", "sex"), names(out))
    if (length(missing_cols)) {
      if (strict) {
        .abort_if_required(paste0(label, "_bad_columns"), sprintf(
          "%s must return column(s): %s.", label, paste(missing_cols, collapse = ", ")))
        warning(sprintf("%s missing column(s) %s. Using fallback.", label,
                        paste(missing_cols, collapse = ", ")), call. = FALSE)
      }
      return(NULL)
    }
    out
  }

  cohort_source <- NA_character_

  if (!is.null(cliff_query)) {
    # Caller-supplied SQL already encodes cohort, joins and confidence filter.
    cliff_data <- .try_query(cliff_query, "cliff_query", strict = TRUE)
    if (is.null(cliff_data)) return(.weibull_return("hwsm_weibull_analogy_query_failed"))
    cohort_source <- "cliff_query"
    cols <- names(cliff_data)
  } else {
    tables <- DBI::dbGetQuery(conn,
      "SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'main'"
    )$table_name

    ret_table <- intersect(
      c("physician_retirement_signals", "retirement_signals", "cliff_results"),
      tables
    )[1]

    if (is.na(ret_table)) {
      # The legacy scan only sees schema 'main'. Rather than fall straight to
      # the analogy tier -- which returns a full-looking 102-row curve and so
      # reads as success at the call site -- try the canonical URPS query
      # before giving up. On a DuckDB without those relations it simply errors
      # and we fall through, so this cannot make any existing caller worse.
      cliff_data <- .try_query(urps_cliff_query(min_confidence = min_confidence),
                               "urps_cliff_query", strict = FALSE)

      if (!is.null(cliff_data) && nrow(cliff_data) > 0) {
        cohort_source <- "urps_cliff_query_auto"
        cols <- names(cliff_data)
        if (verbose) {
          message(sprintf(
            "build_urps_exit_hazard(): no retirement table in schema 'main'; auto-resolved %d departures via urps_cliff_query().",
            nrow(cliff_data)))
        }
      } else {
        .abort_if_required("hwsm_weibull_analogy_no_table", paste(
          "None of physician_retirement_signals/retirement_signals/cliff_results",
          "exist in schema 'main', and urps_cliff_query() returned no rows against",
          "this database."))
        warning("No retirement table found in cliff DuckDB. Using fallback.", call. = FALSE)
        return(list(
          exit_probs     = weibull_fallback(),
          source         = "hwsm_weibull_analogy_no_table",
          n_events       = 0L,
          hazard_cv      = 0.15,
          weibull_params = list(scale_shift = scale_shift)
        ))
      }
    } else {
      cols <- DBI::dbGetQuery(conn, sprintf(
        "SELECT column_name FROM information_schema.columns
         WHERE table_name = '%s'", ret_table
      ))$column_name

      conf_col    <- intersect(c("retirement_confidence_score", "confidence_score"), cols)[1]
      conf_clause <- if (!is.na(conf_col))
        sprintf("WHERE %s >= %f", conf_col, min_confidence) else ""

      cliff_data <- DBI::dbGetQuery(conn,
        sprintf("SELECT * FROM %s %s", ret_table, conf_clause)
      )
      cohort_source <- "legacy_main_schema"
    }
  }

  if (nrow(cliff_data) < 30) {
    .abort_if_required("hwsm_weibull_analogy_insufficient_cliff", sprintf(
      "Only %d departure records passed the filters; %d are required.",
      nrow(cliff_data), 30L))
    warning(sprintf(
      "Only %d cliff records. Using fallback.", nrow(cliff_data)
    ), call. = FALSE)
    return(list(
      exit_probs = weibull_fallback(),
      source        = "hwsm_weibull_analogy_insufficient_cliff",
      n_events      = nrow(cliff_data),
      hazard_cv     = 0.15,
      weibull_params = list(scale_shift = scale_shift)
    ))
  }

  retire_col <- intersect(
    c("retirement_year_estimated", "estimated_retirement_year"), cols)[1]
  age_col    <- intersect(c("age", "age_at_event"), cols)[1]

  all_fits <- purrr::map_dfr(c("Female", "Male"), function(s) {
    sex_data <- cliff_data
    if ("sex" %in% cols)
      sex_data <- sex_data[tolower(sex_data$sex) == tolower(s), ]

    if (nrow(sex_data) < 10)
      return(weibull_fallback()[weibull_fallback()$sex == s, ])

    # Neither an age column nor a retirement-year column: fall back to the
    # analogy tier rather than indexing sex_data[[NA]] (subscript error).
    if (is.na(age_col) && is.na(retire_col))
      return(weibull_fallback()[weibull_fallback()$sex == s, ])

    ref_yr     <- 2023L
    age_at_ret <- if (!is.na(age_col)) sex_data[[age_col]]
                  else ref_yr - sex_data[[retire_col]] + 50L
    age_at_ret <- pmax(30L, pmin(80L, as.integer(age_at_ret)))

    expanded <- data.frame(age = age_at_ret, event = 1L,
                           stringsAsFactors = FALSE)

    fit <- tryCatch(
      flexsurv::flexsurvreg(survival::Surv(age, event) ~ 1,
                             data = expanded, dist = "gompertz"),
      error = function(e) NULL
    )

    if (is.null(fit))
      return(weibull_fallback()[weibull_fallback()$sex == s, ])

    coef_vals <- stats::coef(fit)
    shape     <- coef_vals[["shape"]]
    rate      <- exp(coef_vals[["rate"]])

    S      <- function(t) exp(-rate / shape * (exp(shape * t) - 1))
    S_age  <- pmax(1e-10, S(ages))
    S_age1 <- pmax(1e-10, S(ages + 1))
    p_exit <- pmax(0, pmin(0.99, 1 - S_age1 / S_age))

    if (smooth && length(ages) > 10) {
      sf <- tryCatch(
        stats::loess(p_exit ~ ages, span = 0.5), error = function(e) NULL
      )
      if (!is.null(sf)) p_exit <- pmax(0, pmin(0.99, stats::predict(sf)))
    }

    vcov_mat  <- tryCatch(stats::vcov(fit), error = function(e) NULL)
    hcv       <- if (!is.null(vcov_mat))
      sqrt(diag(vcov_mat))[["shape"]] / abs(shape) else 0

    data.frame(
      age              = ages,
      sex              = s,
      prob_exit        = p_exit,
      se_prob_exit     = p_exit * hcv,
      calibration_tier = "calibrated",
      stringsAsFactors = FALSE
    )
  })

  # A per-sex fallback inside the fit above leaves analogy rows in an otherwise
  # "calibrated" result -- the partial case the caller also needs to know about.
  uncalibrated <- unique(all_fits$sex[all_fits$calibration_tier != "calibrated"])
  if (length(uncalibrated)) {
    .abort_if_required("partial_fit", sprintf(
      "Gompertz fit fell back to the analogy tier for: %s (too few events, or the fit did not converge).",
      paste(uncalibrated, collapse = ", ")))
    if (verbose) {
      message(sprintf(
        "build_urps_exit_hazard(): analogy-tier fallback for %s; result is only partially calibrated.",
        paste(uncalibrated, collapse = ", ")))
    }
  }

  n_events  <- nrow(cliff_data)
  hazard_cv <- if (any(all_fits$se_prob_exit > 0, na.rm = TRUE))
    mean(all_fits$se_prob_exit / pmax(all_fits$prob_exit, 0.001),
         na.rm = TRUE)
  else 0

  if (verbose) {
    message(sprintf(
      "build_urps_exit_hazard(): cliff empirical | n_events=%d | hazard_cv=%.3f",
      n_events, hazard_cv
    ))
  }

  return(list(
    exit_probs    = all_fits,
    source        = "cliff_empirical_gompertz",
    cohort_source = cohort_source,
    n_events      = n_events,
    hazard_cv     = hazard_cv
  ))
}
