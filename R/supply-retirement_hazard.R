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

# ---- cliff age-band empirical hazard (calibrated) ---------------------------
#
# The cliff pipeline computes a proper exposure-based departure hazard: for each
# age band it observes person-years at risk and departure events, so
# annual_hazard = events / person_years is a real age-conditional hazard with a
# risk set -- not the age-at-retirement distribution among leavers that an
# events-only parametric fit would give. This ingests that finished table
# (vendored under inst/extdata/provider_year) and expands it to a per-age curve
# with exposure-based uncertainty, at the `calibrated` tier.

# Parse the age-band label column ("<45", "45-49", ..., "70+") to numeric
# [lo, hi] bounds, clamped to the requested age window.
.parse_urps_ageband_bounds <- function(raw, min_age, max_age) {
  parse_one <- function(lbl) {
    lbl <- trimws(as.character(lbl))
    if (grepl("^<", lbl)) {
      return(c(min_age, as.integer(sub("^<", "", lbl)) - 1L))
    }
    if (grepl("\\+$", lbl)) {
      return(c(as.integer(sub("\\+$", "", lbl)), max_age))
    }
    parts <- as.integer(strsplit(lbl, "-", fixed = TRUE)[[1]])
    c(parts[1L], parts[2L])
  }
  b <- vapply(raw$age_band, parse_one, integer(2))
  data.frame(
    age_band      = as.character(raw$age_band),
    age_lo        = b[1L, ],
    age_hi        = b[2L, ],
    person_years  = as.numeric(raw$person_years),
    events        = as.integer(raw$events),
    annual_hazard = as.numeric(raw$annual_hazard),
    stringsAsFactors = FALSE
  )
}

# Expand the age-band table to one exit probability per integer age, for one
# sex. A positive `scale_shift` delays retirement: the hazard at age a is read
# from age (a - scale_shift), matching the Weibull path's "+2 = later" lever.
# Bands with zero observed events (the sparse 70+ band: 0 events / 16 py) carry
# no information that a hazard is zero, so those ages fall back to the Weibull
# analogy, floored at the highest observed-band hazard so an old-age hazard is
# never below a younger observed one (retirement risk does not fall with age).
.urps_ageband_exit_probs <- function(ageband_tbl, ages, sex, scale_shift = 0,
                                     observed_floor = 0) {
  lookup_age <- ages - scale_shift
  band_index <- function(a) {
    idx <- which(a >= ageband_tbl$age_lo & a <= ageband_tbl$age_hi)
    if (length(idx) == 0L) {
      idx <- if (a < min(ageband_tbl$age_lo)) which.min(ageband_tbl$age_lo)
             else which.max(ageband_tbl$age_hi)
    }
    idx[1L]
  }
  prob <- numeric(length(ages))
  se   <- numeric(length(ages))
  tier <- character(length(ages))
  for (i in seq_along(ages)) {
    b  <- band_index(lookup_age[i])
    ev <- ageband_tbl$events[b]
    if (ev > 0L) {
      h <- ageband_tbl$annual_hazard[b]
      prob[i] <- h
      # Poisson relative SE of an events/exposure rate is 1/sqrt(events).
      se[i]   <- h / sqrt(ev)
      tier[i] <- "calibrated"
    } else {
      wp <- as.numeric(urps_weibull_exit_probs(ages[i], sex, "ABOG", scale_shift))
      prob[i] <- max(wp, observed_floor)
      se[i]   <- prob[i] * 0.15
      tier[i] <- "derived_by_analogy"
    }
  }
  data.frame(age = ages, sex = sex, prob_exit = prob,
             se_prob_exit = se, calibration_tier = tier,
             stringsAsFactors = FALSE)
}

# Build the full exit-hazard contract from the cliff age-band CSV. Returns NULL
# (so the caller can fall back) if the file lacks the required columns.
.cliff_ageband_exit_hazard <- function(csv_path, ages, scale_shift, smooth, verbose) {
  raw <- utils::read.csv(csv_path, stringsAsFactors = FALSE,
                         check.names = FALSE)
  required <- c("age_band", "person_years", "events", "annual_hazard")
  if (!all(required %in% names(raw))) {
    return(NULL)
  }
  ageband_tbl <- .parse_urps_ageband_bounds(raw, min(ages), max(ages))
  # Floor for zero-event (extrapolated) ages: the highest hazard we actually
  # observed, so retirement risk never falls with age across the boundary.
  observed_floor <- if (any(ageband_tbl$events > 0L)) {
    max(ageband_tbl$annual_hazard[ageband_tbl$events > 0L])
  } else 0
  probs <- dplyr::bind_rows(lapply(c("Female", "Male"), function(s) {
    .urps_ageband_exit_probs(ageband_tbl, ages, s, scale_shift, observed_floor)
  }))
  if (isTRUE(smooth) && length(ages) > 10L) {
    probs <- dplyr::bind_rows(lapply(split(probs, probs$sex), function(d) {
      d <- d[order(d$age), ]
      sf <- tryCatch(stats::loess(prob_exit ~ age, data = d, span = 0.5),
                     error = function(e) NULL)
      if (!is.null(sf)) {
        d$prob_exit <- pmax(0, pmin(0.99, as.numeric(stats::predict(sf))))
      }
      d
    }))
    rownames(probs) <- NULL
  }
  total_events <- sum(ageband_tbl$events)
  # Overall relative uncertainty of the pooled rate is 1/sqrt(total events):
  # a real, data-driven hazard_cv (contrast the old fixed 0.15 / the assumed 0).
  hazard_cv <- if (total_events > 0L) 1 / sqrt(total_events) else 0
  if (isTRUE(verbose)) {
    message(sprintf(
      paste0("build_urps_exit_hazard(): cliff age-band empirical (calibrated) | ",
             "n_events=%d | hazard_cv=%.3f | scale_shift=%.1f"),
      total_events, hazard_cv, scale_shift))
  }
  list(exit_probs = probs, source = "cliff_ageband_empirical",
       n_events = as.integer(total_events), hazard_cv = hazard_cv,
       weibull_params = list(scale_shift = scale_shift))
}

#' Build URPS Age-Specific Retirement Hazard
#'
#' Returns a per-age exit probability table. Its consumer,
#' `advance_urps_agents()`, is archived in inst/archive/supply.R.
#' By default the calibrated source is the cliff pipeline's exposure-based
#' age-band hazard (person-years at risk and departure events per age band,
#' vendored under `inst/extdata/provider_year`), expanded to a per-age curve at
#' the `calibrated` tier with Poisson exposure-based uncertainty. This is a real
#' age-conditional hazard (a risk set), preferred over both the events-only
#' DuckDB Gompertz fit and the Weibull analogy. Sparse zero-event bands (the
#' 70+ band has 0 events in 16 person-years) fall back to the Weibull analogy
#' for those ages rather than asserting no retirement. When the age-band file is
#' absent and a cliff DuckDB is supplied, a Gompertz model is fitted to observed
#' departure events; otherwise the Weibull survival curves from
#' [urps_weibull_exit_probs()] (derived-by-analogy from HWSM Exhibits 17–18) are
#' used.
#'
#' @param cliff_duckdb_path Character path to the cliff DuckDB, or NULL. Used
#'   only when `cliff_ageband_csv` is unavailable.
#' @param cliff_ageband_csv Character path to the cliff exposure-based age-band
#'   hazard CSV (columns `age_band`, `person_years`, `events`, `annual_hazard`).
#'   Defaults to the vendored copy under `inst/extdata/provider_year`; set to
#'   `NULL` to force the DuckDB/Weibull path.
#' @param min_confidence Minimum cliff confidence score to include. Default 0.60.
#' @param smooth Logical; loess-smooth the per-age predictions. Default TRUE.
#' @param scale_shift Numeric years to shift the hazard curve along age
#'   (`+2` = later/delayed retirement). Applied to the cliff age-band and
#'   Weibull paths alike.
#' @param verbose Logical.
#' @return Named list: `exit_probs` (data frame), `source`, `n_events`,
#'   `hazard_cv`, and `weibull_params` (present when a Weibull path is used).
#' @importFrom assertthat assert_that
#' @importFrom dplyr mutate filter case_when select bind_rows if_else
#' @importFrom purrr map_dfr
#' @family retirement hazard
#' @concept supply
#' @export
build_urps_exit_hazard <- function(cliff_duckdb_path = NULL,
                                   cliff_ageband_csv = system.file(
                                     "extdata", "provider_year",
                                     "retirement_hazard_by_ageband.csv",
                                     package = "urpssim"),
                                   min_confidence   = 0.60,
                                   smooth           = TRUE,
                                   scale_shift      = 0,
                                   verbose          = TRUE) {
  ages <- 30:80

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

  # Preferred calibrated source: cliff's exposure-based age-band hazard. It is
  # the correct empirical construction (events over person-years at risk), so it
  # takes precedence over the events-only DuckDB Gompertz fit and the analogy.
  if (!is.null(cliff_ageband_csv) && nzchar(cliff_ageband_csv) &&
      file.exists(cliff_ageband_csv)) {
    ab <- .cliff_ageband_exit_hazard(cliff_ageband_csv, ages, scale_shift,
                                     smooth, verbose)
    if (!is.null(ab)) {
      return(ab)
    }
    if (verbose) {
      message("build_urps_exit_hazard(): cliff age-band CSV lacks required columns; continuing to DuckDB/Weibull.")
    }
  }

  if (is.null(cliff_duckdb_path) || !file.exists(cliff_duckdb_path)) {
    if (verbose) {
      message(sprintf(
        "build_urps_exit_hazard(): cliff DuckDB unavailable. Using Weibull survival curves (HWSM Exhibits 17-18 analogy, scale_shift=%.1f).",
        scale_shift
      ))
    }
    return(.weibull_return("hwsm_weibull_analogy"))
  }

  if (!requireNamespace("flexsurv", quietly = TRUE)) {
    warning("flexsurv not installed. Using Weibull fallback.", call. = FALSE)
    return(.weibull_return("hwsm_weibull_analogy_flexsurv_missing"))
  }

  conn <- DBI::dbConnect(duckdb::duckdb(), cliff_duckdb_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  tables <- DBI::dbGetQuery(conn,
    "SELECT table_name FROM information_schema.tables
     WHERE table_schema = 'main'"
  )$table_name

  ret_table <- intersect(
    c("physician_retirement_signals", "retirement_signals", "cliff_results"),
    tables
  )[1]

  if (is.na(ret_table)) {
    warning("No retirement table found in cliff DuckDB. Using fallback.", call. = FALSE)
    return(list(
      exit_probs = weibull_fallback(),
      source        = "hwsm_weibull_analogy_no_table",
      n_events      = 0L,
      hazard_cv     = 0.15,
      weibull_params = list(scale_shift = scale_shift)
    ))
  }

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

  if (nrow(cliff_data) < 30) {
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
    exit_probs = all_fits,
    source     = "cliff_empirical_gompertz",
    n_events   = n_events,
    hazard_cv  = hazard_cv
  ))
}
