################################################################################
# R/39-cliff_retirement_hazard.R
# Empirical URPS retirement hazard from mufflyt/cliff pipeline
#
# Addresses README item 4: retirement uncertainty unquantified.
# Calibration tier: calibrated (with cliff) / derived_by_analogy (fallback)
################################################################################

#' Build URPS Age-Specific Retirement Hazard
#'
#' @param cliff_duckdb_path Character or NULL.
#' @param min_confidence Numeric. Default: 0.60.
#' @param smooth Logical. Default: TRUE.
#' @param verbose Logical. Default: TRUE.
#'
#' @return Named list: exit_probs, source, n_events, hazard_cv.
#' @importFrom assertthat assert_that
#' @importFrom dplyr mutate filter case_when select bind_rows if_else
#' @importFrom purrr map_dfr
#' @export
build_urps_exit_hazard <- function(cliff_duckdb_path = NULL,
                                   min_confidence   = 0.60,
                                   smooth           = TRUE,
                                   verbose          = TRUE) {
  ages <- 30:80

  fraher_fallback <- function() {
    female_probs <- dplyr::case_when(
      ages < 55 ~ 0.005,
      ages < 60 ~ 0.012,
      ages < 63 ~ 0.048,
      ages < 66 ~ 0.105,
      ages < 70 ~ 0.205,
      TRUE      ~ 0.385
    )
    male_probs <- dplyr::case_when(
      ages < 55 ~ 0.004,
      ages < 60 ~ 0.010,
      ages < 63 ~ 0.042,
      ages < 66 ~ 0.095,
      ages < 70 ~ 0.190,
      TRUE      ~ 0.370
    )
    rbind(
      data.frame(age = ages, sex = "Female",
                 prob_exit = female_probs, se_prob_exit = 0,
                 calibration_tier = "derived_by_analogy",
                 stringsAsFactors = FALSE),
      data.frame(age = ages, sex = "Male",
                 prob_exit = male_probs, se_prob_exit = 0,
                 calibration_tier = "derived_by_analogy",
                 stringsAsFactors = FALSE)
    )
  }

  if (is.null(cliff_duckdb_path) || !file.exists(cliff_duckdb_path)) {
    if (verbose) {
      message(
        "build_urps_exit_hazard(): cliff DuckDB not available.\n",
        "  Using Fraher (2024) Figure 4 pediatric analogy.\n",
        "  Calibration tier: derived_by_analogy\n",
        "  hazard_cv = 0 (announced, not invented)."
      )
    }
    return(list(
      exit_probs = fraher_fallback(),
      source     = "fraher_fig4_analogy",
      n_events   = 0L,
      hazard_cv  = 0
    ))
  }

  if (!requireNamespace("flexsurv", quietly = TRUE)) {
    warning("flexsurv not installed. Falling back to Fraher Fig 4 analogy.")
    return(list(
      exit_probs = fraher_fallback(),
      source     = "fraher_fig4_analogy_flexsurv_missing",
      n_events   = 0L,
      hazard_cv  = 0
    ))
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
    warning("No retirement table found in cliff DuckDB. Using fallback.")
    return(list(
      exit_probs = fraher_fallback(),
      source     = "fraher_fig4_analogy_no_table",
      n_events   = 0L,
      hazard_cv  = 0
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
    ))
    return(list(
      exit_probs = fraher_fallback(),
      source     = "fraher_fig4_analogy_insufficient_cliff",
      n_events   = nrow(cliff_data),
      hazard_cv  = 0
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
      return(fraher_fallback()[fraher_fallback()$sex == s, ])

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
      return(fraher_fallback()[fraher_fallback()$sex == s, ])

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
