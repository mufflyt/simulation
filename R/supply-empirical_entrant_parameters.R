# Build evidence-based URPS entrant parameters from empirical sources ------


#' Build evidence-based URPS entrant parameters
#'
#' Constructs cohort-specific entrant parameters from the strongest available
#' empirical evidence. ACGME year-1 counts determine parent-specialty mix.
#' AAMC URPS-specific counts inform sex, geographic retention, and full-time
#' academic faculty probabilities. Individual provider profiles, when supplied,
#' determine age, clinical FTE, employment, urbanicity, and practice setting.
#'
#' The function deliberately fails closed for important quantities that cannot
#' be estimated from empirical provider profiles unless `strict = FALSE`.
#'
#' @param cohort_counts Tibble with `cohort_year` and `n_entrants`.
#' @param provider_profiles Optional historical entrant-level provider tibble.
#' @param available_by Latest publication year permitted in calibration.
#' @param recent_years Number of recent years used for empirical profiles.
#' @param strict If TRUE, stop when age or clinical FTE lacks empirical data.
#' @param seed Random seed.
#' @param save_dir Optional directory for timestamped parameter output.
#'
#' @return List containing cohort parameters, evidence registry, historical
#'   ACGME series, and a dynamic summary sentence.
#'
#' @family supply
#' @concept supply
#' @export
build_empirical_entrant_parameters <- function(
    cohort_counts,
    provider_profiles = NULL,
    available_by = 2026L,
    recent_years = 10L,
    strict = TRUE,
    seed = 20260821L,
    save_dir = NULL) {

  base::message(
    "[entrant-evidence] Starting empirical entrant calibration."
  )
  base::message(
    "[entrant-evidence] Publication cutoff: ",
    available_by,
    "."
  )
  base::message(
    "[entrant-evidence] Random seed: ",
    seed,
    "."
  )

  base::set.seed(seed)

  required_counts <- base::c(
    "cohort_year",
    "n_entrants"
  )

  missing_counts <- base::setdiff(
    required_counts,
    base::names(cohort_counts)
  )

  if (base::length(missing_counts) > 0L) {
    base::stop(
      "Missing cohort-count columns: ",
      base::paste(
        missing_counts,
        collapse = ", "
      ),
      ".",
      call. = FALSE
    )
  }

  cohort_tbl <- cohort_counts |>
    tibble::as_tibble() |>
    dplyr::transmute(
      cohort_year = base::as.integer(.data$cohort_year),
      n_entrants = base::as.integer(.data$n_entrants)
    ) |>
    dplyr::arrange(.data$cohort_year)

  if (base::anyNA(cohort_tbl)) {
    base::stop(
      "`cohort_counts` cannot contain missing values.",
      call. = FALSE
    )
  }

  if (base::any(cohort_tbl$n_entrants < 0L)) {
    base::stop(
      "`n_entrants` cannot be negative.",
      call. = FALSE
    )
  }

  base::message(
    "[entrant-evidence] Requested ",
    scales::comma(base::sum(cohort_tbl$n_entrants)),
    " entrants across ",
    scales::comma(base::nrow(cohort_tbl)),
    " cohorts."
  )

  base::message(
    "[entrant-evidence] Reading ACGME year-1 fellow counts."
  )

  acgme_tbl <- acgme_urps_fellows(
    available_by = available_by
  ) |>
    tibble::as_tibble() |>
    dplyr::filter(
      base::is.finite(.data$year_1),
      .data$year_1 >= 0
    )

  if (base::nrow(acgme_tbl) == 0L) {
    base::stop(
      "No usable ACGME entrant observations.",
      call. = FALSE
    )
  }

  pathway_tbl <- acgme_tbl |>
    dplyr::group_by(.data$entry_year) |>
    dplyr::summarise(
      n_obgyn = base::sum(
        .data$year_1[
          .data$parent == "obgyn"
        ],
        na.rm = TRUE
      ),
      n_urology = base::sum(
        .data$year_1[
          .data$parent == "urology"
        ],
        na.rm = TRUE
      ),
      n_total = .data$n_obgyn + .data$n_urology,
      prop_obgyn = .data$n_obgyn / .data$n_total,
      .groups = "drop"
    ) |>
    dplyr::filter(.data$n_total > 0L) |>
    dplyr::arrange(.data$entry_year)

  base::message(
    "[entrant-evidence] ACGME years: ",
    base::min(pathway_tbl$entry_year),
    "-",
    base::max(pathway_tbl$entry_year),
    "."
  )

  base::message(
    "[entrant-evidence] Latest ACGME cohort: ",
    scales::comma(
      pathway_tbl$n_total[
        base::nrow(pathway_tbl)
      ]
    ),
    " entrants."
  )

  pathway_model <- stats::glm(
    cbind(n_obgyn, n_urology) ~ entry_year,
    data = pathway_tbl,
    family = stats::binomial()
  )

  pathway_prediction_tbl <- cohort_tbl |>
    dplyr::transmute(
      entry_year = .data$cohort_year
    )

  predicted_obgyn <- stats::predict(
    pathway_model,
    newdata = pathway_prediction_tbl,
    type = "response"
  )

  predicted_obgyn <- base::pmin(
    0.995,
    base::pmax(
      0.005,
      predicted_obgyn
    )
  )

  base::message(
    "[entrant-evidence] Adding AAMC URPS-specific sex evidence."
  )

  aamc_sex_tbl <- tibble::tribble(
    ~parent, ~female, ~total,
    "obgyn", 108L, 138L,
    "urology", 28L, 33L
  ) |>
    dplyr::mutate(
      male = .data$total - .data$female,
      beta_alpha = .data$female + 0.5,
      beta_beta = .data$male + 0.5,
      posterior_mean =
        .data$beta_alpha /
        (.data$beta_alpha + .data$beta_beta)
    )

  female_obgyn <- aamc_sex_tbl |>
    dplyr::filter(.data$parent == "obgyn") |>
    dplyr::pull(.data$posterior_mean)

  female_urology <- aamc_sex_tbl |>
    dplyr::filter(.data$parent == "urology") |>
    dplyr::pull(.data$posterior_mean)

  predicted_female <- (
    predicted_obgyn * female_obgyn
  ) + (
    (1 - predicted_obgyn) * female_urology
  )

  fellowship_years_obgyn <- 3
  fellowship_years_urology <- 2

  base::message(
    "[entrant-evidence] Estimating ACGME-to-certification realization."
  )

  conversion_fit <- entrant_to_cert_ratio(
    source = "acgme",
    through_year = available_by,
    pooled = TRUE,
    exclude_disrupted = TRUE
  )

  pipeline_realization <- conversion_fit$ratio

  if (!base::is.finite(pipeline_realization) ||
      pipeline_realization <= 0 ||
      pipeline_realization > 1) {
    base::stop(
      "Empirical pipeline realization is outside (0, 1].",
      call. = FALSE
    )
  }

  base::message(
    "[entrant-evidence] Pooled entry-to-certification realization: ",
    scales::percent(
      pipeline_realization,
      accuracy = 0.1
    ),
    "."
  )

  base::message(
    "[entrant-evidence] Adding AAMC same-state retention evidence."
  )

  retention_tbl <- tibble::tribble(
    ~parent, ~retained, ~total,
    "obgyn", 81L, 147L,
    "urology", 23L, 41L
  ) |>
    dplyr::mutate(
      moved = .data$total - .data$retained,
      beta_alpha = .data$retained + 0.5,
      beta_beta = .data$moved + 0.5,
      posterior_mean =
        .data$beta_alpha /
        (.data$beta_alpha + .data$beta_beta)
    )

  retention_obgyn <- retention_tbl |>
    dplyr::filter(.data$parent == "obgyn") |>
    dplyr::pull(.data$posterior_mean)

  retention_urology <- retention_tbl |>
    dplyr::filter(.data$parent == "urology") |>
    dplyr::pull(.data$posterior_mean)

  predicted_retention <- (
    predicted_obgyn * retention_obgyn
  ) + (
    (1 - predicted_obgyn) * retention_urology
  )

  base::message(
    "[entrant-evidence] Adding AAMC full-time faculty lower bound."
  )

  faculty_tbl <- tibble::tribble(
    ~parent, ~faculty, ~total,
    "obgyn", 55L, 150L,
    "urology", 14L, 42L
  ) |>
    dplyr::mutate(
      nonfaculty = .data$total - .data$faculty,
      beta_alpha = .data$faculty + 0.5,
      beta_beta = .data$nonfaculty + 0.5,
      posterior_mean =
        .data$beta_alpha /
        (.data$beta_alpha + .data$beta_beta)
    )

  faculty_obgyn <- faculty_tbl |>
    dplyr::filter(.data$parent == "obgyn") |>
    dplyr::pull(.data$posterior_mean)

  faculty_urology <- faculty_tbl |>
    dplyr::filter(.data$parent == "urology") |>
    dplyr::pull(.data$posterior_mean)

  faculty_lower_bound <- (
    predicted_obgyn * faculty_obgyn
  ) + (
    (1 - predicted_obgyn) * faculty_urology
  )

  empirical_age_mean <- NA_real_
  empirical_age_sd <- NA_real_
  empirical_age_median <- NA_real_
  empirical_age_p25 <- NA_real_
  empirical_age_p75 <- NA_real_

  empirical_fte_mean <- NA_real_
  empirical_fte_sd <- NA_real_
  empirical_fte_median <- NA_real_
  empirical_fte_p25 <- NA_real_
  empirical_fte_p75 <- NA_real_

  empirical_academic <- NA_real_
  empirical_employed <- NA_real_
  empirical_urban <- NA_real_

  profile_n <- 0L

  if (!base::is.null(provider_profiles)) {

    base::message(
      "[entrant-evidence] Processing provider-level entrant profiles."
    )

    profile_tbl <- provider_profiles |>
      tibble::as_tibble()

    if ("entry_year" %in% base::names(profile_tbl)) {

      max_profile_year <- base::max(
        profile_tbl$entry_year,
        na.rm = TRUE
      )

      profile_tbl <- profile_tbl |>
        dplyr::filter(
          .data$entry_year >=
            max_profile_year - recent_years + 1L
        )

      base::message(
        "[entrant-evidence] Restricted provider profiles to ",
        max_profile_year - recent_years + 1L,
        "-",
        max_profile_year,
        "."
      )
    }

    profile_n <- base::nrow(profile_tbl)

    base::message(
      "[entrant-evidence] Empirical donor profiles: ",
      scales::comma(profile_n),
      "."
    )

    if ("age_at_entry" %in% base::names(profile_tbl)) {

      age_values <- profile_tbl$age_at_entry

      age_values <- age_values[
        base::is.finite(age_values) &
          age_values >= 25 &
          age_values <= 60
      ]

      if (base::length(age_values) >= 10L) {

        empirical_age_mean <- base::mean(age_values)
        empirical_age_sd <- stats::sd(age_values)
        empirical_age_median <- stats::median(age_values)
        empirical_age_p25 <- base::unname(
          stats::quantile(age_values, 0.25)
        )
        empirical_age_p75 <- base::unname(
          stats::quantile(age_values, 0.75)
        )

        base::message(
          "[entrant-evidence] Age at entry: mean ",
          base::sprintf("%.1f", empirical_age_mean),
          " (SD ",
          base::sprintf("%.1f", empirical_age_sd),
          "), median ",
          base::sprintf("%.1f", empirical_age_median),
          " (p25 ",
          base::sprintf("%.1f", empirical_age_p25),
          ", p75 ",
          base::sprintf("%.1f", empirical_age_p75),
          ")."
        )
      }
    }

    if ("initial_clinical_fte" %in%
        base::names(profile_tbl)) {

      fte_values <- profile_tbl$initial_clinical_fte

      fte_values <- fte_values[
        base::is.finite(fte_values) &
          fte_values > 0 &
          fte_values <= 2
      ]

      if (base::length(fte_values) >= 10L) {

        empirical_fte_mean <- base::mean(fte_values)
        empirical_fte_sd <- stats::sd(fte_values)
        empirical_fte_median <- stats::median(fte_values)
        empirical_fte_p25 <- base::unname(
          stats::quantile(fte_values, 0.25)
        )
        empirical_fte_p75 <- base::unname(
          stats::quantile(fte_values, 0.75)
        )

        base::message(
          "[entrant-evidence] Clinical FTE: mean ",
          base::sprintf("%.2f", empirical_fte_mean),
          " (SD ",
          base::sprintf("%.2f", empirical_fte_sd),
          "), median ",
          base::sprintf("%.2f", empirical_fte_median),
          " (p25 ",
          base::sprintf("%.2f", empirical_fte_p25),
          ", p75 ",
          base::sprintf("%.2f", empirical_fte_p75),
          ")."
        )
      }
    }

    if ("academic" %in% base::names(profile_tbl)) {
      empirical_academic <- base::mean(
        profile_tbl$academic,
        na.rm = TRUE
      )
    }

    if ("employed" %in% base::names(profile_tbl)) {
      empirical_employed <- base::mean(
        profile_tbl$employed,
        na.rm = TRUE
      )
    }

    if ("urban" %in% base::names(profile_tbl)) {
      empirical_urban <- base::mean(
        profile_tbl$urban,
        na.rm = TRUE
      )
    }
  }

  if (base::isTRUE(strict) &&
      !base::is.finite(empirical_age_mean)) {
    base::stop(
      paste0(
        "No empirical `age_at_entry` distribution is available. ",
        "Do not silently restore age_mean = 34.5. Build age at entry ",
        "from the provider-level entrant cohort or set `strict = FALSE` ",
        "for exploratory analysis."
      ),
      call. = FALSE
    )
  }

  if (base::isTRUE(strict) &&
      !base::is.finite(empirical_fte_mean)) {
    base::stop(
      paste0(
        "No empirical `initial_clinical_fte` distribution is available. ",
        "HRSA professional hours are not clinical FTE. Supply an ",
        "entrant-level clinical-FTE source before production simulation."
      ),
      call. = FALSE
    )
  }

  age_mean_used <- if (base::is.finite(empirical_age_mean)) {
    empirical_age_mean
  } else {
    34.5
  }

  age_sd_used <- if (base::is.finite(empirical_age_sd)) {
    empirical_age_sd
  } else {
    2.8
  }

  fte_mean_used <- if (base::is.finite(empirical_fte_mean)) {
    empirical_fte_mean
  } else {
    0.82
  }

  fte_sd_used <- if (base::is.finite(empirical_fte_sd)) {
    empirical_fte_sd
  } else {
    0.12
  }

  academic_used <- if (base::is.finite(empirical_academic)) {
    empirical_academic
  } else {
    faculty_lower_bound
  }

  employed_used <- if (base::is.finite(empirical_employed)) {
    empirical_employed
  } else {
    0.84
  }

  urban_used <- if (base::is.finite(empirical_urban)) {
    empirical_urban
  } else {
    0.90
  }

  parameter_tbl <- cohort_tbl |>
    dplyr::mutate(
      age_mean = age_mean_used,
      age_sd = age_sd_used,
      age_min = base::pmax(
        25,
        .data$age_mean - 3 * .data$age_sd
      ),
      age_max = base::pmin(
        60,
        .data$age_mean + 4 * .data$age_sd
      ),
      prob_female = predicted_female,
      prob_obgyn = predicted_obgyn,
      fellowship_years_obgyn =
        fellowship_years_obgyn,
      fellowship_years_urology =
        fellowship_years_urology,
      completion_prob_obgyn =
        pipeline_realization,
      completion_prob_urology =
        pipeline_realization,
      prob_academic = academic_used,
      prob_employed = employed_used,
      prob_urban = urban_used,
      fte_mean = fte_mean_used,
      fte_sd = fte_sd_used,
      same_state_retention =
        predicted_retention,
      academic_faculty_lower_bound =
        faculty_lower_bound
    )

  evidence_tbl <- tibble::tribble(
    ~parameter,
    ~source,
    ~evidence_type,
    ~status,
    ~interpretation,

    "entrant_count",
    "ACGME Data Resource Book",
    "URPS-specific direct count",
    "empirical",
    "Year-1 fellows on duty; preferred entrant flow.",

    "parent_specialty",
    "ACGME Data Resource Book",
    "URPS-specific direct count",
    "empirical",
    "OB/GYN and urology year-1 fellows modeled separately.",

    "fellowship_duration",
    "NRMP URPS Match",
    "URPS-specific program rule",
    "empirical",
    "Three years after OB/GYN; two after urology.",

    "sex",
    "AAMC Report on Residents Table B3",
    "URPS-specific active-fellow counts",
    "empirical_prior",
    paste0(
      "2024-25 stock distribution; used as a Beta prior, ",
      "not an exact entrant-flow rate."
    ),

    "pipeline_realization",
    "ACGME + ABOG/ABU certification series",
    "URPS-specific longitudinal aggregate",
    "empirical",
    paste0(
      "Entry-to-certification realization; must not be ",
      "described as pure fellowship completion."
    ),

    "training_state_retention",
    "AAMC Report on Residents Table C4",
    "URPS-specific post-GME location",
    "empirical_prior",
    "Same-state retention after training.",

    "academic_probability",
    "AAMC faculty appointment tables",
    "URPS-specific post-GME faculty status",
    "lower_bound",
    paste0(
      "Full-time U.S. medical-school faculty undercounts ",
      "all academic practice."
    ),

    "age_at_entry",
    "Provider-level entrant panel",
    "Individual-level",
    dplyr::if_else(
      base::is.finite(empirical_age_mean),
      "empirical",
      "legacy_exploratory"
    ),
    dplyr::if_else(
      base::is.finite(empirical_age_mean),
      "Estimated from observed recent entrants.",
      "No direct empirical distribution supplied."
    ),

    "initial_clinical_fte",
    "Provider productivity/practice survey",
    "Individual-level",
    dplyr::if_else(
      base::is.finite(empirical_fte_mean),
      "empirical",
      "legacy_exploratory"
    ),
    dplyr::if_else(
      base::is.finite(empirical_fte_mean),
      "Estimated from observed recent entrants.",
      paste0(
        "HRSA professional hours deliberately not treated ",
        "as clinical FTE."
      )
    ),

    "employment",
    "Provider-level affiliation/practice data",
    "Individual-level",
    dplyr::if_else(
      base::is.finite(empirical_employed),
      "empirical",
      "legacy_exploratory"
    ),
    "Should come from NPPES/PECOS/practice affiliations.",

    "urbanicity",
    "Provider geography + RUCC",
    "Individual-level geography",
    dplyr::if_else(
      base::is.finite(empirical_urban),
      "empirical",
      "legacy_exploratory"
    ),
    "Should be derived from provider location, not assumed."
  )

  entrant_mean <- base::mean(
    pathway_tbl$n_total
  )

  entrant_sd <- stats::sd(
    pathway_tbl$n_total
  )

  entrant_median <- stats::median(
    pathway_tbl$n_total
  )

  entrant_p25 <- base::unname(
    stats::quantile(
      pathway_tbl$n_total,
      0.25
    )
  )

  entrant_p75 <- base::unname(
    stats::quantile(
      pathway_tbl$n_total,
      0.75
    )
  )

  entrant_trend <- stats::lm(
    n_total ~ entry_year,
    data = pathway_tbl
  )

  trend_coef <- summary(
    entrant_trend
  )$coefficients

  trend_per_year <- trend_coef[
    "entry_year",
    "Estimate"
  ]

  trend_p <- trend_coef[
    "entry_year",
    "Pr(>|t|)"
  ]

  trend_direction <- dplyr::if_else(
    trend_per_year >= 0,
    "increased",
    "decreased"
  )

  summary_sentence <- base::sprintf(
    paste0(
      "Across %d-%d, ACGME recorded a mean of %s ",
      "(SD %.1f) first-year URPS fellows per year and a median ",
      "of %.1f (p25 %.1f, p75 %.1f); entrant volume %s by ",
      "%.2f fellows per year (p = %.4g)."
    ),
    base::min(pathway_tbl$entry_year),
    base::max(pathway_tbl$entry_year),
    scales::comma(
      base::round(entrant_mean, 1)
    ),
    entrant_sd,
    entrant_median,
    entrant_p25,
    entrant_p75,
    trend_direction,
    base::abs(trend_per_year),
    trend_p
  )

  base::message(
    "[entrant-evidence] ",
    summary_sentence
  )

  saved_path <- NULL

  if (!base::is.null(save_dir)) {

    if (!base::dir.exists(save_dir)) {
      base::dir.create(
        save_dir,
        recursive = TRUE
      )
    }

    timestamp <- base::format(
      base::Sys.time(),
      "%Y%m%d_%H%M%S"
    )

    saved_path <- base::file.path(
      save_dir,
      base::paste0(
        "entrant_parameters_empirical_",
        timestamp,
        ".csv"
      )
    )

    readr::write_csv(
      parameter_tbl,
      saved_path
    )

    base::message(
      "[entrant-evidence] Saved parameter table: ",
      base::normalizePath(
        saved_path,
        mustWork = FALSE
      )
    )
  }

  base::list(
    parameters = parameter_tbl,
    evidence_registry = evidence_tbl,
    acgme_series = pathway_tbl,
    summary_sentence = summary_sentence,
    saved_path = saved_path
  )
}
