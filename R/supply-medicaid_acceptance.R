# Physician Medicaid acceptance model -----------------------------------
#
# Models whether an individual URPS physician/practice accepts new
# Medicaid patients.
#
# Empirical audit anchors:
#
#   overall Medicaid acceptance          = 42.1%
#   academic medical center OR           = 3.42
#   hospital outpatient department OR    = 2.15
#   fee-ratio OR per +0.20               = 1.85
#   SVI OR per 1.0                       = 1.45
#   years since certification beta       = -0.015 / year
#
# IMPORTANT:
# The 42.1% rate is a POPULATION-AVERAGE acceptance rate. It should not
# automatically be used as the regression intercept.
#
# State Medicaid-to-Medicare fee ratios should be year-specific.
# KFF's 2024 all-services national ratio is 0.75, versus 0.72 in 2019.


# ---- Empirical model specification ------------------------------------

MEDICAID_ACCEPTANCE_OVERALL_TARGET <- 0.421

MEDICAID_ACCEPTANCE_REFERENCE <- base::list(
  private_acceptance = 0.24,
  academic_or = 3.42,
  hospital_outpatient_or = 2.15,
  fee_or_per_0_20 = 1.85,
  svi_or_per_unit = 1.45,
  years_certified_beta = -0.015,
  fee_reference = 0.72,
  svi_reference = 0.50,
  years_reference = 10
)


#' Build Medicaid acceptance logistic coefficients
#'
#' @description
#' Converts interpretable empirical odds ratios into logistic regression
#' coefficients.
#'
#' In particular, an odds ratio of 1.85 for each 0.20 increase in the
#' Medicaid-to-Medicare fee ratio implies:
#'
#' `beta_fee = log(1.85) / 0.20`
#'
#' @param reference Named list of empirical anchors.
#'
#' @return Named list of logistic coefficients and reference values.
#' @family supply
#' @concept access
#' @export
medicaid_acceptance_coefficients <- function(
    reference = MEDICAID_ACCEPTANCE_REFERENCE) {

  private_acceptance <- reference$private_acceptance

  if (!base::is.finite(private_acceptance) ||
      private_acceptance <= 0 ||
      private_acceptance >= 1) {
    base::stop(
      "`private_acceptance` must lie strictly between 0 and 1.",
      call. = FALSE
    )
  }

  coefficient_spec <- base::list(
    intercept = stats::qlogis(
      private_acceptance
    ),

    academic_setting = base::log(
      reference$academic_or
    ),

    hospital_outpatient = base::log(
      reference$hospital_outpatient_or
    ),

    medicaid_fee_ratio =
      base::log(
        reference$fee_or_per_0_20
      ) / 0.20,

    svi = base::log(
      reference$svi_or_per_unit
    ),

    years_certified =
      reference$years_certified_beta,

    fee_reference =
      reference$fee_reference,

    svi_reference =
      reference$svi_reference,

    years_reference =
      reference$years_reference
  )

  base::message(
    "[medicaid-acceptance] Fee coefficient = ",
    base::format(
      coefficient_spec$medicaid_fee_ratio,
      digits = 4
    ),
    " per 1.0 fee-ratio unit."
  )

  base::message(
    "[medicaid-acceptance] Implied OR per +0.20 = ",
    base::format(
      base::exp(
        coefficient_spec$medicaid_fee_ratio *
          0.20
      ),
      digits = 4
    ),
    "."
  )

  coefficient_spec
}


# Backward-compatible package constant.
MEDICAID_ACCEPTANCE_COEF <-
  medicaid_acceptance_coefficients()


#' Medicaid-to-Medicare physician fee index
#'
#' @description
#' 2024 KFF / Urban Institute Medicaid-to-Medicare physician fee
#' index. Ratios compare Medicaid FFS physician fees with Medicare
#' physician fees for comparable services.
#'
#' Tennessee has no Medicaid fee-for-service program and therefore has
#' no directly comparable fee index.
#'
#' @return One row per state plus District of Columbia.
#' @family supply
#' @concept access
#' @export
medicaid_medicare_fee_index_table <- function() {

  state_tbl <- tibble::tribble(
    ~state_abbr, ~all_services, ~obstetric_care, ~other_services,
    "AL", 0.92, 1.21, 0.93,
    "AK", 1.30, 1.29, 1.32,
    "AZ", 0.98, 1.38, 1.07,
    "AR", 0.76, 0.70, 1.32,
    "CA", 0.67, 0.85, 0.81,
    "CO", 0.83, 0.82, 0.89,
    "CT", 0.79, 1.14, 0.68,
    "DE", 0.96, 0.93, 0.99,
    "DC", 0.81, 0.81, 0.82,
    "FL", 0.64, 0.82, 0.73,
    "GA", 0.83, 0.92, 1.02,
    "HI", 0.84, 1.01, 0.70,
    "ID", 0.94, 0.90, 0.90,
    "IL", 0.63, 0.80, 0.85,
    "IN", 0.96, 0.85, 1.01,
    "IA", 0.77, 0.82, 1.12,
    "KS", 0.69, 0.78, 0.99,
    "KY", 0.69, 0.95, 0.91,
    "LA", 0.64, 0.66, 0.72,
    "ME", 0.78, 0.70, 0.71,
    "MD", 0.95, 0.89, 0.81,
    "MA", 0.74, 0.94, 0.74,
    "MI", 0.76, 0.88, 0.66,
    "MN", 0.74, 0.67, 0.76,
    "MS", 0.93, 0.89, 0.91,
    "MO", 0.86, 0.83, 0.92,
    "MT", 1.32, 1.31, 1.33,
    "NE", 1.01, 1.17, 1.68,
    "NV", 0.90, 0.98, 0.97,
    "NH", 0.73, 1.00, 0.71,
    "NJ", 0.61, 0.89, 0.56,
    "NM", 1.21, 1.20, 1.21,
    "NY", 0.76, 0.83, 0.88,
    "NC", 0.82, 0.82, 0.96,
    "ND", 1.06, 1.05, 1.07,
    "OH", 0.63, 0.67, 0.78,
    "OK", 0.94, 0.93, 0.95,
    "OR", 0.88, 1.18, 0.74,
    "PA", 0.68, 1.06, 0.70,
    "RI", 0.52, 0.40, 0.54,
    "SC", 0.89, 1.07, 0.90,
    "SD", 0.91, 1.05, 1.07,
    "TN", NA_real_, NA_real_, NA_real_,
    "TX", 0.63, 0.75, 0.86,
    "UT", 0.80, 0.87, 0.82,
    "VT", 0.87, 0.87, 0.87,
    "VA", 0.83, 0.97, 0.87,
    "WA", 0.64, 0.83, 0.56,
    "WV", 0.82, 0.97, 0.74,
    "WI", 0.66, 0.65, 0.95,
    "WY", 1.00, 0.98, 0.97
  )

  state_tbl |>
    dplyr::mutate(
      year = 2024L,
      source = base::paste(
        "KFF State Health Facts / Urban Institute",
        "2024 Medicaid-to-Medicare Fee Index"
      )
    ) |>
    dplyr::relocate(
      .data$year,
      .data$state_abbr
    )
}


#' Look up a state Medicaid-to-Medicare fee ratio
#'
#' @param state_abbr State postal abbreviation.
#' @param year Policy year.
#' @param fee_component One of `all_services`, `obstetric_care`,
#'   or `other_services`.
#' @param fee_ratio_tbl State-year fee table.
#' @param policy_override_tbl Optional scenario table with
#'   `state_abbr`, `year`, and `medicaid_fee_ratio`.
#' @param missing_action Either `national` or `error`.
#'
#' @return Numeric vector of Medicaid-to-Medicare fee ratios.
#' @family supply
#' @concept access
#' @export
lookup_state_medicaid_fee_ratio <- function(
    state_abbr,
    year = 2024L,
    fee_component = "all_services",
    fee_ratio_tbl = medicaid_medicare_fee_index_table(),
    policy_override_tbl = NULL,
    missing_action = base::c(
      "national",
      "error"
    )) {

  missing_action <- base::match.arg(
    missing_action
  )

  allowed_components <- base::c(
    "all_services",
    "obstetric_care",
    "other_services"
  )

  if (!fee_component %in% allowed_components) {
    base::stop(
      "`fee_component` must be one of: ",
      base::paste(
        allowed_components,
        collapse = ", "
      ),
      ".",
      call. = FALSE
    )
  }

  state_chr <- base::toupper(
    base::trimws(
      base::as.character(state_abbr)
    )
  )

  year_vector <- if (base::length(year) == 1L) {
    base::rep(
      base::as.integer(year),
      base::length(state_chr)
    )
  } else {
    base::as.integer(year)
  }

  if (base::length(year_vector) !=
      base::length(state_chr)) {
    base::stop(
      "`year` must have length 1 or match `state_abbr`.",
      call. = FALSE
    )
  }

  lookup_tbl <- fee_ratio_tbl |>
    dplyr::transmute(
      state_abbr =
        base::toupper(.data$state_abbr),
      year =
        base::as.integer(.data$year),
      medicaid_fee_ratio =
        base::as.numeric(
          .data[[fee_component]]
        )
    )

  if (!base::is.null(policy_override_tbl)) {

    required_override_cols <- base::c(
      "state_abbr",
      "year",
      "medicaid_fee_ratio"
    )

    missing_override_cols <- base::setdiff(
      required_override_cols,
      base::names(policy_override_tbl)
    )

    if (base::length(missing_override_cols) > 0L) {
      base::stop(
        "policy_override_tbl is missing: ",
        base::paste(
          missing_override_cols,
          collapse = ", "
        ),
        call. = FALSE
      )
    }

    override_tbl <- policy_override_tbl |>
      dplyr::transmute(
        state_abbr =
          base::toupper(.data$state_abbr),
        year =
          base::as.integer(.data$year),
        medicaid_fee_ratio =
          base::as.numeric(
            .data$medicaid_fee_ratio
          )
      )

    lookup_tbl <- lookup_tbl |>
      dplyr::rows_upsert(
        override_tbl,
        by = base::c(
          "state_abbr",
          "year"
        )
      )
  }

  request_tbl <- tibble::tibble(
    row_id = base::seq_along(state_chr),
    state_abbr = state_chr,
    year = year_vector
  )

  matched_tbl <- request_tbl |>
    dplyr::left_join(
      lookup_tbl,
      by = base::c(
        "state_abbr",
        "year"
      )
    ) |>
    dplyr::arrange(
      .data$row_id
    )

  national_ratio <- if (
    fee_component == "all_services"
  ) {
    0.75
  } else if (
    fee_component == "obstetric_care"
  ) {
    0.88
  } else {
    0.84
  }

  missing_index <- base::which(
    base::is.na(
      matched_tbl$medicaid_fee_ratio
    )
  )

  if (base::length(missing_index) > 0L) {

    if (missing_action == "error") {
      base::stop(
        "No Medicaid fee ratio for ",
        base::length(missing_index),
        " requested state-year combination(s).",
        call. = FALSE
      )
    }

    matched_tbl$medicaid_fee_ratio[
      missing_index
    ] <- national_ratio
  }

  matched_tbl$medicaid_fee_ratio
}


#' Predict physician Medicaid acceptance
#'
#' @description
#' Predicts the probability that a physician/practice accepts a new
#' Medicaid patient.
#'
#' The model is:
#'
#' logit(P_i) =
#'   beta_0 +
#'   beta_A * academic_i +
#'   beta_H * hospital_outpatient_i +
#'   beta_R * (fee_ratio_i - fee_reference) +
#'   beta_S * (SVI_i - SVI_reference) +
#'   beta_Y * (years_i - years_reference)
#'
#' @param academic_setting Logical or 0/1.
#' @param hospital_outpatient Logical or 0/1.
#' @param medicaid_fee_ratio State Medicaid-to-Medicare fee ratio.
#' @param svi Social Vulnerability Index in [0, 1].
#' @param years_certified Years since board certification.
#' @param coef Logistic coefficient specification.
#' @param probability_bounds Optional lower and upper probability bounds.
#'
#' @return Numeric acceptance probability.
#' @family supply
#' @concept access
#' @export
predict_medicaid_acceptance <- function(
    academic_setting = FALSE,
    hospital_outpatient = FALSE,
    medicaid_fee_ratio = 0.72,
    svi = 0.50,
    years_certified = 10,
    coef = MEDICAID_ACCEPTANCE_COEF,
    probability_bounds = base::c(
      0.05,
      0.95
    )) {

  input_lengths <- base::c(
    base::length(academic_setting),
    base::length(hospital_outpatient),
    base::length(medicaid_fee_ratio),
    base::length(svi),
    base::length(years_certified)
  )

  n_provider <- base::max(
    input_lengths
  )

  recycle_input <- function(value, argument_name) {

    if (base::length(value) == 1L) {
      return(
        base::rep(
          value,
          n_provider
        )
      )
    }

    if (base::length(value) != n_provider) {
      base::stop(
        "`",
        argument_name,
        "` must have length 1 or ",
        n_provider,
        ".",
        call. = FALSE
      )
    }

    value
  }

  academic_value <- recycle_input(
    academic_setting,
    "academic_setting"
  )

  hospital_value <- recycle_input(
    hospital_outpatient,
    "hospital_outpatient"
  )

  fee_value <- base::as.numeric(
    recycle_input(
      medicaid_fee_ratio,
      "medicaid_fee_ratio"
    )
  )

  svi_value <- base::as.numeric(
    recycle_input(
      svi,
      "svi"
    )
  )

  years_value <- base::as.numeric(
    recycle_input(
      years_certified,
      "years_certified"
    )
  )

  if (base::any(
    svi_value < 0 |
      svi_value > 1,
    na.rm = TRUE
  )) {
    base::stop(
      "`svi` must lie between 0 and 1.",
      call. = FALSE
    )
  }

  if (base::any(
    fee_value <= 0,
    na.rm = TRUE
  )) {
    base::stop(
      "Medicaid fee ratios must be positive.",
      call. = FALSE
    )
  }

  academic_indicator <- base::as.numeric(
    base::as.logical(
      academic_value
    )
  )

  hospital_indicator <- base::as.numeric(
    base::as.logical(
      hospital_value
    )
  )

  linear_predictor <-
    coef$intercept +
    coef$academic_setting *
      academic_indicator +
    coef$hospital_outpatient *
      hospital_indicator +
    coef$medicaid_fee_ratio *
      (
        fee_value -
          coef$fee_reference
      ) +
    coef$svi *
      (
        svi_value -
          coef$svi_reference
      ) +
    coef$years_certified *
      (
        years_value -
          coef$years_reference
      )

  acceptance_probability <- stats::plogis(
    linear_predictor
  )

  if (!base::is.null(probability_bounds)) {

    if (base::length(probability_bounds) != 2L ||
        probability_bounds[[1]] < 0 ||
        probability_bounds[[2]] > 1 ||
        probability_bounds[[1]] >=
          probability_bounds[[2]]) {
      base::stop(
        "`probability_bounds` must contain valid lower/upper bounds.",
        call. = FALSE
      )
    }

    acceptance_probability <- base::pmax(
      probability_bounds[[1]],
      base::pmin(
        probability_bounds[[2]],
        acceptance_probability
      )
    )
  }

  acceptance_probability
}


#' Add state Medicaid reimbursement policy to provider supply
#'
#' @param provider_supply Provider table containing `state_abbr`.
#' @param year Simulation year.
#' @param fee_component KFF fee-index component.
#' @param policy_override_tbl Optional future policy scenario.
#'
#' @return Provider table with `medicaid_fee_ratio`.
#' @family supply
#' @concept access
#' @export
attach_state_medicaid_fee_policy <- function(
    provider_supply,
    year,
    fee_component = "all_services",
    policy_override_tbl = NULL) {

  if (!"state_abbr" %in%
      base::names(provider_supply)) {
    base::stop(
      "provider_supply must contain `state_abbr`.",
      call. = FALSE
    )
  }

  base::message(
    "[medicaid-policy] Attaching Medicaid fee ratios for ",
    year,
    "."
  )

  provider_tbl <- provider_supply |>
    dplyr::mutate(
      medicaid_fee_ratio =
        lookup_state_medicaid_fee_ratio(
          state_abbr =
            .data$state_abbr,
          year = year,
          fee_component =
            fee_component,
          policy_override_tbl =
            policy_override_tbl
        ),
      medicaid_fee_policy_year =
        base::as.integer(year),
      medicaid_fee_component =
        fee_component
    )

  base::message(
    "[medicaid-policy] Providers assigned a fee ratio: ",
    base::format(
      base::nrow(provider_tbl),
      big.mark = ","
    ),
    "."
  )

  provider_tbl
}


#' Apply insurance acceptance to provider capacity
#'
#' @description
#' Converts clinical provider FTE into insurance-accessible provider FTE.
#'
#' For Medicaid:
#'
#' expected-capacity mode:
#'
#' `effective_fte_i = clinical_fte_i * P(accept_i)`
#'
#' stochastic mode:
#'
#' `accept_i ~ Bernoulli(P(accept_i))`
#'
#' `effective_fte_i = clinical_fte_i * accept_i`
#'
#' @param provider_supply Provider supply table.
#' @param insurance Insurance category.
#' @param supply_col FTE/capacity column.
#' @param mode Medicaid acceptance implementation.
#' @param seed Optional random seed.
#'
#' @return Provider supply with insurance-accessible FTE.
#' @family spatial access
#' @concept geography
#' @export
filter_supply_by_insurance <- function(
    provider_supply,
    insurance = "Commercial",
    supply_col = "supply",
    mode = base::c(
      "expected_capacity",
      "stochastic_acceptance",
      "threshold"
    ),
    seed = NULL) {

  mode <- base::match.arg(
    mode
  )

  if (!supply_col %in%
      base::names(provider_supply)) {
    base::stop(
      "provider_supply is missing `",
      supply_col,
      "`.",
      call. = FALSE
    )
  }

  insurance_chr <- base::tolower(
    base::as.character(insurance)
  )

  provider_tbl <- provider_supply |>
    dplyr::mutate(
      clinical_fte =
        base::as.numeric(
          .data[[supply_col]]
        )
    )

  if (insurance_chr != "medicaid") {

    return(
      provider_tbl |>
        dplyr::mutate(
          insurance =
            insurance_chr,
          insurance_acceptance_probability =
            1,
          insurance_accessible_fte =
            .data$clinical_fte
        )
    )
  }

  n_provider <- base::nrow(
    provider_tbl
  )

  get_or_default <- function(
      column_name,
      default_value) {

    if (column_name %in%
        base::names(provider_tbl)) {
      return(
        provider_tbl[[column_name]]
      )
    }

    base::rep(
      default_value,
      n_provider
    )
  }

  acceptance_probability <-
    predict_medicaid_acceptance(
      academic_setting =
        get_or_default(
          "academic_setting",
          FALSE
        ),
      hospital_outpatient =
        get_or_default(
          "hospital_outpatient",
          FALSE
        ),
      medicaid_fee_ratio =
        get_or_default(
          "medicaid_fee_ratio",
          0.72
        ),
      svi =
        get_or_default(
          "svi",
          0.50
        ),
      years_certified =
        get_or_default(
          "years_certified",
          10
        )
    )

  if (mode == "expected_capacity") {

    accepts_medicaid <- NA

    accessible_fte <-
      provider_tbl$clinical_fte *
      acceptance_probability

  } else if (
    mode == "stochastic_acceptance"
  ) {

    if (!base::is.null(seed)) {
      base::set.seed(seed)
    }

    accepts_medicaid <- stats::rbinom(
      n = n_provider,
      size = 1L,
      prob = acceptance_probability
    )

    accessible_fte <-
      provider_tbl$clinical_fte *
      accepts_medicaid

  } else {

    accepts_medicaid <-
      base::as.integer(
        acceptance_probability >= 0.50
      )

    accessible_fte <-
      provider_tbl$clinical_fte *
      accepts_medicaid
  }

  medicaid_tbl <- provider_tbl |>
    dplyr::mutate(
      insurance = "medicaid",
      medicaid_acceptance_probability =
        acceptance_probability,
      accepts_medicaid =
        accepts_medicaid,
      insurance_accessible_fte =
        accessible_fte
    )

  base::message(
    "[medicaid-access] Clinical FTE: ",
    base::format(
      base::sum(
        medicaid_tbl$clinical_fte,
        na.rm = TRUE
      ),
      big.mark = ",",
      digits = 5
    )
  )

  base::message(
    "[medicaid-access] Medicaid-accessible FTE: ",
    base::format(
      base::sum(
        medicaid_tbl$insurance_accessible_fte,
        na.rm = TRUE
      ),
      big.mark = ",",
      digits = 5
    )
  )

  medicaid_tbl
}
