# Practice economics and payer-mix viability ---------------------------

#' Evidence and assumptions used by the practice-economics engine
#'
#' @description
#' Rabice and Lizeth are kept as separate estimands: Rabice measured Medicare
#' appointment obtainment, while Lizeth measured Medicaid refusal among calls
#' with a definite response. Neither directly observes practice profit.
#'
#' @return A tibble with source, estimand, value, unit, status, and use.
#' @family supply
#' @concept economics
#' @export
practice_economics_evidence <- function() {
  base::message("[practice-economics] Building evidence registry.")
  evidence_tbl <- tibble::tribble(
    ~source, ~estimand, ~value, ~unit, ~status, ~use,
    "CMS 2026 PFS final rule", "non-QP conversion factor", 33.40,
    "USD per RVU", "final_rule", "Medicare revenue",
    "CMS 2026 PFS final rule", "QP conversion factor", 33.57,
    "USD per RVU", "final_rule", "Medicare revenue",
    "CMS QPP", "maximum negative MIPS adjustment", -0.09,
    "proportion", "statutory_bound", "sensitivity bound",
    "KFF review of physician payments", "commercial payment ratio", 1.43,
    "ratio to Medicare", "literature_mean", "commercial prior median",
    "KFF review of physician payments", "commercial ratio lower", 1.18,
    "ratio to Medicare", "literature_range", "commercial prior bound",
    "KFF review of physician payments", "commercial ratio upper", 1.79,
    "ratio to Medicare", "literature_range", "commercial prior bound",
    "Lizeth/Acosta 2026", "Medicaid acceptance, definite responses", 0.77,
    "proportion", "preliminary", "access validation only",
    "Rabice et al. 2021", "Medicare appointment obtained", 226 / 427,
    "proportion", "peer_reviewed", "access validation only",
    "User-specified scenario", "base overhead lower", 280000,
    "2026 USD per clinical FTE", "assumption", "cost prior bound",
    "User-specified scenario", "base overhead upper", 380000,
    "2026 USD per clinical FTE", "assumption", "cost prior bound"
  )
  base::message(
    "[practice-economics] Registered ", base::nrow(evidence_tbl),
    " evidence and assumption rows."
  )
  evidence_tbl
}

#' Default uncertain inputs for practice economics
#'
#' @return A named list of simulation inputs.
#' @family supply
#' @concept economics
#' @export
practice_economics_defaults <- function() {
  base::message("[practice-economics] Loading default uncertain inputs.")
  base::list(
    medicare_conversion_factor = 33.40,
    qp_conversion_factor = 33.57,
    commercial_ratio_median = 1.43,
    commercial_ratio_lower = 1.18,
    commercial_ratio_upper = 1.79,
    overhead_lower = 280000,
    overhead_mode = 330000,
    overhead_upper = 380000,
    malpractice_median = 45000,
    malpractice_cv = 0.35,
    app_compensation_mean = 145000,
    app_compensation_sd = 15000,
    medicare_collection = 0.98,
    medicaid_collection = 0.94,
    commercial_collection = 0.96,
    self_pay_collection = 0.72,
    acquisition_margin_threshold = 0,
    cash_pay_margin_threshold = -0.05,
    transition_slope = 12
  )
}

.practice_triangular <- function(size, lower, mode, upper) {
  random_u <- stats::runif(size)
  split_point <- (mode - lower) / (upper - lower)
  base::ifelse(
    random_u < split_point,
    lower + base::sqrt(
      random_u * (upper - lower) * (mode - lower)
    ),
    upper - base::sqrt(
      (1 - random_u) * (upper - lower) * (upper - mode)
    )
  )
}

.practice_check_inputs <- function(practice_tbl) {
  required_names <- base::c(
    "practice_id", "year", "clinical_fte", "annual_wrvu",
    "medicare_share", "medicaid_share", "commercial_share",
    "self_pay_share", "practice_setting", "app_fte"
  )
  missing_names <- base::setdiff(required_names, base::names(practice_tbl))
  if (base::length(missing_names) > 0L) {
    base::stop(
      "Missing practice input(s): ",
      base::paste(missing_names, collapse = ", "), call. = FALSE
    )
  }
  payer_sum <- practice_tbl$medicare_share +
    practice_tbl$medicaid_share + practice_tbl$commercial_share +
    practice_tbl$self_pay_share
  if (base::any(!base::is.finite(payer_sum)) ||
      base::any(base::abs(payer_sum - 1) > 1e-8)) {
    base::stop("Payer shares must be finite and sum to 1.", call. = FALSE)
  }
  if (base::any(practice_tbl$clinical_fte <= 0) ||
      base::any(practice_tbl$annual_wrvu < 0) ||
      base::any(practice_tbl$app_fte < 0)) {
    base::stop(
      "Clinical FTE must be positive; wRVU and APP FTE cannot be negative.",
      call. = FALSE
    )
  }
  base::invisible(TRUE)
}

#' Simulate URPS practice economics and structural transitions
#'
#' @description
#' Produces payer-specific collected professional revenue, operating cost,
#' margin, and exploratory practice transitions. Work RVUs are a transparent
#' proxy, not an exact PFS payment quantity. Payment-accurate analyses should
#' supply geographically adjusted total RVUs in `payment_rvu`.
#'
#' @param practice_tbl One row per practice-year. Required columns are
#'   `practice_id`, `year`, `clinical_fte`, `annual_wrvu`, four payer-share
#'   columns, `practice_setting`, and `app_fte`. Optional columns are
#'   `payment_rvu`, `medicaid_fee_ratio`, `mips_factor`, and `qp_status`.
#' @param draws Number of Monte Carlo draws.
#' @param seed Reproducible random seed.
#' @param inputs Named list from [practice_economics_defaults()].
#' @param rvu_basis Either `work_rvu_proxy` or `payment_rvu`.
#'
#' @return A list with draws, summaries, evidence, and a summary sentence.
#' @family supply
#' @concept economics
#' @export
simulate_practice_economics <- function(
    practice_tbl,
    draws = 1000L,
    seed = 20260821L,
    inputs = practice_economics_defaults(),
    rvu_basis = base::c("work_rvu_proxy", "payment_rvu")) {
  rvu_basis <- base::match.arg(rvu_basis)
  .practice_check_inputs(practice_tbl)
  if (!base::is.numeric(draws) || base::length(draws) != 1L ||
      !base::is.finite(draws) || draws < 100L) {
    base::stop("`draws` must be one finite number of at least 100.",
               call. = FALSE)
  }
  draws <- base::as.integer(draws)
  if (rvu_basis == "payment_rvu" &&
      !"payment_rvu" %in% base::names(practice_tbl)) {
    base::stop("`payment_rvu` is required for payment-RVU mode.",
               call. = FALSE)
  }
  base::message(
    "[practice-economics] Inputs: ", base::nrow(practice_tbl),
    " practice-years; ", draws, " draws; RVU basis: ", rvu_basis, "."
  )
  base::set.seed(seed)

  prepared_tbl <- practice_tbl
  if (!"payment_rvu" %in% base::names(prepared_tbl)) {
    prepared_tbl$payment_rvu <- prepared_tbl$annual_wrvu
  }
  if (!"medicaid_fee_ratio" %in% base::names(prepared_tbl)) {
    prepared_tbl$medicaid_fee_ratio <- 0.75
  }
  if (!"mips_factor" %in% base::names(prepared_tbl)) {
    prepared_tbl$mips_factor <- 1
  }
  if (!"qp_status" %in% base::names(prepared_tbl)) {
    prepared_tbl$qp_status <- FALSE
  }
  prepared_tbl <- prepared_tbl |>
    dplyr::mutate(practice_row = dplyr::row_number())
  expanded_tbl <- tidyr::crossing(
    practice_row = base::seq_len(base::nrow(prepared_tbl)),
    draw = base::seq_len(draws)
  ) |>
    dplyr::left_join(prepared_tbl, by = "practice_row")
  base::message("[practice-economics] Expanded Monte Carlo practice panel.")

  draw_count <- base::nrow(expanded_tbl)
  commercial_sd <- (base::log(inputs$commercial_ratio_upper) -
    base::log(inputs$commercial_ratio_lower)) / (2 * 1.96)
  payer_draw_tbl <- expanded_tbl |>
    dplyr::mutate(
      payment_units = base::ifelse(
        rvu_basis == "payment_rvu", .data$payment_rvu,
        .data$annual_wrvu
      ),
      conversion_factor = base::ifelse(
        .data$qp_status, inputs$qp_conversion_factor,
        inputs$medicare_conversion_factor
      ),
      commercial_ratio = stats::rlnorm(
        draw_count, base::log(inputs$commercial_ratio_median), commercial_sd
      ),
      overhead_per_fte = .practice_triangular(
        draw_count, inputs$overhead_lower, inputs$overhead_mode,
        inputs$overhead_upper
      ),
      malpractice_per_fte = stats::rlnorm(
        draw_count, base::log(inputs$malpractice_median),
        base::sqrt(base::log1p(inputs$malpractice_cv^2))
      ),
      app_compensation = base::pmax(
        0, stats::rnorm(
          draw_count, inputs$app_compensation_mean,
          inputs$app_compensation_sd
        )
      )
    )
  base::message("[practice-economics] Drew payment and cost uncertainty.")

  economics_draw_tbl <- payer_draw_tbl |>
    dplyr::mutate(
      medicare_revenue = .data$payment_units * .data$conversion_factor *
        .data$mips_factor * .data$medicare_share *
        inputs$medicare_collection,
      medicaid_revenue = .data$payment_units * .data$conversion_factor *
        .data$medicaid_fee_ratio * .data$medicaid_share *
        inputs$medicaid_collection,
      commercial_revenue = .data$payment_units *
        .data$conversion_factor * .data$commercial_ratio *
        .data$commercial_share * inputs$commercial_collection,
      self_pay_revenue = .data$payment_units * .data$conversion_factor *
        .data$commercial_ratio * .data$self_pay_share *
        inputs$self_pay_collection,
      gross_revenue = .data$medicare_revenue + .data$medicaid_revenue +
        .data$commercial_revenue + .data$self_pay_revenue,
      operating_cost = .data$clinical_fte *
        (.data$overhead_per_fte + .data$malpractice_per_fte) +
        .data$app_fte * .data$app_compensation,
      operating_income = .data$gross_revenue - .data$operating_cost,
      operating_margin = dplyr::if_else(
        .data$gross_revenue > 0,
        .data$operating_income / .data$gross_revenue, -Inf
      ),
      acquisition_probability = dplyr::if_else(
        .data$practice_setting == "independent",
        stats::plogis(
          -inputs$transition_slope *
            (.data$operating_margin - inputs$acquisition_margin_threshold)
        ), 0
      ),
      cash_pay_probability = dplyr::if_else(
        .data$practice_setting == "independent",
        stats::plogis(
          -inputs$transition_slope *
            (.data$operating_margin - inputs$cash_pay_margin_threshold)
        ) * .data$commercial_share, 0
      ),
      random_transition = stats::runif(draw_count),
      simulated_transition = dplyr::case_when(
        .data$random_transition < .data$cash_pay_probability ~
          "cash_pay_or_concierge",
        .data$random_transition < .data$cash_pay_probability +
          (1 - .data$cash_pay_probability) *
            .data$acquisition_probability ~ "hospital_acquisition",
        TRUE ~ "remain_current"
      )
    )
  base::message("[practice-economics] Calculated revenue, cost, and margins.")

  summary_tbl <- economics_draw_tbl |>
    dplyr::group_by(.data$practice_id, .data$year) |>
    dplyr::summarise(
      mean_gross_revenue = base::mean(.data$gross_revenue),
      sd_gross_revenue = stats::sd(.data$gross_revenue),
      median_operating_margin = stats::median(.data$operating_margin),
      p25_operating_margin = stats::quantile(
        .data$operating_margin, 0.25, names = FALSE
      ),
      p75_operating_margin = stats::quantile(
        .data$operating_margin, 0.75, names = FALSE
      ),
      loss_probability = base::mean(.data$operating_income < 0),
      acquisition_probability = base::mean(
        .data$simulated_transition == "hospital_acquisition"
      ),
      cash_pay_probability = base::mean(
        .data$simulated_transition == "cash_pay_or_concierge"
      ),
      .groups = "drop"
    )
  base::message("[practice-economics] Summarised practice-year uncertainty.")

  first_year <- base::min(summary_tbl$year)
  last_year <- base::max(summary_tbl$year)
  first_margin <- base::mean(
    summary_tbl$median_operating_margin[summary_tbl$year == first_year]
  )
  last_margin <- base::mean(
    summary_tbl$median_operating_margin[summary_tbl$year == last_year]
  )
  direction <- base::ifelse(
    last_margin >= first_margin, "increased", "decreased"
  )
  summary_sentence <- base::paste0(
    base::sprintf(
      "From %s to %s, mean median operating margin %s from %.1f%% to %.1f%%; ",
      first_year, last_year, direction, 100 * first_margin, 100 * last_margin
    ),
    "the ending-year mean gross revenue was $",
    scales::comma(base::round(base::mean(
      summary_tbl$mean_gross_revenue[summary_tbl$year == last_year]
    ))),
    " per practice-year."
  )
  base::message("[practice-economics] Output: ", summary_sentence)

  base::list(
    draws = economics_draw_tbl,
    summary = summary_tbl,
    evidence = practice_economics_evidence(),
    summary_sentence = summary_sentence,
    revenue_basis_warning = base::ifelse(
      rvu_basis == "work_rvu_proxy",
      base::paste(
        "wRVU times conversion factor is a workload proxy,",
        "not exact PFS payment."
      ),
      NA_character_
    )
  )
}
