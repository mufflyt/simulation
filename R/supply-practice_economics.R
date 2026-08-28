# Practice economics and payer-mix viability ---------------------------

#' Evidence and assumptions used by the practice-economics engine
#'
#' @description
#' Rabice and Lizeth are kept as separate estimands: Rabice measured Medicare
#' appointment obtainment, while Lizeth measured Medicaid refusal among calls
#' with a definite response. Neither directly observes practice profit.
#'
#' Every parameter [simulate_practice_economics()] draws from carries real
#' provenance here -- `lower`/`upper` (the actual distributional bounds the
#' simulator uses, not an invented range), `year`, and `evidence_quality`
#' (`"high"`: official regulatory/administrative rate, a real survey at NCHS
#' reliability, or a large peer-reviewed administrative-data study;
#' `"medium"`: literature-derived, cross-check-only, or a small-but-usable
#' fielded/industry sample; `"low"`: single-site, preliminary, or below a
#' reliability floor; `"uncited"`: no external source at all -- currently
#' just overhead). APP compensation (real BLS OEWS wage data and a real BLS
#' ECEC benefits load factor), malpractice (real AMA/MLM 2024 OB/GYN premium
#' data), and all four payer collection rates (real Dunn et al. 2024 QJE
#' national remittance data for Medicare/Medicaid/commercial, real
#' Superscript 2025 patient-collections data for self-pay) are no longer
#' uncited -- overhead is now the ONLY uncited cost input.
#'
#' Collection-rate note: Dunn et al. report TWO realization measures --
#' cash-flow realization (cash ultimately collected / initial claim value)
#' and an administrative-loss-adjusted variant that additionally nets out
#' the office cost of chasing denied claims (Medicaid 82.4% vs. the 85.2%
#' cash-flow figure used here). This model uses the cash-flow figures
#' because `simulate_practice_economics()` already charges a separate,
#' undecomposed `overhead` cost per FTE (see below) that plausibly already
#' includes routine billing/collections staffing -- using the
#' admin-loss-adjusted collection rate on top of that would risk double-
#' counting the same administrative cost once on the revenue side and again
#' inside `overhead`. If `overhead` is ever decomposed into cost categories
#' that provably EXCLUDE billing/collections staffing, switch to the
#' admin-loss-adjusted rates instead (Medicare 95.3%, Medicaid 82.4%,
#' commercial 97.6%) -- do not apply both adjustments simultaneously.
#'
#' State-level Medicaid note: Dunn et al.'s public replication data
#' (Harvard Dataverse) includes state-specific Medicaid collection-realization
#' estimates -- the 85.2% here is the national mean. Not yet wired into a
#' per-state parameter (unlike the Medicaid FEE ratio, which already varies
#' by state via [medicaid_medicare_fee_index_table()] -- collection realization
#' and fee level are separate mechanisms and this repo only geographically
#' varies the latter so far). Flagged as follow-up work, not implemented here.
#'
#' Also carries three MedPAC physician/APP compensation figures as an
#' external PLAUSIBILITY benchmark for `physician_compensation_capacity`
#' (see [physician_compensation_plausibility()]) -- these are reference
#' points to check whether modeled compensation capacity overlaps a
#' plausible real-world range, never a calibration target this model is
#' tuned to reproduce.
#'
#' @return A tibble with source, estimand, value, lower, upper, unit, year,
#'   evidence_quality, status, and use.
#' @family supply
#' @concept economics
#' @export
practice_economics_evidence <- function() {
  base::message("[practice-economics] Building evidence registry.")
  evidence_tbl <- tibble::tribble(
    ~source, ~estimand, ~value, ~lower, ~upper, ~unit, ~year,
    ~evidence_quality, ~status, ~use,

    "CMS 2026 PFS final rule", "non-QP conversion factor", 33.40, NA_real_,
    NA_real_, "USD per RVU", "2026", "high", "final_rule", "Medicare revenue",

    "CMS 2026 PFS final rule", "QP conversion factor", 33.57, NA_real_,
    NA_real_, "USD per RVU", "2026", "high", "final_rule", "Medicare revenue",

    "CMS QPP", "maximum negative MIPS adjustment", -0.09, NA_real_, NA_real_,
    "proportion", "2026", "high", "statutory_bound", "sensitivity bound",

    "KFF review of physician payments", "commercial payment ratio", 1.43,
    1.18, 1.79, "ratio to Medicare", NA_character_, "medium",
    "literature_mean", "commercial prior",

    "User-specified scenario", "base overhead", 330000, 280000, 380000,
    "2026 USD per clinical FTE", NA_character_, "uncited", "assumption",
    "cost prior -- NO EXTERNAL CITATION, see practice_overhead_by_setting()",

    "AMA Economic and Health Policy Research (Hardiman 2025), citing Medical Liability Monitor Annual Rate Survey",
    "OB/GYN malpractice premium, $1M/$3M policy, 2024, 7 states (CA/CT/FL/IL/NJ/NY/PA), lognormal median/5th/95th",
    154591, 78514, 304385, "2024 USD per clinical FTE", "2024", "medium",
    "administrative_survey",
    "cost prior -- real, but a 7-state data-availability sample (not a national random sample), likely skewed toward higher-litigation states",

    "BLS OEWS May 2025, SOC 29-1171", "nurse practitioner mean base wage",
    137300, NA_real_, NA_real_, "2025 USD per FTE", "2025", "high",
    "administrative_survey", "APP compensation base wage component",

    "BLS OEWS May 2025, SOC 29-1071", "physician assistant mean base wage",
    141280, NA_real_, NA_real_, "2025 USD per FTE", "2025", "high",
    "administrative_survey", "APP compensation base wage component",

    "BLS Employer Costs for Employee Compensation, June 2024, healthcare and social assistance industry",
    "employer benefits load factor (1 / wage share of total comp, 70.4%)",
    1 / 0.704, NA_real_, NA_real_, "ratio, total comp to base wage", "2024",
    "high", "administrative_survey",
    "APP compensation load factor -- kept separate from base wage, not buried",

    "Derived: mean(NP, PA) x load factor -- see practice_economics_defaults()",
    "APP compensation (normal, mean/5th/95th)", 197855, 164189, 231522,
    "2026 USD per APP FTE", "2025", "medium", "derived_from_bls",
    "cost prior -- mean/load factor are real BLS figures, SD is an assumption",

    "Dunn, Gottlieb, Shapiro & Sonnenstuhl, \"A Denial a Day Keeps the Doctor Away\", Quarterly Journal of Economics 2024, Table II (national remittance data, ~90M visits, >100k physicians, 2013-2015)",
    "Medicare collection rate (cash ultimately collected / initial claim value)",
    0.958, NA_real_, NA_real_, "proportion", "2013-2015", "high",
    "peer_reviewed",
    "collection rate -- cash-flow realization, not the admin-loss-adjusted variant; see note below",

    "Dunn, Gottlieb, Shapiro & Sonnenstuhl 2024 QJE, Table II",
    "Medicaid collection rate (cash ultimately collected / initial claim value)",
    0.852, NA_real_, NA_real_, "proportion", "2013-2015", "high",
    "peer_reviewed",
    "collection rate -- national mean; the same paper reports large state variation (implicit incomplete-payment burden >25% in some states, <10% in CO/ID/WA/MN), not yet modeled here -- see practice_economics_defaults() note",

    "Dunn, Gottlieb, Shapiro & Sonnenstuhl 2024 QJE, Table II",
    "commercial collection rate (cash ultimately collected / initial claim value)",
    0.974, NA_real_, NA_real_, "proportion", "2013-2015", "high",
    "peer_reviewed",
    "collection rate -- cash-flow realization, not the admin-loss-adjusted variant; see note below",

    "Superscript, \"The State of Patient Collections\" 2025 (1.9M patient-liability claims, 35 practices, 44 states, 20 specialties; not peer-reviewed)",
    "self-pay collection rate (2025 realized collection of patient-liability balances)",
    0.540, 0.52, 0.63, "proportion", "2025", "medium", "industry_survey",
    "collection rate -- lower evidence tier than the three Dunn-sourced payers (industry report, not peer-reviewed); lower/upper is the reported 2019-2025 range, not a CI",

    "Lizeth/Acosta 2026", "Medicaid acceptance, definite responses", 0.77,
    NA_real_, NA_real_, "proportion", "2026", "low", "preliminary",
    "access validation only, superseded below",

    "Rabice et al. 2021", "Medicare appointment obtained", 226 / 427,
    NA_real_, NA_real_, "proportion", "2021", "medium", "peer_reviewed",
    "access validation only",

    "NAMCS 2015-2019 pooled, URPS-filtered", "Medicare share of URPS visits",
    0.5586, NA_real_, NA_real_, "proportion", "2015-2019", "high",
    "survey_derived", "payer mix default",

    "NAMCS 2015-2019 pooled, URPS-filtered", "Medicaid share of URPS visits",
    0.0468, NA_real_, NA_real_, "proportion", "2015-2019", "high",
    "survey_derived", "payer mix default",

    "NAMCS 2015-2019 pooled, URPS-filtered", "commercial share of URPS visits",
    0.3939, NA_real_, NA_real_, "proportion", "2015-2019", "high",
    "survey_derived", "payer mix default",

    "NAMCS 2015-2019 pooled, URPS-filtered", "self-pay share of URPS visits",
    0.0008, NA_real_, NA_real_, "proportion", "2015-2019", "low",
    "survey_derived_unreliable", "payer mix default",

    "AHRQ 3P-RD Physician Geographic PUF (13 states)",
    "Medicare share of government-payer claims volume, all specialties",
    0.5873, NA_real_, NA_real_, "proportion", "2019-2020", "medium",
    "administrative_crosscheck", "payer mix cross-check only, not blended",

    "CHIA Case Mix, FY2015-2018 pooled (female adults, non-newborn)",
    "Medicare share of government-payer discharges", 0.7315, NA_real_,
    NA_real_, "proportion", "2015-2018", "medium",
    "administrative_crosscheck", "payer mix cross-check only, not blended",

    "Lizeth national URPS mystery-caller study, 2026 (estimate_lizeth_access_anchor(), by_insurance)",
    "Blue Cross Blue Shield appointment obtainment, n=64 calls/47 physicians",
    0.8906, NA_real_, NA_real_, "proportion", "2026", "medium", "fielded",
    "acceptance validation only, not a revenue share",

    "Lizeth national URPS mystery-caller study, 2026 (estimate_lizeth_access_anchor(), by_insurance)",
    "Medicaid appointment obtainment, n=74 calls/58 physicians, supersedes preliminary 0.77 above (p=0.020 vs BCBS)",
    0.7162, NA_real_, NA_real_, "proportion", "2026", "medium", "fielded",
    "acceptance validation only, not a revenue share",

    "MedPAC March 2025 Report to Congress, Ch. 4 (SullivanCotter Physician Compensation and Productivity Survey, 2024)",
    "median physician compensation, all specialties", 352000, NA_real_,
    NA_real_, "2023 USD", "2023", "high", "external_benchmark",
    "physician-compensation plausibility benchmark, NOT a calibration target",

    "MedPAC March 2025 Report to Congress, Ch. 4 (SullivanCotter Physician Compensation and Productivity Survey, 2024)",
    "median physician compensation, surgical specialties (incl. OB/GYN, urology)",
    496000, NA_real_, NA_real_, "2023 USD", "2023", "high",
    "external_benchmark",
    "physician-compensation plausibility benchmark, NOT a calibration target",

    "MedPAC March 2025 Report to Congress, Ch. 4 (SullivanCotter Physician Compensation and Productivity Survey, 2024)",
    "median advanced practice provider (NP/PA) compensation", 138000,
    NA_real_, NA_real_, "2023 USD", "2023", "high", "external_benchmark",
    "physician-compensation plausibility benchmark, NOT a calibration target"
  )
  base::message(
    "[practice-economics] Registered ", base::nrow(evidence_tbl),
    " evidence and assumption rows (",
    base::sum(evidence_tbl$evidence_quality == "uncited"),
    " with no external citation)."
  )
  evidence_tbl
}

#' Default uncertain inputs for practice economics
#'
#' @description
#' `app_compensation_mean` is now built from two real, separately-declared
#' components rather than one uncited number: BLS OEWS May 2025 mean base
#' wages for nurse practitioners ($137,300, SOC 29-1171) and physician
#' assistants ($141,280, SOC 29-1071), averaged, then grossed up by a real
#' BLS employer-benefits load factor (`app_benefits_load_factor`, from BLS
#' Employer Costs for Employee Compensation, June 2024, healthcare and
#' social assistance industry: wages/salaries are 70.4% of total employer
#' compensation cost, i.e. total cost = wages / 0.704). Both the base wage
#' and the load factor are exposed in the returned list, not folded silently
#' into one number -- see [practice_economics_evidence()] for the citations.
#' `app_compensation_sd`'s RELATIVE uncertainty (previously 15000/145000,
#' about 10.3%) is carried over onto the new mean; BLS OEWS reports a
#' national mean and percentile bands, not a usable individual-level SD, so
#' this specific figure remains an assumption, not a BLS number.
#'
#' `malpractice_median`/`malpractice_cv` are now the real median and
#' coefficient of variation of seven real 2024 OB/GYN $1M/$3M-policy
#' premiums (CA/CT/FL/IL/NJ/NY/PA) published in AMA Economic and Health
#' Policy Research (Hardiman 2025), itself citing the Medical Liability
#' Monitor Annual Rate Survey -- see `malpractice_state_premiums_2024` for
#' the seven underlying values and [practice_economics_evidence()] for the
#' full citation and its real limitation (a 10-year data-availability
#' sample, not a national random sample; likely skewed toward
#' higher-litigation states).
#'
#' @return A named list of simulation inputs.
#' @family supply
#' @concept economics
#' @export
practice_economics_defaults <- function() {
  base::message("[practice-economics] Loading default uncertain inputs.")
  app_base_wage_np <- 137300
  app_base_wage_pa <- 141280
  app_base_wage_mean <- base::mean(base::c(app_base_wage_np, app_base_wage_pa))
  app_benefits_load_factor <- 1 / 0.704
  app_compensation_mean <- app_base_wage_mean * app_benefits_load_factor
  # 2024 OB/GYN $1M/$3M-policy manual premiums, AMA Economic and Health
  # Policy Research (Hardiman 2025) Exhibit 3, citing the Medical Liability
  # Monitor Annual Rate Survey. Seven states chosen by the source for data
  # availability across all ten years (2015-2024), not representativeness.
  malpractice_state_premiums_2024 <- base::c(
    California = 49804, Connecticut = 154591, Florida = 243988,
    Illinois = 207907, `New Jersey` = 94640, `New York` = 171672,
    Pennsylvania = 122906
  )
  malpractice_median <- stats::median(malpractice_state_premiums_2024)
  malpractice_cv <- stats::sd(malpractice_state_premiums_2024) / malpractice_median
  base::list(
    medicare_conversion_factor = 33.40,
    qp_conversion_factor = 33.57,
    commercial_ratio_median = 1.43,
    commercial_ratio_lower = 1.18,
    commercial_ratio_upper = 1.79,
    overhead_lower = 280000,
    overhead_mode = 330000,
    overhead_upper = 380000,
    malpractice_state_premiums_2024 = malpractice_state_premiums_2024,
    malpractice_median = malpractice_median,
    malpractice_cv = malpractice_cv,
    app_base_wage_np = app_base_wage_np,
    app_base_wage_pa = app_base_wage_pa,
    app_benefits_load_factor = app_benefits_load_factor,
    app_compensation_mean = app_compensation_mean,
    app_compensation_sd = app_compensation_mean *
      (15000 / 145000),
    medicare_collection = 0.958,
    medicaid_collection = 0.852,
    commercial_collection = 0.974,
    self_pay_collection = 0.540,
    acquisition_margin_threshold = 0,
    cash_pay_margin_threshold = -0.05,
    transition_slope = 12
  )
}

#' Setting-specific practice overhead structure (legacy scenario)
#'
#' @description
#' The single overhead range in [practice_economics_defaults()]
#' ($280k-380k/FTE, `status = "assumption"`, no external citation) is applied
#' identically to every `practice_setting`. Real independent, hospital-
#' employed, academic, and safety-net practices have materially different
#' nonphysician cost structures, but this repository has no sourced,
#' setting-specific overhead data yet -- building setting-specific
#' DISTRIBUTIONS from nothing would just be a more elaborate guess.
#'
#' This function exists so [simulate_practice_economics()] has a real
#' plumbing point for that data once it is sourced (real
#' MGMA/AMGA-style practice-cost benchmarks by setting), without silently
#' recalibrating anything today: every setting currently maps to the SAME
#' `$280k/$330k/$380k` triple, `status = "legacy_scenario"`, so passing this
#' table's output as `overhead_by_setting` to [simulate_practice_economics()]
#' changes nothing about today's numbers -- only the setting-specific
#' plumbing is new.
#'
#' @return Tibble: `practice_setting`, `overhead_lower`, `overhead_mode`,
#'   `overhead_upper`, `source`, `status`.
#' @concept economics
#' @export
practice_overhead_by_setting <- function() {
  tibble::tribble(
    ~practice_setting, ~overhead_lower, ~overhead_mode, ~overhead_upper,
    ~source, ~status,
    "independent", 280000, 330000, 380000,
    "User-specified scenario", "legacy_scenario",
    "hospital_employed", 280000, 330000, 380000,
    "User-specified scenario", "legacy_scenario",
    "academic", 280000, 330000, 380000,
    "User-specified scenario", "legacy_scenario",
    "safety_net", 280000, 330000, 380000,
    "User-specified scenario", "legacy_scenario"
  )
}

.practice_triangular <- function(size, lower, mode, upper) {
  random_u <- stats::runif(size)
  # degenerate = lower == upper (a fixed value, e.g. a sensitivity-decomposition
  # scenario pinning overhead to a single number): (mode-lower)/(upper-lower)
  # is 0/0 without this guard, propagating NaN into every downstream dollar.
  degenerate <- (upper - lower) == 0
  split_point <- base::ifelse(degenerate, 0, (mode - lower) / (upper - lower))
  base::ifelse(
    degenerate,
    lower,
    base::ifelse(
      random_u < split_point,
      lower + base::sqrt(
        random_u * (upper - lower) * (mode - lower)
      ),
      upper - base::sqrt(
        (1 - random_u) * (upper - lower) * (upper - mode)
      )
    )
  )
}

# Exact inverse-CDF (quantile) of the same triangular distribution
# .practice_triangular() draws from -- the p-th quantile at the SAME split
# point used there, not a re-derivation. Needed to report overhead's real
# p25/p75 without inventing a second, inconsistent characterization of the
# same declared distribution.
.practice_triangular_quantile <- function(p, lower, mode, upper) {
  split_point <- (mode - lower) / (upper - lower)
  base::ifelse(
    p < split_point,
    lower + base::sqrt(p * (upper - lower) * (mode - lower)),
    upper - base::sqrt((1 - p) * (upper - lower) * (upper - mode))
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
#' @param overhead_by_setting Optional tibble from
#'   [practice_overhead_by_setting()] (or the same shape). `NULL` (default)
#'   draws overhead from the flat `inputs$overhead_*` range for every
#'   practice, exactly as before this parameter existed -- zero behavior
#'   change unless a caller opts in with real setting-specific bounds.
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
    rvu_basis = base::c("work_rvu_proxy", "payment_rvu"),
    overhead_by_setting = NULL) {
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

  # Overhead bounds default to the flat inputs$overhead_* range for every
  # row (today's exact behavior). Supplying overhead_by_setting overrides
  # those bounds per practice_setting -- see practice_overhead_by_setting().
  expanded_tbl$overhead_lower <- inputs$overhead_lower
  expanded_tbl$overhead_mode <- inputs$overhead_mode
  expanded_tbl$overhead_upper <- inputs$overhead_upper
  if (!base::is.null(overhead_by_setting)) {
    setting_bounds <- overhead_by_setting |>
      dplyr::select(
        "practice_setting", "overhead_lower", "overhead_mode", "overhead_upper"
      )
    expanded_tbl <- expanded_tbl |>
      dplyr::select(
        -"overhead_lower", -"overhead_mode", -"overhead_upper"
      ) |>
      dplyr::left_join(setting_bounds, by = "practice_setting")
    if (base::anyNA(expanded_tbl$overhead_mode)) {
      base::stop(
        "overhead_by_setting is missing bounds for practice_setting ",
        "value(s): ", base::paste(
          base::unique(
            expanded_tbl$practice_setting[base::is.na(expanded_tbl$overhead_mode)]
          ),
          collapse = ", "
        ),
        call. = FALSE
      )
    }
    base::message(
      "[practice-economics] Applied setting-specific overhead bounds."
    )
  }

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
        draw_count, .data$overhead_lower, .data$overhead_mode,
        .data$overhead_upper
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
      # PRIMARY ESTIMAND. There is no physician-compensation line item
      # anywhere in this cost model -- operating_cost is entirely overhead +
      # malpractice + APP labor. `operating_income`/`operating_margin`
      # (below) named that quantity in a way readers could mistake for
      # ordinary bottom-line practice profit. nonphysician_operating_cost /
      # net_revenue_before_physician_compensation / physician_compensation_
      # capacity are the SAME arithmetic, renamed so a negative value has an
      # unambiguous reading: modeled professional revenue does not cover
      # nonphysician practice costs, before the physician is paid at all.
      # operating_income/operating_margin are kept as deprecated aliases.
      nonphysician_operating_cost = .data$operating_cost,
      net_revenue_before_physician_compensation =
        .data$gross_revenue - .data$operating_cost,
      physician_compensation_capacity =
        .data$gross_revenue - .data$operating_cost,
      operating_income = .data$gross_revenue - .data$operating_cost,
      operating_margin = dplyr::if_else(
        .data$gross_revenue > 0,
        .data$operating_income / .data$gross_revenue, -Inf
      ),
      # BREAK-EVEN DIAGNOSTICS. More interpretable than a margin percentage:
      # how many wRVU/FTE the practice would need to realize (at its ACTUAL
      # realized $/wRVU rate) to cover nonphysician cost alone, and what
      # $/wRVU rate it would need (at its ACTUAL wRVU/FTE) to do the same.
      nonphysician_cost_per_fte = .data$operating_cost / .data$clinical_fte,
      realized_revenue_per_wrvu = dplyr::if_else(
        .data$annual_wrvu > 0, .data$gross_revenue / .data$annual_wrvu, NA_real_
      ),
      annual_wrvu_per_fte = .data$annual_wrvu / .data$clinical_fte,
      break_even_wrvu_per_fte = dplyr::if_else(
        .data$realized_revenue_per_wrvu > 0,
        .data$nonphysician_cost_per_fte / .data$realized_revenue_per_wrvu,
        NA_real_
      ),
      required_revenue_per_wrvu = dplyr::if_else(
        .data$annual_wrvu_per_fte > 0,
        .data$nonphysician_cost_per_fte / .data$annual_wrvu_per_fte,
        NA_real_
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
      mean_nonphysician_operating_cost = base::mean(
        .data$nonphysician_operating_cost
      ),
      mean_net_revenue_before_physician_compensation = base::mean(
        .data$net_revenue_before_physician_compensation
      ),
      mean_physician_compensation_capacity = base::mean(
        .data$physician_compensation_capacity
      ),
      mean_break_even_wrvu_per_fte = base::mean(
        .data$break_even_wrvu_per_fte, na.rm = TRUE
      ),
      mean_required_revenue_per_wrvu = base::mean(
        .data$required_revenue_per_wrvu, na.rm = TRUE
      ),
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
      "From %s to %s, mean median operating margin %s from %.1f%% to %.1f%%",
      first_year, last_year, direction, 100 * first_margin, 100 * last_margin
    ),
    " (this margin is BEFORE physician compensation -- see",
    " physician_compensation_capacity, not a bottom-line practice profit); ",
    "the ending-year mean gross revenue was $",
    scales::comma(base::round(base::mean(
      summary_tbl$mean_gross_revenue[summary_tbl$year == last_year]
    ))),
    " per practice-year, mean physician compensation capacity $",
    scales::comma(base::round(base::mean(
      summary_tbl$mean_physician_compensation_capacity[
        summary_tbl$year == last_year
      ]
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
