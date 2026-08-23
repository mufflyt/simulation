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
#' (`"high"`: official regulatory/administrative rate or a real survey at
#' NCHS reliability; `"medium"`: literature-derived, cross-check-only, or a
#' small-but-usable fielded sample; `"low"`: single-site, preliminary, or
#' below a reliability floor; `"uncited"`: no external source at all --
#' currently overhead and all four payer collection rates). The revenue
#' side (conversion factors, payer mix) is materially better sourced than
#' the cost side; this table makes that asymmetry visible rather than
#' treating every input as equally solid. APP compensation (real BLS OEWS
#' wage data and a real BLS ECEC benefits load factor) and malpractice
#' (real AMA/MLM 2024 OB/GYN premium data) are no longer uncited --
#' overhead is now the single largest uncited cost input.
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

    "User-specified scenario", "Medicare collection rate", 0.98, NA_real_,
    NA_real_, "proportion", NA_character_, "uncited", "assumption",
    "collection rate -- NO EXTERNAL CITATION",

    "User-specified scenario", "Medicaid collection rate", 0.94, NA_real_,
    NA_real_, "proportion", NA_character_, "uncited", "assumption",
    "collection rate -- NO EXTERNAL CITATION",

    "User-specified scenario", "commercial collection rate", 0.96, NA_real_,
    NA_real_, "proportion", NA_character_, "uncited", "assumption",
    "collection rate -- NO EXTERNAL CITATION",

    "User-specified scenario", "self-pay collection rate", 0.72, NA_real_,
    NA_real_, "proportion", NA_character_, "uncited", "assumption",
    "collection rate -- NO EXTERNAL CITATION",

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
    medicare_collection = 0.98,
    medicaid_collection = 0.94,
    commercial_collection = 0.96,
    self_pay_collection = 0.72,
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

#' One-at-a-time sensitivity decomposition for practice economics
#'
#' @description
#' Holds every assumption at baseline except one, sets that one family to a
#' single favorable alternative, and reports how much of the baseline
#' shortfall to break-even (`0 - physician_compensation_capacity`) closing
#' just that one lever would explain. Answers "is this a productivity
#' problem, a reimbursement problem, or a cost-model problem?" rather than
#' leaving six confounded assumptions inside one margin number.
#'
#' Each lever's alternative is a single, named, defensible value -- not a
#' search for whatever number makes the practice profitable:
#' \describe{
#'   \item{revenue_realization}{All four payer collection rates set to 1.0
#'     (perfect billing/collections) instead of `inputs$*_collection`.}
#'   \item{wrvu_productivity}{`annual_wrvu` per FTE raised to
#'     `WRVU_PER_FTE_BENCHMARK[["high"]]` instead of the supplied value.}
#'   \item{overhead}{Overhead fixed at `inputs$overhead_lower` instead of
#'     drawn from the triangular distribution.}
#'   \item{malpractice}{Malpractice fixed at the 10th percentile of its
#'     lognormal distribution instead of drawn.}
#'   \item{app_intensity}{`app_fte` set to 0 (no APP cost) instead of the
#'     supplied value.}
#'   \item{payer_mix}{Payer mix set to 100% commercial (this model's
#'     highest-paying payer) instead of the supplied mix.}
#' }
#'
#' @param practice_tbl Baseline practice-tbl, as for
#'   [simulate_practice_economics()].
#' @param inputs Named list from [practice_economics_defaults()].
#' @param draws Monte Carlo draws per scenario.
#' @param seed Reproducible random seed (same seed reused per scenario so
#'   differences reflect the perturbation, not draw noise).
#'
#' @return Tibble: `assumption_family`, `baseline_physician_compensation_capacity`,
#'   `perturbed_physician_compensation_capacity`, `delta`,
#'   `counterfactual_shortfall_closed_pct` (`NA` when the baseline is
#'   already break-even or better). Named deliberately as a COUNTERFACTUAL
#'   percentage -- how much of the shortfall THIS ONE arbitrary favorable
#'   value would close, holding everything else fixed -- not a variance
#'   attribution or a causal decomposition. The six levers' alternatives
#'   differ in magnitude and their effects can overlap, so the values do not
#'   partition 100% of anything; a lever showing 137.8% does not mean it
#'   "explains" 137.8% of the problem, only that its one chosen alternative
#'   value, alone, would over-close the shortfall.
#' @concept economics
#' @export
practice_economics_sensitivity_decomposition <- function(
    practice_tbl,
    inputs = practice_economics_defaults(),
    draws = 1000L,
    seed = 20260821L) {
  capacity_of <- function(tbl, inputs_arg) {
    base::mean(
      simulate_practice_economics(
        tbl, draws = draws, seed = seed, inputs = inputs_arg
      )$draws$physician_compensation_capacity
    )
  }
  baseline_capacity <- capacity_of(practice_tbl, inputs)
  shortfall <- -baseline_capacity

  perfect_collection_inputs <- inputs
  perfect_collection_inputs$medicare_collection <- 1
  perfect_collection_inputs$medicaid_collection <- 1
  perfect_collection_inputs$commercial_collection <- 1
  perfect_collection_inputs$self_pay_collection <- 1

  high_wrvu_tbl <- practice_tbl |>
    dplyr::mutate(
      annual_wrvu = WRVU_PER_FTE_BENCHMARK[["high"]] * .data$clinical_fte
    )

  low_overhead_inputs <- inputs
  low_overhead_inputs$overhead_mode <- inputs$overhead_lower
  low_overhead_inputs$overhead_upper <- inputs$overhead_lower

  malpractice_p10 <- stats::qlnorm(
    0.10, base::log(inputs$malpractice_median),
    base::sqrt(base::log1p(inputs$malpractice_cv^2))
  )
  low_malpractice_inputs <- inputs
  low_malpractice_inputs$malpractice_median <- malpractice_p10
  low_malpractice_inputs$malpractice_cv <- 1e-6

  no_app_tbl <- practice_tbl |> dplyr::mutate(app_fte = 0)

  commercial_only_tbl <- practice_tbl |>
    dplyr::mutate(
      medicare_share = 0, medicaid_share = 0,
      commercial_share = 1, self_pay_share = 0
    )

  scenarios <- base::list(
    revenue_realization = base::list(
      tbl = practice_tbl, inputs = perfect_collection_inputs
    ),
    wrvu_productivity = base::list(tbl = high_wrvu_tbl, inputs = inputs),
    overhead = base::list(tbl = practice_tbl, inputs = low_overhead_inputs),
    malpractice = base::list(
      tbl = practice_tbl, inputs = low_malpractice_inputs
    ),
    app_intensity = base::list(tbl = no_app_tbl, inputs = inputs),
    payer_mix = base::list(tbl = commercial_only_tbl, inputs = inputs)
  )

  results <- purrr::imap_dfr(scenarios, function(scenario, family_name) {
    perturbed_capacity <- capacity_of(scenario$tbl, scenario$inputs)
    tibble::tibble(
      assumption_family = family_name,
      baseline_physician_compensation_capacity = baseline_capacity,
      perturbed_physician_compensation_capacity = perturbed_capacity,
      delta = perturbed_capacity - baseline_capacity,
      counterfactual_shortfall_closed_pct = if (shortfall > 0) {
        100 * (perturbed_capacity - baseline_capacity) / shortfall
      } else {
        NA_real_
      }
    )
  })
  results |> dplyr::arrange(dplyr::desc(.data$delta))
}

#' Standardized elasticities for practice-economics assumptions
#'
#' @description
#' [practice_economics_sensitivity_decomposition()] perturbs each assumption
#' family to a single, differently-sized "favorable" alternative -- useful
#' for ranking levers, not for comparing their sensitivity on a common
#' scale, since a bigger counterfactual shift on one input isn't the same
#' claim as a bigger underlying sensitivity. This function reports the
#' standard elasticity, `(pct change in physician_compensation_capacity) /
#' (pct change in the input)`, for the model's continuous, distributionally-
#' declared inputs only.
#'
#' `payer_mix` is excluded: it is compositional (four shares summing to 1,
#' not a single scalar that moves by "+-10\%"). `annual_wrvu`/`app_fte` are
#' excluded too: they are `practice_tbl` fields with no uncertainty
#' distribution declared for them in this model -- see
#' [practice_economics_sensitivity_decomposition()]'s `wrvu_productivity`/
#' `app_intensity` scenarios for those instead.
#'
#' Two perturbation schemes, reported side by side:
#' \describe{
#'   \item{pct10}{Every continuous input's central value (mode/median/mean)
#'     moved +-`pct_perturbation`, holding its declared spread/shape fixed
#'     -- symmetric and comparable across inputs.}
#'   \item{p25_p75}{Only for `overhead`, `malpractice`, `app_compensation`,
#'     and `commercial_ratio` -- inputs with an uncertainty distribution
#'     ALREADY declared in `inputs`. Uses the real p25/p75 of that exact
#'     declared distribution (collapsed to a point at that value, the same
#'     technique [practice_economics_sensitivity_decomposition()] uses),
#'     not an invented range. `NA` for the four collection rates, which
#'     have no declared distribution at all.}
#' }
#'
#' Elasticity on a near-zero or sign-changing baseline
#' `physician_compensation_capacity` is numerically unstable (a small
#' denominator inflates the ratio, and a perturbation that crosses zero
#' makes "percent change" discontinuous) -- `unstable_baseline` flags this
#' rather than reporting a number that looks precise but isn't.
#'
#' @param practice_tbl Baseline practice-tbl, as for
#'   [simulate_practice_economics()].
#' @param inputs Named list from [practice_economics_defaults()].
#' @param draws Monte Carlo draws per scenario.
#' @param seed Reproducible random seed (same seed reused per scenario so
#'   differences reflect the perturbation, not draw noise).
#' @param pct_perturbation Symmetric perturbation fraction for the `pct10`
#'   scheme (default `0.10`).
#'
#' `elasticity_low`/`elasticity_high` are not forced to be equal, even for
#' an input (like `overhead`) that enters the cost formula linearly: the
#' same `seed` is reused across scenarios so differences reflect the
#' perturbation rather than fresh draw noise, but pinning one input to a
#' fixed value changes how many random draws the OTHER stochastic inputs
#' consume from the same seeded stream, so a few-percent low/high asymmetry
#' on an otherwise-linear input is expected Monte Carlo artifact, not a
#' sign of real nonlinearity -- a large asymmetry (multiples, not a few
#' percent) is the signal worth investigating.
#'
#' @return Tibble: `input_name`, `perturbation_type` (`"pct10"`/`"p25_p75"`),
#'   `baseline_value`, `low_value`, `high_value`, `elasticity_low`,
#'   `elasticity_high`, `unstable_baseline`.
#' @concept economics
#' @export
practice_economics_elasticity <- function(
    practice_tbl,
    inputs = practice_economics_defaults(),
    draws = 1000L,
    seed = 20260821L,
    pct_perturbation = 0.10) {
  capacity_of <- function(inputs_arg) {
    base::mean(
      simulate_practice_economics(
        practice_tbl, draws = draws, seed = seed, inputs = inputs_arg
      )$draws$physician_compensation_capacity
    )
  }
  baseline_capacity <- capacity_of(inputs)
  baseline_revenue <- base::mean(
    simulate_practice_economics(
      practice_tbl, draws = draws, seed = seed, inputs = inputs
    )$draws$gross_revenue
  )
  # A baseline within 1% of national mean gross revenue is "near enough to
  # zero" for a percent-change ratio to blow up; this is a property of the
  # denominator, not of any one input's real effect.
  unstable_baseline <- base::abs(baseline_capacity) < 0.01 * baseline_revenue

  elasticity_row <- function(input_name, baseline_value, low_value,
                              high_value, low_inputs, high_inputs,
                              perturbation_type) {
    low_capacity <- capacity_of(low_inputs)
    high_capacity <- capacity_of(high_inputs)
    pct_change_capacity_low <-
      (low_capacity - baseline_capacity) / base::abs(baseline_capacity)
    pct_change_capacity_high <-
      (high_capacity - baseline_capacity) / base::abs(baseline_capacity)
    pct_change_input_low <-
      (low_value - baseline_value) / base::abs(baseline_value)
    pct_change_input_high <-
      (high_value - baseline_value) / base::abs(baseline_value)
    tibble::tibble(
      input_name = input_name,
      perturbation_type = perturbation_type,
      baseline_value = baseline_value,
      low_value = low_value,
      high_value = high_value,
      elasticity_low = pct_change_capacity_low / pct_change_input_low,
      elasticity_high = pct_change_capacity_high / pct_change_input_high,
      unstable_baseline = unstable_baseline
    )
  }

  scale_central <- function(inputs_arg, field, factor) {
    inputs_arg[[field]] <- inputs_arg[[field]] * factor
    inputs_arg
  }
  collapse_to_point <- function(inputs_arg, mode_field, lower_field,
                                 upper_field, value) {
    inputs_arg[[mode_field]] <- value
    if (!base::is.null(lower_field)) inputs_arg[[lower_field]] <- value
    if (!base::is.null(upper_field)) inputs_arg[[upper_field]] <- value
    inputs_arg
  }

  rows <- base::list()

  # --- overhead: triangular(lower, mode, upper) -----------------------
  rows$overhead_pct10 <- elasticity_row(
    "overhead", inputs$overhead_mode,
    inputs$overhead_mode * (1 - pct_perturbation),
    inputs$overhead_mode * (1 + pct_perturbation),
    low_inputs = base::within(inputs, {
      overhead_lower <- overhead_lower * (1 - pct_perturbation)
      overhead_mode <- overhead_mode * (1 - pct_perturbation)
      overhead_upper <- overhead_upper * (1 - pct_perturbation)
    }),
    high_inputs = base::within(inputs, {
      overhead_lower <- overhead_lower * (1 + pct_perturbation)
      overhead_mode <- overhead_mode * (1 + pct_perturbation)
      overhead_upper <- overhead_upper * (1 + pct_perturbation)
    }),
    perturbation_type = "pct10"
  )
  overhead_p25 <- .practice_triangular_quantile(
    0.25, inputs$overhead_lower, inputs$overhead_mode, inputs$overhead_upper
  )
  overhead_p75 <- .practice_triangular_quantile(
    0.75, inputs$overhead_lower, inputs$overhead_mode, inputs$overhead_upper
  )
  rows$overhead_p25p75 <- elasticity_row(
    "overhead", inputs$overhead_mode, overhead_p25, overhead_p75,
    low_inputs = collapse_to_point(
      inputs, "overhead_mode", "overhead_lower", "overhead_upper",
      overhead_p25
    ),
    high_inputs = collapse_to_point(
      inputs, "overhead_mode", "overhead_lower", "overhead_upper",
      overhead_p75
    ),
    perturbation_type = "p25_p75"
  )

  # --- malpractice: lognormal(median, cv) -----------------------------
  malpractice_sdlog <- base::sqrt(base::log1p(inputs$malpractice_cv^2))
  rows$malpractice_pct10 <- elasticity_row(
    "malpractice", inputs$malpractice_median,
    inputs$malpractice_median * (1 - pct_perturbation),
    inputs$malpractice_median * (1 + pct_perturbation),
    low_inputs = scale_central(
      inputs, "malpractice_median", 1 - pct_perturbation
    ),
    high_inputs = scale_central(
      inputs, "malpractice_median", 1 + pct_perturbation
    ),
    perturbation_type = "pct10"
  )
  malpractice_p25 <- stats::qlnorm(
    0.25, base::log(inputs$malpractice_median), malpractice_sdlog
  )
  malpractice_p75 <- stats::qlnorm(
    0.75, base::log(inputs$malpractice_median), malpractice_sdlog
  )
  rows$malpractice_p25p75 <- elasticity_row(
    "malpractice", inputs$malpractice_median, malpractice_p25,
    malpractice_p75,
    low_inputs = base::within(inputs, {
      malpractice_median <- malpractice_p25
      malpractice_cv <- 1e-6
    }),
    high_inputs = base::within(inputs, {
      malpractice_median <- malpractice_p75
      malpractice_cv <- 1e-6
    }),
    perturbation_type = "p25_p75"
  )

  # --- APP compensation: normal(mean, sd) -----------------------------
  rows$app_compensation_pct10 <- elasticity_row(
    "app_compensation", inputs$app_compensation_mean,
    inputs$app_compensation_mean * (1 - pct_perturbation),
    inputs$app_compensation_mean * (1 + pct_perturbation),
    low_inputs = scale_central(
      inputs, "app_compensation_mean", 1 - pct_perturbation
    ),
    high_inputs = scale_central(
      inputs, "app_compensation_mean", 1 + pct_perturbation
    ),
    perturbation_type = "pct10"
  )
  app_p25 <- stats::qnorm(
    0.25, inputs$app_compensation_mean, inputs$app_compensation_sd
  )
  app_p75 <- stats::qnorm(
    0.75, inputs$app_compensation_mean, inputs$app_compensation_sd
  )
  rows$app_compensation_p25p75 <- elasticity_row(
    "app_compensation", inputs$app_compensation_mean, app_p25, app_p75,
    low_inputs = base::within(inputs, {
      app_compensation_mean <- app_p25
      app_compensation_sd <- 1e-6
    }),
    high_inputs = base::within(inputs, {
      app_compensation_mean <- app_p75
      app_compensation_sd <- 1e-6
    }),
    perturbation_type = "p25_p75"
  )

  # --- commercial ratio: lognormal(median, sd from 95% CI [lower,upper]) --
  commercial_sdlog <- (base::log(inputs$commercial_ratio_upper) -
    base::log(inputs$commercial_ratio_lower)) / (2 * 1.96)
  rows$commercial_ratio_pct10 <- elasticity_row(
    "commercial_ratio", inputs$commercial_ratio_median,
    inputs$commercial_ratio_median * (1 - pct_perturbation),
    inputs$commercial_ratio_median * (1 + pct_perturbation),
    low_inputs = scale_central(
      inputs, "commercial_ratio_median", 1 - pct_perturbation
    ),
    high_inputs = scale_central(
      inputs, "commercial_ratio_median", 1 + pct_perturbation
    ),
    perturbation_type = "pct10"
  )
  commercial_p25 <- stats::qlnorm(
    0.25, base::log(inputs$commercial_ratio_median), commercial_sdlog
  )
  commercial_p75 <- stats::qlnorm(
    0.75, base::log(inputs$commercial_ratio_median), commercial_sdlog
  )
  rows$commercial_ratio_p25p75 <- elasticity_row(
    "commercial_ratio", inputs$commercial_ratio_median, commercial_p25,
    commercial_p75,
    low_inputs = scale_central(
      inputs, "commercial_ratio_median",
      commercial_p25 / inputs$commercial_ratio_median
    ),
    high_inputs = scale_central(
      inputs, "commercial_ratio_median",
      commercial_p75 / inputs$commercial_ratio_median
    ),
    perturbation_type = "p25_p75"
  )

  # --- payer collection rates: point values, no declared distribution --
  # +-10% only, capped at 1.0 (collection cannot exceed 100%); p25/p75 is
  # NA because none is declared for these four inputs.
  for (rate_field in base::c(
    "medicare_collection", "medicaid_collection",
    "commercial_collection", "self_pay_collection"
  )) {
    baseline_rate <- inputs[[rate_field]]
    low_rate <- baseline_rate * (1 - pct_perturbation)
    high_rate <- base::min(1, baseline_rate * (1 + pct_perturbation))
    rows[[base::paste0(rate_field, "_pct10")]] <- elasticity_row(
      rate_field, baseline_rate, low_rate, high_rate,
      low_inputs = scale_central(inputs, rate_field, 1 - pct_perturbation),
      high_inputs = `[[<-`(
        inputs, rate_field, base::min(1, baseline_rate * (1 + pct_perturbation))
      ),
      perturbation_type = "pct10"
    )
  }

  dplyr::bind_rows(rows)
}

#' MedPAC physician/APP compensation benchmarks
#'
#' @description
#' Extracts the three `external_benchmark` rows from
#' [practice_economics_evidence()] (MedPAC March 2025 Report to Congress,
#' Ch. 4, citing SullivanCotter's 2024 survey, 2023 compensation data) as a
#' small lookup tibble, so [physician_compensation_plausibility()] and any
#' other caller read the same cited numbers rather than a second copy.
#'
#' @return Tibble: `benchmark_name` (`"all_specialties"`, `"surgical"`,
#'   `"app"`), `value` (2023 USD).
#' @concept economics
#' @export
physician_compensation_benchmarks <- function() {
  evidence_tbl <- practice_economics_evidence()
  benchmark_rows <- evidence_tbl |>
    dplyr::filter(.data$status == "external_benchmark")
  tibble::tibble(
    benchmark_name = base::c("all_specialties", "surgical", "app"),
    value = base::c(
      benchmark_rows$value[
        base::grepl("all specialties", benchmark_rows$estimand)
      ],
      benchmark_rows$value[
        base::grepl("surgical specialties", benchmark_rows$estimand)
      ],
      benchmark_rows$value[
        base::grepl("advanced practice provider", benchmark_rows$estimand)
      ]
    )
  )
}

#' Physician-compensation plausibility check (benchmark, not calibration target)
#'
#' @description
#' Reports whether modeled `physician_compensation_capacity` overlaps a
#' plausible real-world range implied by [physician_compensation_benchmarks()]
#' -- it does not adjust any input, and nothing in this package feeds this
#' comparison back into `practice_economics_defaults()` or any simulation
#' parameter. The banding (implausibly low / plausible range / implausibly
#' high) is a documented heuristic (50%-150% of the benchmark), not a
#' statistical test -- MedPAC publishes a median, not a distribution this
#' model's compensation-capacity draws could be formally tested against.
#'
#' @param physician_compensation_capacity Numeric vector of
#'   `physician_compensation_capacity` draws (or their mean), as produced by
#'   [simulate_practice_economics()].
#' @param benchmarks Tibble from [physician_compensation_benchmarks()];
#'   recomputed when `NULL`.
#'
#' @return Tibble: `benchmark_name`, `benchmark_value`,
#'   `modeled_compensation_capacity`, `pct_of_benchmark`, `plausibility_band`.
#' @concept economics
#' @export
physician_compensation_plausibility <- function(
    physician_compensation_capacity, benchmarks = NULL) {
  if (base::is.null(benchmarks)) benchmarks <- physician_compensation_benchmarks()
  modeled_capacity <- base::mean(physician_compensation_capacity, na.rm = TRUE)

  benchmarks |>
    dplyr::mutate(
      modeled_compensation_capacity = modeled_capacity,
      pct_of_benchmark = 100 * modeled_capacity / .data$value,
      plausibility_band = dplyr::case_when(
        modeled_capacity <= 0 ~
          "non-positive (model implies unpayable compensation)",
        .data$pct_of_benchmark < 50 ~ "implausibly low (<50% of benchmark)",
        .data$pct_of_benchmark <= 150 ~
          "plausible range (50-150% of benchmark)",
        TRUE ~ "implausibly high (>150% of benchmark)"
      )
    ) |>
    dplyr::rename(benchmark_value = "value")
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
