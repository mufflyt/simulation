# scripts/calibration/build_urps_base_year_access_anchor.R
#
# Purpose:
# Build an empirical evidence-supported base-year URPS gap calibration.
#
# IMPORTANT:
# This does NOT pretend that wait time directly identifies required FTE.
#
# Evidence roles:
#   Lizeth  -> direct national URPS access pressure
#   Rabice  -> direct prior national URPS access pressure
#   CADR    -> observed Medicare pelvic-floor utilization
#   CHIA    -> observed regional all-payer pelvic-floor utilization
#
# The first two support the existence of a current access shortfall.
# The latter two guard the demand/use side against implausible calibration.
#
# The resulting gap remains "assumed_with_evidence" until a direct
# URPS capacity-to-demand mapping is measured or externally identified.
#
# calibration_weight is deliberate: CADR and CHIA carry weight 0 in computing
# the access shortfall (they observe treated USE, not unmet access -- giving
# them shortfall weight would be pseudo-precision). They still "count" as
# independent empirical checks that the demand side represents real pelvic-floor
# care. Lizeth and Rabice carry weight 1: two national URPS mystery-caller
# studies are replication across TIME within the same specialty, which is
# stronger for a base-year access gap than borrowing another specialty's
# capacity distribution.
#
# Reporting guidance: run the WHOLE gap grid (0%, 5%, ..., 25%) and show how the
# workforce-cliff conclusion behaves across it. Do not select 10% or 15% because
# the mystery-call numbers look bad. If the cliff persists across most of the
# range, the conclusion does not depend on pretending we know today's exact
# national FTE shortage.
#
# Production follow-up (not this script): read the actual Lizeth, Rabice, CADR,
# and Cadish/CHIA source files automatically instead of passing values by hand.

build_urps_access_evidence <- function(
    base_supply_fte,
    lizeth_n_calls = 880L,
    lizeth_n_offered = 283L,
    lizeth_medicaid_known = 103L,
    lizeth_medicaid_refused = 35L,
    lizeth_wait_median = NA_real_,
    lizeth_wait_p25 = NA_real_,
    lizeth_wait_p75 = NA_real_,
    rabice_n_calls,
    rabice_n_offered,
    rabice_wait_median,
    rabice_wait_p25 = NA_real_,
    rabice_wait_p75 = NA_real_,
    cadr_sling_annual = 5566,
    cadr_pessary_annual = 9616,
    cadr_pt_annual = 7381,
    chia_pop_hysterectomy = 1306,
    gap_grid = seq(0.00, 0.25, by = 0.025),
    save_dir = "artifacts/calibration") {
  base::message(
    "build_urps_access_evidence(): starting empirical calibration."
  )
  base::message(
    "Base-year supplied FTE: ",
    base::format(base_supply_fte, big.mark = ",")
  )
  if (!base::is.numeric(base_supply_fte) ||
      base::length(base_supply_fte) != 1L ||
      !base::is.finite(base_supply_fte) ||
      base_supply_fte <= 0) {
    base::stop("`base_supply_fte` must be one positive finite number.")
  }
  if (!base::is.numeric(gap_grid) ||
      base::any(!base::is.finite(gap_grid)) ||
      base::any(gap_grid < 0 | gap_grid >= 1)) {
    base::stop("`gap_grid` must contain values in [0, 1).")
  }
  base::message("Calculating Lizeth access measures.")
  lizeth_offer_rate <- lizeth_n_offered / lizeth_n_calls
  lizeth_medicaid_refusal <- (
    lizeth_medicaid_refused / lizeth_medicaid_known
  )
  base::message(
    "Lizeth appointment-offer rate: ",
    base::sprintf("%.1f%%", 100 * lizeth_offer_rate)
  )
  base::message(
    "Lizeth explicit Medicaid nonacceptance: ",
    base::sprintf("%.1f%%", 100 * lizeth_medicaid_refusal)
  )
  base::message("Calculating Rabice access measures.")
  rabice_offer_rate <- rabice_n_offered / rabice_n_calls
  base::message(
    "Rabice appointment-offer rate: ",
    base::sprintf("%.1f%%", 100 * rabice_offer_rate)
  )
  evidence_tbl <- tibble::tibble(
    source = c(
      "Lizeth mystery caller",
      "Lizeth mystery caller",
      "Lizeth mystery caller",
      "Rabice mystery caller",
      "Rabice mystery caller",
      "CADR Medicare",
      "CADR Medicare",
      "CADR Medicare",
      "CHIA Massachusetts"
    ),
    evidence_domain = c(
      "appointment_availability",
      "payer_access",
      "wait_time",
      "appointment_availability",
      "wait_time",
      "treated_utilization",
      "treated_utilization",
      "treated_utilization",
      "treated_utilization"
    ),
    metric = c(
      "appointment_offer_rate",
      "explicit_medicaid_nonacceptance",
      "conditional_wait_business_days",
      "appointment_offer_rate",
      "conditional_wait_business_days",
      "sling_episodes_per_year",
      "pessary_episodes_per_year",
      "pelvic_floor_pt_episodes_per_year",
      "pop_indication_hysterectomy_encounters"
    ),
    estimate = c(
      lizeth_offer_rate,
      lizeth_medicaid_refusal,
      lizeth_wait_median,
      rabice_offer_rate,
      rabice_wait_median,
      cadr_sling_annual,
      cadr_pessary_annual,
      cadr_pt_annual,
      chia_pop_hysterectomy
    ),
    p25 = c(
      NA_real_,
      NA_real_,
      lizeth_wait_p25,
      NA_real_,
      rabice_wait_p25,
      NA_real_,
      NA_real_,
      NA_real_,
      NA_real_
    ),
    p75 = c(
      NA_real_,
      NA_real_,
      lizeth_wait_p75,
      NA_real_,
      rabice_wait_p75,
      NA_real_,
      NA_real_,
      NA_real_,
      NA_real_
    ),
    numerator = c(
      lizeth_n_offered,
      lizeth_medicaid_refused,
      NA_real_,
      rabice_n_offered,
      NA_real_,
      NA_real_,
      NA_real_,
      NA_real_,
      chia_pop_hysterectomy
    ),
    denominator = c(
      lizeth_n_calls,
      lizeth_medicaid_known,
      lizeth_n_offered,
      rabice_n_calls,
      rabice_n_offered,
      NA_real_,
      NA_real_,
      NA_real_,
      NA_real_
    ),
    geography = c(
      "United States",
      "United States",
      "United States",
      "United States",
      "United States",
      "United States",
      "United States",
      "United States",
      "Massachusetts"
    ),
    population = c(
      "board-certified URPS contacted",
      "URPS practices with definitive Medicaid response",
      "URPS calls receiving appointment offer",
      "URPS practices contacted",
      "URPS calls receiving appointment offer",
      "Medicare beneficiaries receiving treatment",
      "Medicare beneficiaries receiving treatment",
      "Medicare beneficiaries receiving treatment",
      "all-payer inpatient encounters"
    ),
    evidence_role = c(
      "direct_access_anchor",
      "direct_access_anchor",
      "direct_access_anchor",
      "direct_access_anchor",
      "direct_access_anchor",
      "demand_validation",
      "demand_validation",
      "demand_validation",
      "regional_demand_validation"
    ),
    calibration_weight = c(
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      0
    ),
    evidence_tier = c(
      base::rep("direct_urps_access", 5),
      base::rep("direct_empirical_population_limited", 3),
      "direct_empirical_regional"
    )
  )
  base::message(
    "Evidence ledger assembled: ",
    base::nrow(evidence_tbl),
    " empirical measures."
  )
  access_tbl <- evidence_tbl |>
    dplyr::filter(.data$evidence_role == "direct_access_anchor")
  utilization_tbl <- evidence_tbl |>
    dplyr::filter(
      .data$evidence_role %in% c(
        "demand_validation",
        "regional_demand_validation"
      )
    )
  base::message(
    "Direct URPS access measures: ",
    base::nrow(access_tbl)
  )
  base::message(
    "Utilization validation measures: ",
    base::nrow(utilization_tbl)
  )
  evidence_text <- c(
    base::sprintf(
      paste0(
        "Lizeth national URPS mystery-caller study: ",
        "%s of %s evaluable calls received an appointment date ",
        "(%.1f%%)."
      ),
      base::format(lizeth_n_offered, big.mark = ","),
      base::format(lizeth_n_calls, big.mark = ","),
      100 * lizeth_offer_rate
    ),
    base::sprintf(
      paste0(
        "Lizeth: explicit Medicaid nonacceptance occurred in ",
        "%s of %s practices with a definitive response (%.1f%%)."
      ),
      base::format(lizeth_medicaid_refused, big.mark = ","),
      base::format(lizeth_medicaid_known, big.mark = ","),
      100 * lizeth_medicaid_refusal
    ),
    base::sprintf(
      paste0(
        "Rabice national URPS mystery-caller study: %s of %s ",
        "calls received an appointment offer (%.1f%%)."
      ),
      base::format(rabice_n_offered, big.mark = ","),
      base::format(rabice_n_calls, big.mark = ","),
      100 * rabice_offer_rate
    ),
    base::sprintf(
      paste0(
        "Rabice conditional wait: median %.1f business days ",
        "(p25 %.1f, p75 %.1f)."
      ),
      rabice_wait_median,
      rabice_wait_p25,
      rabice_wait_p75
    ),
    base::sprintf(
      paste0(
        "CADR Medicare pelvic-floor treatment: approximately ",
        "%s sling, %s pessary, and %s pelvic-floor PT episodes ",
        "per year."
      ),
      base::format(cadr_sling_annual, big.mark = ","),
      base::format(cadr_pessary_annual, big.mark = ","),
      base::format(cadr_pt_annual, big.mark = ",")
    ),
    base::sprintf(
      paste0(
        "CHIA Massachusetts all-payer validation identified ",
        "%s POP-indication hysterectomy encounters."
      ),
      base::format(chia_pop_hysterectomy, big.mark = ",")
    )
  )
  base::message(
    "Running evidence-supported baseline-gap sensitivity."
  )
  sensitivity_tbl <- tibble::tibble(
    gap_fraction = gap_grid
  ) |>
    dplyr::mutate(
      adequacy = 1 - .data$gap_fraction,
      required_fte = base_supply_fte / .data$adequacy,
      shortfall_fte = .data$required_fte - base_supply_fte,
      additional_fte_vs_equilibrium = .data$shortfall_fte,
      calibration_status = "assumed_with_evidence",
      method = "assumed",
      evidence_n = base::length(evidence_text)
    )
  if (base::exists(
    "assumed_baseline_gap",
    mode = "function",
    inherits = TRUE
  )) {
    base::message(
      "Validating each sensitivity point with assumed_baseline_gap()."
    )
    anchor_objects <- base::lapply(
      sensitivity_tbl$gap_fraction,
      function(gap_value) {
        assumed_baseline_gap(
          gap_fraction = gap_value,
          evidence = evidence_text,
          base_supply_fte = base_supply_fte
        )
      }
    )
    base::stopifnot(
      base::length(anchor_objects) == base::nrow(sensitivity_tbl)
    )
  } else {
    base::message(
      paste0(
        "assumed_baseline_gap() is not loaded; arithmetic was ",
        "computed directly. Source the package R files or load urpssim ",
        "to validate the objects."
      )
    )
  }
  base::dir.create(
    save_dir,
    recursive = TRUE,
    showWarnings = FALSE
  )
  timestamp <- base::format(
    base::Sys.time(),
    "%Y%m%d_%H%M%S"
  )
  evidence_path <- base::file.path(
    save_dir,
    base::paste0(
      "urps_base_year_evidence_ledger_",
      timestamp,
      ".csv"
    )
  )
  sensitivity_path <- base::file.path(
    save_dir,
    base::paste0(
      "urps_base_year_gap_sensitivity_",
      timestamp,
      ".csv"
    )
  )
  readr::write_csv(
    evidence_tbl,
    evidence_path,
    na = ""
  )
  base::message(
    "Saved evidence ledger: ",
    base::normalizePath(evidence_path, mustWork = TRUE)
  )
  readr::write_csv(
    sensitivity_tbl,
    sensitivity_path,
    na = ""
  )
  base::message(
    "Saved gap sensitivity: ",
    base::normalizePath(sensitivity_path, mustWork = TRUE)
  )
  direction_text <- dplyr::case_when(
    rabice_offer_rate < lizeth_offer_rate ~
      "improved in the newer Lizeth study",
    rabice_offer_rate > lizeth_offer_rate ~
      "worsened in the newer Lizeth study",
    TRUE ~
      "was unchanged between studies"
  )
  summary_sentence <- base::sprintf(
    paste0(
      "Across two national URPS mystery-caller studies, ",
      "appointment access %s; current Lizeth data show a %.1f%% ",
      "appointment-offer rate and %.1f%% explicit Medicaid ",
      "nonacceptance, while CADR and CHIA provide independent ",
      "empirical utilization validation. These observations support ",
      "an assumed-with-evidence base-year gap sensitivity analysis, ",
      "not a directly measured FTE requirement."
    ),
    direction_text,
    100 * lizeth_offer_rate,
    100 * lizeth_medicaid_refusal
  )
  base::message(summary_sentence)
  base::message(
    "build_urps_access_evidence(): calibration assembly complete."
  )
  base::list(
    evidence_ledger = evidence_tbl,
    access_evidence = access_tbl,
    utilization_validation = utilization_tbl,
    gap_sensitivity = sensitivity_tbl,
    evidence_text = evidence_text,
    summary_sentence = summary_sentence,
    saved_files = c(
      evidence_ledger = evidence_path,
      gap_sensitivity = sensitivity_path
    )
  )
}

# ---------------------------------------------------------------------------
# Example usage (fill in the real Rabice values before running):
#
# access_anchor <- build_urps_access_evidence(
#   base_supply_fte  = BASE_YEAR_URPS_FTE,
#   rabice_n_calls   = RABICE_N_CALLS,
#   rabice_n_offered = RABICE_N_OFFERED,
#   rabice_wait_median = RABICE_WAIT_MEDIAN,
#   rabice_wait_p25  = RABICE_WAIT_P25,
#   rabice_wait_p75  = RABICE_WAIT_P75
# )
# access_anchor$gap_sensitivity |>
#   dplyr::select(
#     gap_fraction, adequacy, required_fte, shortfall_fte, calibration_status
#   )
# ---------------------------------------------------------------------------
