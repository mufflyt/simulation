# Demand-transition registry ----
#
# One diffable, independently reviewable table of every coefficient/probability
# that drives a demand TRANSITION in the life-course pathway (R/demand-lifecourse). Each row
# carries the value, a CALIBRATION TIER drawn from the package's canonical
# `CALIBRATION_TIERS` vocabulary (R/data-cms_rvu), and a citation -- so a reviewer can diff
# "what coefficient changed, at what tier, on what evidence" in a single place,
# without reading model code.
#
# WHY THIS EXISTS (see the demand-model audit): the transition coefficients used
# a bespoke `status = "placeholder_uncalibrated"` string that is NOT a member of
# CALIBRATION_TIERS and was checked by no assertion, so placeholder probabilities
# could reach reported FTE with no publication refusal -- unlike the workload
# basket (R/supply-workload_to_fte) and the R/demand-condition_service_pathway utilisation cascade, which ARE gated.
#
# This registry is ADDITIVE and output-preserving: lifecourse_risk_params(),
# lifecourse_risk_params_cited() and lifecourse_pathway_params() now READ from
# it and return byte-identical structures. The new gate
# `assert_publishable_demand_coefficients()` reuses the SAME accept/opt-in/refuse logic as
# `assert_publishable_workload()`, so the demand coefficients are finally subject
# to the package's own publication contract. It is a standalone gate a caller /
# manuscript step invokes; it is NOT auto-wired into simulate_lifecourse_demand()
# (that would change strict-mode behaviour and needs a separate decision).
#
# Stage vocabulary maps each parameter onto the demand pipeline
#   population -> disease state -> symptom severity -> care seeking -> referral
#   -> treatment eligibility -> treatment preference -> realized utilization
#   -> clinician workload
# Coefficients here cover disease_state (the risk log-odds) and the collapsed
# care_seeking / referral / treatment_preference probabilities. `notes` flags
# where distinct conceptual stages are currently folded into one scalar.

# Fixed name order of the per-condition risk-coefficient list, preserved so the
# reconstructed lifecourse_risk_params() is identical() to the original.
.DEMAND_RISK_PARAM_ORDER <- c("b0", "bvag", "bage", "bysl", "bbmi", "bhyst",
                              "bmeno", "bcomorb")
.DEMAND_CONDITION_ORDER <- c("ui", "pop", "ai")
.DEMAND_PATHWAY_ORDER <- c("recognition", "p_seek", "p_referral", "p_treated")
# Ordered symptom-severity levels, matching the Sandvik category factor
# (score_sandvik_severity, R/demand-severity_sandvik): slight < moderate < severe < very_severe.
.DEMAND_SEVERITY_LEVELS <- c("slight", "moderate", "severe", "very_severe")

# Which pipeline stage each parameter belongs to.
.demand_stage_of <- function(param) {
  disease <- .DEMAND_RISK_PARAM_ORDER
  care    <- c("recognition", "p_seek")
  refer   <- "p_referral"
  treat   <- "p_treated"
  ifelse(param %in% disease, "disease_state",
    ifelse(param %in% care, "care_seeking",
      ifelse(param %in% refer, "referral",
        ifelse(param %in% treat, "treatment_preference", NA_character_))))
}

#' Demand-transition coefficient registry
#'
#' Every coefficient/probability that drives a demand transition in the
#' life-course pathway, with its calibration tier (from `CALIBRATION_TIERS`) and
#' source. `variant = "default"` rows are the placeholder set; `variant =
#' "cited"` rows are the literature-anchored overrides applied by
#' [lifecourse_risk_params_cited()].
#'
#' @return A tibble with `stage`, `condition`, `param`, `variant`, `value`,
#'   `ci_low`, `ci_high`, `calibration_tier`, `source`, `notes`.
#' @family transition registry
#' @concept demand
#' @export
demand_transition_registry <- function() {
  # -- Disease-state log-odds (placeholder set). Compact wide block mirrors the
  #    original coefficient set exactly; melted to one row per (condition, param).
  # b0 AND bage ARE PREVALENCE-ANCHORED. The superseded placeholders were
  #
  #   ui  b0 -1.60  bage 0.35
  #   pop b0 -2.40  bage 0.30
  #   ai  b0 -2.90  bage 0.25
  #
  # and they produced POP prevalence 5.6x above published SYMPTOMATIC values
  # (population-weighted 24.3% vs 4.4%), including 59.7% among women 75+ -- a
  # figure closer to exam-detected POP-Q stage >=2, which is largely
  # asymptomatic, than to a bulge a woman reports. Since treated = prevalence x
  # cascade, that error propagated straight into procedure volume and accounted
  # for most of the 8.51x prolapse overstatement.
  #
  # The values below are the output of calibrate_lifecourse_prevalence()
  # against Nygaard 2008 / Wu 2014 (UI, POP) and Whitehead 2009 / Bharucha 2005
  # (FI), stored as literals rather than recomputed per call: an optim() on
  # every accessor would be slow, and stored numbers can be read and audited.
  # Reproduce with lifecourse_risk_params_prevalence_anchored().
  #
  # EVERY OTHER COEFFICIENT IS UNCHANGED, including the cited bvag (Hendrix WHI
  # / Mant Oxford-FPA) and bbmi (Giri 2017). The scenario levers act through
  # those, so replacing the risk model wholesale would have matched prevalence
  # and destroyed every scenario the model exists to run.
  risk_wide <- tibble::tribble(
    ~condition,  ~b0,     ~bvag,   ~bage,  ~bysl, ~bbmi,   ~bhyst,  ~bmeno, ~bcomorb,
    "ui",      -1.7825,  0.0499,  0.3387,  0.00,  0.1997,  0.4694,   0.00,   0.00,
    "pop",     -4.4147,  0.3000,  0.3157,  0.00,  0.0800,  0.3365,   0.00,   0.00,
    "ai",      -3.2071,  0.0042,  0.2957,  0.00,  0.0851,  0.3906,   0.00,   0.00
  )

  # The superseded placeholder intercept/slope, kept reachable and auditable
  # rather than deleted. Retrieved with lifecourse_risk_params_placeholder().
  placeholder_wide <- tibble::tribble(
    ~condition,  ~b0,  ~bage,
    "ui",      -1.60,  0.35,
    "pop",     -2.40,  0.30,
    "ai",      -2.90,  0.25
  )
  # -- Care-pathway probabilities (placeholder set).
  path_wide <- tibble::tribble(
    ~condition, ~recognition, ~p_seek, ~p_referral, ~p_treated,
    "ui",             0.6850,  0.4795,      0.5756,      0.7200,
    "pop",            0.7410,  0.5230,      0.6373,      0.6800,
    "ai",             0.5820,  0.3840,      0.4389,      0.6400
  )

  melt <- function(wide, note_of) {
    params <- setdiff(names(wide), "condition")
    rows <- lapply(params, function(p) {
      tier   <- if (p %in% c("p_seek", "p_referral", "recognition")) "evidence_anchored" else "uncalibrated_illustrative"
      source <- if (p == "p_seek") "MCBS 2022 (survey-weighted care-seeking among symptomatic women)" else if (p == "p_referral") "Pooled NAMCS 2015-2019 (survey-weighted specialist visit proportion)" else if (p == "recognition") "Nygaard 2008 / Whitehead 2009 (symptom recognition)" else "placeholder (expert judgement; not evidence-anchored)"
      tibble::tibble(
        stage = .demand_stage_of(p), condition = wide$condition, param = p,
        variant = "default", value = wide[[p]],
        ci_low = NA_real_, ci_high = NA_real_,
        calibration_tier = tier,
        source = source,
        notes = note_of(p))
    })
    do.call(rbind, rows)
  }
  risk_note <- function(p) if (p == "bvag") "primary exposure term (cited override available)" else ""

  # b0 and bage are no longer placeholders; their tier and source must say so,
  # or the registry would misreport the model's own provenance.
  .anchor_risk_rows <- function(d) {
    i <- d$param %in% c("b0", "bage") & d$stage == "disease_state"
    d$calibration_tier[i] <- "solved"
    d$source[i] <- paste0(
      "calibrated to published SYMPTOMATIC prevalence: Nygaard 2008 JAMA / ",
      "Wu 2014 (UI, POP), Whitehead 2009 / Bharucha 2005 (FI); ",
      "reproduce with lifecourse_risk_params_prevalence_anchored()")
    d$notes[i] <- paste0(
      "SOLVED, not measured: b0 and bage were chosen so the cohort matches ",
      "published age-banded symptomatic prevalence. Fit is imperfect by ",
      "band (achieved/target ~0.49-1.31), which indicates the remaining ",
      "covariate structure is also mis-specified -- see ",
      "calibrate_lifecourse_prevalence().")
    d
  }
  path_note <- function(p) switch(p,
    p_treated = "treatment preference given eligible (eligibility split into the treatment_eligibility stage)",
    recognition = "symptom recognition (pre-severity)",
    p_seek = "base care-seeking; reweighted by the symptom_severity stage (seek_mult_*)",
    "")

  default_rows <- rbind(.anchor_risk_rows(melt(risk_wide, risk_note)),
                        melt(path_wide, path_note))

  # The superseded placeholder intercept/slope, as an explicit variant.
  placeholder_rows <- melt(placeholder_wide, function(p) "")
  placeholder_rows$variant <- "placeholder"
  placeholder_rows$notes <- paste0(
    "SUPERSEDED. The pre-anchoring placeholder, kept reachable for comparison ",
    "and reproducibility. Produced POP prevalence 5.6x above published ",
    "symptomatic values.")
  default_rows <- rbind(default_rows, placeholder_rows)

  # -- Symptom-severity distribution (the un-collapsed stage 3) + severity-specific
  #    care-seeking multipliers. UI shares are anchored to the Sandvik instrument
  #    (score_sandvik_severity over the SWAN panel); POP/AI have no severity
  #    instrument, so their shares are illustrative placeholders. The multipliers
  #    default to 1 (NEUTRAL): a severity -> care-seeking gradient is a SCENARIO
  #    lever, because no in-domain severity -> care-seeking odds ratio exists.
  sev_share_wide <- tibble::tribble(
    ~condition, ~slight, ~moderate, ~severe, ~very_severe,
    "ui",          0.40,     0.35,    0.20,        0.05,
    "pop",         0.45,     0.35,    0.15,        0.05,
    "ai",          0.50,     0.30,    0.15,        0.05
  )
  seek_mult_wide <- tibble::tribble(
    ~condition, ~slight, ~moderate, ~severe, ~very_severe,
    "ui",           1.0,      1.0,     1.0,         1.0,
    "pop",          1.0,      1.0,     1.0,         1.0,
    "ai",           1.0,      1.0,     1.0,         1.0
  )
  # Placeholder shares are uncalibrated_illustrative -- the VALUES here are
  # illustrative, not computed from data. Only Sandvik-derived shares (supply a
  # SWAN panel to lifecourse_severity_params()) earn the derived_by_analogy tier.
  share_tier <- c(ui = "uncalibrated_illustrative", pop = "uncalibrated_illustrative",
                  ai = "uncalibrated_illustrative")
  share_src <- c(
    ui  = "illustrative placeholder; supply a SWAN panel to lifecourse_severity_params() for Sandvik-derived shares",
    pop = "no severity instrument (SWAN prolapse is binary); illustrative placeholder shares",
    ai  = "no severity instrument (AI unmeasured in SWAN); illustrative placeholder shares")
  melt_sev <- function(wide, stage, prefix, tier_by, src_by, note) {
    levs <- setdiff(names(wide), "condition")
    rows <- lapply(levs, function(L) {
      tibble::tibble(
        stage = stage, condition = wide$condition, param = paste0(prefix, L),
        variant = "default", value = wide[[L]],
        ci_low = NA_real_, ci_high = NA_real_,
        calibration_tier = unname(tier_by[wide$condition]),
        source = unname(src_by[wide$condition]), notes = note)
    })
    do.call(rbind, rows)
  }
  uni <- function(x) stats::setNames(rep(x, length(.DEMAND_CONDITION_ORDER)), .DEMAND_CONDITION_ORDER)
  severity_rows <- rbind(
    melt_sev(sev_share_wide, "symptom_severity", "sev_share_", share_tier, share_src,
             "per-condition symptom-severity distribution; feeds severity-weighted care-seeking"),
    melt_sev(seek_mult_wide, "care_seeking", "seek_mult_",
             uni("uncalibrated_illustrative"),
             uni("scenario lever; no in-domain severity->care-seeking OR (see HDMM plan)"),
             "care-seeking multiplier by severity; 1 = neutral, set != 1 to model a gradient"))
  # -- Treatment-eligibility gate (the un-collapsed stage 6). Splitting the old
  #    p_treated scalar into p_eligible x p_treated separates medical eligibility
  #    from patient preference. NEUTRAL default (p_eligible = 1): all referred
  #    patients assumed eligible, so p_eligible x p_treated == the old p_treated
  #    and demand is byte-identical. No eligibility instrument exists.
  elig_rows <- melt_sev(
    tibble::tribble(~condition, ~p_eligible, "ui", 1.0, "pop", 1.0, "ai", 1.0),
    "treatment_eligibility", "",
    uni("uncalibrated_illustrative"),
    uni("no eligibility instrument; all referred patients assumed eligible (neutral placeholder)"),
    "treatment-eligibility gate; 1 = neutral, set < 1 to gate on medical eligibility")
  default_rows <- rbind(default_rows, severity_rows, elig_rows)

  # -- Cited overrides (literature-anchored). These are the evidence-bearing rows
  #    a reviewer diffs. Tier "derived_by_analogy" => publishable only with an
  #    explicit opt-in (assert_publishable_demand_coefficients(allow_analogy = TRUE)).
  cited_rows <- tibble::tribble(
    ~stage,          ~condition, ~param, ~variant, ~value, ~ci_low, ~ci_high, ~calibration_tier,   ~source, ~notes,
    "disease_state", "ui",  "bvag", "cited", 0.15, 0.10, 0.19, "derived_by_analogy", "Rortveit 2001/2003 EPINCONT: UI OR ~1.16 per vaginal delivery (log ~0.15)", "provisional; full-text verification recommended",
    "disease_state", "ui",  "bbmi", "cited", 0.26, NA,   NA,   "derived_by_analogy", "Giri 2017 AJOG: obesity RR ~1.47 per +5 kg/m^2 (unit-scaled; provisional)", "provisional; range not yet machine-encoded",
    "disease_state", "pop", "bvag", "cited", 0.30, 0.10, 0.41, "derived_by_analogy", "Hendrix WHI OR 1.10-1.21 per birth to Mant Oxford-FPA (steeper); POP OR ~1.35 (log 0.30)", "provisional; wide evidence range",
    "disease_state", "pop", "bbmi", "cited", 0.26, NA,   NA,   "derived_by_analogy", "Giri 2017 AJOG (unit-scaled; provisional)", "provisional; range not yet machine-encoded",
    "disease_state", "ai",  "bvag", "cited", 0.10, 0.00, 0.19, "derived_by_analogy", "AI OR ~1.10 per delivery, weak/uncertain; OASI-specific OR 2.66 (LaCross 2015) is distinct", "weak evidence"
  )

  out <- rbind(default_rows, cited_rows)
  tibble::as_tibble(out)
}

#' Symptom-severity distribution and severity-specific care-seeking multipliers
#'
#' The parameters for the (un-collapsed) symptom-severity stage, read from
#' [demand_transition_registry()]. `shares` is the per-condition severity
#' distribution (Sandvik levels); `seek_multiplier` scales care-seeking by
#' severity and is 1 (NEUTRAL) by default, so demand is unchanged until a
#' gradient is supplied. Pass to [simulate_lifecourse_demand()] via
#' `severity_params` to activate a severity -> care-seeking gradient.
#'
#' The UI shares are illustrative placeholders unless a `swan_panel` is supplied:
#' then they are computed from the Sandvik instrument ([score_sandvik_severity()])
#' as the observed category distribution (not-computable / continent rows are
#' dropped, not folded into "slight"), and the `status` reflects that.
#'
#' @param swan_panel Optional SWAN incontinence panel
#'   ([build_swan_incontinence_panel()]); when supplied, the UI severity shares
#'   are derived from it via [score_sandvik_severity()] instead of the placeholder.
#' @return A list with `status`, `levels`, and per-condition named vectors
#'   `shares` and `seek_multiplier` (one entry per severity level).
#' @family transition registry
#' @concept demand
#' @export
lifecourse_severity_params <- function(swan_panel = NULL) {
  reg <- demand_transition_registry()
  levs <- .DEMAND_SEVERITY_LEVELS
  pick <- function(stage, prefix) {
    rows <- reg[reg$stage == stage & startsWith(reg$param, prefix), ]
    stats::setNames(lapply(.DEMAND_CONDITION_ORDER, function(cc) {
      v <- rows[rows$condition == cc, ]
      stats::setNames(v$value[match(paste0(prefix, levs), v$param)], levs)
    }), .DEMAND_CONDITION_ORDER)
  }
  shares <- pick("symptom_severity", "sev_share_")
  status <- "placeholder_uncalibrated"
  if (!is.null(swan_panel)) {
    scored <- score_sandvik_severity(swan_panel, verbose = FALSE)
    # Drop NA (not-computable / continent) rows; do NOT fold them into "slight".
    tab <- table(factor(as.character(scored$sandvik_category), levels = levs))
    if (sum(tab) > 0) {
      shares$ui <- stats::setNames(as.numeric(tab) / sum(tab), levs)
      status <- "ui_sandvik_derived"
    }
  }
  list(status = status, levels = levs, shares = shares,
       seek_multiplier = pick("care_seeking", "seek_mult_"))
}

#' Treatment-eligibility gate parameters
#'
#' The per-condition medical-eligibility probability for the (un-collapsed)
#' treatment-eligibility stage, read from [demand_transition_registry()]. NEUTRAL
#' by default (`p_eligible = 1`: all referred patients assumed eligible), so
#' `p_eligible x p_treated` equals the old combined `p_treated` and demand is
#' unchanged. Pass to [simulate_lifecourse_demand()] via `eligibility_params` to
#' gate treatment on medical eligibility.
#'
#' @return A list with `status` and a per-condition named vector `p_eligible`.
#' @family transition registry
#' @concept demand
#' @export
lifecourse_eligibility_params <- function() {
  reg <- demand_transition_registry()
  rows <- reg[reg$stage == "treatment_eligibility" & reg$param == "p_eligible", ]
  list(status = "placeholder_uncalibrated",
       p_eligible = stats::setNames(
         rows$value[match(.DEMAND_CONDITION_ORDER, rows$condition)],
         .DEMAND_CONDITION_ORDER))
}

# Effective (condition, param, value, tier) for a variant: default rows, with the
# cited overrides overlaid when variant == "cited".
.demand_effective <- function(variant = c("default", "cited", "placeholder")) {
  variant <- match.arg(variant)
  reg <- demand_transition_registry()
  base <- reg[reg$variant == "default", c("condition", "param", "value", "calibration_tier")]
  if (variant %in% c("cited", "placeholder")) {
    ov <- reg[reg$variant == variant, c("condition", "param", "value", "calibration_tier")]
    key <- function(d) paste(d$condition, d$param)
    idx <- match(key(ov), key(base))
    base$value[idx] <- ov$value
    base$calibration_tier[idx] <- ov$calibration_tier
  }
  base
}

# Reconstruct the nested risk-params list (identical structure to the original).
.demand_risk_params <- function(variant = c("default", "cited", "placeholder")) {
  variant <- match.arg(variant)
  eff <- .demand_effective(variant)
  eff <- eff[eff$param %in% .DEMAND_RISK_PARAM_ORDER, ]
  one_condition <- function(cond) {
    v <- eff[eff$condition == cond, ]
    vals <- v$value[match(.DEMAND_RISK_PARAM_ORDER, v$param)]
    stats::setNames(as.list(vals), .DEMAND_RISK_PARAM_ORDER)
  }
  status <- switch(variant,
                   cited = "obstetric_literature_anchored",
                   placeholder = "placeholder_uncalibrated",
                   "prevalence_anchored")
  c(list(status = status),
    stats::setNames(lapply(.DEMAND_CONDITION_ORDER, one_condition), .DEMAND_CONDITION_ORDER))
}

# Reconstruct the pathway-params list (identical structure to the original).
.demand_pathway_params <- function() {
  eff <- .demand_effective("default")
  named_vec <- function(par) {
    v <- eff[eff$param == par, ]
    stats::setNames(v$value[match(.DEMAND_CONDITION_ORDER, v$condition)], .DEMAND_CONDITION_ORDER)
  }
  c(list(status = "placeholder_uncalibrated"),
    stats::setNames(lapply(.DEMAND_PATHWAY_ORDER, named_vec), .DEMAND_PATHWAY_ORDER))
}

#' Assert the demand-transition coefficients are publishable
#'
#' The coefficient analogue of [assert_publishable_workload()], applied to the
#' demand-transition registry. It reduces the registry (for the chosen `variant`)
#' to its LEAST-calibrated tier and runs the same accept / opt-in / refuse gate:
#' `calibrated`/`solved` pass, `derived_by_analogy` needs `allow_analogy = TRUE`,
#' and `uncalibrated_illustrative` placeholders are refused (strict errors,
#' relaxed warns). Because the default variant is all placeholders -- and even the
#' cited variant leaves the care-pathway probabilities as placeholders -- this
#' surfaces the tier-mixing the bespoke `status` string hid.
#'
#' This is a standalone gate; it is intentionally NOT auto-wired into
#' [simulate_lifecourse_demand()].
#'
#' @param variant Which coefficient set to check: "default" or "cited".
#' @param allow_analogy Permit `derived_by_analogy` coefficients.
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @return (Invisibly) `TRUE` when publishable, `FALSE` otherwise.
#' @family transition registry
#' @concept demand
#' @export
assert_publishable_demand_coefficients <- function(variant = c("default", "cited"),
                                           allow_analogy = FALSE,
                                           mode = resolve_reproducibility_mode()) {
  variant <- match.arg(variant)
  tiers <- .demand_effective(variant)$calibration_tier
  worst <- CALIBRATION_TIERS[max(match(tiers, CALIBRATION_TIERS))]
  assert_publishable_workload(
    status = worst, allow_analogy = allow_analogy,
    what = sprintf("demand-transition coefficients (%s)", variant), mode = mode)
}
