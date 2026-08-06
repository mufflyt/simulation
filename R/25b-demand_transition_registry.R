# Demand-transition registry ----
#
# One diffable, independently reviewable table of every coefficient/probability
# that drives a demand TRANSITION in the life-course pathway (R/25). Each row
# carries the value, a CALIBRATION TIER drawn from the package's canonical
# `CALIBRATION_TIERS` vocabulary (R/23), and a citation -- so a reviewer can diff
# "what coefficient changed, at what tier, on what evidence" in a single place,
# without reading model code.
#
# WHY THIS EXISTS (see the demand-model audit): the transition coefficients used
# a bespoke `status = "placeholder_uncalibrated"` string that is NOT a member of
# CALIBRATION_TIERS and was checked by no assertion, so placeholder probabilities
# could reach reported FTE with no publication refusal -- unlike the workload
# basket (R/17) and the R/51 utilisation cascade, which ARE gated.
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
#' @export
demand_transition_registry <- function() {
  # -- Disease-state log-odds (placeholder set). Compact wide block mirrors the
  #    original coefficient set exactly; melted to one row per (condition, param).
  risk_wide <- tibble::tribble(
    ~condition,  ~b0,  ~bvag, ~bage, ~bysl, ~bbmi, ~bhyst, ~bmeno, ~bcomorb,
    "ui",      -1.60,  0.18,  0.35,  0.05,  0.12,   0.10,   0.25,     0.15,
    "pop",     -2.40,  0.42,  0.30,  0.08,  0.08,   0.45,   0.20,     0.05,
    "ai",      -2.90,  0.22,  0.25,  0.04,  0.06,   0.05,   0.10,     0.20
  )
  # -- Care-pathway probabilities (placeholder set).
  path_wide <- tibble::tribble(
    ~condition, ~recognition, ~p_seek, ~p_referral, ~p_treated,
    "ui",             0.55,     0.45,       0.40,       0.70,
    "pop",            0.60,     0.50,       0.55,       0.65,
    "ai",             0.35,     0.30,       0.45,       0.60
  )

  melt <- function(wide, note_of) {
    params <- setdiff(names(wide), "condition")
    rows <- lapply(params, function(p) {
      tibble::tibble(
        stage = .demand_stage_of(p), condition = wide$condition, param = p,
        variant = "default", value = wide[[p]],
        ci_low = NA_real_, ci_high = NA_real_,
        calibration_tier = "uncalibrated_illustrative",
        source = "placeholder (expert judgement; not evidence-anchored)",
        notes = note_of(p))
    })
    do.call(rbind, rows)
  }
  risk_note <- function(p) if (p == "bvag") "primary exposure term (cited override available)" else ""
  path_note <- function(p) switch(p,
    p_treated = "conflates treatment_eligibility + treatment_preference (stages not yet separated)",
    recognition = "symptom recognition; severity gradient not yet modelled",
    "")

  default_rows <- rbind(melt(risk_wide, risk_note), melt(path_wide, path_note))

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

# Effective (condition, param, value, tier) for a variant: default rows, with the
# cited overrides overlaid when variant == "cited".
.demand_effective <- function(variant = c("default", "cited")) {
  variant <- match.arg(variant)
  reg <- demand_transition_registry()
  base <- reg[reg$variant == "default", c("condition", "param", "value", "calibration_tier")]
  if (variant == "cited") {
    ov <- reg[reg$variant == "cited", c("condition", "param", "value", "calibration_tier")]
    key <- function(d) paste(d$condition, d$param)
    idx <- match(key(ov), key(base))
    base$value[idx] <- ov$value
    base$calibration_tier[idx] <- ov$calibration_tier
  }
  base
}

# Reconstruct the nested risk-params list (identical structure to the original).
.demand_risk_params <- function(variant = c("default", "cited")) {
  variant <- match.arg(variant)
  eff <- .demand_effective(variant)
  eff <- eff[eff$param %in% .DEMAND_RISK_PARAM_ORDER, ]
  one_condition <- function(cond) {
    v <- eff[eff$condition == cond, ]
    vals <- v$value[match(.DEMAND_RISK_PARAM_ORDER, v$param)]
    stats::setNames(as.list(vals), .DEMAND_RISK_PARAM_ORDER)
  }
  status <- if (variant == "cited") "obstetric_literature_anchored" else "placeholder_uncalibrated"
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
