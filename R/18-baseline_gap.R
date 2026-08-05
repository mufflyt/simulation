# Base-Year Supply Adequacy (the starting shortfall) ----
#
# The single most consequential correction to the previous model. Rebasing both
# supply and demand to 1.0 in the base year GUARANTEES that base-year adequacy
# equals 1.0 regardless of whether the workforce is already short. It can say
# whether supply grows faster than demand; it cannot estimate a shortage.
#
# The Dall documentation names this failure explicitly (HWMM v5.19.20, "Model
# limitations -> Supply adequacy"):
#
#   "Most workforce studies start with the assumption that at the national level
#    supply is adequate to meet demand for services unless there is clear
#    evidence of a supply shortfall or surplus. This assumption of base year
#    national equilibrium essentially presents future adequacy relative to
#    current levels... To the extent that there are current gaps between national
#    supply and demand, then such base year imbalances persist into the
#    projections of future supply adequacy."
#
# and sanctions two ways out:
#   1. a provider workload-CAPACITY SURVEY ("survey practices on their
#      'busyness'"), used by Dall 2021 (physiatry, 940 FTE / 10.6%) and
#      Zarek 2025 (physical therapy, 12,070 FTE / 5.2%);
#   2. HPSA-REMOVAL counts -- the number of additional providers HRSA estimates
#      are required to remove Health Professional Shortage Area designations.
#
# Where neither is available, Dall 2013 (neurology) simply ASSUMED a shortfall
# (10% adult, 20% child) and defended it with wait times, Medicaid non-acceptance
# and recruitment vacancies. That is also supported here, and the assumption is
# recorded as such so it can never be mistaken for a measurement.

BASELINE_GAP_METHODS <- c("capacity_survey", "hpsa_removal", "assumed", "external_anchor")

# THE METHOD IS NOT THE EVIDENCE TIER
#
# `method` names the arithmetic; it says nothing about whether the number was
# measured on urogynaecologists. Zarek's instrument applied to a URPS sample and
# Zarek's published physical-therapy distribution borrowed wholesale produce
# byte-identical output through `capacity_survey_adequacy()`, and the base-year
# shortfall is a pass-through of that number with a coefficient of one
# (test-workload-to-fte.R, "the base-year gap is a pass-through"). Recording only
# the method therefore lets a borrowed distribution be reported as a measurement.
#
# Every gap object consequently carries a `calibration_status` alongside its
# method. The vocabulary mirrors `CALIBRATION_TIERS` (R/23) with two differences:
# "solved" cannot apply -- there is no internal constraint that determines a
# base-year shortfall, which is the entire reason this module exists -- and
# "assumed_with_evidence" is added for the Dall 2013 route, an assumption
# defended by indirect indicators, which has no workload analogue.
#
#   "calibrated"                measured on urogynaecologists (a fielded URPS
#                               capacity survey, a URPS-specific external anchor)
#   "derived_by_analogy"        the structure or the level comes from another
#                               specialty (the physical-therapy stand-in)
#   "assumed_with_evidence"     assumed, with an evidence ledger (Dall 2013)
#   "uncalibrated_illustrative" assumed with nothing behind it
#
# `assert_baseline_gap_estimated()` accepts the first, requires explicit opt-in
# for the middle two, and refuses the last.
BASELINE_GAP_TIERS <- c("calibrated", "derived_by_analogy",
                        "assumed_with_evidence", "uncalibrated_illustrative")

# ---- Method 1: provider capacity survey -----------------------------------

# The four response categories of the Zarek 2025 instrument, with the adequacy
# ratio each implies. Adequacy is supply divided by demand: 1.0 means the
# respondent's capacity exactly meets the demand they face, below 1.0 means they
# are short, above 1.0 means they have spare capacity.
#
# Zarek's published example, reproduced in the tests:
#   equilibrium   n=343 (24%)  adequacy 100.0%
#   surplus       n=268 (19%)  adequacy 116.7%   (25 seen + 5 spare)
#   shortage 1    n=449 (32%)  adequacy  85.2%   (31 seen, 4 only by extra hours)
#   shortage 2    n=363 (26%)  adequacy  85.8%   (35 seen, 5 unmet)
#   weighted mean              adequacy  94.8%  ->  5.2% gap
CAPACITY_SURVEY_TEMPLATE <- tibble::tribble(
  ~category,       ~label,
  "equilibrium",   "Met all requests; could not take more without extending hours",
  "surplus",       "Met all requests; could have taken more without extending hours",
  "shortage_hours","Met all requests but only by extending work hours",
  "shortage_unmet","Could not meet all requests"
)

#' Adequacy ratio implied by one capacity-survey category
#'
#' Reproduces Zarek 2025's published arithmetic exactly. Note that each category
#' expresses the adjustment as a share of a DIFFERENT base, which is why a single
#' generic formula does not reproduce their numbers:
#'
#'   surplus         1 + additional / (seen + additional)
#'                   "the 25 reported ... plus the 5 additional for a total of
#'                    30. The additional 5 appointments make up 16.7% of this
#'                    group's total capacity"                        -> 116.7%
#'   shortage_hours  1 - additional / (seen - additional)
#'                   "the 31 median ... minus the 4 accommodated only by
#'                    extending work hours equals 28. The 4 additional
#'                    appointments make up 14.8% of total demand"    ->  85.2%
#'   shortage_unmet  1 - additional / seen
#'                   "the median 5 additional appointments divided by the median
#'                    35 appointments per week ... 14.2% of total demand"
#'                                                                   ->  85.8%
#'
#' @param category One of the `CAPACITY_SURVEY_TEMPLATE` categories.
#' @param seen Median appointments actually delivered per week.
#' @param additional Median additional appointments: spare capacity for
#'   "surplus", appointments accommodated only by extending hours for
#'   "shortage_hours", appointments that could not be accommodated for
#'   "shortage_unmet". Ignored for "equilibrium".
#' @return Numeric adequacy ratio (supply / demand).
#' @export
capacity_category_adequacy <- function(category, seen = NA_real_, additional = NA_real_) {
  category <- match.arg(category, CAPACITY_SURVEY_TEMPLATE$category)
  if (category != "equilibrium") {
    # Each formula has its own denominator, and each can vanish: `seen` of zero
    # for the surplus and unmet forms, and `seen == additional` for the
    # extended-hours form (a respondent whose entire load was overtime). An
    # unguarded divide gives Inf, which would enter the weighted mean as a
    # finite-looking adequacy.
    denom <- switch(category,
                    surplus = seen + additional,
                    shortage_hours = seen - additional,
                    shortage_unmet = seen)
    if (!is.finite(denom) || denom <= 0) {
      stop(sprintf(paste("capacity_category_adequacy: the '%s' denominator is %s.",
                         "seen = %s, additional = %s. This respondent group cannot",
                         "yield an adequacy ratio."),
                   category, format(denom), format(seen), format(additional)),
           call. = FALSE)
    }
  }
  switch(
    category,
    equilibrium    = 1.0,
    surplus        = 1 + additional / (seen + additional),
    shortage_hours = 1 - additional / (seen - additional),
    shortage_unmet = 1 - additional / seen
  )
}

#' Base-year adequacy from a provider capacity survey
#'
#' Weighted mean of the per-category adequacy ratios, weighting by the share of
#' respondents (or of FTE) in each category.
#'
#' @param responses Tibble with `category`, `n` (or `weight`), and the `seen` /
#'   `additional` medians required by [capacity_category_adequacy()].
#' @return List with `adequacy`, `gap_fraction`, per-category detail, and the
#'   method label.
#' @export
capacity_survey_adequacy <- function(responses) {
  assertthat::assert_that(is.data.frame(responses),
                          "category" %in% names(responses))
  if (!"n" %in% names(responses) && !"weight" %in% names(responses)) {
    stop("capacity_survey_adequacy: responses need an `n` or `weight` column",
         call. = FALSE)
  }
  w <- if ("weight" %in% names(responses)) responses$weight else responses$n
  if (sum(w, na.rm = TRUE) <= 0) {
    stop("capacity_survey_adequacy: response weights sum to zero", call. = FALSE)
  }

  detail <- responses
  detail$adequacy <- vapply(seq_len(nrow(responses)), function(i) {
    capacity_category_adequacy(
      responses$category[i],
      seen = if ("seen" %in% names(responses)) responses$seen[i] else NA_real_,
      additional = if ("additional" %in% names(responses)) responses$additional[i] else NA_real_
    )
  }, numeric(1))
  detail$share <- w / sum(w, na.rm = TRUE)

  adequacy <- sum(detail$adequacy * detail$share, na.rm = TRUE)

  list(
    adequacy = adequacy,
    gap_fraction = 1 - adequacy,
    detail = detail,
    method = "capacity_survey"
  )
}

#' Bottom-up base-year shortfall in FTE (physiatry route)
#'
#' Dall 2021 built the 940-FTE physiatry shortfall by multiplying the national
#' FTE in each capacity category by that category's average over/under capacity
#' and summing: 410 + 650 - 120 = 940. The negative term matters -- respondents
#' who were "not busy enough" carried -33.7% capacity and REDUCED the estimated
#' shortfall. A single scalar adequacy fraction cannot represent that offset,
#' which is why this route is offered alongside the ratio route.
#'
#' @param responses Tibble with `category`, `national_fte`, and
#'   `over_under_capacity` (positive = over capacity, negative = under).
#' @return List with `shortfall_fte`, `shortfall_pct`, detail, and the method.
#' @export
capacity_survey_shortfall_bottom_up <- function(responses) {
  assertthat::assert_that(is.data.frame(responses))
  need <- c("national_fte", "over_under_capacity")
  missing <- setdiff(need, names(responses))
  if (length(missing) > 0) {
    stop(sprintf("capacity_survey_shortfall_bottom_up: missing column(s): %s",
                 paste(missing, collapse = ", ")), call. = FALSE)
  }
  detail <- dplyr::mutate(responses,
                          shortfall_fte = .data$national_fte * .data$over_under_capacity)
  total_fte <- sum(responses$national_fte, na.rm = TRUE)
  shortfall <- sum(detail$shortfall_fte, na.rm = TRUE)

  list(
    shortfall_fte = shortfall,
    shortfall_pct = if (total_fte > 0) 100 * shortfall / total_fte else NA_real_,
    total_fte = total_fte,
    detail = detail,
    method = "capacity_survey"
  )
}

# ---- Method 2: HPSA-removal counts ----------------------------------------

#' Base-year shortfall from provider counts required to remove shortage areas
#'
#' The second route named in the HWMM limitations section. `providers_to_remove`
#' is the number of additional providers HRSA (or an equivalent designation
#' authority) estimates are needed to lift shortage-area designations.
#'
#' @param providers_to_remove Additional providers required.
#' @param base_supply_fte Base-year supplied FTE.
#' @return List with `shortfall_fte`, `adequacy`, `gap_fraction`, method.
#' @export
hpsa_removal_shortfall <- function(providers_to_remove, base_supply_fte) {
  assertthat::assert_that(is.numeric(providers_to_remove), providers_to_remove >= 0)
  assertthat::assert_that(is.numeric(base_supply_fte), base_supply_fte > 0)
  required <- base_supply_fte + providers_to_remove
  list(
    shortfall_fte = providers_to_remove,
    required_fte = required,
    adequacy = base_supply_fte / required,
    gap_fraction = providers_to_remove / required,
    method = "hpsa_removal"
  )
}

# ---- Method 3: assumed, with an evidence ledger ---------------------------

#' Assume a base-year shortfall and record the evidence supporting it
#'
#' Dall 2013 assumed 10% for adult and 20% for child neurology because no
#' capacity survey existed, and justified it with wait times (34.8 business days
#' for a new patient, up from 28.1 two years earlier), Medicaid non-acceptance,
#' and 12-month vacancies at 39% of children's hospitals. The assumption is a
#' first-class, labelled input here -- never a silent default.
#'
#' @param gap_fraction Assumed shortfall as a fraction of base-year demand.
#' @param evidence Character vector of supporting indicators.
#' @param base_supply_fte Base-year supplied FTE.
#' @return List with `required_fte`, `shortfall_fte`, `adequacy`, evidence, method.
#' @export
assumed_baseline_gap <- function(gap_fraction, evidence = character(0),
                                 base_supply_fte) {
  assertthat::assert_that(is.numeric(gap_fraction), gap_fraction > -1, gap_fraction < 1)
  assertthat::assert_that(is.numeric(base_supply_fte), base_supply_fte > 0)
  if (length(evidence) == 0) {
    .msg_warn("assumed_baseline_gap(): no supporting evidence recorded. ",
              "An assumed shortfall with no evidence ledger is not defensible.")
  }
  required <- base_supply_fte / (1 - gap_fraction)
  list(
    required_fte = required,
    shortfall_fte = required - base_supply_fte,
    adequacy = 1 - gap_fraction,
    gap_fraction = gap_fraction,
    evidence = evidence,
    method = "assumed",
    # An empty ledger is not the same claim as a defended assumption, and the
    # tier has to say which one this is.
    calibration_status = if (length(evidence)) "assumed_with_evidence"
                         else "uncalibrated_illustrative"
  )
}

# ---- Method 4: an external anchor -----------------------------------------

#' Base-year shortfall from an external published anchor
#'
#' The fourth route, and in practice the reachable one for a surgical
#' subspecialty. HRSA designates shortage areas for primary medical care, dental
#' and mental health -- not subspecialties -- so [hpsa_removal_shortfall()] has
#' no URPS input to read, which leaves a fielded capacity survey or an anchor
#' borrowed from outside this model.
#'
#' An anchor is any externally published estimate that pins the base-year level:
#' a provider-to-population benchmark, a payer network-adequacy standard, an
#' access-based required-supply estimate, or another study's URPS shortfall.
#' Supply exactly one of `adequacy` or `required_fte`; the other is derived.
#'
#' Unlike [assumed_baseline_gap()], this route demands a `citation`. An anchor is
#' external evidence, so it is attributable by construction; an unattributable
#' one is an assumption and belongs in [assumed_baseline_gap()].
#'
#' Returns a finished [baseline_gap()] object rather than a bare list, because
#' this is the only route that already carries everything the object needs --
#' the tier and the source travel with the anchor itself.
#'
#' @param base_supply_fte Base-year supplied FTE.
#' @param adequacy Base-year adequacy ratio (supply / demand) implied by the
#'   anchor. Give this or `required_fte`, not both.
#' @param required_fte Base-year required FTE implied by the anchor.
#' @param anchor Short label for what the anchor is (e.g. "ACOG/AUGS
#'   urogynaecologists per 100,000 women benchmark").
#' @param citation Attribution for the anchor. Required and non-empty.
#' @param calibration_status `"calibrated"` when the anchor was measured on
#'   urogynaecologists, `"derived_by_analogy"` when it comes from another
#'   specialty.
#' @param evidence Additional supporting indicators.
#' @param access_components_counted Access deficits already inside this anchor,
#'   passed through to [assert_access_not_double_counted()].
#' @return An object of class `urps_baseline_gap`.
#' @export
external_anchor_gap <- function(base_supply_fte,
                                adequacy = NULL,
                                required_fte = NULL,
                                anchor,
                                citation,
                                calibration_status = c("calibrated", "derived_by_analogy"),
                                evidence = character(0),
                                access_components_counted = character(0)) {
  assertthat::assert_that(is.numeric(base_supply_fte), length(base_supply_fte) == 1L,
                          base_supply_fte > 0)
  calibration_status <- match.arg(calibration_status)

  if (missing(anchor) || !is.character(anchor) || !nzchar(paste(anchor, collapse = ""))) {
    stop("external_anchor_gap: `anchor` must name what the anchor is.", call. = FALSE)
  }
  if (missing(citation) || !is.character(citation) || !nzchar(paste(citation, collapse = ""))) {
    stop(paste("external_anchor_gap: `citation` is required. An anchor with no",
               "attribution is an assumption -- use assumed_baseline_gap() with",
               "an evidence ledger instead."), call. = FALSE)
  }

  given <- c(!is.null(adequacy), !is.null(required_fte))
  if (sum(given) != 1L) {
    stop("external_anchor_gap: supply exactly one of `adequacy` or `required_fte`.",
         call. = FALSE)
  }
  if (is.null(adequacy)) {
    assertthat::assert_that(is.numeric(required_fte), length(required_fte) == 1L,
                            required_fte > 0)
    adequacy <- base_supply_fte / required_fte
  }
  assertthat::assert_that(is.numeric(adequacy), length(adequacy) == 1L, adequacy > 0)

  baseline_gap(
    base_supply_fte = base_supply_fte,
    adequacy = adequacy,
    method = "external_anchor",
    evidence = c(anchor, evidence),
    access_components_counted = access_components_counted,
    calibration_status = calibration_status,
    source = paste(citation, collapse = "; ")
  )
}

# ---- Assembling the base-year demand anchor -------------------------------

#' Base-year required FTE from supplied FTE and an adequacy estimate
#'
#' required = supply / adequacy. With adequacy 0.948 (Zarek) this is the same as
#' `supply / (1 - 0.052)`.
#'
#' @param base_supply_fte Base-year supplied FTE.
#' @param adequacy Base-year adequacy ratio (supply / demand).
#' @return Numeric required FTE.
#' @export
required_fte_base_year <- function(base_supply_fte, adequacy) {
  assertthat::assert_that(is.numeric(adequacy), adequacy > 0)
  if (isTRUE(all.equal(adequacy, 1))) {
    .msg_warn("required_fte_base_year(): adequacy is exactly 1.0. ",
              "That is the base-year-equilibrium assumption the Dall documentation ",
              "warns against; confirm it was measured, not assumed.")
  }
  base_supply_fte / adequacy
}

#' Build a labelled base-year gap object
#'
#' Central constructor. Carries the method, the adequacy, the FTE anchor, and an
#' `access_components_counted` field used downstream to prevent an access-equity
#' demand scenario from double-counting a deficit already inside the base-year
#' gap.
#'
#' @param base_supply_fte Base-year supplied FTE.
#' @param adequacy Base-year adequacy ratio.
#' @param method One of `BASELINE_GAP_METHODS`. Names the arithmetic only.
#' @param evidence Character vector of supporting indicators.
#' @param access_components_counted Character vector naming access deficits
#'   already embedded in this gap (e.g. "nonmetro", "uninsured", "racial_equity").
#' @param calibration_status One of `BASELINE_GAP_TIERS`: whether this number was
#'   measured on urogynaecologists, borrowed from another specialty, or assumed.
#'   Inferred only for `method = "assumed"` (from whether an evidence ledger is
#'   present); for every other method it must be declared, because the method
#'   cannot distinguish a fielded URPS survey from a borrowed distribution.
#' @param source Attribution for the number (the study, survey or anchor).
#' @return An object of class `urps_baseline_gap`.
#' @export
baseline_gap <- function(base_supply_fte, adequacy,
                         method = c("capacity_survey", "hpsa_removal", "assumed", "external_anchor"),
                         evidence = character(0),
                         access_components_counted = character(0),
                         calibration_status = NULL,
                         source = NA_character_) {
  method <- match.arg(method)
  calibration_status <- .resolve_baseline_gap_tier(calibration_status, method, evidence)
  required <- required_fte_base_year(base_supply_fte, adequacy)
  structure(
    list(
      base_supply_fte = base_supply_fte,
      required_fte = required,
      shortfall_fte = required - base_supply_fte,
      shortfall_pct = 100 * (required - base_supply_fte) / required,
      adequacy = adequacy,
      method = method,
      calibration_status = calibration_status,
      source = source,
      evidence = evidence,
      access_components_counted = access_components_counted
    ),
    class = "urps_baseline_gap"
  )
}

# Fail closed on an undeclared tier rather than guessing one.
#
# Only the "assumed" route is self-describing: an evidence ledger is either
# present or it is not. For the other three, identical arithmetic can carry
# either tier, so silence has to be an error -- a default of "calibrated" would
# reintroduce exactly the mislabelling this field exists to prevent, and a
# default of "derived_by_analogy" would understate a survey that was genuinely
# fielded.
.resolve_baseline_gap_tier <- function(status, method, evidence) {
  if (!is.null(status)) {
    if (length(status) != 1L || !status %in% BASELINE_GAP_TIERS) {
      stop(sprintf("baseline_gap: unknown calibration_status '%s'. Expected one of: %s",
                   paste(status, collapse = ", "),
                   paste(BASELINE_GAP_TIERS, collapse = ", ")), call. = FALSE)
    }
    return(status)
  }
  if (identical(method, "assumed")) {
    return(if (length(evidence)) "assumed_with_evidence" else "uncalibrated_illustrative")
  }
  stop(sprintf(paste(
    "baseline_gap: `calibration_status` must be declared for method '%s'.",
    "It cannot be inferred. A capacity survey fielded in urogynaecology is",
    "'calibrated'; the same instrument's published distribution borrowed from",
    "another specialty is 'derived_by_analogy'; the two produce identical",
    "arithmetic, and the base-year shortfall is a pass-through of the result",
    "with a coefficient of one. Declare one of: %s."),
    method, paste(BASELINE_GAP_TIERS, collapse = ", ")), call. = FALSE)
}

#' @export
print.urps_baseline_gap <- function(x, ...) {
  tier <- x$calibration_status %||% NA_character_
  cat(sprintf("Base-year supply adequacy (%s, %s)\n", x$method, tier))
  cat(sprintf("  supplied FTE   %s\n", format(round(x$base_supply_fte, 1), big.mark = ",")))
  cat(sprintf("  required FTE   %s\n", format(round(x$required_fte, 1), big.mark = ",")))
  cat(sprintf("  shortfall      %s FTE (%.1f%% of demand)\n",
              format(round(x$shortfall_fte, 1), big.mark = ","), x$shortfall_pct))
  if (!identical(tier, "calibrated")) {
    cat("  NOT MEASURED ON UROGYNAECOLOGISTS: this shortfall passes through to the\n")
    cat("  headline gap with a coefficient of one.\n")
  }
  if (!is.na(x$source %||% NA_character_)) cat("  source:        ", x$source, "\n")
  if (length(x$evidence)) cat("  evidence:      ", paste(x$evidence, collapse = "; "), "\n")
  if (length(x$access_components_counted)) {
    cat("  access already counted: ", paste(x$access_components_counted, collapse = ", "), "\n")
  }
  invisible(x)
}

#' Refuse a base-year gap that was never estimated
#'
#' Regression guard for the whole point of this module: a projection may not be
#' published on an assumption of base-year equilibrium.
#'
#' The tier is checked as well as the presence. A gap estimate that exists but
#' was borrowed from another specialty is a declared modelling choice, not a
#' measurement, and it reaches the headline number undamped -- so it needs the
#' same explicit opt-in [assert_publishable_workload()] requires of an
#' analogy-derived delegation matrix.
#'
#' @param gap A [baseline_gap()] object, or NULL.
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @param allow_analogy Permit a `derived_by_analogy` or `assumed_with_evidence`
#'   base-year gap. `uncalibrated_illustrative` is refused regardless.
#' @return (Invisibly) TRUE when a genuine gap estimate is present.
#' @export
assert_baseline_gap_estimated <- function(gap, mode = resolve_reproducibility_mode(),
                                          allow_analogy = FALSE) {
  refuse <- function(msg) {
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
    invisible(FALSE)
  }

  msg <- paste(
    "No base-year supply-adequacy estimate: the projection would assume base-year",
    "equilibrium, which forces the starting shortfall to zero and can only report",
    "growth relative to current levels. Estimate it with capacity_survey_adequacy(),",
    "hpsa_removal_shortfall(), external_anchor_gap(), or assumed_baseline_gap()",
    "with an evidence ledger."
  )
  if (!inherits(gap, "urps_baseline_gap") || isTRUE(all.equal(gap$adequacy, 1))) {
    return(refuse(msg))
  }

  tier <- gap$calibration_status %||% NA_character_
  if (identical(tier, "calibrated")) return(invisible(TRUE))

  if (identical(tier, "uncalibrated_illustrative")) {
    return(refuse(paste(
      "The base-year gap is 'uncalibrated_illustrative': a shortfall was assumed",
      "with no evidence ledger behind it. It becomes the headline shortage with a",
      "coefficient of one, so no published number may rest on it. Field a URPS",
      "capacity survey, cite an anchor via external_anchor_gap(), or record the",
      "indicators supporting the assumption in assumed_baseline_gap(evidence = ).")))
  }

  what <- if (identical(tier, "derived_by_analogy")) {
    "borrowed from another specialty"
  } else {
    "assumed, defended only by indirect indicators"
  }
  analogy_msg <- sprintf(paste(
    "The base-year gap (%s, %.1f%% shortfall) is '%s' -- %s, not measured on",
    "urogynaecologists. It passes through to the headline shortage with a",
    "coefficient of one. Report it as an assumption, or pass allow_analogy = TRUE",
    "to proceed knowingly."),
    gap$method, gap$shortfall_pct, tier, what)

  if (isTRUE(allow_analogy)) {
    .msg_info(sprintf("Proceeding with a '%s' base-year gap (declared).", tier))
    return(invisible(TRUE))
  }
  refuse(analogy_msg)
}

# ---- Double-counting guard for access scenarios ---------------------------

#' Guard against double-counting an access deficit
#'
#' Dall 2013 flagged this precisely: an extra 460 FTE would be needed if
#' non-metropolitan utilisation matched metropolitan utilisation, "but this
#' amount is part of the assumed current shortfall". If a base-year gap was
#' derived from access indicators (E2SFCA zero-access shares, rural deficits) and
#' a reduced-barriers demand scenario is then added on top, the same deficit is
#' booked twice.
#'
#' @param gap A [baseline_gap()] object.
#' @param scenario_components Character vector of access components the demand
#'   scenario increases (e.g. "nonmetro", "uninsured", "racial_equity").
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @return (Invisibly) the overlapping components (empty when safe).
#' @export
assert_access_not_double_counted <- function(gap, scenario_components,
                                             mode = resolve_reproducibility_mode()) {
  counted <- if (inherits(gap, "urps_baseline_gap")) gap$access_components_counted else character(0)
  overlap <- intersect(counted, scenario_components)
  if (length(overlap) > 0) {
    msg <- sprintf(
      paste("Access double-count: component(s) %s are already inside the base-year gap",
            "and would be added again by this demand scenario. Either remove them from",
            "the base-year gap or exclude them from the scenario (Dall 2013, Neurology:",
            "the +460 FTE non-metropolitan equity effect 'is part of the assumed current",
            "shortfall')."),
      paste(overlap, collapse = ", ")
    )
    if (identical(mode, "strict")) stop(msg, call. = FALSE)
    .msg_warn(msg)
  }
  invisible(overlap)
}

# ---- Published reference values (for tests and sanity checks) -------------

#' Published base-year adequacy estimates from the Dall-family literature
#' @return Tibble of reference values.
#' @export
published_baseline_gaps <- function() {
  tibble::tribble(
    ~study,                      ~occupation,          ~base_year, ~supply_fte, ~shortfall_fte, ~shortfall_pct, ~method,
    "Dall 2021 AJPMR",           "Physiatrist",              2017L,        8853,            940,           10.6, "capacity_survey",
    "Zarek 2025 PTJ",            "Physical therapist",       2022L,      233890,          12070,            5.2, "capacity_survey",
    "Dall 2013 Neurology",       "Neurologist (adult)",      2012L,       16366,           1814,           11.0, "assumed"
  )
}
