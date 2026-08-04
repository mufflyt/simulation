# SWAN -> DMDM person-year panel ---------------------------------------------
#
# The missing link between the SWAN data and the hazard fitter.
#
# build_swan_incontinence_panel() (R/42) harmonises the incontinence ITEMS, and
# dmdm_transition_data() (R/31) consumes a person-year panel of STATES plus
# covariates. Nothing joined the two: R/42 returns swan_id/visit/leakage_ever/
# severity, while R/31 requires person_id, year, and seven covariates. This
# module does the covariate reshape and the join.
#
# WHAT SWAN CAN AND CANNOT MEASURE
#
# Verified against swan_all_visits.rds (16,142 x 9,159) rather than assumed from
# variable names, because the names mislead:
#
#  * UI is available at EVERY visit 0-10. The gate is `INVOLEA` at visits 0-6 and
#    10 but `LEKINVO` at visits 7-9. The pre-built swan_ui_modeling.csv covers
#    only 0-6 and 10, so it silently drops three visits by missing that rename;
#    reading the items directly recovers them.
#  * POP is a BINARY self-report (`PROLAPS`, "(1) No"/"(2) Yes"), present at
#    visits 2-10 but empty at 6, 8 and 9. It is NOT POP-Q staging, so it supports
#    crude onset/remission only -- never the graded per-stage hazards that
#    fit_dmdm_transitions(stage_conditions = "pop") expects.
#  * AI is NOT measurable. The only bowel item is `BOWEL0`, baseline-only, with
#    no follow-up. Requesting "ai" is an error rather than a column of NA.
#
# THE PRIMARY EXPOSURE IS NOT IN SWAN
#
# The DMDM's primary exposure is `cumulative_vaginal_deliveries`. SWAN carries
# `NUMCHILD` (total parity, 0-5+, asked once) and no delivery mode at all. Note
# that `VAGINDR*` is NOT vaginal delivery: its levels are
# "(1) Not At All / (2) 1-5 Days / ... / (5) Every Day", identical to `HOTFLAS*`,
# because it is vaginal DRYNESS from the menopause symptom battery. Wiring it in
# on the strength of its name would have populated the model's primary exposure
# with a hot-flash-adjacent symptom score that never errors.
#
# So total parity stands in for vaginal parity. That is a MODELLING CHOICE, not a
# cleaning step: it assumes cesarean deliveries carry the same pelvic-floor risk
# as vaginal ones, which the literature this model is built on explicitly denies.
# The substitution is recorded in the provenance attribute and re-stated by
# swan_panel_fit_caveats() so it cannot be lost between here and a fitted object.
#
# `years_since_last_vaginal_birth` is not derivable at all (no birth dates). It
# is emitted as a constant so the panel satisfies R/31's column contract; a
# zero-variance predictor makes glm() return NA for that slope, which
# .fit_onset_coefs() maps to 0. The fitted coefficient is therefore structurally
# zero and means "unmeasured", not "no effect".

#' Visit-to-variable crosswalk for the SWAN covariates
#'
#' Verified column-by-column against `swan_all_visits.rds`. Two irregularities
#' are load-bearing: hysterectomy is unsuffixed `HYSTERE` at visit 0 (every other
#' visit suffixes it), and the UI gate changes stem at visits 7-9.
#'
#' @param visit Integer visit number.
#' @return Named list of the SWAN column names for that visit.
#' @noRd
swan_covariate_columns <- function(visit) {
  suffix <- as.character(visit)
  list(
    age          = paste0("AGE", suffix),
    bmi          = paste0("BMI", suffix),
    # Visit 0 predates the suffixing convention for this item only.
    hysterectomy = if (visit == 0L) "HYSTERE" else paste0("HYSTERE", suffix),
    menopause    = paste0("STATUS", suffix),
    ui           = if (visit %in% 7:9) paste0("LEKINVO", suffix)
                   else paste0("INVOLEA", suffix),
    pop          = if (visit >= 2L) paste0("PROLAPS", suffix) else NA_character_
  )
}

# SWAN STATUS codes. 1 = post by BSO and 2 = natural post are post-menopausal;
# 3 (late peri), 4 (early peri) and 5 (pre) are not. 7 and 8 are "unknown due to
# HT use / hysterectomy" and become NA rather than being folded into "not post",
# which would misclassify women whose status is genuinely unknown as pre.
SWAN_POSTMENOPAUSAL_CODES <- c(1L, 2L)
SWAN_MENOPAUSE_UNKNOWN_CODES <- c(7L, 8L)

# Baseline comorbidity items. SWAN suffixes only some of these at some visits
# (CANCER2 and DIABETE2 exist; ARTHRIT2 and HEART2 do not), so a per-visit
# comorbidity index would silently change definition between visits. Baseline is
# used for every visit and the panel is explicit that the term is time-invariant.
SWAN_COMORBIDITY_ITEMS <- c("HIGH_BP", "DIABETE", "HEART",
                            "ARTHRIT", "OSTEOPR", "CANCER")

#' Build the SWAN person-year panel that the DMDM hazard fitter consumes
#'
#' Reshapes the wide SWAN frame into one row per participant-visit carrying the
#' exact columns [dmdm_transition_data()] requires, so a fitted transition set
#' can replace the placeholder coefficients in [dmdm_default_transitions()].
#'
#' `year` is the SWAN visit number, not a calendar year. SWAN visits are nominally
#' annual and [dmdm_transition_data()] keeps only pairs where
#' `next_year == year + 1`, so using the visit index makes consecutive visits
#' exactly one step apart and lets the fitter drop real gaps (the 6-to-10 jump in
#' any visit set that omits 7-9) on its own rather than silently treating a
#' four-year interval as a one-year transition.
#'
#' @param swan_wide A wide SWAN data frame, one row per participant, with
#'   visit-suffixed columns. `swan_all_visits.rds` is the intended input.
#' @param visits Integer visit numbers to include. Defaults to 0:10, the full
#'   span over which UI is measurable.
#' @param conditions Conditions to emit state columns for. Defaults to `"ui"`
#'   alone. `"pop"` is available but warns: fitting it on this data produces a
#'   degenerate model (see the module header), so it is opt-in. `"ai"` is an
#'   error because SWAN does not follow it.
#' @param participant_id_column Column naming the participant key.
#' @param verbose Narrate the reshape and the resulting panel shape.
#' @return A tibble with `person_id`, `year`, the seven DMDM covariates, and
#'   `has_ui` (plus `has_pop` when requested). Carries a `swan_dmdm_provenance`
#'   attribute recording which covariates are measured, proxied or unmeasured.
#' @seealso [swan_panel_fit_caveats()], [dmdm_transition_data()]
#' @export
build_swan_dmdm_panel <- function(swan_wide,
                                  visits = 0:10,
                                  conditions = "ui",
                                  participant_id_column = "SWANID",
                                  verbose = TRUE) {
  stopifnot(is.data.frame(swan_wide), length(visits) > 0)
  if ("ai" %in% conditions) {
    stop("SWAN does not follow anal incontinence: the only bowel item is ",
         "BOWEL0, baseline-only. Fit AI from another cohort.", call. = FALSE)
  }
  conditions <- match.arg(conditions, c("ui", "pop"), several.ok = TRUE)
  if ("pop" %in% conditions) {
    # Measured on the real data, not anticipated: hysterectomy separates the POP
    # onset model almost completely (1 event in 164 at-risk person-years against
    # 150 in 17,241), which drives the hysterectomy coefficient to about -12.8 --
    # a separation artefact, not an effect size. Remission is worse: 176 of 282
    # prevalent visits report resolution, a 62%/year "remission" that reflects
    # self-report instability rather than prolapse resolving.
    .msg_warn(paste(
      "conditions includes 'pop': SWAN's PROLAPS item is a binary self-report,",
      "and fitting it here yields a degenerate model (hysterectomy separates the",
      "onset fit; apparent remission is ~62%/year). Use the cited literature POP",
      "transitions (dmdm_transitions_with_pop_literature()) unless you are",
      "deliberately inspecting the SWAN POP data."))
  }
  if (!participant_id_column %in% names(swan_wide)) {
    stop("Participant id column '", participant_id_column, "' not found.",
         call. = FALSE)
  }
  visits <- sort(unique(as.integer(visits)))

  if (!"NUMCHILD" %in% names(swan_wide)) {
    stop("NUMCHILD is absent, so even the parity proxy is unavailable.",
         call. = FALSE)
  }
  parity <- swan_label_to_integer(swan_wide[["NUMCHILD"]])

  # Comorbidity: any baseline condition present. Items absent from the frame are
  # skipped rather than treated as absent-in-the-patient, and the count of items
  # actually used is reported, because "0 of 6 items found" and "no comorbidity"
  # produce the same column of zeros otherwise.
  comorbidity_items <- intersect(SWAN_COMORBIDITY_ITEMS, names(swan_wide))
  comorbidity <- if (length(comorbidity_items)) {
    flags <- vapply(comorbidity_items,
                    function(nm) isTRUE_vec(swan_yes_no_to_logical(swan_wide[[nm]])),
                    logical(nrow(swan_wide)))
    as.integer(apply(flags, 1L, any))
  } else {
    rep(NA_integer_, nrow(swan_wide))
  }

  if (isTRUE(verbose)) {
    message("build_swan_dmdm_panel(): ", nrow(swan_wide), " participants, visits ",
            paste(range(visits), collapse = "-"), "; comorbidity from ",
            length(comorbidity_items), " of ", length(SWAN_COMORBIDITY_ITEMS),
            " baseline items")
  }

  per_visit <- lapply(visits, function(v) {
    cols <- swan_covariate_columns(v)
    present <- function(nm) !is.na(nm) && nm %in% names(swan_wide)
    # A visit missing its UI gate contributes nothing the fitter can use.
    if (!present(cols$ui)) {
      if (isTRUE(verbose)) message("  visit ", v, ": no UI item (", cols$ui, "); skipped")
      return(NULL)
    }
    pull_num <- function(nm) if (present(nm)) as.numeric(as.character(swan_wide[[nm]])) else NA_real_
    meno_code <- if (present(cols$menopause)) swan_label_to_integer(swan_wide[[cols$menopause]]) else NA_integer_
    meno <- ifelse(meno_code %in% SWAN_MENOPAUSE_UNKNOWN_CODES, NA_integer_,
                   as.integer(meno_code %in% SWAN_POSTMENOPAUSAL_CODES))

    out <- tibble::tibble(
      person_id = swan_wide[[participant_id_column]],
      year      = v,
      age       = pull_num(cols$age),
      # Time-invariant proxy; see the module header.
      cumulative_vaginal_deliveries = parity,
      # Not derivable from SWAN. Constant by construction.
      years_since_last_vaginal_birth = 0,
      bmi              = pull_num(cols$bmi),
      hysterectomy     = if (present(cols$hysterectomy))
                           as.integer(swan_yes_no_to_logical(swan_wide[[cols$hysterectomy]])) else NA_integer_,
      menopause_status = meno,
      comorbidity      = comorbidity,
      has_ui           = as.integer(swan_yes_no_to_logical(swan_wide[[cols$ui]]))
    )
    if ("pop" %in% conditions) {
      out$has_pop <- if (present(cols$pop))
        as.integer(swan_yes_no_to_logical(swan_wide[[cols$pop]])) else NA_integer_
    }
    out
  })

  panel <- dplyr::bind_rows(Filter(Negate(is.null), per_visit))
  # A participant-visit with no state observation is not a person-year at risk;
  # dmdm_transition_data() would drop it anyway, and carrying it inflates every
  # row count reported to the user. Build the keep vector with an explicit
  # branch: `&`/`|` are vectorised rather than short-circuiting, so referencing
  # an absent has_pop yields logical(0) and silently empties the subscript.
  keep <- !is.na(panel$has_ui)
  if ("has_pop" %in% names(panel)) keep <- keep | !is.na(panel$has_pop)
  panel <- panel[keep, , drop = FALSE]
  panel <- panel[order(panel$person_id, panel$year), , drop = FALSE]

  attr(panel, "swan_dmdm_provenance") <- list(
    visits_used = visits,
    n_participants = length(unique(panel$person_id)),
    # Carried forward from load_swan_archive() when the input came through it, so
    # a fitted transition set can be traced to specific bytes on a specific disk.
    # NULL when the caller read the .rds themselves.
    archive = attr(swan_wide, "swan_archive_provenance"),
    comorbidity_items = comorbidity_items,
    measured = c("age", "bmi", "hysterectomy", "menopause_status", "comorbidity",
                 "has_ui", if ("pop" %in% conditions) "has_pop"),
    proxied = c(cumulative_vaginal_deliveries =
                  "NUMCHILD total parity; SWAN records no delivery mode"),
    unmeasured = c(years_since_last_vaginal_birth =
                     "no birth dates in SWAN; emitted constant, fitted slope is structurally 0"),
    pop_basis = if ("pop" %in% conditions)
      "PROLAPS binary self-report, visits 2-10 (empty at 6, 8, 9); NOT POP-Q staging" else NULL
  )

  if (isTRUE(verbose)) {
    message("  panel: ", format(nrow(panel), big.mark = ","), " participant-visits, ",
            length(unique(panel$person_id)), " participants")
  }
  panel
}

# apply() over a one-column matrix drops to a vector, and vapply insists on a
# consistent length, so wrap the logical coercion to guarantee a full-length
# column even when a comorbidity item is entirely NA.
#' @noRd
isTRUE_vec <- function(x) {
  out <- as.logical(x)
  out[is.na(out)] <- FALSE
  out
}

#' Caveats that must travel with anything fitted from a SWAN panel
#'
#' The provenance attribute is easy to drop: subsetting a tibble discards it, and
#' the fitted object returned by [fit_dmdm_transitions()] carries coefficients,
#' not their pedigree. This returns the caveats as text so they can be printed
#' beside a fit, pasted into a methods note, or attached to a transitions object
#' before it reaches [export_dmdm_demand_contract()].
#'
#' @param panel A panel from [build_swan_dmdm_panel()].
#' @return A character vector of caveats, one per line.
#' @export
swan_panel_fit_caveats <- function(panel) {
  prov <- attr(panel, "swan_dmdm_provenance")
  if (is.null(prov)) {
    return("No swan_dmdm_provenance attribute: this panel did not come from build_swan_dmdm_panel().")
  }
  c(
    sprintf("SWAN panel: %s participants, visits %s.",
            prov$n_participants, paste(range(prov$visits_used), collapse = "-")),
    # Name the bytes when they are known. An unverified or absent archive record
    # is stated rather than omitted: "we did not check" and "it checked out" must
    # not look the same in a methods note.
    if (is.null(prov$archive)) {
      "SOURCE: archive provenance not recorded (input did not come through load_swan_archive())."
    } else if (isTRUE(prov$archive$verified)) {
      sprintf("SOURCE: %s, sha256 %s, verified against %s.",
              prov$archive$file, substr(prov$archive$sha256, 1, 16),
              prov$archive$reference_source)
    } else {
      sprintf("SOURCE: %s, NOT verified against a reference checksum.", prov$archive$file)
    },
    "PROXY: cumulative_vaginal_deliveries is TOTAL parity (NUMCHILD). SWAN records no delivery mode, so the fitted `avag` coefficient assumes cesarean and vaginal delivery carry equal pelvic-floor risk.",
    "UNMEASURED: years_since_last_vaginal_birth is constant; its fitted slope (`absl`) is structurally 0 and means 'not measured', not 'no effect'.",
    if (!is.null(prov$pop_basis)) paste0("POP: ", prov$pop_basis, ". Crude onset/remission only -- do not read as graded POP-Q hazards."),
    "AI: not fitted from SWAN under any option.",
    sprintf("Comorbidity: baseline-only, time-invariant, from %d item(s): %s.",
            length(prov$comorbidity_items), paste(prov$comorbidity_items, collapse = ", "))
  )
}
