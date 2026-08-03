# Literature-derived POP transition parameters (onset + staged progression) ----
#
# WHY THIS EXISTS
# The DMDM (R/29-R/31) treats every condition as a two-state (present/absent)
# process with placeholder onset log-odds and a single remission probability.
# That is wrong for pelvic organ prolapse (POP) in two ways the incontinence
# literature does not share:
#
#   1. POP is GRADED (POP-Q stage 0-4), not merely present/absent, and care
#      demand scales with stage (a stage-1 cystocele rarely reaches a surgeon; a
#      stage-3 prolapse often does).
#   2. POP REGRESSES. Longitudinal POP-Q cohorts (WHI: Handa 2004, Bradley 2007)
#      show mild prolapse resolving spontaneously at a high annual rate, unlike
#      urinary incontinence, which is far more persistent once established.
#
# Until a longitudinal POP panel is fitted (via R/31), this module supplies a
# CITED, literature-derived POP transition set so the DMDM can model prolapse
# with defensible provenance rather than the bare placeholder row. The numbers
# are drawn by analogy from the studies below and carry
# `calibration_status = "derived_by_analogy"` -- one notch above
# "placeholder_uncalibrated", still below "fitted"/"calibrated".
#
# Data (cited): inst/extdata/pop/pop_transition_parameters.csv
#   onset       : baseline annual onset for a reference woman + covariate ORs
#                 (cumulative vaginal deliveries PRIMARY), from SWEPOP (Gyhagen
#                 2013), WHI (Hendrix 2002), Mant 1997, MOAD (Blomquist 2018).
#   progression : annual P(stage k -> k+1), from WHI (Handa 2004; Bradley 2007).
#   regression  : annual P(stage k -> k-1), from WHI (Handa 2004; Bradley 2007).
#   remission   : binary present -> absent collapse of the staged regression, for
#                 the two-state engine.

.pop_transition_extdata <- function(file = "pop_transition_parameters.csv") {
  p <- system.file("extdata", "pop", file, package = "urpssim")
  if (!nzchar(p)) p <- file.path("inst/extdata/pop", file)   # dev (load_all)
  if (!file.exists(p))
    stop(sprintf("POP transition data not found: %s", file), call. = FALSE)
  utils::read.csv(p, stringsAsFactors = FALSE)
}

#' Cited POP transition parameters (onset + staged progression/regression)
#'
#' Reads the literature-derived POP transition table shipped with the package.
#' Each row is a single parameter with an effect measure, value, confidence and
#' source, so the provenance travels with the number.
#'
#' @return A data frame with columns `transition` (onset/progression/regression/
#'   remission), `term`, `stage_from`, `stage_to`, `measure` (annual_prob or OR),
#'   `value`, `ci_low`, `ci_high`, `confidence`, `source`, `notes`.
#' @seealso [dmdm_transitions_with_pop_literature()] to compile these into a
#'   transition object the DMDM engines can run.
#' @export
pop_transition_parameters <- function() {
  .pop_transition_extdata()
}

# Compile the `onset` rows into the engine's log-odds coefficient vector
# c(a0, avag, aage, absl, abmi, ahyst, ameno, acom). The baseline annual onset
# probability becomes the intercept (a0 = qlogis(p)); the covariate ORs become
# their log-odds. Term names in the CSV are mapped to engine positions here.
.pop_onset_coefs <- function(params = pop_transition_parameters()) {
  onset <- params[params$transition == "onset", , drop = FALSE]
  get_val <- function(term) {
    row <- onset[onset$term == term, , drop = FALSE]
    if (nrow(row) != 1L)
      stop(sprintf("POP onset term '%s' missing or duplicated in parameter table", term),
           call. = FALSE)
    as.numeric(row$value)
  }
  base_p <- get_val("baseline_annual")
  if (!(base_p > 0 && base_p < 1))
    stop("POP baseline_annual onset must be a probability in (0, 1)", call. = FALSE)
  lor <- function(term) log(get_val(term))   # OR -> log-odds
  c(a0    = stats::qlogis(base_p),
    avag  = lor("per_vaginal_delivery"),
    aage  = lor("per_decade_age"),
    absl  = lor("years_since_last_vaginal_birth_per_decade"),
    abmi  = lor("per_bmi_5units"),
    ahyst = lor("hysterectomy"),
    ameno = lor("postmenopausal"),
    acom  = lor("comorbidity"))
}

# Compile the staged progression/regression rows into named annual-probability
# vectors indexed by the originating stage (as a character key, e.g. "1", "2").
.pop_stage_probs <- function(kind, params = pop_transition_parameters()) {
  rows <- params[params$transition == kind, , drop = FALSE]
  rows <- rows[!is.na(rows$stage_from), , drop = FALSE]
  stats::setNames(as.numeric(rows$value), as.character(rows$stage_from))
}

#' A DMDM transition object with literature-derived POP dynamics
#'
#' Overlays the placeholder POP row of a base transition object (default
#' [dmdm_default_transitions()]) with the cited parameters from
#' [pop_transition_parameters()]: literature onset log-odds (cumulative vaginal
#' deliveries primary), a binary remission probability, and -- because POP is
#' graded and regresses -- explicit per-stage `pop_progression` and
#' `pop_regression` annual probabilities. UI and AI are left untouched.
#'
#' The engines ([simulate_dmdm()], [simulate_dmdm_open()]) read only the fields
#' they know, so the extra `pop_progression`/`pop_regression` elements are inert
#' for the two-state run and available to a staged consumer. The object is marked
#' `calibration_status = "derived_by_analogy"`, and `provenance` records the
#' per-condition status so the mixed pedigree (POP literature-derived; UI/AI still
#' placeholder) is explicit.
#'
#' @param base A base transition object to overlay. Defaults to
#'   [dmdm_default_transitions()].
#' @param params The POP parameter table. Defaults to [pop_transition_parameters()].
#' @return A transition list as in [dmdm_default_transitions()], with the POP
#'   `onset`/`remission` replaced by literature values, plus `pop_progression`,
#'   `pop_regression`, `calibration_status` and `provenance`.
#' @export
dmdm_transitions_with_pop_literature <- function(base = dmdm_default_transitions(),
                                                 params = pop_transition_parameters()) {
  tr <- base
  tr$onset$pop <- .pop_onset_coefs(params)
  remit <- params[params$transition == "remission", , drop = FALSE]
  if (nrow(remit) == 1L) tr$remission[["pop"]] <- as.numeric(remit$value)
  tr$pop_progression <- .pop_stage_probs("progression", params)
  tr$pop_regression  <- .pop_stage_probs("regression", params)
  tr$status <- "derived_by_analogy"
  tr$calibration_status <- "derived_by_analogy"
  tr$provenance <- list(ui = "placeholder_uncalibrated",
                        pop = "derived_by_analogy",
                        ai = "placeholder_uncalibrated")
  tr
}
