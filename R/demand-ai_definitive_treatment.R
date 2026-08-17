# AI definitive-treatment rates ----
#
# THERE IS NO SINGLE SNM-VERSUS-SPHINCTEROPLASTY SHARE.
#
# p_snm + p_sphincteroplasty DOES NOT equal 1. Many women with FI receive
# neither, and the two do not have identical indications: SNM is a first-line
# surgical option with or without a sphincter defect, while sphincteroplasty is
# for selected patients with an external anal sphincter defect. A forced split
# would invent a competition between treatments whose eligibility sets differ,
# and would make each rate depend on the other's uptake.
#
# Each treatment gets its OWN rate against a STATED denominator. A conditional
# fraction may be DERIVED afterwards, never used as the primary probability.

#' Permitted denominators for an AI definitive-treatment rate
#' @family ai treatment
#' @concept demand
#' @export
AI_TREATMENT_DENOMINATORS <- c(
  "ai_care_population",          # P(treatment | in AI/FI care)  <- canonical
  "repair_eligible_population",  # sphincteroplasty only, if the state is modelled
  "treated_with_either"          # DERIVED ONLY -- see ai_conditional_treatment_share()
)

#' AI definitive-treatment evidence register
#' @return Tibble of candidate AI treatment-rate evidence.
#' @family ai treatment
#' @concept demand
#' @export
ai_treatment_evidence_register <- function() {
  p <- system.file("extdata", "ai_treatment_evidence.csv", package = "urpssim")
  if (!nzchar(p) || !file.exists(p)) {
    root <- if (file.exists("config/ai_treatment_evidence.csv")) "." else "../.."
    p <- file.path(root, "config", "ai_treatment_evidence.csv")
  }
  if (!file.exists(p)) stop("ai_treatment_evidence.csv not found.", call. = FALSE)
  tibble::as_tibble(utils::read.csv(p, comment.char = "#", stringsAsFactors = FALSE))
}

#' Separate AI definitive-treatment rates
#'
#' @details
#' Returns SNM and sphincteroplasty rates SEPARATELY, each with its own
#' numerator, denominator and interval. It deliberately does not return a
#' two-element vector summing to 1.
#'
#' INDICATION LINKAGE IS MANDATORY FOR SNM. The same implantation codes serve
#' urinary urgency/OAB and retention, so a bare SNM CPT count is not a
#' fecal-incontinence quantity. `snm_indication_linked` must be `TRUE`, and the
#' window used must be recorded.
#'
#' @param snm_n,sphincteroplasty_n Procedure counts.
#' @param ai_treated_population_n Denominator: the AI/FI care population.
#' @param snm_indication_linked Was each SNM procedure linked to an FI
#'   diagnosis? A bare CPT count is refused.
#' @param snm_indication_window Preregistered window, e.g. `"same_claim"`,
#'   `"+/-30d"`, `"+/-90d"`.
#' @param repair_eligible_n Optional denominator for sphincteroplasty when the
#'   repair-eligible state is modelled.
#' @param source Provenance string. Required.
#' @return Tibble with one row per treatment.
#' @family ai treatment
#' @concept demand
#' @export
ai_definitive_treatment_rates <- function(snm_n,
                                          sphincteroplasty_n,
                                          ai_treated_population_n,
                                          snm_indication_linked,
                                          snm_indication_window = NULL,
                                          repair_eligible_n = NULL,
                                          source = NULL) {
  if (is.null(source) || !nzchar(source)) {
    stop("source is required. An AI treatment rate whose provenance is ",
         "unrecorded cannot be assessed for era or transportability.",
         call. = FALSE)
  }
  if (!isTRUE(snm_indication_linked)) {
    stop("snm_indication_linked must be TRUE. A bare SNM CPT count is NOT a ",
         "fecal-incontinence quantity: the same implantation codes are used ",
         "for urinary urgency/OAB and retention, so the procedure codes are ",
         "not indication-specific. Require an FI diagnosis on the procedure ",
         "claim or within a preregistered peri-procedure window.",
         call. = FALSE)
  }
  if (is.null(snm_indication_window) || !nzchar(snm_indication_window)) {
    stop("snm_indication_window must be recorded (e.g. 'same_claim', ",
         "'+/-30d', '+/-90d'). The window materially changes the count and ",
         "is a preregistered choice, not an implementation detail.",
         call. = FALSE)
  }
  if (!is.numeric(ai_treated_population_n) || ai_treated_population_n <= 0) {
    stop("ai_treated_population_n must be a positive denominator.", call. = FALSE)
  }
  if (snm_n > ai_treated_population_n ||
      sphincteroplasty_n > ai_treated_population_n) {
    stop("A treatment count exceeds the AI care population. Check that the ",
         "numerator and denominator describe the same population.",
         call. = FALSE)
  }

  snm_ci <- wilson_ci(snm_n, ai_treated_population_n)
  sph_den <- if (is.null(repair_eligible_n)) ai_treated_population_n else repair_eligible_n
  if (!is.null(repair_eligible_n) && sphincteroplasty_n > repair_eligible_n) {
    stop("sphincteroplasty_n exceeds the repair-eligible denominator.", call. = FALSE)
  }
  sph_ci <- wilson_ci(sphincteroplasty_n, sph_den)

  tibble::tibble(
    treatment   = c("snm", "sphincteroplasty"),
    n           = c(snm_n, sphincteroplasty_n),
    denominator_n = c(ai_treated_population_n, sph_den),
    denominator = c("ai_care_population",
                    if (is.null(repair_eligible_n)) "ai_care_population"
                    else "repair_eligible_population"),
    rate        = c(snm_n / ai_treated_population_n, sphincteroplasty_n / sph_den),
    rate_lo     = c(snm_ci$lo, sph_ci$lo),
    rate_hi     = c(snm_ci$hi, sph_ci$hi),
    indication_linked = c(TRUE, NA),
    indication_window = c(snm_indication_window, NA_character_),
    source      = source
  )
}

#' Conditional treatment share, DERIVED ONLY
#'
#' @details
#' `N_snm / (N_snm + N_sph)`. This is a derived diagnostic, NOT a pathway
#' probability. Using it as the primary parameter would make each treatment's
#' modelled uptake depend on the other's, which is false when their eligibility
#' sets differ and when many patients receive neither.
#'
#' @param rates Output of [ai_definitive_treatment_rates()].
#' @return A one-row tibble carrying the derived share and its warning label.
#' @family ai treatment
#' @concept demand
#' @export
ai_conditional_treatment_share <- function(rates) {
  n_snm <- rates$n[rates$treatment == "snm"]
  n_sph <- rates$n[rates$treatment == "sphincteroplasty"]
  if (!length(n_snm) || !length(n_sph)) {
    stop("rates must contain both snm and sphincteroplasty rows.", call. = FALSE)
  }
  tibble::tibble(
    quantity = "P(SNM | received SNM or sphincteroplasty)",
    value = n_snm / (n_snm + n_sph),
    denominator = "treated_with_either",
    use = "DERIVED DIAGNOSTIC ONLY -- must not be used as a pathway probability"
  )
}

#' Status of the AI definitive-treatment parameters
#' @return A length-one character calibration tier.
#' @family ai treatment
#' @concept demand
#' @export
ai_treatment_rate_status <- function() {
  # No row in the evidence register is canonical_compatible. The Medicare 5%
  # benchmark is older women only and its procedure-specific rates have not
  # been extracted with their denominators; the New York study is a single
  # state at 2011-2014 and its denominator is the CONDITIONAL one.
  "unresolved_requires_source"
}
