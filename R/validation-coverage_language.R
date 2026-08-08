# Coverage language: what a single-target back-test may and may not be called.
#
# THE ERROR THIS PREVENTS
#
# The 2020->2023 back-test scores 10 arms. Two of them produced an interval
# containing the observed 2023 value. Writing that as "20% coverage" is wrong,
# and wrong in a way that is easy to miss because the arithmetic is right.
#
# Coverage is a repeated-sampling property: the share of INDEPENDENT forecast
# occasions on which the interval contains the realised value. These 10 arms are
# not 10 occasions. They are 10 alternative model specifications scored against
# the SAME single truth -- the 2023 count of 1,306. Vary the specification and
# you learn which specifications were consistent with that one number; you learn
# nothing about how often this method's intervals contain the truth, because
# there is only one truth in the sample.
#
# The denominator is not the problem, so restricting to the five
# definition-matched arms and reporting 2/5 = 40% does not fix it. There is
# still exactly one realised target.
#
# WHAT IS SAYABLE
#
#   - 2 of 10 model configurations contained the observed 2023 value.
#   - That is a single-target containment check, not a coverage estimate.
#   - Interval score still ranks the configurations, because it evaluates width
#     and miss jointly and is computable on one observation.
#   - Establishing calibration needs repeated out-of-sample targets, which is
#     precisely what the rolling-origin validation exists to provide.
#
# That last point is why this is a strengthening rather than a retraction: a
# single target can evaluate forecast error and interval score but cannot
# establish repeated-sampling coverage, which is the principled argument for the
# rolling-origin design.

#' Phrasings that assert a coverage rate
#'
#' Matched case-insensitively against manuscript-facing text. The list targets
#' the CLAIM (a rate, a proportion, a threshold comparison), not the word
#' "coverage" itself -- "coverage is not estimable here" must remain sayable.
#'
#' @format Character vector of regular expressions.
#' @family backtest status
#' @concept validation
#' @export
COVERAGE_RATE_PHRASINGS <- c(
  "coverage (was|is|of|=)\\s*[0-9]",       # "coverage was 20%"
  "[0-9]+\\s*%\\s*coverage",               # "20% coverage"
  "empirical coverage",
  "coverage rate",
  "observed coverage",
  # \\b matters: without it "met" matches inside "coverage metrics", which flags
  # the entirely legitimate sentence "conventional coverage metrics may fail to
  # reveal this". Found when the manuscript tripped on its own thesis statement.
  "coverage (failed|fails|met|meets|below|above)\\b",
  "achieved [0-9.]+\\s*%?\\s*coverage"
)

#' Is a coverage rate estimable from this back-test?
#'
#' FALSE whenever every arm is scored against the same single target, which is
#' the case for the 2020->2023 back-test.
#'
#' @param status A [backtest_status()] object.
#' @return Logical.
#' @family backtest status
#' @concept validation
#' @export
coverage_is_estimable <- function(status = backtest_status()) {
  isTRUE(status$coverage_is_estimable)
}

#' Refuse coverage-rate language when coverage is not estimable
#'
#' Fail-closed guard for anything manuscript-facing. Call it on generated text
#' before it reaches a figure caption, abstract, or exported artifact.
#'
#' @param text Character vector of manuscript-facing text.
#' @param status A [backtest_status()] object.
#' @param mode Reproducibility mode; strict errors, relaxed warns.
#' @return (Invisibly) TRUE when the language is permitted.
#' @family backtest status
#' @concept validation
#' @export
assert_no_coverage_rate_claim <- function(text, status = backtest_status(),
                                          mode = resolve_reproducibility_mode()) {
  if (coverage_is_estimable(status)) return(invisible(TRUE))
  joined <- paste(text, collapse = " ")

  # NEGATED MENTIONS ARE THE DISCLAIMERS THIS MODULE EXISTS TO WRITE.
  # "a containment count, not a 20% coverage rate" contains "coverage rate" and
  # must pass; "the coverage rate was 40%" must not. This manuscript tripped its
  # own guard on exactly that sentence, which is how the omission was found.
  #
  # A phrase is treated as negated when a negator appears in the ~48 characters
  # preceding it -- wide enough to span "not a 20%", narrow enough that a
  # negation in an earlier clause does not launder a later claim.
  is_negated <- function(txt, start) {
    lo <- max(1L, start - 48L)
    grepl("\\b(not|never|cannot|can not|is not|isn't|rather than|instead of|no)\\b",
          substr(txt, lo, start - 1L), ignore.case = TRUE, perl = TRUE)
  }
  matched <- vapply(COVERAGE_RATE_PHRASINGS, function(p) {
    m <- gregexpr(p, joined, ignore.case = TRUE, perl = TRUE)[[1]]
    if (m[1] == -1L) return(FALSE)
    # An unnegated occurrence anywhere is enough to refuse.
    any(!vapply(as.integer(m), function(st) is_negated(joined, st), logical(1)))
  }, logical(1))
  hits <- COVERAGE_RATE_PHRASINGS[matched]
  if (!length(hits)) return(invisible(TRUE))

  msg <- paste(
    "This text asserts a coverage RATE, but coverage is not estimable from this",
    "back-test: every arm scores the same single target, so the arms are",
    "alternative specifications rather than independent forecast occasions.",
    "Matched:", paste(sQuote(hits), collapse = ", "), ".",
    "Say instead:", sQuote(containment_statement(status)),
    "-- and note that restricting to the definition-matched arms does not help,",
    "because the denominator is not the problem: there is still one realised target.")
  if (identical(mode, "strict")) stop(msg, call. = FALSE)
  .msg_warn(msg)
  invisible(FALSE)
}

#' The sentence a single-target back-test licenses
#'
#' @param status A [backtest_status()] object.
#' @return A single sentence describing containment, never a rate.
#' @family backtest status
#' @concept validation
#' @export
containment_statement <- function(status = backtest_status()) {
  n <- status$n_arms
  k <- round(status$coverage_95 * n)
  if (coverage_is_estimable(status)) {
    return(sprintf(paste("%d of %d back-test arms contained the observed value",
                         "(coverage estimable across independent targets)."), k, n))
  }
  sprintf(paste("%d of %d model configurations produced an interval containing the",
                "single observed target; because every configuration is scored",
                "against the same realised value, this is a containment count and",
                "not an estimate of interval coverage."), k, n)
}
